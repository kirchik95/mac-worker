use mac_worker::test_support::{integration::*, task::model::*};
use serde_json::{Value, json};

#[path = "../support/baseline_ce7f62f.rs"]
mod baseline;

#[test]
fn disabled_ordinary_status_meta_and_followup_keep_baseline_strict_keys_and_bytes() {
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    let prepared = sample_prepared_turn(
        &sample_record(fixture_task(), fixture_source(), "main"),
        IntegrationTurnPurpose::Resolve,
        1,
        1,
    );
    for (kind, wire) in [
        ("meta", serde_json::to_value(ordinary.meta()).unwrap()),
        ("status", serde_json::to_value(ordinary.status()).unwrap()),
        (
            "followup",
            serde_json::to_value(&prepared.followup).unwrap(),
        ),
    ] {
        let accept = |wire: Value| match kind {
            "meta" => serde_json::from_value::<baseline::TaskMeta>(wire).is_ok(),
            "status" => serde_json::from_value::<baseline::TaskStatus>(wire).is_ok(),
            _ => serde_json::from_value::<baseline::PreparedFollowup>(wire).is_ok(),
        };
        assert!(accept(wire.clone()), "{kind}");
        for key in ["integration", "workflow_state", "integrate", "verify_merge"] {
            let mut changed = wire.clone();
            changed[key] = json!(null);
            assert!(!accept(changed), "{kind}:{key}");
        }
    }
    let bytes = serde_json::to_vec(ordinary.meta()).unwrap();
    let decoded: baseline::TaskMeta = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
    let bytes = serde_json::to_vec(ordinary.status()).unwrap();
    let decoded: baseline::TaskStatus = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
    let bytes = serde_json::to_vec(&prepared.followup).unwrap();
    let decoded: baseline::PreparedFollowup = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
}

fn frozen() -> Value {
    json!({
        "task_id":"00000000000000000000000000000002",
        "turn_id":"00000000000000000000000000000003","created_at_millis":1000,
        "prompt":"work","agent":"codex","source":"local","origin_url":"https://example.test/repo.git",
        "publish":["fetch"],"close_on":"never","wip":false,
        "project_id":"a".repeat(64),"worktree_id":"b".repeat(64),"base_oid":"b".repeat(40),
        "timeout_millis":2700000,"max_followups":10,"permissions":"workspace","requires":[],
        "include_untracked":[],"include_empty_dirs":[],"allow_sensitive":[],"cli_includes":[],
        "wait_for_capacity":true
    })
}

#[test]
fn disabled_submit_and_dag_remain_accepted_by_copied_baseline_decoders() {
    let old_submit: baseline::FrozenSubmitBody = serde_json::from_value(frozen()).unwrap();
    let submit: FrozenSubmitBody = serde_json::from_value(frozen()).unwrap();
    let bytes = serde_json::to_vec(&submit).unwrap();
    serde_json::from_slice::<baseline::FrozenSubmitBody>(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&old_submit).unwrap(), bytes);
    let mut dag = frozen();
    for key in [
        "task_id",
        "turn_id",
        "created_at_millis",
        "base_oid",
        "wait_for_capacity",
    ] {
        dag.as_object_mut().unwrap().remove(key);
    }
    dag["project_path"] = json!("/fixture/project");
    let old_dag: baseline::DagFrozenSpec = serde_json::from_value(dag.clone()).unwrap();
    let dag: DagFrozenSpec = serde_json::from_value(dag).unwrap();
    let bytes = serde_json::to_vec(&dag).unwrap();
    serde_json::from_slice::<baseline::DagFrozenSpec>(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&old_dag).unwrap(), bytes);
    let mut wrapped = serde_json::to_value(&submit).unwrap();
    wrapped["integration"] = serde_json::to_value(sample_policy("main")).unwrap();
    assert!(serde_json::from_value::<baseline::FrozenSubmitBody>(wrapped).is_err());
}

#[test]
fn copied_baseline_imported_meta_and_submit_freeze_the_nested_session_codec() {
    let import = json!({
        "agent":"codex", "format":"codex-rollout-v1",
        "package_oid":"d".repeat(40), "source_agent_version":"0.160.0",
    });
    let mut meta =
        serde_json::to_value(sample_ordinary(fixture_task(), fixture_source()).meta()).unwrap();
    meta["session_import"] = import.clone();
    let old_meta: baseline::TaskMeta = serde_json::from_value(meta.clone()).unwrap();
    let current_meta: TaskMeta = serde_json::from_value(meta).unwrap();
    let bytes = serde_json::to_vec(&current_meta).unwrap();
    assert_eq!(bytes, serde_json::to_vec(&old_meta).unwrap());
    serde_json::from_slice::<baseline::TaskMeta>(&bytes).unwrap();
    let mut submit = frozen();
    submit["session_import"] = import;
    submit["questions"] = json!("ask");
    let old: baseline::FrozenSubmitBody = serde_json::from_value(submit.clone()).unwrap();
    let current: FrozenSubmitBody = serde_json::from_value(submit.clone()).unwrap();
    let bytes = serde_json::to_vec(&current).unwrap();
    assert_eq!(bytes, serde_json::to_vec(&old).unwrap());
    serde_json::from_slice::<baseline::FrozenSubmitBody>(&bytes).unwrap();
    for (field, value) in [
        ("agent", json!("cursor")),
        ("format", json!("claude-jsonl-v1")),
        ("package_oid", json!("bad-oid")),
        ("source_agent_version", json!("not-a-version")),
        ("unknown", json!(true)),
    ] {
        let mut changed = submit.clone();
        changed["session_import"][field] = value;
        assert!(
            serde_json::from_value::<baseline::FrozenSubmitBody>(changed).is_err(),
            "{field}"
        );
    }
    let duplicate = serde_json::to_string(&submit).unwrap().replacen(
        r#""session_import":{"agent":"codex""#,
        r#""session_import":{"agent":"codex","agent":"codex""#,
        1,
    );
    assert!(serde_json::from_str::<baseline::FrozenSubmitBody>(&duplicate).is_err());
}

#[test]
fn copied_baseline_meta_validates_titles_identities_limits_and_followup_records() {
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    let valid = serde_json::to_value(ordinary.meta()).unwrap();
    serde_json::from_value::<baseline::TaskMeta>(valid.clone()).unwrap();
    for (pointer, value) in [
        ("/title", json!("x".repeat(121))),
        ("/title", json!("bad\ntitle")),
        ("/git_identity/name", json!("")),
        ("/git_identity/email", json!("bad<email>")),
        ("/limits/turn/timeout_millis", json!(0)),
        ("/limits/turn/max_turns", json!(0)),
        ("/limits/max_followups", json!(101)),
        ("/base_oid", json!("B".repeat(40))),
        ("/agent", json!("future_agent")),
        ("/policy", json!("future_policy")),
        ("/publish", json!(["fetch", "fetch"])),
    ] {
        let mut changed = valid.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        assert!(
            serde_json::from_value::<baseline::TaskMeta>(changed).is_err(),
            "{pointer}"
        );
    }
    let mut changed = valid;
    changed["session_import"] = json!({
        "agent":"claude", "format":"claude-jsonl-v1",
        "package_oid":"d".repeat(40), "source_agent_version":"2.1.0",
    });
    assert!(serde_json::from_value::<baseline::TaskMeta>(changed).is_err());
    let prepared = sample_prepared_turn(
        &sample_record(fixture_task(), fixture_source(), "main"),
        IntegrationTurnPurpose::Resolve,
        1,
        1,
    );
    let mut changed = serde_json::to_value(prepared.followup).unwrap();
    serde_json::from_value::<baseline::PreparedFollowup>(changed.clone()).unwrap();
    changed["expected"]["meta"]["limits"]["turn"]["timeout_millis"] = json!(0);
    assert!(serde_json::from_value::<baseline::PreparedFollowup>(changed).is_err());
    let duplicate = serde_json::to_string(ordinary.meta()).unwrap().replacen(
        r#""name":"mac-worker""#,
        r#""name":"mac-worker","name":"mac-worker""#,
        1,
    );
    let error = serde_json::from_str::<baseline::TaskMeta>(&duplicate).unwrap_err();
    assert!(error.to_string().contains("duplicate field"), "{error}");
}

fn integration_fixture(capable: bool) -> super::session_import_e2e::Fixture {
    let f = super::session_import_e2e::Fixture::new();
    let origin = f.laptop.parent().unwrap().join("origin.git");
    f.project
        .git(&["clone", "--bare", ".", origin.to_str().unwrap()]);
    f.project.git(&[
        "remote",
        "add",
        "origin",
        &format!("file://{}", origin.display()),
    ]);
    let mut config = std::fs::read_to_string(&f.config).unwrap();
    config.push_str("capabilities = ['origin:file']\n");
    std::fs::write(&f.config, config).unwrap();
    if capable {
        let ssh = std::fs::read_to_string(&f.ssh).unwrap().replace(
            "probe.update(memory_pressure",
            "probe['features'].append('task.integration')\n    probe.update(memory_pressure",
        );
        std::fs::write(&f.ssh, ssh).unwrap();
        super::session_import_e2e::warm_executable(&f.ssh);
    }
    f
}

fn owner_paths(
    f: &super::session_import_e2e::Fixture,
) -> mac_worker::test_support::core::paths::PathLayout {
    mac_worker::test_support::core::paths::PathLayout {
        config: f.config.clone(),
        state: f.laptop.join(".local/state/mac-worker"),
        cache: f.laptop.join(".cache/mac-worker"),
        data: f.laptop.join(".local/share/mac-worker"),
    }
}

fn submitted_task(output: &std::process::Output) -> TaskId {
    assert!(
        output.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .rfind(|v| v.get("session_import").is_some())
        .unwrap()["task_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn terminal_child_imports_and_acknowledges_the_merge_before_done_close() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        host::{process::SystemProcessRunner, store::HostStore},
        session::SessionAgent,
        task::store::TaskStore,
    };
    let f = integration_fixture(true);
    f.capture_fixture(SessionAgent::Codex);
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap().replace(
        "printf '%s\\n' \"$@\" > \"$HOME/argv\"",
        "printf 'ordinary result\\n' > T6-result\nprintf '%s\\n' \"$@\" > \"$HOME/argv\"",
    );
    std::fs::write(agent, script).unwrap();
    let output = f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "produce work",
        "--integrate",
        "main",
        "--close-on",
        "done",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]);
    let task = submitted_task(&output);
    let wait = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "60s",
    ]);
    assert!(
        wait.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&wait.stdout),
        String::from_utf8_lossy(&wait.stderr)
    );
    let paths = owner_paths(&f);
    let owner = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let record = owner
        .load(task)
        .unwrap()
        .expect("terminal runner must stage an integration cycle");
    assert_eq!(record.snapshot.state, IntegrationStatus::Integrated);
    let receipt = record.receipt.as_ref().unwrap();
    assert!(receipt.imported);
    let merge = receipt
        .merge_oid
        .as_ref()
        .expect("ordinary output needs one merge");
    assert_ne!(merge, &receipt.source_head);
    let local = ClientStateStore::open(&paths.state)
        .unwrap()
        .load_task(task)
        .unwrap();
    assert_eq!(local.status().state(), TaskState::Closed);
    assert_eq!(local.status().head_oid(), Some(merge));
    assert_eq!(local.fetched_head(), Some(merge));
    let host = HostStore::open(&f.host_root()).unwrap();
    let retained = HostIntegrationStore::new(&host)
        .load(&record.policy.project_id, task)
        .unwrap()
        .unwrap();
    assert!(
        retained.receipt.unwrap().imported,
        "host must receive the import acknowledgement before close"
    );
    assert_eq!(
        TaskStore::new(&host, &SystemProcessRunner)
            .load_status(&record.policy.project_id, task)
            .unwrap()
            .state(),
        TaskState::Closed
    );
    assert_eq!(
        f.project
            .git(&["show", &format!("{merge}:T6-result")])
            .stdout,
        b"ordinary result\n"
    );
    let driver: Value = serde_json::from_slice(
        &std::fs::read(
            paths
                .state
                .join(format!("integrations/tasks/{task}/driver.json")),
        )
        .unwrap(),
    )
    .unwrap();
    assert_ne!(
        driver["actor"]["pid"].as_u64(),
        Some(std::process::id() as u64)
    );
}

fn parked_source_fixture() -> (super::session_import_e2e::Fixture, TaskId) {
    configured_parked_source_fixture(
        "never",
        |_| {},
        "printf 'ordinary result\\n' > T6-result",
        ":",
    )
}

fn configured_parked_source_fixture(
    verify: &str,
    prepare: impl FnOnce(&super::session_import_e2e::Fixture),
    source_body: &str,
    auxiliary_body: &str,
) -> (super::session_import_e2e::Fixture, TaskId) {
    use mac_worker::test_support::session::SessionAgent;
    let f = integration_fixture(true);
    prepare(&f);
    f.capture_fixture(SessionAgent::Codex);
    f.install_agent(SessionAgent::Codex);
    let quote = |s: &str| format!("'{}'", s.replace('\'', "'\"'\"'"));
    let drain = format!(
        "if [ ! -e \"$HOME/source-finished\" ]; then\n: > \"$HOME/source-finished\"\n{source_body}\n/usr/bin/env HOME={} {} --config {} controller drain >/dev/null || exit 94\nelse\n{auxiliary_body}\nfi\nprintf '%s\\n' \"$@\" > \"$HOME/argv\"",
        quote(f.laptop.to_str().unwrap()),
        quote(env!("CARGO_BIN_EXE_worker")),
        quote(f.config.to_str().unwrap())
    );
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent)
        .unwrap()
        .replace("printf '%s\\n' \"$@\" > \"$HOME/argv\"", &drain);
    std::fs::write(agent, script).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "produce work",
        "--integrate",
        "main",
        "--verify-merge",
        verify,
        "--close-on",
        "never",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]));
    let owner = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    assert_eq!(
        owner.load(task).unwrap().unwrap().snapshot.state,
        IntegrationStatus::Parked
    );
    (f, task)
}

#[test]
fn native_read_surfaces_expose_the_same_parked_companion_without_driving_git() {
    let (f, task) = parked_source_fixture();
    let state = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let saved = state.load(task).unwrap().unwrap();
    let target = f.project.git(&["ls-remote", "origin", "refs/heads/main"]);
    for command in ["status", "result", "list"] {
        let mut args = vec!["--json", "task", command];
        let id = task.to_string();
        if command != "list" {
            args.push(&id);
        }
        let output = f.worker(&args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        let row = if command == "list" {
            &value["tasks"][0]
        } else {
            &value
        };
        assert_eq!(row["integration"], json!(saved.snapshot), "{command}");
        assert_eq!(row["workflow_state"], "integrating", "{command}");
        if command != "list" {
            let _: baseline::TaskStatus = serde_json::from_value(value["status"].clone()).unwrap();
        }
        let text = if command == "list" {
            f.worker(&["task", command])
        } else {
            f.worker(&["task", command, &id])
        };
        assert!(String::from_utf8_lossy(&text.stdout).contains("integration: parked"));
        assert_eq!(state.load(task).unwrap().unwrap(), saved);
    }
    assert_eq!(
        f.project.git(&["ls-remote", "origin", "refs/heads/main"]),
        target
    );
}

#[test]
fn native_legacy_retention_restores_a_retained_merge_without_resurrection() {
    native_legacy_retention_restore_case("merge", true);
}

#[test]
fn native_legacy_retention_restores_a_reachable_source_without_resurrection() {
    native_legacy_retention_restore_case("source", false);
}

#[test]
fn native_legacy_retention_restore_blocks_when_no_retained_result_is_reachable() {
    native_legacy_retention_restore_case("neither", true);
}

#[test]
fn native_legacy_retention_restore_retries_a_failed_observation_then_imports() {
    native_legacy_retention_restore_case("failed_then_merge", false);
}

#[test]
fn native_legacy_retention_restores_a_merge_after_open_repair_exhaustion() {
    native_legacy_retention_restore_case("open_repair_exhausted", false);
}

fn native_legacy_retention_restore_case(target: &str, parked: bool) {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        host::{
            gc::apply_baseline_retention_close,
            process::SystemProcessRunner,
            store::{HostStore, TASK_RETENTION_MILLIS},
        },
        task::store::TaskStore,
    };
    use std::sync::Arc;
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(&paths, Arc::new(ManualIntegrationRuntime::default()))
        .unwrap();
    let original = state.load(task).unwrap().unwrap();
    let host = HostStore::open(&f.host_root()).unwrap();
    let mut phase = original.clone();
    phase.snapshot.state = IntegrationStatus::Pending;
    phase.snapshot.resume_state = None;
    phase.snapshot.pause_reason = None;
    phase.pause = None;
    let request = HostIntegrationRequest {
        protocol_version: 7,
        task_id: task,
        integration_id: Some(phase.snapshot.integration_id),
        epoch: phase.snapshot.epoch,
        revision: phase.snapshot.revision,
        action: HostIntegrationAction::Step {
            step: IntegrationStep::Prepare,
            record: Box::new(phase),
        },
    };
    let runtime = ManualIntegrationRuntime::default();
    let HostIntegrationResponse::CandidateReady { candidate, .. } =
        HostIntegrationService::new(&host, &SystemProcessRunner, &runtime)
            .execute(&request)
            .unwrap()
    else {
        panic!("clean real candidate required")
    };
    let merge = candidate.merge_oid.clone().unwrap();
    let mut retained = HostIntegrationStore::new(&host)
        .load(&original.policy.project_id, task)
        .unwrap()
        .unwrap();
    retained.snapshot.observed_target_oid = Some(candidate.target_head.clone());
    retained.snapshot.merge_oid = Some(merge.clone());
    retained.snapshot.state = if parked {
        IntegrationStatus::Parked
    } else {
        IntegrationStatus::Blocked
    };
    retained.snapshot.resume_state = parked.then_some(IntegrationStatus::Pushing);
    retained.snapshot.pause_reason = parked.then_some(IntegrationPauseReason::ControllerDrained);
    retained.pause = parked.then_some(IntegrationPauseEvidence {
        reason: IntegrationPauseReason::ControllerDrained,
        effective_at_millis: 1001,
    });
    retained.snapshot.blocked_code = (!parked).then_some(IntegrationCode::IntegrationNetwork);
    retained.push_intent = Some(IntegrationPushIntent {
        candidate: candidate.id,
        expected_target: candidate.target_head.clone(),
        merge_oid: merge.clone(),
        started_at_millis: 1001,
        uncertain: true,
    });
    if target == "open_repair_exhausted" {
        // Restore the durable result of an exhausted Open Repair, independently
        // reproduced with four real-owner calls in the lifecycle review probe.
        retained.phase_retries.push(IntegrationPhaseRetry {
            phase: IntegrationPhase::Repair,
            retries: 3,
            code: IntegrationCode::IntegrationNetwork,
            due_at_millis: 1001,
        });
        retained.snapshot.retry_exhausted = true;
        retained.snapshot.disposition = Some(IntegrationDisposition::Merged);
        retained.receipt = Some(IntegrationReceipt {
            integration_id: retained.snapshot.integration_id,
            epoch: retained.snapshot.epoch,
            source_turn_id: retained.snapshot.source_turn_id,
            source_head: retained.snapshot.source_head.clone(),
            target_head: candidate.target_head.clone(),
            merge_oid: Some(merge.clone()),
            disposition: IntegrationDisposition::Merged,
            imported: false,
            recorded_at_millis: 1001,
        });
    }
    retained.snapshot.revision = original.snapshot.revision.next().unwrap();
    let host_task = host.task_dir(&original.policy.project_id, task).unwrap();
    std::fs::write(
        host_task.join("integration/record.json"),
        encode_record(&retained).unwrap(),
    )
    .unwrap();
    state
        .replace(task, original.snapshot.revision, &retained)
        .unwrap();
    let mirror = host
        .mirror_if_present(&original.policy.project_id)
        .unwrap()
        .unwrap();
    let refs = f.project.git(&[
        "--git-dir",
        mirror.path().to_str().unwrap(),
        "for-each-ref",
        "--format=%(refname) %(objectname)",
        "refs/heads/task",
        "refs/mac-worker/bases",
    ]);
    if target != "neither" {
        let accepted = if target == "source" {
            &retained.snapshot.source_head
        } else {
            &merge
        };
        f.project.git(&[
            "--git-dir",
            mirror.path().to_str().unwrap(),
            "push",
            &original.policy.origin,
            &format!("{accepted}:refs/heads/main"),
        ]);
    }
    let host_status = TaskStore::new(&host, &SystemProcessRunner)
        .load_status(&original.policy.project_id, task)
        .unwrap();
    let expiry = host_status.updated_at_millis() + TASK_RETENTION_MILLIS;
    assert!(
        !apply_baseline_retention_close(
            &host,
            &SystemProcessRunner,
            &original.policy.project_id,
            task,
            expiry - 1
        )
        .unwrap()
    );
    assert!(
        apply_baseline_retention_close(
            &host,
            &SystemProcessRunner,
            &original.policy.project_id,
            task,
            expiry + 1
        )
        .unwrap()
    );
    assert!(!host_task.join("workspace").exists());
    assert_eq!(
        f.project.git(&[
            "--git-dir",
            mirror.path().to_str().unwrap(),
            "for-each-ref",
            "--format=%(refname) %(objectname)",
            "refs/heads/task",
            "refs/mac-worker/bases"
        ]),
        refs
    );
    let local_store = ClientStateStore::open(&paths.state).unwrap();
    let before_close = local_store.load_task(task).unwrap();
    let closed = before_close
        .clone()
        .with_status(
            TaskStore::new(&host, &SystemProcessRunner)
                .load_status(&original.policy.project_id, task)
                .unwrap(),
        )
        .unwrap();
    // Recovery starts after the owner has observed the real legacy close.
    assert!(
        local_store
            .update_task_if_current(&before_close, closed.clone())
            .unwrap()
    );
    assert_eq!(closed.status().state(), TaskState::Closed);
    assert_eq!(
        closed.status().last_outcome(),
        Some(&mac_worker::test_support::task::model::TaskOutcome::Done)
    );
    let config = mac_worker::test_support::core::config::Config::parse(
        &std::fs::read_to_string(&f.config).unwrap(),
    )
    .unwrap();
    let client = mac_worker::test_support::task::client::TaskClient::new(
        &SystemProcessRunner,
        &config,
        &paths,
        &local_store,
        &mac_worker::test_support::task::turn_runner::InlineRunnerExecutor,
    );
    assert_ne!(
        client.integration_parent_gate(&closed).unwrap(),
        mac_worker::test_support::client_state::dag::ParentGate::Ready,
        "Closed+Done alone cannot release a configured child"
    );
    let ssh = std::fs::read_to_string(&f.ssh).unwrap();
    let logging = r#"if command.endswith(' host task-integration'):
    data = sys.stdin.buffer.read()
    action = json.loads(data)['action']
    with open(os.path.join(os.environ['HOME'], 'restore-steps'), 'a') as log: log.write(action.get('step', 'other') + '\n')
    result = subprocess.run(['/bin/sh','-c',command], input=data, capture_output=True)
    marker = os.path.join(os.environ['HOME'], 'observe-offline')
    if os.path.exists(marker):
        os.unlink(marker)
        with open(os.path.join(os.environ['HOME'], 'observation-ready'), 'w') as signal: signal.write('observed\n')
        with open(os.path.join(os.environ['HOME'], 'observation-release')) as signal: signal.readline()
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    sys.exit(result.returncode)
os.execv('/bin/sh', ['/bin/sh', '-c', command])"#;
    std::fs::write(
        &f.ssh,
        ssh.replace("os.execv('/bin/sh', ['/bin/sh', '-c', command])", logging),
    )
    .unwrap();
    assert!(f.worker(&["controller", "drain", "--off"]).status.success());
    let recovered = if target == "failed_then_merge" {
        let origin = f.laptop.parent().unwrap().join("origin.git");
        let hidden = origin.with_extension("offline");
        std::fs::rename(&origin, &hidden).unwrap();
        std::fs::write(f.host.join("observe-offline"), b"once").unwrap();
        let ready = f.host.join("observation-ready");
        let release = f.host.join("observation-release");
        fixture_fifo(&ready);
        fixture_fifo(&release);
        Some(std::thread::scope(|scope| {
            let release_on_drop = ReleaseFixtureFifo(release);
            let waiter = scope.spawn(|| wait_integrated(&f, task));
            let mut signal = String::new();
            std::io::BufRead::read_line(
                &mut std::io::BufReader::new(std::fs::File::open(ready).unwrap()),
                &mut signal,
            )
            .unwrap();
            assert_eq!(signal, "observed\n");
            let uncertain = state.load(task).unwrap().unwrap();
            assert!(uncertain.push_intent.as_ref().unwrap().uncertain);
            assert!(uncertain.receipt.is_none());
            std::fs::rename(hidden, origin).unwrap();
            drop(release_on_drop);
            let done = waiter.join().unwrap();
            assert!(
                done.phase_retries
                    .iter()
                    .any(|retry| retry.phase == IntegrationPhase::Drive
                        && retry.code == IntegrationCode::IntegrationNetwork
                        && retry.retries > 0)
            );
            done
        }))
    } else {
        None
    };
    if target == "neither" {
        let result = f.worker(&[
            "--json",
            "task",
            "wait",
            "--task-id",
            &task.to_string(),
            "--timeout",
            "30s",
        ]);
        assert!(!result.status.success());
        assert_eq!(
            state.load(task).unwrap().unwrap().snapshot.state,
            IntegrationStatus::Blocked
        );
        assert_eq!(
            state.load(task).unwrap().unwrap().snapshot.blocked_code,
            Some(IntegrationCode::IntegrationWorkspaceMissing)
        );
        assert!(state.load(task).unwrap().unwrap().receipt.is_none());
    } else {
        let done = recovered.unwrap_or_else(|| wait_integrated(&f, task));
        let receipt = done.receipt.unwrap();
        assert!(receipt.imported);
        let accepted = if target == "source" {
            &retained.snapshot.source_head
        } else {
            &merge
        };
        assert_eq!(
            receipt.merge_oid.as_ref().unwrap_or(&receipt.target_head),
            accepted
        );
        let local = ClientStateStore::open(&paths.state)
            .unwrap()
            .load_task(task)
            .unwrap();
        assert_eq!(local.status().head_oid(), Some(accepted));
        assert_eq!(local.fetched_head(), Some(accepted));
        assert_eq!(local.status().state(), TaskState::Closed);
        assert!(
            HostIntegrationStore::new(&host)
                .load(&original.policy.project_id, task)
                .unwrap()
                .unwrap()
                .receipt
                .unwrap()
                .imported
        );
    }
    assert_eq!(
        TaskStore::new(&host, &SystemProcessRunner)
            .load_status(&original.policy.project_id, task)
            .unwrap()
            .state(),
        TaskState::Closed
    );
    assert!(!host_task.join("workspace").exists());
    let steps = std::fs::read_to_string(f.host.join("restore-steps")).unwrap();
    assert!(
        steps.lines().all(|step| step == "repair"),
        "{target}: {steps}"
    );
    let late = f.worker(&["--json", "task", "integrate", &task.to_string()]);
    assert!(!late.status.success());
    assert!(!host_task.join("workspace").exists());
    assert_eq!(
        std::fs::read_to_string(f.host.join("ssh-journal"))
            .unwrap()
            .matches("host task-prepare\n")
            .count(),
        1
    );
}

#[test]
fn native_union_uses_frozen_h_attributes_and_pinned_verification_before_receipt_import() {
    let (f, task) = configured_parked_source_fixture(
        "moved-target",
        |f| {
            f.project.write(".gitattributes", b"payload.txt -merge\n");
            f.project.write("payload.txt", b"base\n");
            f.project.commit_all("attribute base");
            f.project.git(&["push", "origin", "HEAD:main"]);
        },
        "printf 'payload.txt merge=union\\n' > .gitattributes\nprintf 'ours\\n' > payload.txt",
        "/usr/bin/git write-tree > \"$HOME/verify-before\"\n/usr/bin/git write-tree > \"$HOME/verify-after\"",
    );
    let owner = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let source = owner.load(task).unwrap().unwrap().snapshot.source_head;
    f.project.write("payload.txt", b"theirs\n");
    f.project.commit_all("outside target");
    let target = String::from_utf8(f.project.git(&["rev-parse", "HEAD"]).stdout).unwrap();
    f.project.git(&["push", "origin", "HEAD:main"]);
    assert!(f.worker(&["controller", "drain", "--off"]).status.success());
    let done = wait_integrated(&f, task);
    let candidate = done.candidates.last().unwrap();
    assert_eq!(candidate.attribute_source, source);
    assert_eq!(candidate.ours, source);
    assert_eq!(candidate.theirs.as_str(), target.trim());
    let merge = done.receipt.as_ref().unwrap().merge_oid.as_ref().unwrap();
    assert_eq!(
        f.project
            .git(&["show", &format!("{merge}:payload.txt")])
            .stdout,
        b"ours\ntheirs\n"
    );
    let parents = String::from_utf8(
        f.project
            .git(&["show", "-s", "--format=%P", merge.as_str()])
            .stdout,
    )
    .unwrap();
    assert_eq!(parents.trim(), format!("{} {source}", target.trim()));
    assert_eq!(done.snapshot.verify_turns, 1);
    for file in ["verify-before", "verify-after"] {
        assert_eq!(
            std::fs::read_to_string(f.host.join(file)).unwrap().trim(),
            candidate.tree_oid.as_ref().unwrap().as_str()
        );
    }
    assert!(done.receipt.as_ref().unwrap().imported);
}

#[test]
fn native_binary_h_attributes_resolve_in_the_same_session() {
    native_attribute_resolution_case(false);
}

#[test]
fn native_verifier_index_tampering_blocks_before_push() {
    native_attribute_resolution_case(true);
}

fn native_attribute_resolution_case(tamper: bool) {
    let attributes = if tamper {
        "payload.txt merge=union"
    } else {
        "payload.txt -merge"
    };
    let source =
        format!("printf '{attributes}\\n' > .gitattributes\nprintf 'ours\\n' > payload.txt");
    let auxiliary = if tamper {
        "printf 'changed\\n' > payload.txt\n/usr/bin/git add payload.txt"
    } else {
        "printf 'resolved\\n' > payload.txt\n/usr/bin/git add payload.txt"
    };
    let (f, task) = configured_parked_source_fixture(
        if tamper { "moved-target" } else { "never" },
        |f| {
            f.project
                .write(".gitattributes", b"payload.txt merge=union\n");
            f.project.write("payload.txt", b"base\n");
            f.project.commit_all("attribute base");
            f.project.git(&["push", "origin", "HEAD:main"]);
        },
        &source,
        auxiliary,
    );
    f.project.write("payload.txt", b"theirs\n");
    f.project.commit_all("outside target");
    f.project.git(&["push", "origin", "HEAD:main"]);
    let target = f.project.git(&["ls-remote", "origin", "refs/heads/main"]);
    assert!(f.worker(&["controller", "drain", "--off"]).status.success());
    if tamper {
        let wait = f.worker(&[
            "--json",
            "task",
            "wait",
            "--task-id",
            &task.to_string(),
            "--timeout",
            "60s",
        ]);
        assert!(
            !wait.status.success(),
            "{}",
            String::from_utf8_lossy(&wait.stdout)
        );
        let owner = RootedIntegrationState::open(
            &owner_paths(&f),
            std::sync::Arc::new(ManualIntegrationRuntime::default()),
        )
        .unwrap();
        let blocked = owner.load(task).unwrap().unwrap();
        assert_eq!(blocked.snapshot.state, IntegrationStatus::Blocked);
        assert_eq!(
            blocked.snapshot.blocked_code,
            Some(IntegrationCode::IntegrationVerifyTreeMismatch)
        );
        assert_eq!(
            f.project.git(&["ls-remote", "origin", "refs/heads/main"]),
            target
        );
    } else {
        let done = wait_integrated(&f, task);
        assert_eq!(done.snapshot.resolve_turns, 1);
        assert_eq!(done.snapshot.verify_turns, 0);
        let merge = done.receipt.as_ref().unwrap().merge_oid.as_ref().unwrap();
        assert_eq!(
            f.project
                .git(&["show", &format!("{merge}:payload.txt")])
                .stdout,
            b"resolved\n"
        );
        assert!(done.receipt.as_ref().unwrap().imported);
    }
    let journal = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    assert_eq!(journal.matches("host task-prepare\n").count(), 1);
    assert_eq!(journal.matches("host task-integration-turn\n").count(), 1);
    assert_eq!(
        std::fs::read_to_string(f.host.join("placed-files"))
            .unwrap()
            .lines()
            .count(),
        1
    );
}

struct RunningDashboard(std::process::Child);
impl Drop for RunningDashboard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn dashboard_request(address: &str, method: &str, path: &str, body: Option<Value>) -> Value {
    let (status, response) = dashboard_request_result(address, method, path, body);
    assert!(status.contains("200"), "{status} {response}");
    response
}

fn dashboard_request_result(
    address: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (String, Value) {
    use std::io::{BufRead, Read, Write};
    let body = body.map(|body| body.to_string()).unwrap_or_default();
    loop {
        let mut socket = std::net::TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(crate::support::HANDSHAKE_TIMEOUT))
            .unwrap();
        write!(socket, "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nx-mac-worker-task: 1\r\nOrigin: http://{address}\r\nContent-Length: {}\r\n\r\n{body}", body.len()).unwrap();
        let mut reader = std::io::BufReader::new(socket);
        let mut status = String::new();
        reader.read_line(&mut status).unwrap();
        let mut length = None;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some((key, value)) = line.split_once(':')
                && key.eq_ignore_ascii_case("content-length")
            {
                length = Some(value.trim().parse::<usize>().unwrap());
            }
        }
        let mut response = vec![0; length.expect("bounded JSON response")];
        reader.read_exact(&mut response).unwrap();
        if status.contains("503")
            && String::from_utf8_lossy(&response).contains("DASHBOARD_SNAPSHOT_PENDING")
        {
            std::thread::yield_now();
            continue;
        }
        return (status, serde_json::from_slice(&response).unwrap());
    }
}

#[test]
fn normally_launched_direct_and_controller_dashboards_read_and_redrive_the_owner() {
    use mac_worker::test_support::client_state::ClientStateStore;
    use std::{
        io::BufRead,
        process::{Command, Stdio},
        sync::{Arc, mpsc},
        thread,
    };
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(&paths, Arc::new(ManualIntegrationRuntime::default()))
        .unwrap();
    for viewer in [false, true] {
        let mut record = state.load(task).unwrap().unwrap();
        let expected = record.snapshot.revision;
        record.snapshot.revision = expected.next().unwrap();
        record.snapshot.state = IntegrationStatus::Blocked;
        record.snapshot.pause_reason = None;
        record.snapshot.resume_state = None;
        record.pause = None;
        record.snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
        assert!(state.replace(task, expected, &record).unwrap());
        let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
        command
            .env_clear()
            .env("HOME", &f.laptop)
            .env("PATH", "/usr/bin:/bin")
            .env("MAC_WORKER_TEST_SSH", &f.ssh)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .current_dir(f.project.root())
            .args([
                "--config",
                f.config.to_str().unwrap(),
                "dashboard",
                "--no-open",
                "--no-facts-refresh",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if viewer {
            command.arg("--controller-viewer");
        }
        let mut process = RunningDashboard(command.spawn().unwrap());
        let stdout = process.0.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        thread::spawn(move || {
            let mut url = String::new();
            std::io::BufReader::new(stdout).read_line(&mut url).unwrap();
            let _ = send.send(url);
        });
        let url = receive
            .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
            .unwrap();
        assert!(url.starts_with("http://"), "dashboard URL: {url:?}");
        let address = url
            .trim()
            .trim_start_matches("http://")
            .trim_end_matches('/');
        let collection = dashboard_request(address, "GET", "/api/v1/snapshot", None);
        assert_eq!(
            collection["tasks"][0]["integration"],
            json!(record.snapshot)
        );
        assert_eq!(collection["tasks"][0]["workflow_state"], "needs_you");
        let detail = dashboard_request(address, "GET", &format!("/api/v1/tasks/{task}"), None);
        assert_eq!(detail["integration"], json!(record.snapshot));
        let ordinary = ClientStateStore::open(&paths.state)
            .unwrap()
            .load_task(task)
            .unwrap();
        let response = dashboard_request(
            address,
            "POST",
            &format!("/api/v1/tasks/{task}/integrate"),
            Some(json!({
                "expected": {"expected_task_id":task, "expected_turn_id":ordinary.status().turns().last().unwrap().turn_id(),
                    "expected_turn_count":ordinary.status().turns().len(), "expected_head_oid":ordinary.status().head_oid(),
                    "expected_updated_at_millis":ordinary.status().updated_at_millis(), "expected_state":"open"},
                "expected_integration_id":record.snapshot.integration_id,
                "integration": {"task_id":task,"expected":record.snapshot.revision,"request_id":uuid::Uuid::new_v4().simple().to_string()}
            })),
        );
        assert_eq!(
            response["integration"]["integration_id"],
            json!(record.snapshot.integration_id)
        );
        assert!(
            response["integration"]["epoch"].as_u64().unwrap() > u64::from(record.snapshot.epoch)
        );
        assert_eq!(
            state.load(task).unwrap().unwrap().snapshot.state,
            IntegrationStatus::Parked
        );
    }
}

#[test]
fn native_recovery_reclaims_a_confirmed_dead_phase_actor_before_reexecuting_the_driver() {
    use mac_worker::test_support::{
        client_state::RunnerLivenessVerdict, host::supervisor::SystemProcessInspector,
    };
    use std::{
        io::Write,
        os::unix::fs::OpenOptionsExt,
        process::{Command, Stdio},
    };
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let mut child = Command::new("/bin/cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let actor = SystemProcessInspector.identity_for_pid(child.id()).unwrap();
    let runtime = std::sync::Arc::new(ManualIntegrationRuntime::default());
    runtime.set_actor_verdict(actor, RunnerLivenessVerdict::Live);
    let state = RootedIntegrationState::open(&paths, runtime).unwrap();
    let mut record = state.load(task).unwrap().unwrap();
    let expected = record.snapshot.revision;
    record.snapshot.state = IntegrationStatus::Fetching;
    record.snapshot.resume_state = None;
    record.snapshot.pause_reason = None;
    record.pause = None;
    record.actor = Some(actor);
    record.snapshot.revision = expected.next().unwrap();
    state.replace(task, expected, &record).unwrap();
    state
        .reserve(
            &record.target_key,
            record.snapshot.integration_id,
            record.snapshot.epoch,
            actor,
        )
        .unwrap()
        .unwrap();
    let binding = json!({"task": task, "intent": record.snapshot.integration_id, "epoch": record.snapshot.epoch, "actor": actor});
    let path = paths
        .state
        .join(format!("integrations/tasks/{task}/driver.json"));
    let mut binding_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    binding_file
        .write_all(&serde_json::to_vec(&binding).unwrap())
        .unwrap();
    binding_file.sync_all().unwrap();
    let undrain = f.worker(&["--json", "controller", "drain", "--off"]);
    assert!(
        undrain.status.success(),
        "{}",
        String::from_utf8_lossy(&undrain.stdout)
    );
    let reconcile = f.worker(&["--json", "task", "reconcile"]);
    assert!(reconcile.status.success());
    assert_eq!(
        state.load(task).unwrap().unwrap().actor,
        Some(actor),
        "live phase was reclaimed"
    );
    drop(child.stdin.take());
    assert!(child.wait().unwrap().success());
    let settled = wait_integrated(&f, task);
    assert!(settled.actor.is_none());
    assert!(settled.receipt.unwrap().imported);
}

#[test]
fn fresh_operator_reconcile_confirms_dead_integration_driver_and_retained_actor() {
    use mac_worker::test_support::{
        client_state::RunnerLivenessVerdict, host::supervisor::SystemProcessInspector,
    };
    use std::{
        io::Write,
        os::unix::fs::OpenOptionsExt,
        process::{Command, Stdio},
    };
    for retained in ["driver", "actor"] {
        let (f, task) = parked_source_fixture();
        let paths = owner_paths(&f);
        let runtime = std::sync::Arc::new(ManualIntegrationRuntime::default());
        let state = RootedIntegrationState::open(&paths, runtime.clone()).unwrap();
        let mut record = state.load(task).unwrap().unwrap();
        let mut child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let actor = SystemProcessInspector.identity_for_pid(child.id()).unwrap();
        let path = paths
            .state
            .join(format!("integrations/tasks/{task}/driver.json"));
        if retained == "actor" {
            runtime.set_actor_verdict(actor, RunnerLivenessVerdict::Live);
            let expected = record.snapshot.revision;
            record.actor = Some(actor);
            record.snapshot.revision = expected.next().unwrap();
            state.replace(task, expected, &record).unwrap();
            state
                .reserve(
                    &record.target_key,
                    record.snapshot.integration_id,
                    record.snapshot.epoch,
                    actor,
                )
                .unwrap()
                .unwrap();
        } else {
            let binding = json!({"task": task, "intent": record.snapshot.integration_id, "epoch": record.snapshot.epoch, "actor": actor});
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
                .unwrap();
            file.write_all(&serde_json::to_vec(&binding).unwrap())
                .unwrap();
            file.sync_all().unwrap();
        }
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success());
        assert!(
            f.worker(&["--json", "controller", "drain", "--off"])
                .status
                .success()
        );
        // Exactly one fresh CLI invocation must confirm absence before its
        // selected recovery. No prior process-local liveness cache is reused.
        let result = f.worker(&["--json", "task", "reconcile"]);
        assert!(
            result.status.success(),
            "out={} err={}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert_ne!(
            state.load(task).unwrap().unwrap().actor,
            Some(actor),
            "fresh reconcile retained the dead {retained}"
        );
        if path.exists() {
            let after: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            assert_ne!(
                after["actor"],
                serde_json::to_value(actor).unwrap(),
                "fresh reconcile retained the dead driver binding"
            );
        }
        let settled = wait_integrated(&f, task);
        assert!(settled.actor.is_none());
        assert!(settled.receipt.unwrap().imported);
    }
}

#[test]
fn native_close_revokes_a_parked_cycle_without_starting_a_git_phase() {
    use mac_worker::test_support::client_state::ClientStateStore;
    let (f, task) = parked_source_fixture();
    let origin = f.laptop.parent().unwrap().join("origin.git");
    let before = f
        .project
        .git(&["--git-dir", origin.to_str().unwrap(), "rev-parse", "main"])
        .stdout;
    let close = f.worker(&["--json", "task", "close", &task.to_string()]);
    assert!(
        close.status.success(),
        "{}",
        String::from_utf8_lossy(&close.stdout)
    );
    let state = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let stopped = state.load(task).unwrap().unwrap();
    assert_eq!(stopped.snapshot.state, IntegrationStatus::Revoked);
    assert!(stopped.tombstone.unwrap().acknowledged);
    assert_eq!(stopped.snapshot.attempts, 0);
    assert_eq!(
        ClientStateStore::open(&owner_paths(&f).state)
            .unwrap()
            .load_task(task)
            .unwrap()
            .status()
            .state(),
        TaskState::Closed
    );
    assert_eq!(
        f.project
            .git(&["--git-dir", origin.to_str().unwrap(), "rev-parse", "main"])
            .stdout,
        before
    );
}

#[test]
fn native_explicit_redrive_advances_one_blocked_epoch_and_recovers_via_reexec() {
    use mac_worker::test_support::{
        client_state::{ClientStateStore, dag::ParentGate},
        core::config::Config,
        host::process::SystemProcessRunner,
        task::{client::TaskClient, turn_runner::InlineRunnerExecutor},
    };
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut record = state.load(task).unwrap().unwrap();
    let expected = record.snapshot.revision;
    record.snapshot.revision = expected.next().unwrap();
    record.snapshot.state = IntegrationStatus::Blocked;
    record.snapshot.resume_state = None;
    record.snapshot.pause_reason = None;
    record.pause = None;
    record.snapshot.blocked_code = Some(IntegrationCode::IntegrationWorkerOffline);
    record.snapshot.retry_exhausted = true;
    state.replace(task, expected, &record).unwrap();
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let config = Config::load(&paths.config).unwrap();
    let client = TaskClient::new(
        &SystemProcessRunner,
        &config,
        &paths,
        &tasks,
        &InlineRunnerExecutor,
    );
    assert_eq!(
        client
            .integration_parent_gate(&tasks.load_task(task).unwrap())
            .unwrap(),
        ParentGate::Waiting
    );
    let undrain = f.worker(&["--json", "controller", "drain", "--off"]);
    assert!(undrain.status.success());
    let redrive = f.worker(&["--json", "task", "integrate", &task.to_string()]);
    assert!(
        redrive.status.success(),
        "{}",
        String::from_utf8_lossy(&redrive.stdout)
    );
    let pending: Value = serde_json::from_slice(&redrive.stdout).unwrap();
    assert_eq!(pending["integration"]["epoch"], 1);
    let done = wait_integrated(&f, task);
    assert_eq!(done.snapshot.integration_id, record.snapshot.integration_id);
    assert_eq!(done.snapshot.epoch, 1);
    assert!(done.receipt.as_ref().unwrap().imported);
    assert_eq!(
        client
            .integration_parent_gate(&tasks.load_task(task).unwrap())
            .unwrap(),
        ParentGate::Ready
    );
    let ordinary = tasks.load_task(task).unwrap();
    for _ in 0..2 {
        let repeated = f.worker(&["--json", "task", "integrate", &task.to_string()]);
        assert!(
            repeated.status.success(),
            "out={} err={}",
            String::from_utf8_lossy(&repeated.stdout),
            String::from_utf8_lossy(&repeated.stderr)
        );
        let result: Value = serde_json::from_slice(&repeated.stdout).unwrap();
        assert_eq!(
            result["integration"],
            serde_json::to_value(&done.snapshot).unwrap()
        );
        assert_eq!(state.load(task).unwrap().unwrap(), done);
        assert_eq!(tasks.load_task(task).unwrap(), ordinary);
        assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
    }
}

#[test]
fn a_brief_helper_rollback_parks_and_restores_the_same_open_source_cycle() {
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let before = state.load(task).unwrap().unwrap();
    let policy = state.load_policy(task).unwrap().unwrap();
    let ssh = std::fs::read_to_string(&f.ssh).unwrap();
    assert!(ssh.contains("probe['features'].append('task.integration')"));
    std::fs::write(
        &f.ssh,
        ssh.replace(
            "probe['features'].append('task.integration')",
            "pass # previous helper fixture",
        ),
    )
    .unwrap();
    let journal_before = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    assert!(f.worker(&["controller", "drain", "--off"]).status.success());
    let waited = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "15s",
    ]);
    assert!(String::from_utf8_lossy(&waited.stdout).contains("WAIT_TIMEOUT"));
    let parked = state.load(task).unwrap().unwrap();
    assert_eq!(parked.snapshot.state, IntegrationStatus::Parked);
    assert_eq!(
        parked.snapshot.pause_reason,
        Some(IntegrationPauseReason::HelperUnavailable)
    );
    assert_eq!(
        parked.snapshot.integration_id,
        before.snapshot.integration_id
    );
    assert_eq!(parked.snapshot.epoch, before.snapshot.epoch);
    assert_eq!(parked.snapshot.source_head, before.snapshot.source_head);
    assert_eq!(parked.cycle_base, before.cycle_base);
    assert_eq!(parked.followups_spent, before.followups_spent);
    assert_eq!(parked.snapshot.attempts, before.snapshot.attempts);
    assert_eq!(state.load_policy(task).unwrap().unwrap(), policy);
    let during = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    assert_eq!(
        during.matches("host task-prepare\n").count(),
        journal_before.matches("host task-prepare\n").count()
    );
    assert_eq!(
        during.matches("host task-integration\n").count(),
        journal_before.matches("host task-integration\n").count()
    );
    std::fs::write(&f.ssh, ssh).unwrap();
    super::session_import_e2e::warm_executable(&f.ssh);
    let restored = wait_integrated(&f, task);
    assert_eq!(
        restored.snapshot.integration_id,
        before.snapshot.integration_id
    );
    assert_eq!(restored.snapshot.epoch, before.snapshot.epoch);
    assert_eq!(restored.followups_spent, before.followups_spent);
    assert!(restored.receipt.unwrap().imported);
    assert_eq!(
        std::fs::read_to_string(f.host.join("ssh-journal"))
            .unwrap()
            .matches("host task-prepare\n")
            .count(),
        1
    );
}

#[test]
fn native_wait_returns_the_blocked_code_after_successful_source_import() {
    use mac_worker::test_support::session::SessionAgent;
    let f = integration_fixture(true);
    f.capture_fixture(SessionAgent::Codex);
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap().replace(r#"\"files_changed\":[]"#, r#"\"files_changed\":[],\"checks\":[{\"name\":\"fixture-check\",\"command\":\"fixture-check\",\"status\":\"fail\",\"detail\":\"fixture failed\"}]"#);
    std::fs::write(agent, script).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "work with failing checks",
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--wait",
    ]));
    let wait = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "60s",
    ]);
    assert_eq!(
        wait.status.code(),
        Some(i32::from(
            IntegrationCode::IntegrationChecksFailed.error().exit_code()
        )),
        "{}",
        String::from_utf8_lossy(&wait.stdout)
    );
    let state = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    assert_eq!(
        state.load(task).unwrap().unwrap().snapshot.blocked_code,
        Some(IntegrationCode::IntegrationChecksFailed)
    );
}

#[test]
fn native_dag_uses_the_imported_parent_merge_for_its_configured_child() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        core::config::Config,
        host::process::SystemProcessRunner,
        task::{client::TaskClient, turn_runner::InlineRunnerExecutor},
    };
    let f = integration_fixture(true);
    let agent = f.host.join("bin/codex");
    std::fs::write(&agent, r#"#!/bin/sh
case "$1" in --fixture-warm) exit 0;; --version) printf '0.160.0\n'; exit 0;; auth|login) printf '{"loggedIn":true}\n'; exit 0;; esac
printf '%s\n' "$@" >> "$HOME/dag-agent-argv"
printf 'fixture task result\n' >> dag-result
printf '%s\n' '{"type":"thread.started","thread_id":"00000000-0000-0000-0000-000000000031"}' '{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"done\",\"summary\":\"dag work done\",\"questions\":[],\"files_changed\":[],\"checks\":[]}"}}'
"#).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o700)).unwrap();
    super::session_import_e2e::warm_executable(&agent);
    std::fs::write(
        f.project.root().join(".worker.toml"),
        "[task]\nintegrate = 'main'\n",
    )
    .unwrap();
    let batch = f.project.root().join("tasks.toml");
    std::fs::write(&batch, "close_on = 'never'\n[[tasks]]\nid = 'parent'\nprompt = 'parent work'\n[[tasks]]\nid = 'child'\nprompt = 'child work'\ndepends_on = ['parent']\nbase = 'from:parent'\n").unwrap();
    mac_worker::test_support::controller::drain::set_drained(
        &owner_paths(&f).controller_state_root(),
        true,
    )
    .unwrap();
    let output = f.worker(&["--json", "task", "batch", batch.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    let run: RunId = report["run_id"].as_str().unwrap().parse().unwrap();
    let paths = owner_paths(&f);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let dag = tasks.load_run_dag(run).unwrap().unwrap();
    let parent = dag.nodes["parent"].task_id;
    let child = dag.nodes["child"].task_id;
    let config = Config::load(&paths.config).unwrap();
    // Advancing is done by real finalizers/wait/driver. This read proves the
    // unimported source cannot release its configured dependency.
    let client = TaskClient::new(
        &SystemProcessRunner,
        &config,
        &paths,
        &tasks,
        &InlineRunnerExecutor,
    );
    assert!(tasks.load_task_optional(child).unwrap().is_none());
    assert_ne!(
        client
            .integration_parent_gate(&tasks.load_task(parent).unwrap())
            .unwrap(),
        mac_worker::test_support::client_state::dag::ParentGate::Ready
    );
    assert!(f.worker(&["controller", "drain", "--off"]).status.success());
    let wait = f.worker(&[
        "--json",
        "task",
        "wait",
        "--run",
        &run.to_string(),
        "--timeout",
        "90s",
    ]);
    assert!(
        wait.status.success(),
        "{}",
        String::from_utf8_lossy(&wait.stdout)
    );
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let parent_record = state.load(parent).unwrap().unwrap();
    let receipt = parent_record.receipt.unwrap();
    assert!(receipt.imported);
    assert_eq!(
        tasks.load_task(child).unwrap().meta().base_oid(),
        receipt.merge_oid.as_ref().unwrap_or(&receipt.target_head)
    );
    assert_eq!(
        state.load(child).unwrap().unwrap().snapshot.state,
        IntegrationStatus::Integrated
    );
    assert_eq!(
        tasks.load_task(parent).unwrap().status().state(),
        TaskState::Open
    );
}

#[test]
fn native_mismatched_from_parent_policy_refuses_before_capture_or_admission() {
    use mac_worker::test_support::client_state::ClientStateStore;
    let f = integration_fixture(true);
    let batch = f.project.root().join("tasks.toml");
    std::fs::write(&batch, "[[tasks]]\nid = 'parent'\nprompt = 'parent work'\nintegrate = 'main'\n[[tasks]]\nid = 'child'\nprompt = 'child work'\nintegrate = 'different'\ndepends_on = ['parent']\nbase = 'from:parent'\n").unwrap();
    let output = f.worker(&["--json", "task", "batch", batch.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("TASK_CONFIG_INVALID"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let paths = owner_paths(&f);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    assert!(tasks.list_tasks().unwrap().is_empty());
    assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
    assert!(!paths.cache.join("transfer").exists());
    assert!(!f.host.join("dag-agent-argv").exists());
}

fn refuse_integration_transport(f: &super::session_import_e2e::Fixture) -> String {
    let original = std::fs::read_to_string(&f.ssh).unwrap();
    std::fs::write(&f.ssh, original.replace("os.execv('/bin/sh', ['/bin/sh', '-c', command])", "if command.endswith(' host task-integration'):\n    sys.stdin.buffer.read()\n    sys.exit(255)\nos.execv('/bin/sh', ['/bin/sh', '-c', command])")).unwrap();
    super::session_import_e2e::warm_executable(&f.ssh);
    original
}

#[test]
fn native_offline_cancel_close_and_discard_remain_unconfirmed_until_revoke_proof() {
    use mac_worker::test_support::client_state::ClientStateStore;
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let original_ssh = refuse_integration_transport(&f);
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let initial = state.load(task).unwrap().unwrap();
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    for args in [
        vec!["--json", "task", "cancel"],
        vec!["--json", "task", "close"],
        vec!["--json", "task", "close", "--discard"],
    ] {
        let id = task.to_string();
        let mut arguments = args;
        arguments.push(&id);
        let refused = f.worker(&arguments);
        assert!(!refused.status.success());
        assert!(
            String::from_utf8_lossy(&refused.stdout).contains("INTEGRATION_STOP_UNCONFIRMED"),
            "{}",
            String::from_utf8_lossy(&refused.stdout)
        );
        let pending = state.load(task).unwrap().unwrap();
        assert_eq!(
            pending.snapshot.integration_id,
            initial.snapshot.integration_id
        );
        assert_eq!(pending.snapshot.epoch, initial.snapshot.epoch);
        assert!(!pending.tombstone.unwrap().acknowledged);
        assert_eq!(
            tasks.load_task(task).unwrap().status().state(),
            TaskState::Open
        );
    }
    std::fs::write(&f.ssh, original_ssh).unwrap();
    super::session_import_e2e::warm_executable(&f.ssh);
    assert!(
        f.worker(&["--json", "task", "cancel", &task.to_string()])
            .status
            .success()
    );
    assert!(
        state
            .load(task)
            .unwrap()
            .unwrap()
            .tombstone
            .unwrap()
            .acknowledged
    );
    assert!(
        f.worker(&["--json", "task", "close", "--discard", &task.to_string()])
            .status
            .success()
    );
    assert_eq!(
        tasks.load_task(task).unwrap().status().state(),
        TaskState::Abandoned
    );
    assert_eq!(state.load(task).unwrap().unwrap().snapshot.attempts, 0);
}

#[test]
fn native_interrupted_say_waits_for_revoke_then_queues_one_ordinary_turn() {
    use mac_worker::test_support::client_state::ClientStateStore;
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut record = state.load(task).unwrap().unwrap();
    let expected = record.snapshot.revision;
    record.snapshot.revision = expected.next().unwrap();
    record.snapshot.state = IntegrationStatus::Blocked;
    record.snapshot.blocked_code = Some(IntegrationCode::IntegrationWorkerOffline);
    record.snapshot.resume_state = None;
    record.snapshot.pause_reason = None;
    record.pause = None;
    state.replace(task, expected, &record).unwrap();
    let original_ssh = refuse_integration_transport(&f);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let say = || {
        f.worker(&[
            "--json",
            "task",
            "say",
            &task.to_string(),
            "--message",
            "ordinary replacement",
        ])
    };
    let refused = say();
    assert!(
        String::from_utf8_lossy(&refused.stdout).contains("INTEGRATION_STOP_UNCONFIRMED"),
        "{}",
        String::from_utf8_lossy(&refused.stdout)
    );
    assert_eq!(tasks.load_task(task).unwrap().status().turns().len(), 1);
    assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
    std::fs::write(&f.ssh, original_ssh).unwrap();
    super::session_import_e2e::warm_executable(&f.ssh);
    let queued = say();
    assert!(
        queued.status.success(),
        "{}",
        String::from_utf8_lossy(&queued.stdout)
    );
    assert_eq!(tasks.load_task(task).unwrap().status().turns().len(), 2);
    assert_eq!(tasks.queue_snapshot().unwrap().entries().len(), 1);
    assert!(
        state
            .load(task)
            .unwrap()
            .unwrap()
            .tombstone
            .unwrap()
            .acknowledged
    );
    assert_eq!(state.load(task).unwrap().unwrap().snapshot.attempts, 0);
}

struct ReleaseFixtureFifo(std::path::PathBuf);
impl Drop for ReleaseFixtureFifo {
    fn drop(&mut self) {
        use std::io::Write;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.0)
        {
            let _ = file.write_all(b"continue\n");
        }
    }
}

#[test]
fn native_rpc_leader_and_direct_recovery_share_the_detached_git_driver() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{decode_frame, encode_json_frame, serve_rpc_with_integration_features},
        core::config::Config,
        core::error::WorkerError,
        host::process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
        task::{client::TaskClient, turn_runner::InlineRunnerExecutor},
    };
    struct LocalOnly(std::path::PathBuf);
    impl ProcessRunner for LocalOnly {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let mut request = request.clone();
            if request.program == "ssh" || request.program == "/usr/bin/ssh" {
                request.program = self.0.clone().into_os_string();
            }
            SystemProcessRunner.run(&request)
        }
    }
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let owner = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let origin_before = f.project.git(&["ls-remote", "origin", "refs/heads/main"]);
    let ready = f.host.join("driver-ready");
    let release = f.host.join("driver-release");
    fixture_fifo(&ready);
    fixture_fifo(&release);
    let script = std::fs::read_to_string(&f.ssh).unwrap();
    let hold = r#"if command.endswith(' host task-integration'):
    data = sys.stdin.buffer.read()
    action = json.loads(data)['action']
    if action.get('step') == 'push':
        with open(os.path.join(os.environ['HOME'], 'driver-ready'), 'w') as signal: signal.write('admitted\n')
        with open(os.path.join(os.environ['HOME'], 'driver-release')) as signal: signal.readline()
    result = subprocess.run(['/bin/sh','-c',command], input=data, capture_output=True)
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    sys.exit(result.returncode)
os.execv('/bin/sh', ['/bin/sh', '-c', command])"#;
    std::fs::write(
        &f.ssh,
        script.replace("os.execv('/bin/sh', ['/bin/sh', '-c', command])", hold),
    )
    .unwrap();
    assert!(f.worker(&["controller", "drain", "--off"]).status.success());
    let config = Config::parse(&std::fs::read_to_string(&f.config).unwrap()).unwrap();
    let runner = LocalOnly(f.ssh.clone());
    let state = ClientStateStore::open(&paths.state).unwrap();
    std::thread::scope(|scope| {
        let release_on_drop = ReleaseFixtureFifo(release);
        let waiter = scope.spawn(|| wait_integrated(&f, task));
        let mut admitted = String::new();
        std::io::BufRead::read_line(
            &mut std::io::BufReader::new(std::fs::File::open(ready).unwrap()),
            &mut admitted,
        )
        .unwrap();
        assert_eq!(admitted, "admitted\n");
        let held = owner.load(task).unwrap().unwrap();
        assert!(held.actor.is_some());
        assert_eq!(held.snapshot.state, IntegrationStatus::Pushing);
        let direct = scope.spawn(|| f.worker(&["--json", "task", "reconcile"]));
        let leader = scope.spawn(|| {
            TaskClient::new(&runner, &config, &paths, &state, &InlineRunnerExecutor)
                .tick_selected_recovery()
                .unwrap()
        });
        let rpc=scope.spawn(|| {
            let wire=serde_json::json!({"protocol_version":7,"request_id":uuid::Uuid::new_v4().simple().to_string(),"command":"task.wait.poll","body":{"task_id":task}});
            let input=encode_json_frame(&wire).unwrap(); let mut out=vec![];
            serve_rpc_with_integration_features(&paths,&config,&runner,&mut std::io::Cursor::new(input),&mut out,&[CONTROLLER_FEATURE_INTEGRATION.into()]).unwrap();
            let reply:Value=serde_json::from_slice(decode_frame(&out).unwrap()).unwrap();
            assert_eq!(reply["result"]["quiescent"],false);
        });
        let result = direct.join().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stdout)
        );
        leader.join().unwrap();
        rpc.join().unwrap();
        assert_eq!(owner.load(task).unwrap().unwrap().actor, held.actor);
        assert_eq!(
            f.project.git(&["ls-remote", "origin", "refs/heads/main"]),
            origin_before
        );
        assert_eq!(
            std::fs::read_to_string(f.host.join("ssh-journal"))
                .unwrap()
                .matches("host task-integration\n")
                .count(),
            3
        );
        drop(release_on_drop);
        let done = waiter.join().unwrap();
        assert!(done.receipt.unwrap().imported);
        assert_eq!(done.candidates.len(), 1);
    });
}
fn fixture_fifo(path: &std::path::Path) {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
}

#[test]
fn native_cancel_with_a_lost_push_reply_imports_the_committed_success_before_close() {
    use mac_worker::test_support::{client_state::ClientStateStore, session::SessionAgent};
    use std::io::BufRead;
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let original_source = std::fs::read(&source).unwrap();
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap().replace(
        "printf '%s\\n' \"$@\" > \"$HOME/argv\"",
        "printf 'lost reply source\\n' > T6-result\nprintf '%s\\n' \"$@\" > \"$HOME/argv\"",
    );
    std::fs::write(agent, script).unwrap();
    let ready = f.laptop.parent().unwrap().join("push-ready");
    let release = f.laptop.parent().unwrap().join("push-release");
    fixture_fifo(&ready);
    fixture_fifo(&release);
    let release_on_drop = ReleaseFixtureFifo(release.clone());
    let interception = format!(
        r#"if command.endswith(' host task-integration'):
    data = sys.stdin.buffer.read()
    action = json.loads(data)['action']
    result = subprocess.run(['/bin/sh','-c',command], input=data, capture_output=True)
    if action.get('step') == 'push' and result.returncode == 0:
        with open({ready:?}, 'w') as f: f.write('published\n')
        with open({release:?}, 'r') as f: f.readline()
        sys.exit(255)
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    sys.exit(result.returncode)
os.execv('/bin/sh', ['/bin/sh', '-c', command])"#,
        ready = ready.to_str().unwrap(),
        release = release.to_str().unwrap()
    );
    let ssh = std::fs::read_to_string(&f.ssh).unwrap().replace(
        "os.execv('/bin/sh', ['/bin/sh', '-c', command])",
        &interception,
    );
    std::fs::write(&f.ssh, ssh).unwrap();
    super::session_import_e2e::warm_executable(&f.ssh);
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "work",
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--wait",
    ]));
    let mut signal = String::new();
    std::io::BufReader::new(std::fs::File::open(ready).unwrap())
        .read_line(&mut signal)
        .unwrap();
    assert_eq!(signal, "published\n");
    let cancel = f.worker(&["--json", "task", "cancel", &task.to_string()]);
    assert!(
        String::from_utf8_lossy(&cancel.stdout).contains("INTEGRATION_ALREADY_COMMITTED"),
        "{}",
        String::from_utf8_lossy(&cancel.stdout)
    );
    drop(release_on_drop);
    let committed = wait_integrated(&f, task);
    let receipt = committed.receipt.unwrap();
    assert!(receipt.imported);
    assert_eq!(receipt.disposition, IntegrationDisposition::Merged);
    let tasks = ClientStateStore::open(&owner_paths(&f).state).unwrap();
    assert_eq!(
        tasks.load_task(task).unwrap().status().last_outcome(),
        Some(&TaskOutcome::Done)
    );
    assert!(
        f.worker(&["--json", "task", "close", &task.to_string()])
            .status
            .success()
    );
    assert_eq!(
        tasks.load_task(task).unwrap().status().state(),
        TaskState::Closed
    );
    let origin = f.laptop.parent().unwrap().join("origin.git");
    assert_eq!(
        String::from_utf8(
            f.project
                .git(&["--git-dir", origin.to_str().unwrap(), "rev-parse", "main"])
                .stdout
        )
        .unwrap()
        .trim(),
        receipt.merge_oid.unwrap().as_str()
    );
    assert_eq!(std::fs::read(source).unwrap(), original_source);
}

fn wait_integrated(f: &super::session_import_e2e::Fixture, task: TaskId) -> IntegrationRecord {
    let wait = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "60s",
    ]);
    assert!(
        wait.status.success(),
        "out={} err={} record={:?} local={:?} steps={:?}",
        String::from_utf8_lossy(&wait.stdout),
        String::from_utf8_lossy(&wait.stderr),
        RootedIntegrationState::open(
            &owner_paths(f),
            std::sync::Arc::new(ManualIntegrationRuntime::default())
        )
        .unwrap()
        .load(task)
        .unwrap(),
        mac_worker::test_support::client_state::ClientStateStore::open(&owner_paths(f).state)
            .unwrap()
            .load_task(task)
            .unwrap(),
        std::fs::read_to_string(f.host.join("restore-steps"))
    );
    let owner = RootedIntegrationState::open(
        &owner_paths(f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let record = owner.load(task).unwrap().unwrap();
    assert_eq!(
        record.snapshot.state,
        IntegrationStatus::Integrated,
        "{:?}",
        record.snapshot
    );
    record
}

#[test]
fn never_receipt_is_idempotent_and_the_next_say_uses_its_accepted_head() {
    use mac_worker::test_support::{client_state::ClientStateStore, session::SessionAgent};
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let original = std::fs::read(&source).unwrap();
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap().replace(
        "printf '%s\\n' \"$@\" > \"$HOME/argv\"",
        "count=0; [ ! -f \"$HOME/turn-count\" ] || count=$(cat \"$HOME/turn-count\")\ncount=$((count + 1)); printf '%s' \"$count\" > \"$HOME/turn-count\"\nprintf 'ordinary %s\\n' \"$count\" > T6-result\nprintf '%s\\n' \"$@\" > \"$HOME/argv\"",
    );
    std::fs::write(agent, script).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "produce work",
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]));
    let first = wait_integrated(&f, task);
    let accepted = first.receipt.as_ref().unwrap().merge_oid.as_ref().unwrap();
    let reconcile = f.worker(&["--json", "task", "reconcile"]);
    assert!(
        reconcile.status.success(),
        "{}",
        String::from_utf8_lossy(&reconcile.stdout)
    );
    let owner = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    assert_eq!(
        owner.load(task).unwrap().unwrap().snapshot.integration_id,
        first.snapshot.integration_id
    );
    let say = f.worker(&[
        "--json",
        "task",
        "say",
        &task.to_string(),
        "--message",
        "produce later work",
        "--wait",
    ]);
    assert!(
        say.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&say.stdout),
        String::from_utf8_lossy(&say.stderr)
    );
    let next = wait_integrated(&f, task);
    assert_ne!(next.snapshot.integration_id, first.snapshot.integration_id);
    assert_eq!(&next.cycle_base, accepted);
    assert_eq!(next.archived_receipts, vec![first.receipt.unwrap()]);
    let local = ClientStateStore::open(&owner_paths(&f).state)
        .unwrap()
        .load_task(task)
        .unwrap();
    assert_eq!(local.status().state(), TaskState::Open);
    assert_eq!(local.status().turns().len(), 2);
    assert_eq!(std::fs::read(source).unwrap(), original);
    let journal = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    assert_eq!(journal.matches("host task-prepare\n").count(), 1);
}

#[test]
fn conflict_auxiliary_resumes_the_imported_session_without_ordinary_publication() {
    use mac_worker::test_support::{client_state::ClientStateStore, session::SessionAgent};
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let original = std::fs::read(&source).unwrap();
    f.install_agent(SessionAgent::Codex);
    let target = f.laptop.parent().unwrap().join("outside");
    let origin = f.laptop.parent().unwrap().join("origin.git");
    f.project
        .git(&["clone", origin.to_str().unwrap(), target.to_str().unwrap()]);
    f.project.git(&[
        "-C",
        target.to_str().unwrap(),
        "config",
        "user.email",
        "fixture@example.test",
    ]);
    f.project.git(&[
        "-C",
        target.to_str().unwrap(),
        "config",
        "user.name",
        "Fixture",
    ]);
    let agent = f.host.join("bin/codex");
    let work = format!(
        r#"count=0; [ ! -f "$HOME/turn-count" ] || count=$(cat "$HOME/turn-count")
count=$((count + 1)); printf '%s' "$count" > "$HOME/turn-count"
if [ "$count" = 1 ]; then
  printf 'ordinary\n' > README
  (cd '{}' && printf 'outside\n' > README && /usr/bin/git add README && /usr/bin/git commit -m outside && /usr/bin/git push origin main) >/dev/null 2>&1 || exit 95
else
  /usr/bin/git rev-parse MERGE_HEAD > "$HOME/aux-target" || exit 96
  /usr/bin/git rev-parse HEAD > "$HOME/aux-head"
  printf 'resolved\n' > README
fi
printf '%s\n' "$@" > "$HOME/argv""#,
        target.display()
    );
    let script = std::fs::read_to_string(&agent).unwrap().replace("printf '%s\\n' \"$@\" > \"$HOME/argv\"", &work)
        .replace("done < \"$HOME/placed-files\"\n", "done < \"$HOME/placed-files\"\nwhile IFS= read -r file; do printf '%s\\n' '{\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"native append\"}}' >> \"$file\"; done < \"$HOME/placed-files\"\n");
    std::fs::write(agent, script).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "produce conflict",
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]));
    let record = wait_integrated(&f, task);
    assert_eq!(record.snapshot.resolve_turns, 1);
    assert_eq!(
        record.snapshot.verification,
        IntegrationVerification::ResolveAgentReport
    );
    let auxiliary = &record.auxiliaries[0];
    assert!(auxiliary.accepted && auxiliary.completed);
    assert!(auxiliary.queue_position.is_some());
    let local = ClientStateStore::open(&owner_paths(&f).state)
        .unwrap()
        .load_task(task)
        .unwrap();
    assert_eq!(local.status().turns().len(), 2);
    assert_eq!(local.status().turns()[1].turn_id(), auxiliary.turn_id);
    let merge = record.receipt.unwrap().merge_oid.unwrap();
    assert_eq!(
        f.project.git(&["show", &format!("{merge}:README")]).stdout,
        b"resolved\n"
    );
    assert_eq!(std::fs::read(source).unwrap(), original);
    let placed = std::fs::read_to_string(f.host.join("placed-files")).unwrap();
    assert_eq!(
        std::fs::read_to_string(placed.lines().next().unwrap())
            .unwrap()
            .matches("native append")
            .count(),
        2
    );
    let journal = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    assert_eq!(journal.matches("host task-prepare\n").count(), 1);
    assert_eq!(journal.matches("host task-turn\n").count(), 1);
    assert_eq!(journal.matches("host task-integration-turn\n").count(), 1);
}

struct ReleaseAuxiliary(std::fs::File);
impl Drop for ReleaseAuxiliary {
    fn drop(&mut self) {
        use std::io::Write;
        let _ = self.0.write_all(b"resume\n");
    }
}
struct RunningAuxiliaryFixture {
    f: super::session_import_e2e::Fixture,
    task: TaskId,
    ready: std::fs::File,
    _release: ReleaseAuxiliary,
    source: std::path::PathBuf,
    original: Vec<u8>,
}
fn running_auxiliary_fixture(timeout: &str) -> RunningAuxiliaryFixture {
    use mac_worker::test_support::session::SessionAgent;
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let original = std::fs::read(&source).unwrap();
    f.install_agent(SessionAgent::Codex);
    let target = f.laptop.parent().unwrap().join("outside");
    let origin = f.laptop.parent().unwrap().join("origin.git");
    f.project
        .git(&["clone", origin.to_str().unwrap(), target.to_str().unwrap()]);
    f.project.git(&[
        "-C",
        target.to_str().unwrap(),
        "config",
        "user.email",
        "fixture@example.test",
    ]);
    f.project.git(&[
        "-C",
        target.to_str().unwrap(),
        "config",
        "user.name",
        "Fixture",
    ]);
    let fifo = |name: &str| {
        use std::os::unix::ffi::OsStrExt;
        let path = f.host.join(name);
        let path_c = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path_c.as_ptr(), 0o600) }, 0);
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    };
    let ready = fifo("aux-ready");
    let release = ReleaseAuxiliary(fifo("aux-release"));
    let agent = f.host.join("bin/codex");
    let work = format!(
        r#"count=0; [ ! -f "$HOME/turn-count" ] || count=$(cat "$HOME/turn-count")
count=$((count + 1)); printf '%s' "$count" > "$HOME/turn-count"
if [ "$count" = 1 ]; then
  printf 'ordinary\n' > README
  (cd '{}' && printf 'outside\n' > README && /usr/bin/git add README && /usr/bin/git commit -m outside && /usr/bin/git push origin main) >/dev/null 2>&1 || exit 95
else
  /usr/bin/git rev-parse MERGE_HEAD > "$HOME/aux-target" || exit 96
  /usr/bin/git rev-parse HEAD > "$HOME/aux-head"
  printf 'a' > "$HOME/aux-ready"
  IFS= read -r release < "$HOME/aux-release"
  printf 'resolved\n' > README
fi
printf '%s\n' "$@" > "$HOME/argv""#,
        target.display()
    );
    let script = std::fs::read_to_string(&agent).unwrap().replace("printf '%s\\n' \"$@\" > \"$HOME/argv\"", &work)
        .replace("done < \"$HOME/placed-files\"\n", "done < \"$HOME/placed-files\"\nwhile IFS= read -r file; do printf '%s\\n' '{\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"native append\"}}' >> \"$file\"; done < \"$HOME/placed-files\"\n");
    std::fs::write(agent, script).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "produce conflict",
        "--timeout",
        timeout,
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]));
    RunningAuxiliaryFixture {
        f,
        task,
        ready,
        _release: release,
        source,
        original,
    }
}

#[test]
fn cancelling_a_live_auxiliary_retains_stop_until_the_host_and_runner_retire() {
    use mac_worker::test_support::client_state::ClientStateStore;
    use std::io::Read;
    let RunningAuxiliaryFixture {
        f,
        task,
        mut ready,
        _release,
        source,
        original,
    } = running_auxiliary_fixture("45m");
    ready.read_exact(&mut [0]).unwrap();
    let origin = f.laptop.parent().unwrap().join("origin.git");
    let origin_before = f
        .project
        .git(&["--git-dir", origin.to_str().unwrap(), "rev-parse", "main"])
        .stdout;
    let cancel = f.worker(&["--json", "task", "cancel", &task.to_string()]);
    assert!(
        !cancel.status.success(),
        "a live auxiliary was acknowledged as cancelled"
    );
    assert!(String::from_utf8_lossy(&cancel.stdout).contains("INTEGRATION_STOP_UNCONFIRMED"));
    let client = ClientStateStore::open(&owner_paths(&f).state).unwrap();
    let owner = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let pending = owner.load(task).unwrap().unwrap();
    assert!(
        pending
            .tombstone
            .as_ref()
            .is_some_and(|stop| !stop.acknowledged)
    );
    if let Some(entry) = client.queue_entry_for_task_turn(task).unwrap() {
        assert!(
            entry.is_cancel_requested(),
            "stop did not reach the live auxiliary queue"
        );
    }
    let wait = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "60s",
    ]);
    assert!(
        !String::from_utf8_lossy(&wait.stdout).contains("WAIT_TIMEOUT"),
        "{}",
        String::from_utf8_lossy(&wait.stdout)
    );
    let settled = owner.load(task).unwrap().unwrap();
    assert_eq!(settled.snapshot.state, IntegrationStatus::Revoked);
    assert!(settled.tombstone.unwrap().acknowledged);
    assert!(client.queue_entry_for_task_turn(task).unwrap().is_none());
    assert!(client.load_task(task).unwrap().runner().is_none());
    assert_eq!(
        f.project
            .git(&["--git-dir", origin.to_str().unwrap(), "rev-parse", "main"])
            .stdout,
        origin_before
    );
    assert_eq!(std::fs::read(source).unwrap(), original);
    let close = f.worker(&["--json", "task", "close", &task.to_string(), "--discard"]);
    assert!(
        close.status.success(),
        "{}",
        String::from_utf8_lossy(&close.stdout)
    );
}

#[test]
fn a_running_auxiliary_times_out_while_the_native_owner_gate_is_drained() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        task::model::{TaskOutcome, TaskState},
    };
    use std::io::Read;
    let RunningAuxiliaryFixture {
        f,
        task,
        mut ready,
        _release,
        source,
        original,
    } = running_auxiliary_fixture("30s");
    ready.read_exact(&mut [0]).unwrap();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let running = state.load(task).unwrap().unwrap();
    let turn = running.auxiliaries[0].turn_id;
    let prepared = state.load_prepared(task, turn).unwrap().unwrap();
    assert_eq!(prepared.approved_turn_limits.timeout_millis, 30_000);
    assert!(f.worker(&["controller", "drain"]).status.success());
    // The real supervisor's lease clock keeps running. Waiting observes its
    // terminal result while the owner remains barred from another Git phase.
    let waited = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "60s",
    ]);
    assert!(
        String::from_utf8_lossy(&waited.stdout).contains("WAIT_TIMEOUT"),
        "the parked integration must still keep wait pending: {}",
        String::from_utf8_lossy(&waited.stdout)
    );
    let client = ClientStateStore::open(&paths.state).unwrap();
    let ordinary = client.load_task(task).unwrap();
    assert_eq!(ordinary.status().state(), TaskState::Open);
    assert_eq!(ordinary.status().turns().last().unwrap().turn_id(), turn);
    assert_eq!(
        ordinary.status().last_outcome(),
        Some(&TaskOutcome::TimedOut)
    );
    assert!(ordinary.runner().is_none());
    assert!(client.queue_entry_for_task_turn(task).unwrap().is_none());
    let parked = state.load(task).unwrap().unwrap();
    assert_eq!(parked.snapshot.state, IntegrationStatus::Parked);
    assert_eq!(
        parked.snapshot.pause_reason,
        Some(IntegrationPauseReason::ControllerDrained)
    );
    assert!(parked.auxiliaries[0].accepted && parked.auxiliaries[0].completed);
    assert_eq!(parked.followups_spent, 1);
    assert!(parked.receipt.is_none());
    assert_eq!(state.load_prepared(task, turn).unwrap().unwrap(), prepared);
    assert_eq!(std::fs::read(source).unwrap(), original);
}

#[test]
fn direct_import_is_complete_and_host_is_armed_before_the_first_launch() {
    use mac_worker::test_support::session::SessionAgent;
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let before = std::fs::read(&source).unwrap();
    let origin = f.laptop.parent().unwrap().join("origin.git");
    let origin_refs = f
        .project
        .git(&[
            "--git-dir",
            origin.to_str().unwrap(),
            "for-each-ref",
            "--format=%(refname) %(objectname)",
        ])
        .stdout;
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap().replace(
        "done < \"$HOME/placed-files\"\n",
        r#"done < "$HOME/placed-files"
while IFS= read -r file; do
  printf '%s\n' '{"type":"event_msg","payload":{"type":"agent_message","message":"T6 native tail"}}' >> "$file"
done < "$HOME/placed-files"
"#,
    );
    let check = "find \"$HOME/.local/share/mac-worker/host/tasks\" -name policy.json > \"$HOME/armed-policies\"\n[ -s \"$HOME/armed-policies\" ] || exit 94\n";
    std::fs::write(
        &agent,
        script.replace(
            "printf '%s\\n' \"$@\" > \"$HOME/argv\"",
            &format!("{check}printf '%s\\n' \"$@\" > \"$HOME/argv\""),
        ),
    )
    .unwrap();
    let output = f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "continue safely",
        "--integrate",
        "main",
        "--close-on",
        "done",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]);
    assert!(
        output.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .rfind(|v| v.get("session_import").is_some())
        .unwrap();
    let task: TaskId = report["task_id"].as_str().unwrap().parse().unwrap();
    let owner_policy = f
        .laptop
        .join(".local/state/mac-worker/integrations/tasks")
        .join(task.to_string())
        .join("policy.json");
    let policy: FrozenIntegrationPolicy =
        serde_json::from_slice(&std::fs::read(owner_policy).unwrap()).unwrap();
    assert_eq!(policy.requested_close, ClosePolicy::Done);
    let journal = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    let prepare = journal.find("host task-prepare").unwrap();
    let arm = journal.find("host task-integration\n").unwrap();
    let launch = journal.find("host task-turn").unwrap();
    assert!(prepare < arm && arm < launch, "{journal}");
    let store = mac_worker::test_support::host::store::HostStore::open(&f.host_root()).unwrap();
    let meta = mac_worker::test_support::task::store::TaskStore::new(
        &store,
        &mac_worker::test_support::host::process::SystemProcessRunner,
    )
    .load_meta(&policy.project_id, task)
    .unwrap();
    assert_eq!(meta.close_policy(), ClosePolicy::Never);
    let task_dir = store.task_dir(&policy.project_id, task).unwrap();
    let receipt: Value =
        serde_json::from_slice(&std::fs::read(task_dir.join("session-import.json")).unwrap())
            .unwrap();
    assert_eq!(receipt["stage"], "complete");
    let placed = std::fs::read_to_string(f.host.join("placed-files")).unwrap();
    let native = std::fs::read_to_string(placed.lines().next().unwrap()).unwrap();
    assert!(native.contains("T6 native tail"));
    assert_eq!(
        f.project
            .git(&[
                "--git-dir",
                origin.to_str().unwrap(),
                "for-each-ref",
                "--format=%(refname) %(objectname)"
            ])
            .stdout,
        origin_refs
    );
    assert_eq!(
        std::fs::read(source).unwrap(),
        before,
        "integration recaptured the source transcript"
    );
}

#[test]
fn direct_missing_helper_feature_refuses_before_pins_and_admission() {
    let f = integration_fixture(false);
    let output = f.worker(&[
        "--json",
        "task",
        "submit",
        "--prompt",
        "work",
        "--integrate",
        "main",
        "--worker",
        "fixture",
        "--no-wait",
    ]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("INTEGRATION_UNAVAILABLE"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let tasks = mac_worker::test_support::client_state::ClientStateStore::open(
        &f.laptop.join(".local/state/mac-worker"),
    )
    .unwrap();
    assert!(tasks.list_tasks().unwrap().is_empty());
    assert!(!f.laptop.join(".cache/mac-worker/transfer").exists());
    assert!(!f.host.join("argv").exists());
}

#[test]
fn direct_integrating_batch_publishes_policies_and_requirements_before_drained_launch() {
    use mac_worker::test_support::{
        client_state::ClientStateStore, controller::drain::set_drained, core::paths::PathLayout,
    };
    let f = integration_fixture(true);
    std::fs::write(
        f.project.root().join(".worker.toml"),
        "[task]\nintegrate = 'main'\n",
    )
    .unwrap();
    let batch = f.project.root().join("tasks.toml");
    std::fs::write(&batch, "[[tasks]]\nid = 'enabled'\nprompt = 'work'\n[[tasks]]\nid = 'ordinary'\nprompt = 'work'\nintegrate = false\n").unwrap();
    let paths = PathLayout {
        config: f.config.clone(),
        state: f.laptop.join(".local/state/mac-worker"),
        cache: f.laptop.join(".cache/mac-worker"),
        data: f.laptop.join(".local/share/mac-worker"),
    };
    set_drained(&paths.controller_state_root(), true).unwrap();
    let output = f.worker(&["--json", "task", "batch", batch.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let rows = tasks.list_tasks().unwrap();
    assert_eq!(rows.len(), 2);
    let mut configured = 0;
    for row in &rows {
        let policy = paths.state.join(format!(
            "integrations/tasks/{}/policy.json",
            row.meta().task_id()
        ));
        if policy.exists() {
            let policy: FrozenIntegrationPolicy =
                serde_json::from_slice(&std::fs::read(policy).unwrap()).unwrap();
            assert_eq!(policy.requested_close, ClosePolicy::Done);
            assert_eq!(row.meta().close_policy(), ClosePolicy::Never);
            let entry = tasks
                .queue_entry_for_task_turn(row.meta().task_id())
                .unwrap()
                .unwrap();
            assert!(
                entry
                    .requirements()
                    .contains(&"feature:task.integration".to_owned())
            );
            assert!(entry.requirements().contains(&"origin:file".to_owned()));
            configured += 1;
        } else {
            assert_eq!(row.meta().close_policy(), ClosePolicy::Done);
        }
    }
    assert_eq!(configured, 1);
    assert!(!f.host.join("argv").exists());
}

#[test]
fn review_native_dashboard_replay_returns_the_durable_request_result() {
    use mac_worker::test_support::client_state::ClientStateStore;
    use std::sync::Arc;
    use std::{
        io::BufRead,
        process::{Command, Stdio},
        sync::mpsc,
        thread,
    };
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(&paths, Arc::new(ManualIntegrationRuntime::default()))
        .unwrap();
    let mut record = state.load(task).unwrap().unwrap();
    let previous = record.snapshot.revision;
    record.snapshot.revision = previous.next().unwrap();
    record.snapshot.state = IntegrationStatus::Blocked;
    record.snapshot.pause_reason = None;
    record.snapshot.resume_state = None;
    record.pause = None;
    record.snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
    state.replace(task, previous, &record).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    command
        .env_clear()
        .env("HOME", &f.laptop)
        .env("PATH", "/usr/bin:/bin")
        .env("MAC_WORKER_TEST_SSH", &f.ssh)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .current_dir(f.project.root())
        .args([
            "--config",
            f.config.to_str().unwrap(),
            "dashboard",
            "--no-open",
            "--no-facts-refresh",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut process = RunningDashboard(command.spawn().unwrap());
    let stdout = process.0.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    thread::spawn(move || {
        let mut url = String::new();
        std::io::BufReader::new(stdout).read_line(&mut url).unwrap();
        let _ = send.send(url);
    });
    let url = receive
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .unwrap();
    let address = url
        .trim()
        .trim_start_matches("http://")
        .trim_end_matches('/');
    let ordinary = ClientStateStore::open(&paths.state)
        .unwrap()
        .load_task(task)
        .unwrap();
    let request = json!({
        "expected": {"expected_task_id":task,"expected_turn_id":ordinary.status().turns().last().unwrap().turn_id(),
            "expected_turn_count":ordinary.status().turns().len(),"expected_head_oid":ordinary.status().head_oid(),
            "expected_updated_at_millis":ordinary.status().updated_at_millis(),"expected_state":"open"},
        "expected_integration_id":record.snapshot.integration_id,
        "integration":{"task_id":task,"expected":record.snapshot.revision,"request_id":uuid::Uuid::new_v4().simple().to_string()}
    });
    let route = format!("/api/v1/tasks/{task}/integrate");
    let first = dashboard_request(address, "POST", &route, Some(request.clone()));
    let after = state.load(task).unwrap().unwrap();
    assert_eq!(after.snapshot.epoch, record.snapshot.epoch + 1);
    let second = dashboard_request(address, "POST", &route, Some(request.clone()));
    assert_eq!(
        second["integration"]["integration_id"],
        first["integration"]["integration_id"]
    );
    assert_eq!(
        state.load(task).unwrap().unwrap().snapshot.epoch,
        after.snapshot.epoch
    );
    assert_eq!(
        second, first,
        "exact replay returns the saved complete response"
    );
    assert_eq!(state.load(task).unwrap().unwrap(), after);
    let mut changed = request.clone();
    changed["expected"]["expected_updated_at_millis"] =
        json!(ordinary.status().updated_at_millis() + 1);
    let (status, error) = dashboard_request_result(address, "POST", &route, Some(changed));
    assert!(status.contains("502"), "{status}: {error}");
    assert_eq!(error["error"]["code"], "INTEGRATION_STATE_INVALID");
    assert_eq!(state.load(task).unwrap().unwrap(), after);
    let mut fresh = request.clone();
    fresh["integration"]["request_id"] = json!(uuid::Uuid::new_v4().simple().to_string());
    let (status, error) = dashboard_request_result(address, "POST", &route, Some(fresh));
    assert!(status.contains("409"), "{status}: {error}");
    assert_eq!(error["error"]["code"], "TASK_REVISION_CONFLICT");
    // Reopen a normal dashboard after the owner becomes Closed. The old request
    // remains replayable; first requests keep their Open/revision fences.
    drop(process);
    let client = ClientStateStore::open(&paths.state).unwrap();
    let before = client.load_task(task).unwrap();
    let mut wire = serde_json::to_value(before.status()).unwrap();
    wire["state"] = json!("closed");
    let closed = before
        .clone()
        .with_status(serde_json::from_value(wire).unwrap())
        .unwrap();
    assert!(client.update_task_if_current(&before, closed).unwrap());
    let mut process = RunningDashboard(command.spawn().unwrap());
    let stdout = process.0.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    thread::spawn(move || {
        let mut url = String::new();
        std::io::BufReader::new(stdout).read_line(&mut url).unwrap();
        let _ = send.send(url);
    });
    let url = receive
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .unwrap();
    let address = url
        .trim()
        .trim_start_matches("http://")
        .trim_end_matches('/');
    assert_eq!(
        dashboard_request(address, "POST", &route, Some(request)),
        first
    );
    assert_eq!(state.load(task).unwrap().unwrap(), after);
}
