use mac_worker::test_support::core::redaction::RedactionBoundary;
use mac_worker::test_support::integration::*;
use mac_worker::test_support::integration::{
    IntegrationSnapshot, MAX_PUBLIC_SNAPSHOT_BYTES, decode_snapshot, encode_snapshot,
    public_target_display,
};
use mac_worker::test_support::task::model::{ClosePolicy, RunId, TaskId, TurnId};
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn submit() -> FrozenSubmitBody {
    serde_json::from_value(json!({
        "task_id":fixture_task(),"turn_id":fixture_source(),"created_at_millis":1000,
        "prompt":"work","agent":"codex","source":"local","origin_url":"https://example.test/repo.git",
        "publish":["fetch"],"close_on":"done","wip":false,"project_id":"a".repeat(64),
        "worktree_id":"b".repeat(64),"base_oid":"b".repeat(40),"timeout_millis":2700000,
        "max_followups":10,"permissions":"workspace","requires":[],"include_untracked":[],
        "include_empty_dirs":[],"allow_sensitive":[],"cli_includes":[],"wait_for_capacity":true
    })).unwrap()
}

fn batch() -> FrozenBatchBody {
    let mut spec = serde_json::to_value(submit()).unwrap();
    for field in [
        "task_id",
        "turn_id",
        "created_at_millis",
        "base_oid",
        "wait_for_capacity",
    ] {
        spec.as_object_mut().unwrap().remove(field);
    }
    spec["project_path"] = json!("/fixture/repo");
    let spec: DagFrozenSpec = serde_json::from_value(spec).unwrap();
    let run_id = RunId::new(uuid::Uuid::from_u128(4));
    let parent = DagNode {
        batch_id: "parent".into(),
        task_id: fixture_task(),
        turn_id: fixture_source(),
        depends_on: vec![],
        base: DagBase::Frozen {
            oid: "b".repeat(40).parse().unwrap(),
            pin_ref: format!("refs/mac-worker/dag/{run_id}/parent"),
            wip: false,
        },
        frozen: spec.clone(),
        state: DagNodeState::Waiting,
        bound_oid: None,
        bound_turn_id: None,
        pin_ref: None,
        blocked_by: None,
        claimed_by: None,
        claimed_at_millis: None,
    };
    let child = DagNode {
        batch_id: "child".into(),
        task_id: TaskId::new(uuid::Uuid::from_u128(5)),
        turn_id: TurnId::new(uuid::Uuid::from_u128(6)),
        depends_on: vec!["parent".into()],
        base: DagBase::From {
            parent: "parent".into(),
        },
        frozen: spec,
        ..parent.clone()
    };
    FrozenBatchBody {
        kind: BatchKind::Dag,
        run_id,
        max_parallel: None,
        name: None,
        created_at_millis: 1000,
        nodes: BTreeMap::from([("parent".into(), parent), ("child".into(), child)]),
        sources: vec![FrozenBatchSource {
            request_id: "00000000000000000000000000000007".into(),
            project_id: "a".repeat(64),
            worktree_id: "b".repeat(64),
            expected_oid: "b".repeat(40).parse().unwrap(),
        }],
    }
}
fn policies() -> BTreeMap<TaskId, Option<FrozenIntegrationPolicy>> {
    let mut parent = sample_policy("main");
    parent.requested_close = ClosePolicy::Done;
    let mut child = parent.clone();
    child.base_kind = IntegrationBaseKind::FromTask;
    child.base_task = Some(fixture_task());
    child.base_oid = None;
    child.base_preflight = IntegrationBasePreflight::Unknown;
    BTreeMap::from([
        (fixture_task(), Some(parent)),
        (TaskId::new(uuid::Uuid::from_u128(5)), Some(child)),
    ])
}

#[test]
fn noncanonical_origins_are_rejected_before_real_owner_and_host_policy_persistence() {
    use mac_worker::test_support::{core::paths::PathLayout, host::process::SystemProcessRunner};
    use sha2::{Digest, Sha256};
    use std::os::unix::fs::DirBuilderExt;
    use std::sync::Arc;
    for origin in [
        "https://review-user@example.invalid/repo.git",
        "https://review-user:invented-password@example.invalid/repo.git",
        "https://example.invalid/repo.git?token=invented-token",
        "https://example.invalid/repo.git#invented-fragment",
    ] {
        let mut f = GitIntegrationFixture::new();
        f.commit_base();
        f.commit_task();
        let paths = PathLayout {
            state: f.workspace().join("owner-state"),
            data: f.workspace().join("owner-data"),
            cache: f.workspace().join("owner-cache"),
            config: f.workspace().join("owner-config"),
        };
        let state =
            RootedIntegrationState::open(&paths, Arc::new(ManualIntegrationRuntime::default()))
                .unwrap();
        let mut policy = f.record.policy.clone();
        policy.origin = origin.into();
        // Give the host a matching task for this invented canonical remote. Arm
        // does no Git/network I/O; an unrelated project mismatch must not mask
        // the policy persistence defect.
        let old_task = f
            .store
            .task_dir(&policy.project_id, f.record.task_id)
            .unwrap();
        policy.project_id = format!(
            "{:x}",
            Sha256::digest(b"origin\0https://example.invalid/repo.git")
        );
        let task_root = f
            .store
            .task_dir(&policy.project_id, f.record.task_id)
            .unwrap();
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&task_root)
            .unwrap();
        for name in ["meta.json", "status.json"] {
            std::fs::copy(old_task.join(name), task_root.join(name)).unwrap();
        }
        let meta_path = task_root.join("meta.json");
        let mut meta: Value = serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
        meta["project_id"] = json!(policy.project_id);
        let meta: mac_worker::test_support::task::model::TaskMeta =
            serde_json::from_value(meta).unwrap();
        std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
        let request = HostIntegrationRequest {
            protocol_version: 7,
            task_id: f.record.task_id,
            integration_id: None,
            epoch: 0,
            revision: IntegrationRevision(0),
            action: HostIntegrationAction::Arm {
                policy: policy.clone(),
            },
        };
        let host = HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime);
        assert_eq!(
            host.execute(&request).unwrap_err().public_code(),
            "INTEGRATION_STATE_INVALID"
        );
        assert!(!task_root.join("integration/policy.json").exists());
        assert_eq!(
            state
                .publish_policy(f.record.task_id, &policy)
                .unwrap_err()
                .public_code(),
            "INTEGRATION_STATE_INVALID"
        );
        assert_eq!(state.load_policy(f.record.task_id).unwrap(), None);
        assert!(
            serde_json::from_value::<FrozenIntegrationPolicy>(
                serde_json::to_value(&policy).unwrap()
            )
            .is_err()
        );
    }
}

#[test]
fn canonical_file_and_remote_policies_remain_valid_on_the_strict_wire() {
    let f = GitIntegrationFixture::new();
    for policy in [f.record.policy.clone(), sample_policy("main")] {
        policy.validate().unwrap();
        assert_eq!(
            serde_json::from_value::<FrozenIntegrationPolicy>(
                serde_json::to_value(&policy).unwrap()
            )
            .unwrap(),
            policy
        );
    }
}

#[test]
fn integrating_submit_freezes_effective_never_and_refuses_unsafe_bindings() {
    let mut policy = sample_policy("main");
    policy.requested_close = ClosePolicy::Done;
    let wrapped = prepare_integrating_submit(submit(), policy.clone()).unwrap();
    assert_eq!(wrapped.submit.close_on, ClosePolicy::Never);
    assert_eq!(wrapped.integration.requested_close, ClosePolicy::Done);
    assert!(
        wrapped
            .submit
            .requires
            .contains(&"feature:task.integration".into())
    );
    assert!(
        wrapped
            .submit
            .requires
            .contains(&"origin:example.test".into())
    );
    for (field, value, code) in [
        ("wip", json!(true), "INTEGRATION_WIP_BASE"),
        (
            "origin_url",
            json!("https://elsewhere.test/repo.git"),
            "INTEGRATION_STATE_INVALID",
        ),
        (
            "base_oid",
            json!("c".repeat(40)),
            "INTEGRATION_STATE_INVALID",
        ),
        (
            "project_id",
            json!("c".repeat(64)),
            "INTEGRATION_STATE_INVALID",
        ),
        (
            "publish_branch",
            json!("main"),
            "INTEGRATION_PUBLISH_TARGET_COLLISION",
        ),
    ] {
        let mut wire = serde_json::to_value(submit()).unwrap();
        wire[field] = value;
        let error =
            prepare_integrating_submit(serde_json::from_value(wire).unwrap(), policy.clone())
                .unwrap_err();
        assert_eq!(error.public_code(), code, "{field}");
    }
}

#[test]
fn integrating_batch_requires_a_complete_task_keyed_matching_policy_map() {
    let wrapped = prepare_integrating_batch(batch(), policies()).unwrap();
    assert!(
        wrapped
            .batch
            .nodes
            .values()
            .all(|node| node.frozen.close_on == ClosePolicy::Never)
    );
    for mutation in [0, 1, 2, 3, 4] {
        let mut map = policies();
        match mutation {
            0 => {
                map.remove(&fixture_task());
            }
            1 => {
                map.insert(TaskId::new(uuid::Uuid::from_u128(9)), None);
            }
            2 => {
                map.get_mut(&fixture_task())
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .project_id = "c".repeat(64);
            }
            3 => {
                map.get_mut(&fixture_task())
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .base_oid = Some(fixture_head());
            }
            _ => {
                map.get_mut(&TaskId::new(uuid::Uuid::from_u128(5)))
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .base_task = Some(TaskId::new(uuid::Uuid::from_u128(9)));
            }
        }
        assert!(
            prepare_integrating_batch(batch(), map).is_err(),
            "mutation {mutation}"
        );
    }
    let mut disabled = policies();
    disabled.insert(TaskId::new(uuid::Uuid::from_u128(5)), None);
    let wrapped = prepare_integrating_batch(batch(), disabled).unwrap();
    assert_eq!(
        wrapped.batch.nodes["child"].frozen.close_on,
        ClosePolicy::Done
    );
}

#[test]
fn integrating_from_child_refuses_disabled_or_different_target_parent_early() {
    for target in [None, Some("other")] {
        let mut map = policies();
        match target {
            None => {
                map.insert(fixture_task(), None);
            }
            Some(branch) => {
                map.get_mut(&fixture_task())
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .target = validate_integration_target(branch).unwrap();
            }
        }
        assert_eq!(
            prepare_integrating_batch(batch(), map)
                .unwrap_err()
                .public_code(),
            "TASK_CONFIG_INVALID"
        );
    }
}

#[test]
fn integration_state_read_is_bounded_and_has_no_effects() {
    let state = MemoryIntegrationState::default();
    let record = sample_record(fixture_task(), fixture_source(), "main");
    state
        .publish_policy(fixture_task(), &record.policy)
        .unwrap();
    assert!(
        state
            .replace(fixture_task(), IntegrationRevision(0), &record)
            .unwrap()
    );
    let absent = TaskId::new(uuid::Uuid::from_u128(9));
    let result = serve_integration_read(&state, &[fixture_task(), absent]).unwrap();
    assert_eq!(
        result.integrations[&fixture_task()],
        Some(record.snapshot.clone())
    );
    assert_eq!(result.integrations[&absent], None);
    assert_eq!(state.load(fixture_task()).unwrap(), Some(record));
    assert!(serve_integration_read(&state, &[fixture_task(), fixture_task()]).is_err());
    let ids = (1..=17)
        .map(|id| TaskId::new(uuid::Uuid::from_u128(id)))
        .collect::<Vec<_>>();
    assert!(serve_integration_read(&state, &ids).is_err());
}

#[test]
fn imported_session_wrapper_preserves_distinct_source_and_package_identity() {
    use mac_worker::test_support::session::{SessionAgent, SessionImportMeta};
    let mut body = submit();
    body.session_import =
        Some(SessionImportMeta::new(SessionAgent::Codex, "d".repeat(40), "0.160.0").unwrap());
    let source = body.base_oid.clone();
    let import = body.session_import.clone();
    let mut policy = sample_policy("main");
    policy.requested_close = ClosePolicy::Done;
    let wrapped = prepare_integrating_submit(body, policy).unwrap();
    assert_eq!(wrapped.submit.base_oid, source);
    assert_eq!(wrapped.submit.session_import, import);
    let decoded: FrozenIntegratingSubmit =
        serde_json::from_value(serde_json::to_value(&wrapped).unwrap()).unwrap();
    assert_eq!(decoded, wrapped);
}

fn request(command: &str, body: Value) -> mac_worker::test_support::controller::ControllerRequest {
    mac_worker::test_support::controller::parse_request(&serde_json::to_vec(&json!({
        "protocol_version":7,"request_id":"00000000000000000000000000000011","command":command,"body":body,
    })).unwrap()).unwrap()
}

#[test]
fn gated_wrappers_hash_the_entire_body_and_publish_policy_before_effects() {
    let wrapped = prepare_integrating_batch(batch(), policies()).unwrap();
    let req = request(
        "task.batch-integrating",
        serde_json::to_value(&wrapped).unwrap(),
    );
    assert_eq!(
        parse_integrating_batch(&req, &[])
            .unwrap_err()
            .public_code(),
        "INTEGRATION_UNAVAILABLE"
    );
    let features = [CONTROLLER_FEATURE_INTEGRATION.to_owned()];
    assert_eq!(parse_integrating_batch(&req, &features).unwrap(), wrapped);
    let mut changed = serde_json::to_value(&wrapped).unwrap();
    changed["integrations"][fixture_task().to_string()]["verify"] = json!("moved-target");
    assert_ne!(
        req.payload_sha256(),
        request("task.batch-integrating", changed).payload_sha256()
    );
    let state = MemoryIntegrationState::default();
    publish_integrating_batch(&state, &wrapped).unwrap();
    publish_integrating_batch(&state, &wrapped).unwrap();
    for (task, policy) in &wrapped.integrations {
        assert_eq!(state.load_policy(*task).unwrap(), *policy);
        assert!(state.load(*task).unwrap().is_none());
    }
    // Exact replay cannot replace policy while an ordinary task/admission effect
    // is still pending. No fallback into ordinary submission is permitted.
    let mut conflict = wrapped.clone();
    conflict
        .integrations
        .get_mut(&fixture_task())
        .unwrap()
        .as_mut()
        .unwrap()
        .verify = VerifyPolicy::MovedTarget;
    assert!(publish_integrating_batch(&state, &conflict).is_err());
}

#[test]
fn owner_decode_requires_integration_admission_requirements() {
    let mut policy = sample_policy("main");
    policy.requested_close = ClosePolicy::Done;
    let wrapped = prepare_integrating_submit(submit(), policy).unwrap();
    let mut wire = serde_json::to_value(&wrapped).unwrap();
    wire["submit"]["requires"] = json!([]);
    assert!(
        parse_integrating_submit(
            &request("task.submit-integrating", wire),
            &[CONTROLLER_FEATURE_INTEGRATION.to_owned()]
        )
        .is_err()
    );
    let wrapped = prepare_integrating_batch(batch(), policies()).unwrap();
    let mut wire = serde_json::to_value(&wrapped).unwrap();
    wire["batch"]["nodes"]["parent"]["frozen"]["requires"] = json!([]);
    assert!(
        parse_integrating_batch(
            &request("task.batch-integrating", wire),
            &[CONTROLLER_FEATURE_INTEGRATION.to_owned()]
        )
        .is_err()
    );
}

#[test]
fn exclusive_selector_uses_existing_read_framing_and_validates_reply_identity() {
    use mac_worker::test_support::controller::{
        ControllerReadIdentity, ControllerReadReply, decode_frame,
    };
    let state = MemoryIntegrationState::default();
    let req = request(
        "task.list",
        json!({"integration":{"task_ids":[fixture_task()]}}),
    );
    assert!(is_integration_selector(&req));
    assert_eq!(
        serve_integration_selector(&req, &[], &state)
            .unwrap_err()
            .public_code(),
        "INTEGRATION_UNAVAILABLE"
    );
    let bytes =
        serve_integration_selector(&req, &[CONTROLLER_FEATURE_INTEGRATION.to_owned()], &state)
            .unwrap();
    let reply: ControllerReadReply<IntegrationReadResult> =
        serde_json::from_slice(decode_frame(&bytes).unwrap()).unwrap();
    let _: baseline::ReadEnvelope = serde_json::from_slice(decode_frame(&bytes).unwrap()).unwrap();
    reply.verify_envelope(&req).unwrap();
    reply.result().verify_payload(&req).unwrap();
    let wrong = request(
        "task.list",
        json!({"integration":{"task_ids":[TaskId::new(uuid::Uuid::from_u128(9))]}}),
    );
    assert!(reply.result().verify_payload(&wrong).is_err());
    for body in [
        json!({"integration":{"task_ids":[fixture_task()]}, "state":"open"}),
        json!({"integration":{"task_ids":[fixture_task()]}, "controller_events":{}}),
        json!({"integration":{"task_ids":[fixture_task()], "extra":true}}),
        json!({"integration":{"task_ids":[fixture_task(),fixture_task()]}}),
        json!({"integration":{"task_ids":["00000000000000000000000000000000"]}}),
        json!({"integration":null}),
    ] {
        assert!(integration_selector_ids(&request("task.list", body)).is_err());
    }
}

#[test]
fn old_execution_rejects_safe_selector_with_zero_durable_mutation_rows() {
    use mac_worker::test_support::{
        controller::{ControllerFault, ControllerStore, encode_json_frame, serve_rpc_with_runtime},
        core::error::WorkerError,
        core::{config::Config, paths::PathLayout},
        host::process::{ProcessRequest, ProcessResult, ProcessRunner},
    };
    struct NoProcesses;
    impl ProcessRunner for NoProcesses {
        fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            panic!("read rejection must not execute a process");
        }
    }
    let root = tempfile::tempdir().unwrap();
    let base = std::fs::canonicalize(root.path()).unwrap();
    let paths = PathLayout {
        config: base.join("config.toml"),
        state: base.join("state/mac-worker"),
        cache: base.join("cache"),
        data: base.join("data"),
    };
    let config: Config = toml::from_str("version = 1").unwrap();
    let frame = encode_json_frame(&json!({"protocol_version":7,"request_id":"00000000000000000000000000000011", "command":"task.list", "body":{"integration":{"task_ids":[fixture_task()]}}})).unwrap();
    let error = serve_rpc_with_runtime(
        &paths,
        &config,
        &NoProcesses,
        &mut std::io::Cursor::new(frame),
        &mut Vec::new(),
        ControllerFault::None,
    )
    .unwrap_err();
    assert_eq!(error.public_code(), "INVALID_REQUEST", "{error:?}");
    assert_eq!(
        ControllerStore::open(&paths.controller_state_root())
            .unwrap()
            .request_count()
            .unwrap(),
        0
    );
}

#[test]
fn nested_session_cleanup_can_locate_both_pins_even_when_policy_is_rejected() {
    use mac_worker::test_support::session::{SessionAgent, SessionImportMeta};
    let mut body = submit();
    body.session_import =
        Some(SessionImportMeta::new(SessionAgent::Codex, "d".repeat(40), "0.160.0").unwrap());
    let mut policy = sample_policy("main");
    policy.requested_close = ClosePolicy::Done;
    let wrapped = prepare_integrating_submit(body.clone(), policy).unwrap();
    let mut wire = serde_json::to_value(wrapped).unwrap();
    wire["integration"]["base_oid"] = json!("c".repeat(40));
    let req = request("task.submit-integrating", wire.clone());
    assert!(parse_integrating_submit(&req, &[CONTROLLER_FEATURE_INTEGRATION.to_owned()]).is_err());
    wire["integration"] = Value::Null;
    let malformed = request("task.submit-integrating", wire);
    assert!(
        parse_integrating_submit(&malformed, &[CONTROLLER_FEATURE_INTEGRATION.to_owned()]).is_err()
    );
    assert_eq!(
        nested_integration_submit(&malformed)
            .unwrap()
            .unwrap()
            .base_oid,
        body.base_oid
    );
    let nested = nested_integration_submit(&req).unwrap().unwrap();
    assert_eq!(nested.base_oid, body.base_oid);
    assert_eq!(nested.session_import, body.session_import);
    // Fake source-finished/pin lifecycle sees the very same frozen identities;
    // retries are envelope-only and do not invoke a capture callback.
    let package = nested
        .session_import
        .as_ref()
        .unwrap()
        .package_oid()
        .to_owned();
    let mut finished = BTreeMap::from([
        (nested.base_oid.to_string(), true),
        (package.clone(), false),
    ]);
    assert!(
        ![nested.base_oid.to_string(), package.clone()]
            .iter()
            .all(|oid| finished[oid])
    );
    finished.insert(package.clone(), true);
    assert!(
        [nested.base_oid.to_string(), package.clone()]
            .iter()
            .all(|oid| finished[oid])
    );
    let mut pins = BTreeMap::from([(nested.base_oid.to_string(), true), (package.clone(), true)]);
    assert!(pins.values().all(|pinned| *pinned));
    for oid in [nested.base_oid.to_string(), package] {
        pins.insert(oid, false);
    }
    assert!(pins.values().all(|pinned| !pinned));
    assert_eq!(
        nested_integration_submit(&req).unwrap().unwrap().base_oid,
        body.base_oid
    );
}

#[test]
fn redrive_preparation_preserves_expected_revision_and_durable_request_identity() {
    let req = IntegrationRedriveRequest {
        task_id: fixture_task(),
        expected: IntegrationRevision(7),
        request_id: "00000000000000000000000000000012".into(),
    };
    assert_eq!(prepare_integration_redrive(&req).unwrap(), req);
    let mut invalid = req.clone();
    invalid.expected = IntegrationRevision(0);
    assert!(prepare_integration_redrive(&invalid).is_err());
    invalid = req.clone();
    invalid.request_id = "not-an-id".into();
    assert!(prepare_integration_redrive(&invalid).is_err());
}

// Strict top-level key sets copied from 67183a0's DTO/Wire definitions.
// Values intentionally stay opaque: this checks N-1 shape independently of
// today's codecs; today's full typed decoder also validates each payload below.
mod baseline {
    use serde::Deserialize;
    use serde_json::Value;
    macro_rules! decoder {
        ($name:ident { $($required:ident),* } optional { $($optional:ident),* }) => {
            #[allow(dead_code)]
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            pub struct $name {
                $($required: Value,)*
                $(#[serde(default)] $optional: Value,)*
            }
        };
    }
    decoder!(Status { state, last_outcome, worker, session_present, head_oid, summary, questions, files_changed, diff_stat, turns, updated_at_millis } optional { reported_checks });
    decoder!(StatusResult { task_id, run_id, status, runner, exit_code } optional { warnings, events, delivery, deliveries, stage, residual });
    decoder!(Result { task_id, status, branch, fetch } optional { stage, residual, delivery, deliveries, warnings });
    decoder!(HostStatus { protocol_version, status } optional { delivery, deliveries });
    decoder!(Record { meta, status, status_observed_at_millis, runner, fetched_head, repo_id, pinned_worker, wait_for_capacity, abandon_code } optional { questions_policy, auto_continue_intent, submission_intent_turn_id, submission_rollback_turn_id, close_intent, delivery, deliveries, failure_stage, failure_residual });
    decoder!(Turn { turn_number, turn_id, terminal, outcome, agent_committed, log_truncated, started_at_millis, ended_at_millis } optional { auto_continue, herdr, result_parse_reason, agent_identity });
    decoder!(Dag { version, run_id, max_parallel, created_at_millis, nodes } optional { name });
    decoder!(Followup { expected, turn_id, turn_number, created_at_millis, message, composed_prompt, base_oid, agent, model, worker, max_followups } optional { auto_continue });
    decoder!(Envelope { request_id, payload_sha256, command, body, created_at_millis } optional { settled_at_millis, outcome });
    decoder!(ReadEnvelope { protocol_version, command, request_id, payload_sha256, result } optional {});
}

fn baseline_accepts_current_bytes<
    Old: serde::de::DeserializeOwned,
    Current: serde::Serialize + serde::de::DeserializeOwned,
>(
    value: &Current,
) {
    let bytes = serde_json::to_vec(value).unwrap();
    let _: Old = serde_json::from_slice(&bytes).unwrap();
    let decoded: Current = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
    let mut wire: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(wire.get("integration").is_none());
    wire["integration"] = json!({"state":"pending"});
    assert!(serde_json::from_value::<Old>(wire).is_err());
}

#[test]
fn copied_baseline_strict_decoders_prove_disabled_dtos_keep_their_bytes() {
    use mac_worker::test_support::{
        controller::{
            ControllerTaskResult, ControllerTaskStatusResult, persist_operation_envelope,
        },
        task::{
            model::{LocalTaskRecord, TaskStatus, TurnSummary},
            prepared_followup::PreparedFollowup,
            store::TaskStatusResponse,
        },
    };
    let record = sample_ordinary(fixture_task(), fixture_source());
    baseline_accepts_current_bytes::<baseline::Status, TaskStatus>(record.status());
    baseline_accepts_current_bytes::<baseline::Record, LocalTaskRecord>(&record);
    baseline_accepts_current_bytes::<baseline::Turn, TurnSummary>(&record.status().turns()[0]);
    baseline_accepts_current_bytes::<baseline::HostStatus, TaskStatusResponse>(
        &TaskStatusResponse::new(record.status().clone()),
    );
    let status: ControllerTaskStatusResult = serde_json::from_value(json!({"task_id":fixture_task(), "run_id":null,"status":record.status(),"warnings":[],"events":[],"runner":null,"exit_code":0})).unwrap();
    baseline_accepts_current_bytes::<baseline::StatusResult, ControllerTaskStatusResult>(&status);
    let result: ControllerTaskResult = serde_json::from_value(json!({"task_id":fixture_task(),"status":record.status(),"branch":"task/fixture","fetch":"worker task fetch fixture"})).unwrap();
    baseline_accepts_current_bytes::<baseline::Result, ControllerTaskResult>(&result);
    let followup = PreparedFollowup::prepare(
        &record,
        "follow up".into(),
        TurnId::new(uuid::Uuid::from_u128(8)),
        2000,
    )
    .unwrap();
    baseline_accepts_current_bytes::<baseline::Followup, PreparedFollowup>(&followup);
    let frozen_batch = batch();
    let dag = json!({"version":1,"run_id":frozen_batch.run_id,"max_parallel":1,"created_at_millis":1000,"nodes":frozen_batch.nodes});
    let dag: mac_worker::test_support::client_state::dag::DagRecord =
        serde_json::from_value(dag).unwrap();
    baseline_accepts_current_bytes::<
        baseline::Dag,
        mac_worker::test_support::client_state::dag::DagRecord,
    >(&dag);
    let body = submit();
    let before = serde_json::to_vec(&body).unwrap();
    let disabled = resolve_integration_policy(
        &IntegrationProjectPolicy {
            settings: Default::default(),
            project_id: body.project_id.clone(),
            base_oid: Some(body.base_oid.clone()),
            base_task: None,
        },
        None,
        &IntegrationOverride::Disabled,
        None,
        body.close_on,
        "",
        IntegrationBaseKind::Committed,
    )
    .unwrap();
    assert!(disabled.is_none());
    assert_eq!(serde_json::to_vec(&body).unwrap(), before);
    let req = request("task.submit", serde_json::to_value(&body).unwrap());
    let root = tempfile::tempdir().unwrap();
    let envelope = persist_operation_envelope(
        &std::fs::canonicalize(root.path())
            .unwrap()
            .join("envelopes"),
        &req,
    )
    .unwrap();
    baseline_accepts_current_bytes::<
        baseline::Envelope,
        mac_worker::test_support::controller::OperationEnvelope,
    >(&envelope);
}

fn pending() -> Value {
    json!({
        "schema_version":1,
        "integration_id":"00000000-0000-8000-8000-000000000001",
        "epoch":0,"revision":1,"target":"main","state":"pending",
        "resume_state":null,"pause_reason":null,
        "source_turn_id":"00000000000000000000000000000003",
        "source_head":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "merge_oid":null,"observed_target_oid":null,"disposition":null,
        "attempts":0,"resolve_turns":0,"verify_turns":0,
        "blocked_code":null,"retry_exhausted":false,"retry_at_millis":null,
        "verification":"source_agent_report_only","updated_at_millis":0
    })
}

#[test]
fn public_snapshot_roundtrips_with_a_strict_bounded_codec() {
    let bytes = serde_json::to_vec(&pending()).unwrap();
    let snapshot = decode_snapshot(&bytes).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&encode_snapshot(&snapshot).unwrap()).unwrap(),
        pending()
    );
    assert!(decode_snapshot(&vec![b' '; MAX_PUBLIC_SNAPSHOT_BYTES + 1]).is_err());
    let mut unknown = pending();
    unknown["secret_origin"] = json!("private");
    assert!(serde_json::from_value::<IntegrationSnapshot>(unknown).is_err());
}

#[test]
fn snapshot_rejects_invalid_identity_counters_and_park_binding() {
    for (field, value) in [
        ("source_head", json!("oops")),
        ("attempts", json!(4)),
        ("resolve_turns", json!(3)),
        ("verify_turns", json!(4)),
        ("target", json!("é".repeat(65))),
        ("schema_version", json!(2)),
        (
            "integration_id",
            json!("00000000-0000-8000-8000-00000000000A"),
        ),
        ("state", json!("parked")),
        ("pause_reason", json!("controller_drained")),
    ] {
        let mut wire = pending();
        wire[field] = value;
        assert!(
            serde_json::from_value::<IntegrationSnapshot>(wire).is_err(),
            "{field}"
        );
    }
}

#[test]
fn public_target_is_redacted_and_utf8_bounded_with_ellipsis() {
    let boundary = RedactionBoundary::new("/fixture/home");
    let display = public_target_display(&"é".repeat(100), &boundary);
    assert!(display.len() <= 128);
    assert!(display.ends_with('…'));
    assert!(!public_target_display("/fixture/home/token", &boundary).contains("/fixture/home"));
}

#[test]
fn typed_compact_annotation_is_retained_while_disabled_facts_keep_their_bytes() {
    use mac_worker::test_support::{
        events::{SafeOutcome, TaskFacts},
        task::model::{TaskId, TurnId},
    };
    let facts = TaskFacts::test_terminal(
        TaskId::new(uuid::Uuid::from_u128(2)),
        TurnId::new(uuid::Uuid::from_u128(3)),
        SafeOutcome::Done,
        true,
    );
    let disabled = serde_json::to_value(&facts).unwrap();
    assert!(disabled.get("integration").is_none());
    let mut enabled = disabled.clone();
    let annotation = json!({
        "integration_id":"00000000-0000-8000-8000-000000000001",
        "epoch":0,"revision":1,"state":"pending","code":null,"result_oid":null,
    });
    enabled["integration"] = annotation.clone();
    let decoded: TaskFacts = serde_json::from_value(enabled).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap()["integration"],
        annotation
    );
    let decoded: TaskFacts = serde_json::from_value(disabled.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), disabled);
}

#[test]
fn facts_decoder_refuses_an_invalid_or_oversized_annotation() {
    use mac_worker::test_support::{
        events::{SafeOutcome, TaskFacts},
        task::model::{TaskId, TurnId},
    };
    let facts = TaskFacts::test_terminal(
        TaskId::new(uuid::Uuid::from_u128(2)),
        TurnId::new(uuid::Uuid::from_u128(3)),
        SafeOutcome::Done,
        true,
    );
    let mut wire = serde_json::to_value(facts).unwrap();
    wire["integration"] = json!({
        "integration_id":"00000000-0000-8000-8000-000000000001",
        "epoch":0,"revision":1,"state":"pending","code":null,"result_oid":null,
        "snapshot":"s".repeat(513),
    });
    assert!(serde_json::from_value::<TaskFacts>(wire).is_err());
}

#[test]
fn rust_loads_the_same_public_views_codes_and_annotation_boundaries_as_typescript() {
    use mac_worker::test_support::integration::{
        IntegrationCode, IntegrationFactsAnnotation, IntegrationView, MAX_FACTS_ANNOTATION_BYTES,
        decode_bounded, validate_integration_target,
    };
    let fixtures: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/ui/src/lib/integration.fixtures.json"
    )))
    .unwrap();
    let rows = fixtures["cases"].as_array().unwrap();
    assert_eq!(rows.len(), 37);
    for row in rows {
        let view: IntegrationView = serde_json::from_value(row["view"].clone())
            .unwrap_or_else(|error| panic!("{}: {error}", row["name"]));
        assert_eq!(serde_json::to_value(view).unwrap(), row["view"]);
    }
    let codes: Vec<_> = IntegrationCode::ALL
        .iter()
        .map(|code| code.as_str())
        .collect();
    assert_eq!(serde_json::to_value(codes).unwrap(), fixtures["codes"]);
    let at = fixtures["annotation_boundary_json"]
        .as_str()
        .unwrap()
        .as_bytes();
    let over = fixtures["annotation_overflow_json"]
        .as_str()
        .unwrap()
        .as_bytes();
    assert_eq!(at.len(), 512);
    assert_eq!(over.len(), 513);
    let annotation: IntegrationFactsAnnotation =
        decode_bounded(at, MAX_FACTS_ANNOTATION_BYTES).unwrap();
    assert_eq!(
        serde_json::to_value(annotation).unwrap(),
        fixtures["annotation"]
    );
    assert!(
        decode_bounded::<IntegrationFactsAnnotation>(over, MAX_FACTS_ANNOTATION_BYTES).is_err()
    );
    for key in ["authoritative_255", "multibyte_255"] {
        assert!(validate_integration_target(fixtures["targets"][key].as_str().unwrap()).is_ok());
    }
    for key in ["authoritative_256", "multibyte_256"] {
        assert!(validate_integration_target(fixtures["targets"][key].as_str().unwrap()).is_err());
    }
    assert_eq!(
        public_target_display(
            fixtures["targets"]["display_source"].as_str().unwrap(),
            &RedactionBoundary::new("/fixture/home")
        ),
        fixtures["targets"]["display_expected"].as_str().unwrap()
    );
}
