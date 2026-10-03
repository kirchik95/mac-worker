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
