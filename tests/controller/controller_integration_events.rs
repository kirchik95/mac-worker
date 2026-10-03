use mac_worker::test_support::{
    events::{SafeOutcome, TaskFacts},
    integration::*,
    task::model::{RunId, TaskId, TurnId},
};

#[test]
fn integration_hints_are_bounded_title_free_and_unknown_kinds_remain_readable() {
    use mac_worker::test_support::events::{SCHEMA_VERSION, Seq, WireEvent};
    let snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    let mut event = WireEvent {
        schema_version: SCHEMA_VERSION,
        journal_id: uuid::Uuid::from_u128(1),
        seq: Seq::new(1),
        time_millis: 1000,
        kind: "task.integrating".into(),
        data: serde_json::json!({"task_id":fixture_task(),"integration":snapshot.annotation().unwrap()}),
    };
    assert_eq!(event.affected_task(), Some(fixture_task()));
    assert!(serde_json::to_vec(&event.data).unwrap().len() <= MAX_JOURNAL_HINT_BYTES);
    let debug = event.debug_value().unwrap();
    assert!(debug["data"].get("target").is_none());
    assert!(debug["data"].get("title").is_none());
    event.kind = "task.future_integration_kind".into();
    event.validate().unwrap();
    assert!(event.debug_value().unwrap().get("data").is_none());
}

#[test]
fn enabled_facts_bind_revision_keep_compact_identity_and_conservative_proofs() {
    use mac_worker::test_support::events::rpc::task_reads::record_facts;
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    let target = format!("r{}", "é".repeat(127));
    assert_eq!(target.len(), 255);
    let mut snapshot = sample_record(fixture_task(), fixture_source(), &target).snapshot;
    assert!(snapshot.target.len() <= MAX_TARGET_DISPLAY_BYTES);
    let pending = record_facts(&ordinary, Some(false), true)
        .unwrap()
        .with_integration(&ordinary, &snapshot)
        .unwrap();
    assert_eq!(pending.busy, Some(true));
    assert_eq!(pending.quiescent, Some(false));
    assert_eq!(pending.outcome, Some(SafeOutcome::Done));
    snapshot.revision = snapshot.revision.next().unwrap();
    let newer = record_facts(&ordinary, Some(false), true)
        .unwrap()
        .with_integration(&ordinary, &snapshot)
        .unwrap();
    assert_ne!(pending.fact_digest, newer.fact_digest);
    assert_ne!(
        pending.eligibility_signature(),
        newer.eligibility_signature()
    );
    let annotation = serde_json::to_vec(newer.integration.as_ref().unwrap()).unwrap();
    assert!(annotation.len() <= MAX_FACTS_ANNOTATION_BYTES);
    let keys = serde_json::to_value(newer.integration.as_ref().unwrap()).unwrap();
    assert_eq!(keys.as_object().unwrap().len(), 6);
    assert!(keys.get("target").is_none());
    snapshot.state = IntegrationStatus::Blocked;
    snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
    let blocked = record_facts(&ordinary, Some(false), true)
        .unwrap()
        .with_integration(&ordinary, &snapshot)
        .unwrap();
    assert_eq!(blocked.outcome, Some(SafeOutcome::Blocked));
    assert_eq!(blocked.code.unwrap().as_str(), "INTEGRATION_CHECKS_FAILED");
    assert_eq!(blocked.quiescent, Some(true));
    assert_eq!(ordinary.status().last_outcome().unwrap().kind(), "done");
}

#[test]
fn compound_title_budget_never_truncates_annotation_and_decoder_reserves_bound() {
    use mac_worker::test_support::events::rpc::task_reads::record_facts;
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    snapshot.epoch = u32::MAX;
    snapshot.revision = IntegrationRevision(u64::MAX);
    snapshot.state = IntegrationStatus::Blocked;
    snapshot.blocked_code = Some(IntegrationCode::IntegrationDependencyNotIntegrated);
    snapshot.observed_target_oid = Some("f".repeat(40).parse().unwrap());
    let mut facts = record_facts(&ordinary, Some(false), false).unwrap();
    facts.title = Some("\"\\é".repeat(100));
    let facts = facts.with_integration(&ordinary, &snapshot).unwrap();
    let bytes = serde_json::to_vec(&facts).unwrap();
    assert!(bytes.len() <= MAX_COMPOUND_FACTS_BYTES);
    ensure_produced_task_facts_bytes(&bytes).unwrap();
    assert_eq!(facts.integration, Some(snapshot.annotation().unwrap()));
    assert!(serde_json::from_slice::<TaskFacts>(&bytes).is_ok());
    let mut oversized = serde_json::to_value(&facts).unwrap();
    oversized["integration"]["target"] = serde_json::json!("x".repeat(513));
    assert!(serde_json::from_value::<TaskFacts>(oversized).is_err());
}

#[test]
fn shared_compound_facts_fixture_checks_decoder_boundary_before_tolerant_normalization() {
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/ui/src/lib/integration.fixtures.json"
    )))
    .unwrap();
    let at = serde_json::to_vec(&fixtures["facts_boundary"]).unwrap();
    let over = serde_json::to_vec(&fixtures["facts_overflow"]).unwrap();
    assert_eq!(at.len(), MAX_COMPOUND_FACTS_BYTES);
    assert_eq!(over.len(), MAX_COMPOUND_FACTS_BYTES + 1);
    ensure_produced_task_facts_bytes(&at).unwrap();
    assert!(ensure_produced_task_facts_bytes(&over).is_err());
    let decoded: TaskFacts = serde_json::from_slice(&at).unwrap();
    assert!(decoded.integration.is_some());
    assert!(serde_json::from_slice::<TaskFacts>(&over).is_err());
    let mut future = fixtures["facts_boundary"].clone();
    future["future_row_metadata"] = serde_json::json!({"anything":true});
    assert!(serde_json::from_value::<TaskFacts>(future).is_ok());
}

// Baseline tolerant facts shape copied from ce7f62f; new metadata is absent.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct BaselineTaskFactsWire {
    pub task_id: TaskId,
    pub run_id: Option<RunId>,
    pub state: String,
    pub latest_turn_id: Option<TurnId>,
    pub outcome: Option<String>,
    pub code: Option<String>,
    pub runner_present: bool,
    pub close_intent: bool,
    pub auto_continue_intent: bool,
    pub queue_dispatching: Option<bool>,
    pub result_imported: bool,
    pub busy: Option<bool>,
    pub quiescent: Option<bool>,
    pub fact_digest: String,
    pub title: Option<String>,
}

#[test]
fn disabled_facts_serialize_to_the_same_baseline_bytes() {
    let facts = TaskFacts::test_terminal(fixture_task(), fixture_source(), SafeOutcome::Done, true);
    let bytes = serde_json::to_vec(&facts).unwrap();
    let baseline: BaselineTaskFactsWire = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&baseline).unwrap(), bytes);
    assert!(
        serde_json::to_value(&facts)
            .unwrap()
            .get("integration")
            .is_none()
    );
    let decoded: TaskFacts = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
}

#[test]
fn armed_human_outcome_is_quiescent_and_actionable_before_final_done() {
    use mac_worker::test_support::events::rpc::task_reads::record_facts;
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    let mut json = serde_json::to_value(&ordinary).unwrap();
    json["status"]["last_outcome"] = serde_json::json!({"kind":"needs_input"});
    json["status"]["turns"][0]["outcome"] = serde_json::json!({"kind":"needs_input"});
    let ordinary = serde_json::from_value(json).unwrap();
    let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    snapshot.state = IntegrationStatus::Armed;
    let facts = record_facts(&ordinary, Some(false), false)
        .unwrap()
        .with_integration(&ordinary, &snapshot)
        .unwrap();
    assert_eq!(facts.busy, Some(false));
    assert_eq!(facts.quiescent, Some(true));
    assert!(facts.eligibility_signature().current_attention);
}
