use mac_worker::test_support::{
    events::{SafeOutcome, TaskFacts},
    integration::*,
    task::model::{RunId, TaskId, TurnId},
};

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
