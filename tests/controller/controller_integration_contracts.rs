use mac_worker::test_support::core::redaction::RedactionBoundary;
use mac_worker::test_support::integration::{
    IntegrationSnapshot, MAX_PUBLIC_SNAPSHOT_BYTES, decode_snapshot, encode_snapshot,
    public_target_display,
};
use serde_json::{Value, json};

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
