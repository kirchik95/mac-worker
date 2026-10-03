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
