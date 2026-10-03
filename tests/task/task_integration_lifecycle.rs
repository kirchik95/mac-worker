use mac_worker::test_support::integration::*;

#[test]
fn fixture_keeps_imported_result_through_partial_close_replay() {
    let f = IntegrationFixture::new();
    let record = sample_record(f.task(), f.source(), "main");
    let receipt = IntegrationReceipt {
        integration_id: record.snapshot.integration_id,
        epoch: 0,
        source_turn_id: f.source(),
        source_head: fixture_head(),
        target_head: record.cycle_base,
        merge_oid: Some("e".repeat(40).parse().unwrap()),
        disposition: IntegrationDisposition::Merged,
        imported: false,
        recorded_at_millis: 1004,
    };
    let imported = f.turns().import_receipt(f.task(), &receipt).unwrap();
    f.restart();
    assert_eq!(f.imports(f.task()), vec![imported.clone()]);
    assert!(f.closes(f.task()).is_empty());
    f.turns().close_integrated(f.task(), &imported).unwrap();
    f.restart();
    f.turns().close_integrated(f.task(), &imported).unwrap();
    assert_eq!(f.closes(f.task()), vec![imported]);
    assert_eq!(f.accepted_head(f.task()), receipt.merge_oid);
    assert!(f.host_calls().is_empty());
}

// T3 owns this seed and replaces it when implementing the coordinator.
#[test]
fn unwired_coordinator_reports_typed_unavailable_without_claiming_host_completion() {
    let f = IntegrationFixture::new();
    f.enable(f.task(), "main").unwrap();
    assert_eq!(
        f.complete_source(f.task()).unwrap_err().public_code(),
        "INTEGRATION_UNAVAILABLE"
    );
    assert!(f.observer().facts(f.task()).unwrap().result_imported);
    assert_eq!(
        f.drive(f.task()).unwrap_err().public_code(),
        "INTEGRATION_UNAVAILABLE"
    );
    assert!(f.host_calls().is_empty());
    assert!(f.load(f.task()).unwrap().is_none());
}

#[test]
fn private_record_rejects_duplicate_candidates_and_auxiliary_ids() {
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    let candidate = sample_candidate(&record);
    record.candidates = vec![candidate.clone(), candidate];
    assert!(encode_record(&record).is_err());
    record.candidates.clear();
    let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
    let auxiliary = prepared.intent().unwrap();
    record.auxiliaries = vec![auxiliary.clone(), auxiliary];
    assert!(encode_record(&record).is_err());
}

#[test]
fn private_record_enforces_candidate_auxiliary_receipt_and_clock_caps() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let receipt = IntegrationReceipt {
        integration_id: record.snapshot.integration_id,
        epoch: 0,
        source_turn_id: fixture_source(),
        source_head: fixture_head(),
        target_head: record.cycle_base.clone(),
        merge_oid: None,
        disposition: IntegrationDisposition::AlreadyIntegrated,
        imported: true,
        recorded_at_millis: 1001,
    };
    for (field, value) in [
        ("remaining_admission_millis", serde_json::json!(600001)),
        ("remaining_backoff_millis", serde_json::json!(30001)),
        ("source_summary", serde_json::json!("s".repeat(1025))),
        (
            "candidates",
            serde_json::json!(vec![sample_candidate(&record); 4]),
        ),
        (
            "auxiliaries",
            serde_json::json!(vec![
                sample_prepared_turn(
                    &record,
                    IntegrationTurnPurpose::Resolve,
                    1,
                    1
                )
                .intent()
                .unwrap();
                6
            ]),
        ),
        ("archived_receipts", serde_json::json!(vec![receipt; 9])),
    ] {
        let mut wire = serde_json::to_value(&record).unwrap();
        wire[field] = value;
        assert!(
            serde_json::from_value::<IntegrationRecord>(wire).is_err(),
            "{field}"
        );
    }
    let mut at = encode_record(&record).unwrap();
    at.resize(MAX_PRIVATE_RECORD_BYTES, b' ');
    assert_eq!(decode_record(&at).unwrap(), record);
    at.push(b' ');
    assert!(decode_record(&at).is_err());
    assert_eq!(MAX_ARCHIVED_RECEIPTS, 8);
    assert_eq!(TRANSPORT_RETRY_DELAYS_MILLIS, [2000, 10000, 30000]);
}

#[test]
fn fixture_retains_full_authoritative_branch_but_bounds_public_display() {
    let branch = format!("{}a", "é".repeat(127));
    let record = sample_record(fixture_task(), fixture_source(), &branch);
    record.validate().unwrap();
    assert_eq!(record.policy.target.as_str(), branch);
    assert!(record.snapshot.target.len() <= MAX_TARGET_DISPLAY_BYTES);
    assert!(record.snapshot.target.ends_with('…'));
}
