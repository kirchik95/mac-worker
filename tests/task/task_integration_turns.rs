use mac_worker::test_support::integration::*;

#[test]
fn prepared_verify_binding_matches_the_actual_frozen_candidate_tree() {
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    let candidate = sample_candidate(&record);
    record.candidates.push(candidate.clone());
    let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Verify, 1, 1);
    prepared.workspace_binding.validate_for(&candidate).unwrap();
    prepared.validate_for(&record).unwrap();
    let mut wrong = prepared.clone();
    wrong.workspace_binding.pinned_tree = Some("f".repeat(40).parse().unwrap());
    assert!(wrong.workspace_binding.validate_for(&candidate).is_err());
    assert!(wrong.validate_for(&record).is_err());
    let mut wrong_record = record;
    wrong_record.candidates.clear();
    assert!(prepared.validate_for(&wrong_record).is_err());
}

#[test]
fn preparation_has_a_replay_identity_and_a_separate_bounded_sidecar() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Verify, 1, 1);
    let turn = prepared.followup.turn_id();
    assert_eq!(
        turn,
        auxiliary_turn_id(
            prepared.integration_id,
            0,
            1,
            IntegrationTurnPurpose::Verify,
            1
        )
        .unwrap()
    );
    assert_eq!(
        prepared.intent().unwrap().prepared_binding,
        prepared.binding().unwrap()
    );
    let mut at = encode_prepared_turn(&prepared).unwrap();
    at.resize(MAX_PREPARED_TURN_BYTES, b' ');
    assert_eq!(decode_prepared_turn(&at).unwrap(), prepared);
    at.push(b' ');
    assert!(decode_prepared_turn(&at).is_err());
    let original_binding = prepared.binding().unwrap();
    let mut altered = prepared;
    altered.workspace_binding.pinned_tree = Some("f".repeat(40).parse().unwrap());
    assert_ne!(altered.binding().unwrap(), original_binding);
}

#[test]
fn permit_is_short_and_effective_pause_time_survives_restart() {
    let f = IntegrationFixture::new();
    let record = sample_record(f.task(), f.source(), "main");
    let key = IntegrationPhaseKey {
        task: f.task(),
        intent: record.snapshot.integration_id,
        epoch: 0,
        revision: IntegrationRevision(1),
        phase: IntegrationPhase::AuxiliaryAdmission,
    };
    assert!(matches!(
        f.runtime().begin_phase(&key).unwrap(),
        IntegrationDriveAdmission::Permit(_)
    ));
    f.advance(std::time::Duration::from_secs(120));
    f.set_drive_gate(Some(IntegrationPauseReason::ControllerDrained));
    f.advance(std::time::Duration::from_secs(900));
    f.restart();
    match f.runtime().begin_phase(&key).unwrap() {
        IntegrationDriveAdmission::Park(p) => assert_eq!(p.effective_at_millis, 121000),
        IntegrationDriveAdmission::Permit(_) => panic!("park admitted"),
    }
}
