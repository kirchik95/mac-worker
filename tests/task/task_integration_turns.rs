use mac_worker::test_support::integration::*;

#[test]
fn authoritative_preparation_survives_aux_cas_and_enqueue_crashes() {
    use mac_worker::test_support::task::model::{TaskState, TaskStatus};
    use mac_worker::test_support::task::prepared_followup::PreparedFollowup;
    for hook in [
        IntegrationHook::AfterAuxCas,
        IntegrationHook::AfterAuxEnqueue,
    ] {
        let f = IntegrationFixture::new();
        let mut record = sample_record(f.task(), f.source(), "main");
        record.candidates.push(sample_candidate(&record));
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
        let turn = prepared.followup.turn_id();
        let state: &dyn IntegrationState = f.state();
        let turns: &dyn IntegrationTurns = f.turns();
        state.publish_prepared(f.task(), &prepared).unwrap();
        record.auxiliaries.push(prepared.intent().unwrap());
        state
            .replace(f.task(), IntegrationRevision(0), &record)
            .unwrap();
        let ordinary = prepared.followup.expected();
        let queued = ordinary
            .with_status(
                TaskStatus::new(
                    TaskState::Queued,
                    None,
                    ordinary.status().worker().map(str::to_owned),
                    true,
                    Some(fixture_head()),
                    None,
                    vec![],
                    vec![],
                    None,
                    ordinary.status().turns().to_vec(),
                    1003,
                )
                .unwrap(),
            )
            .unwrap();
        f.observer().insert(IntegrationTaskFacts {
            ordinary: queued.clone(),
            cycle_base: record.cycle_base.clone(),
            result_imported: false,
            session_import_complete: true,
            continuation_pending: false,
            runner_present: false,
            stop_requested: false,
            close_pending: false,
            submission_pending: false,
            auxiliary_purpose: Some(IntegrationTurnPurpose::Resolve),
        });
        assert_eq!(
            PreparedFollowup::prepare(&queued, "Rebuild".into(), turn, 1003)
                .unwrap_err()
                .public_code(),
            "TASK_BUSY"
        );
        if hook == IntegrationHook::AfterAuxEnqueue {
            turns.enqueue(&prepared).unwrap();
        }
        f.crash_at(hook);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.runtime().reach(hook)))
                .is_err()
        );
        f.restart();
        assert_eq!(f.observer().facts(f.task()).unwrap().ordinary, queued);
        let loaded = state.load_prepared(f.task(), turn).unwrap().unwrap();
        assert_eq!(loaded.followup.expected(), ordinary);
        assert_eq!(
            f.stored_preparation(f.task(), turn).unwrap(),
            Some(prepared.clone())
        );
        assert_eq!(
            loaded.binding().unwrap(),
            record.auxiliaries[0].prepared_binding
        );
        turns.enqueue(&loaded).unwrap();
        turns.enqueue(&loaded).unwrap();
        assert_eq!(f.enqueue_count(turn), 1);
        assert_eq!(f.turn_observation(turn).unwrap().queue_position, Some(1));
        assert_eq!(f.observations().len(), 1);
    }
}

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

#[test]
fn prepared_auxiliary_pins_identity_session_limits_and_source_check_rule() {
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    record.candidates.push(sample_candidate(&record));
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    let prepared =
        PreparedIntegrationTurn::prepare(&ordinary, &record, IntegrationTurnPurpose::Resolve, 1, 1)
            .unwrap();
    prepared.validate_for(&record).unwrap();
    assert_eq!(prepared.followup.worker(), "fixture-worker");
    assert_eq!(prepared.followup.model(), Some("fixture-model"));
    assert_eq!(prepared.approved_turn_limits.timeout_millis, 600000);
    assert!(
        prepared
            .followup
            .composed_prompt()
            .contains("empty checks list is allowed")
    );
    assert_eq!(
        PreparedIntegrationTurn::prepare(&ordinary, &record, IntegrationTurnPurpose::Resolve, 1, 1)
            .unwrap(),
        prepared
    );
}

#[test]
fn auxiliary_admission_parks_late_and_restores_exactly_eight_minutes_once() {
    use super::task_integration_lifecycle::driver_fixture::*;
    use mac_worker::test_support::task::model::ClosePolicy;
    use std::time::Duration;
    let rig = Rig::new(Mode::Resolve, ClosePolicy::Never);
    let turn = rig.queued();
    let position = rig.turns.queue_position(turn);
    let original = rig
        .state
        .load_prepared(fixture_task(), turn)
        .unwrap()
        .unwrap();
    rig.runtime.advance(Duration::from_secs(120));
    rig.runtime
        .set_drive_gate(Some(IntegrationPauseReason::ControllerDrained));
    rig.runtime.advance(Duration::from_secs(900));
    rig.runtime.restart();
    assert_eq!(rig.drive().state, IntegrationStatus::Parked);
    assert_eq!(rig.record().remaining_admission_millis, Some(480000));
    assert_eq!(rig.record().admission_deadline_millis, None);
    rig.runtime.advance(Duration::from_secs(900));
    rig.runtime.restart();
    rig.drive();
    assert_eq!(rig.record().remaining_admission_millis, Some(480000));
    rig.runtime.set_drive_gate(None);
    rig.drive();
    assert_eq!(
        rig.record().admission_deadline_millis,
        Some(rig.runtime.now_millis() + 480000)
    );
    assert_eq!(rig.turns.queue_position(turn), position);
    assert_eq!(rig.turns.enqueue_count(turn), 1);
    assert_eq!(rig.record().followups_spent, 1);
    assert_eq!(
        rig.state.load_prepared(fixture_task(), turn).unwrap(),
        Some(original)
    );
    rig.runtime.advance(Duration::from_millis(479999));
    assert_ne!(rig.drive().state, IntegrationStatus::Blocked);
    rig.runtime.advance(Duration::from_millis(1));
    assert_eq!(
        rig.drive().blocked_code,
        Some(IntegrationCode::IntegrationTurnQueueTimeout)
    );
}

#[test]
fn completed_auxiliary_is_observed_after_park_without_readmission() {
    use super::task_integration_lifecycle::driver_fixture::*;
    use mac_worker::test_support::task::model::{ClosePolicy, TaskOutcome};
    let rig = Rig::new(Mode::Resolve, ClosePolicy::Never);
    let turn = rig.queued();
    rig.runtime
        .set_drive_gate(Some(IntegrationPauseReason::ControllerDisabled));
    rig.drive();
    rig.complete(turn, TaskOutcome::Done, vec![]);
    rig.runtime.advance(std::time::Duration::from_secs(900));
    rig.runtime.restart();
    rig.runtime.set_drive_gate(None);
    for _ in 0..12 {
        if rig.drive().state == IntegrationStatus::Integrated {
            break;
        }
    }
    assert_eq!(rig.record().snapshot.state, IntegrationStatus::Integrated);
    assert_eq!(rig.record().snapshot.resolve_turns, 1);
    assert_eq!(rig.turns.enqueue_count(turn), 1);
    assert_eq!(rig.record().source_summary, "Fixture work completed");
    assert_eq!(rig.record().cycle_base, "b".repeat(40).parse().unwrap());
}

#[test]
fn auxiliary_check_matrix_and_needs_input_block_without_another_turn() {
    use super::task_integration_lifecycle::driver_fixture::*;
    use mac_worker::test_support::{
        agents::agent::{ReportedCheck, ReportedCheckStatus},
        task::model::{ClosePolicy, TaskOutcome},
    };
    for (source, aux, outcome, want) in [
        (false, None, TaskOutcome::Done, None),
        (
            false,
            Some(ReportedCheckStatus::NotRun),
            TaskOutcome::Done,
            None,
        ),
        (
            true,
            None,
            TaskOutcome::Done,
            Some(IntegrationCode::IntegrationChecksNotRun),
        ),
        (
            true,
            Some(ReportedCheckStatus::NotRun),
            TaskOutcome::Done,
            Some(IntegrationCode::IntegrationChecksNotRun),
        ),
        (
            true,
            Some(ReportedCheckStatus::Pass),
            TaskOutcome::Done,
            None,
        ),
        (
            false,
            Some(ReportedCheckStatus::Fail),
            TaskOutcome::Done,
            Some(IntegrationCode::IntegrationChecksFailed),
        ),
        (
            true,
            Some(ReportedCheckStatus::Error),
            TaskOutcome::Done,
            Some(IntegrationCode::IntegrationChecksFailed),
        ),
        (
            false,
            None,
            TaskOutcome::NeedsInput,
            Some(IntegrationCode::IntegrationResolveBlocked),
        ),
    ] {
        let rig = Rig::new(Mode::Resolve, ClosePolicy::Never);
        if source {
            let mut record = rig.record();
            let rev = record.snapshot.revision;
            record.source_checks = vec![ReportedCheck::new(
                "source",
                "test",
                ReportedCheckStatus::Pass,
                "",
            )];
            record.snapshot.revision = rev.next().unwrap();
            rig.state.replace(fixture_task(), rev, &record).unwrap();
        }
        let turn = rig.queued();
        let checks = aux
            .map(|s| vec![ReportedCheck::new("aux", "test", s, "")])
            .unwrap_or_default();
        rig.complete(turn, outcome, checks);
        for _ in 0..12 {
            if matches!(
                rig.drive().state,
                IntegrationStatus::Integrated | IntegrationStatus::Blocked
            ) {
                break;
            }
        }
        assert_eq!(
            rig.record().snapshot.blocked_code,
            want,
            "source={source}, aux={aux:?}"
        );
        assert_eq!(rig.record().snapshot.resolve_turns, 1);
        assert_eq!(rig.turns.enqueue_count(turn), 1);
    }
}

#[test]
fn opt_in_verifier_uses_its_pinned_tree_and_returns_to_fetch() {
    use super::task_integration_lifecycle::driver_fixture::*;
    use mac_worker::test_support::task::model::{ClosePolicy, TaskOutcome};
    let rig = Rig::new(Mode::Verify, ClosePolicy::Never);
    let turn = rig.queued();
    let p = rig
        .state
        .load_prepared(fixture_task(), turn)
        .unwrap()
        .unwrap();
    assert_eq!(p.purpose, IntegrationTurnPurpose::Verify);
    assert_eq!(
        p.workspace_binding.pinned_tree,
        Some("c".repeat(40).parse().unwrap())
    );
    rig.complete(turn, TaskOutcome::Done, vec![]);
    assert_eq!(rig.drive().state, IntegrationStatus::Fetching);
    for _ in 0..8 {
        if rig.drive().state == IntegrationStatus::Integrated {
            break;
        }
    }
    assert_eq!(rig.record().snapshot.state, IntegrationStatus::Integrated);
    assert_eq!(
        rig.record().snapshot.verification,
        IntegrationVerification::VerifyAgentReport
    );
}
