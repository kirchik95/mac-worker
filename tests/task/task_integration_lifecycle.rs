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

#[test]
fn final_source_stages_once_after_exact_import_and_retirement() {
    let f = IntegrationFixture::new();
    f.enable(f.task(), "main").unwrap();
    f.coordinator().on_terminal(f.task(), f.source()).unwrap();
    assert!(f.load(f.task()).unwrap().is_none());
    f.complete_source(f.task()).unwrap();
    let staged = f.load(f.task()).unwrap().unwrap();
    assert_eq!(staged.snapshot.state, IntegrationStatus::Pending);
    assert_eq!(staged.snapshot.revision, IntegrationRevision(1));
    assert_eq!(staged.source_revision.len(), 64);
    f.complete_source(f.task()).unwrap();
    f.restart();
    f.complete_source(f.task()).unwrap();
    assert_eq!(f.load(f.task()).unwrap(), Some(staged));
    assert!(f.host_calls().is_empty());
}

#[test]
fn final_source_refuses_incomplete_stopped_and_auxiliary_facts() {
    for reason in [
        "import",
        "head",
        "session",
        "continuation",
        "runner",
        "stop",
        "close",
        "submission",
        "auxiliary",
        "closed",
        "cancelled",
        "other_turn",
    ] {
        let f = IntegrationFixture::new();
        f.enable(f.task(), "main").unwrap();
        let mut facts = f.observer().facts(f.task()).unwrap();
        facts.ordinary = sample_ordinary(f.task(), f.source());
        facts.result_imported = true;
        match reason {
            "import" => facts.result_imported = false,
            "head" => {
                facts.ordinary = facts
                    .ordinary
                    .with_fetched_head(Some("f".repeat(40).parse().unwrap()))
                    .unwrap()
            }
            "session" => facts.session_import_complete = false,
            "continuation" => facts.continuation_pending = true,
            "runner" => facts.runner_present = true,
            "stop" => facts.stop_requested = true,
            "close" => facts.close_pending = true,
            "submission" => facts.submission_pending = true,
            "auxiliary" => facts.auxiliary_purpose = Some(IntegrationTurnPurpose::Resolve),
            "closed" | "cancelled" => {
                let mut wire = serde_json::to_value(facts.ordinary.status()).unwrap();
                if reason == "closed" {
                    wire["state"] = "closed".into();
                } else {
                    wire["last_outcome"] = serde_json::json!({"kind":"cancelled"});
                }
                facts.ordinary = facts
                    .ordinary
                    .with_status(serde_json::from_value(wire).unwrap())
                    .unwrap();
            }
            "other_turn" => {
                facts.ordinary = sample_ordinary(
                    f.task(),
                    mac_worker::test_support::task::model::TurnId::generate(),
                )
            }
            _ => unreachable!(),
        }
        f.observer().insert(facts);
        f.coordinator().on_terminal(f.task(), f.source()).unwrap();
        assert!(f.load(f.task()).unwrap().is_none(), "{reason}");
        assert!(f.host_calls().is_empty(), "{reason}");
    }
}

#[test]
fn disabled_terminal_has_no_sidecar_or_host_effect() {
    let f = IntegrationFixture::new();
    f.coordinator().on_terminal(f.task(), f.source()).unwrap();
    assert!(f.load(f.task()).unwrap().is_none());
    assert!(f.host_calls().is_empty());
}

#[test]
fn rooted_state_replays_cas_prepared_binding_and_bounded_due_page() {
    use mac_worker::test_support::{core::paths::PathLayout, task::model::TaskId};
    use std::sync::Arc;
    let f = IntegrationFixture::new();
    let paths = PathLayout {
        state: f.root().join("state"),
        data: f.root().join("data"),
        cache: f.root().join("cache"),
        config: f.root().join("config"),
    };
    let runtime = Arc::new(ManualIntegrationRuntime::default());
    let state = RootedIntegrationState::open(&paths, runtime.clone()).unwrap();
    let mut record = sample_record(f.task(), f.source(), "main");
    record.candidates.push(sample_candidate(&record));
    state.publish_policy(f.task(), &record.policy).unwrap();
    state.publish_policy(f.task(), &record.policy).unwrap();
    assert!(
        state
            .publish_policy(f.task(), &sample_policy("other"))
            .is_err()
    );
    assert!(
        state
            .replace(f.task(), IntegrationRevision(0), &record)
            .unwrap()
    );
    assert!(
        !state
            .replace(f.task(), IntegrationRevision(0), &record)
            .unwrap()
    );
    let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
    state.publish_prepared(f.task(), &prepared).unwrap();
    state.publish_prepared(f.task(), &prepared).unwrap();
    drop(state);
    let state = RootedIntegrationState::open(&paths, runtime).unwrap();
    assert_eq!(state.load(f.task()).unwrap(), Some(record));
    assert_eq!(
        state
            .load_prepared(f.task(), prepared.followup.turn_id())
            .unwrap(),
        Some(prepared)
    );
    for n in 4..44 {
        let task = TaskId::new(uuid::Uuid::from_u128(n));
        let mut record = sample_record(task, f.source(), "main");
        record.run_position = n as u64;
        state.publish_policy(task, &record.policy).unwrap();
        state
            .replace(task, IntegrationRevision(0), &record)
            .unwrap();
    }
    let due = state.due(2000, 100).unwrap();
    assert_eq!(due.len(), 32);
    assert_eq!(due[0], f.task());
    assert_eq!(due[1], TaskId::new(uuid::Uuid::from_u128(4)));
}

#[test]
fn rooted_reservations_require_confirmed_absence_and_cap_four_targets() {
    use mac_worker::test_support::core::paths::PathLayout;
    use std::sync::Arc;
    let f = IntegrationFixture::new();
    let paths = PathLayout {
        state: f.root().join("state"),
        data: f.root().join("data"),
        cache: f.root().join("cache"),
        config: f.root().join("config"),
    };
    let runtime = Arc::new(ManualIntegrationRuntime::default());
    let state = RootedIntegrationState::open(&paths, runtime.clone()).unwrap();
    let record = sample_record(f.task(), f.source(), "main");
    let first = runtime.actor();
    let reserved = state
        .reserve(&record.target_key, record.snapshot.integration_id, 0, first)
        .unwrap()
        .unwrap();
    runtime.set_actor_verdict(first, RunnerLivenessVerdict::Unverifiable);
    runtime.restart();
    runtime.set_actor_verdict(first, RunnerLivenessVerdict::Unverifiable);
    assert!(
        state
            .reserve(
                &record.target_key,
                record.snapshot.integration_id,
                0,
                runtime.actor()
            )
            .unwrap()
            .is_none()
    );
    runtime.set_actor_verdict(first, RunnerLivenessVerdict::Exited);
    let replacement = state
        .reserve(
            &record.target_key,
            record.snapshot.integration_id,
            0,
            runtime.actor(),
        )
        .unwrap()
        .unwrap();
    assert!(state.release(&reserved).is_err());
    for branch in ["one", "two", "three"] {
        let key = TargetKey::new(&record.target_key.origin, branch).unwrap();
        assert!(
            state
                .reserve(&key, record.snapshot.integration_id, 0, runtime.actor())
                .unwrap()
                .is_some()
        );
    }
    let key = TargetKey::new(&record.target_key.origin, "five").unwrap();
    assert!(
        state
            .reserve(&key, record.snapshot.integration_id, 0, runtime.actor())
            .unwrap()
            .is_none()
    );
    state.release(&replacement).unwrap();
    assert!(
        state
            .reserve(&key, record.snapshot.integration_id, 0, runtime.actor())
            .unwrap()
            .is_some()
    );
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
