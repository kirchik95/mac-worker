use mac_worker::test_support::integration::*;
use mac_worker::test_support::task::model::{TaskState, TaskStatus};

fn facts() -> IntegrationTaskFacts {
    IntegrationTaskFacts {
        ordinary: sample_ordinary(fixture_task(), fixture_source()),
        cycle_base: fixture_head(),
        result_imported: true,
        session_import_complete: true,
        continuation_pending: false,
        runner_present: false,
        stop_requested: false,
        close_pending: false,
        submission_pending: false,
        auxiliary_purpose: None,
    }
}

#[test]
fn disabled_projection_preserves_review_and_omits_extension_fields() {
    let view = project_integration(None, &facts()).unwrap();
    let json = serde_json::to_value(view).unwrap();
    assert_eq!(json["review_state"], "ready_for_review");
    assert_eq!(json["attention"], true);
    assert!(json.get("integration").is_none());
    assert!(json.get("workflow_state").is_none());
}

#[test]
fn automatic_phases_and_parked_receipts_never_request_review() {
    let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    for state in [
        IntegrationStatus::Pending,
        IntegrationStatus::Fetching,
        IntegrationStatus::Resolving,
        IntegrationStatus::Verifying,
        IntegrationStatus::CommitReady,
        IntegrationStatus::Pushing,
        IntegrationStatus::Published,
        IntegrationStatus::RetryWait,
        IntegrationStatus::Parked,
    ] {
        snapshot.state = state;
        snapshot.resume_state = matches!(
            state,
            IntegrationStatus::RetryWait | IntegrationStatus::Parked
        )
        .then_some(IntegrationStatus::Published);
        snapshot.pause_reason = (state == IntegrationStatus::Parked)
            .then_some(IntegrationPauseReason::ControllerDrained);
        let view = project_integration(Some(&snapshot), &facts()).unwrap();
        assert_eq!(
            view.workflow_state,
            Some(WorkflowState::Integrating),
            "{state:?}"
        );
        assert!(!view.attention, "{state:?}");
        assert_eq!(
            serde_json::to_value(view.review_state).unwrap(),
            "not_reviewable"
        );
    }
    let mut running = facts();
    running.runner_present = true;
    running.auxiliary_purpose = Some(IntegrationTurnPurpose::Resolve);
    snapshot.state = IntegrationStatus::Resolving;
    snapshot.resume_state = None;
    snapshot.pause_reason = None;
    assert_eq!(
        project_integration(Some(&snapshot), &running)
            .unwrap()
            .workflow_state,
        Some(WorkflowState::Running)
    );
}

#[test]
fn integrated_open_never_is_done_and_blocked_retains_source_done() {
    let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    snapshot.state = IntegrationStatus::Integrated;
    snapshot.merge_oid = Some("e".repeat(40).parse().unwrap());
    snapshot.disposition = Some(IntegrationDisposition::Merged);
    let facts = facts();
    let success = project_integration(Some(&snapshot), &facts).unwrap();
    assert_eq!(success.workflow_state, Some(WorkflowState::Done));
    assert!(!success.attention);
    snapshot.state = IntegrationStatus::Blocked;
    snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
    let blocked = project_integration(Some(&snapshot), &facts).unwrap();
    assert_eq!(blocked.workflow_state, Some(WorkflowState::NeedsYou));
    assert!(blocked.attention);
    assert_eq!(
        serde_json::to_value(blocked.review_state).unwrap(),
        "ready_for_follow_up"
    );
    assert_eq!(
        facts.ordinary.status().last_outcome().unwrap().kind(),
        "done"
    );
}

#[test]
fn armed_admission_dependency_wait_and_terminal_cancellation_keep_ordinary_state() {
    let mut facts = facts();
    let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    snapshot.state = IntegrationStatus::Armed;
    let queued = TaskStatus::new(
        TaskState::Queued,
        None,
        None,
        false,
        None,
        None,
        vec![],
        vec![],
        None,
        vec![],
        1000,
    )
    .unwrap();
    facts.ordinary = facts.ordinary.with_status(queued).unwrap();
    assert_eq!(
        project_integration(Some(&snapshot), &facts)
            .unwrap()
            .workflow_state,
        Some(WorkflowState::Queued)
    );
    snapshot.state = IntegrationStatus::Revoked;
    let closed = TaskStatus::new(
        TaskState::Closed,
        Some(mac_worker::test_support::task::model::TaskOutcome::Cancelled),
        None,
        false,
        None,
        None,
        vec![],
        vec![],
        None,
        vec![],
        1001,
    )
    .unwrap();
    facts.ordinary = facts.ordinary.with_status(closed).unwrap();
    let view = project_integration(Some(&snapshot), &facts).unwrap();
    assert_eq!(view.workflow_state, Some(WorkflowState::Done));
    assert!(!view.attention);
}

#[test]
fn compact_confirmation_requires_the_complete_same_revision_identity() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let annotation = record.snapshot.annotation().unwrap();
    assert!(annotation.confirms(&record.snapshot));
    let mut newer = record.snapshot.clone();
    newer.revision = newer.revision.next().unwrap();
    assert!(!annotation.confirms(&newer));
    let mut other_epoch = record.snapshot.clone();
    other_epoch.epoch += 1;
    assert!(!annotation.confirms(&other_epoch));
    let mut other_target = sample_record(fixture_task(), fixture_source(), "release").snapshot;
    other_target.revision = annotation.revision;
    assert!(!annotation.confirms(&other_target));
}
