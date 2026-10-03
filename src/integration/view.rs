//! Pure projections of durable integration state. Reads never drive work.
use super::contracts::*;
use crate::error::WorkerError;
use crate::task::{LocalTaskRecord, TaskOutcome, TaskState, TurnId};
use crate::task_view::{ReviewState, review_state};

pub(crate) fn read_owner_view(
    paths: &crate::paths::PathLayout,
    ordinary: &LocalTaskRecord,
    runner: bool,
) -> Result<Option<IntegrationView>, WorkerError> {
    let task = ordinary.meta().task_id();
    let (_, record) = super::store::RootedIntegrationState::read_task(paths, task)?;
    let Some(record) = record else {
        return Ok(None);
    };
    let current = snapshot_covers_latest_work(&record.snapshot, ordinary, |turn| {
        super::store::RootedIntegrationState::read_auxiliary(paths, task, turn)
            .map(|prepared| prepared.is_some())
    })?;
    let facts = IntegrationTaskFacts::from_record(ordinary, runner);
    let mut view = project_integration_for_current_work(Some(&record.snapshot), &facts, current)?;
    view.requested_close = record.policy.requested_close;
    // Keep the receipt visible as history once newer ordinary work exists.
    view.integration = Some(record.snapshot);
    Ok(Some(view))
}

impl IntegrationTaskFacts {
    /// Ordinary read facts; an observer may add its admission/auxiliary evidence.
    pub fn from_record(ordinary: &LocalTaskRecord, runner_present: bool) -> Self {
        Self {
            ordinary: ordinary.clone(),
            cycle_base: ordinary.meta().base_oid().clone(),
            result_imported: ordinary
                .status()
                .head_oid()
                .is_some_and(|head| ordinary.fetched_head() == Some(head)),
            session_import_complete: true,
            continuation_pending: ordinary.auto_continue_intent().is_some(),
            runner_present: runner_present || ordinary.runner().is_some(),
            stop_requested: false,
            close_pending: ordinary.close_intent().is_some(),
            submission_pending: ordinary.submission_intent_turn_id().is_some(),
            auxiliary_purpose: None,
        }
    }
}

/// Sidecars identify auxiliary turns across epochs. Stop at the latest ordinary
/// turn, so an older receipt never covers newer questions, failures or runners.
pub fn snapshot_covers_latest_work(
    snapshot: &IntegrationSnapshot,
    record: &LocalTaskRecord,
    mut is_auxiliary: impl FnMut(TurnId) -> Result<bool, WorkerError>,
) -> Result<bool, WorkerError> {
    snapshot.validate()?;
    for turn in record.status().turns().iter().rev() {
        if !is_auxiliary(turn.turn_id())? {
            return Ok(turn.turn_id() == snapshot.source_turn_id);
        }
    }
    // Admission/dependency snapshots may precede materialized ordinary history.
    Ok(record.status().turns().is_empty()
        && matches!(
            snapshot.state,
            IntegrationStatus::Armed | IntegrationStatus::Revoked
        ))
}

/// Pure callers can identify this epoch's deterministic auxiliary IDs. Durable
/// read adapters use prepared sidecars instead, including those of older epochs.
pub(crate) fn snapshot_covers_current_epoch_work(
    snapshot: &IntegrationSnapshot,
    record: &LocalTaskRecord,
) -> Result<bool, WorkerError> {
    snapshot_covers_latest_work(snapshot, record, |turn| {
        for attempt in 1..=MAX_CANDIDATES as u8 {
            for (purpose, limit) in [
                (IntegrationTurnPurpose::Resolve, MAX_RESOLVE_TURNS),
                (IntegrationTurnPurpose::Verify, MAX_VERIFY_TURNS),
            ] {
                for ordinal in 1..=limit {
                    if turn
                        == auxiliary_turn_id(
                            snapshot.integration_id,
                            snapshot.epoch,
                            attempt,
                            purpose,
                            ordinal,
                        )?
                    {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    })
}

pub fn project_integration(
    snapshot: Option<&IntegrationSnapshot>,
    facts: &IntegrationTaskFacts,
) -> Result<IntegrationView, WorkerError> {
    let current = snapshot
        .map(|snapshot| snapshot_covers_current_epoch_work(snapshot, &facts.ordinary))
        .transpose()?
        .unwrap_or(false);
    project_integration_for_current_work(snapshot, facts, current)
}

pub(crate) fn project_integration_for_current_work(
    snapshot: Option<&IntegrationSnapshot>,
    facts: &IntegrationTaskFacts,
    current: bool,
) -> Result<IntegrationView, WorkerError> {
    let record = &facts.ordinary;
    let status = record.status();
    let ordinary_review = review_state(record, status);
    let ordinary_attention = status.state() == TaskState::Open
        || (matches!(
            status.state(),
            TaskState::Queued | TaskState::Active | TaskState::Lost
        ) && status
            .last_outcome()
            .is_some_and(|outcome| !matches!(outcome, TaskOutcome::Done | TaskOutcome::Cancelled)));
    let mut view = IntegrationView {
        integration: snapshot.filter(|_| current).cloned(),
        workflow_state: None,
        review_state: ordinary_review,
        attention: ordinary_attention,
        requested_close: record.meta().close_policy(),
    };
    let Some(snapshot) = snapshot else {
        return Ok(view);
    };
    snapshot.validate()?;
    if !current {
        return Ok(view);
    }
    let ordinary_workflow = match status.state() {
        TaskState::Queued => WorkflowState::Queued,
        TaskState::Active => WorkflowState::Running,
        TaskState::Open => WorkflowState::NeedsYou,
        TaskState::Lost if ordinary_attention => WorkflowState::NeedsYou,
        TaskState::Closed | TaskState::Abandoned | TaskState::Lost => WorkflowState::Done,
    };
    let automatic = !matches!(
        snapshot.state,
        IntegrationStatus::Armed
            | IntegrationStatus::Revoked
            | IntegrationStatus::Integrated
            | IntegrationStatus::Blocked
    );
    view.workflow_state = Some(match snapshot.state {
        IntegrationStatus::Integrated => WorkflowState::Done,
        IntegrationStatus::Blocked => WorkflowState::NeedsYou,
        IntegrationStatus::Parked => WorkflowState::Integrating,
        _ if facts.runner_present || status.state() == TaskState::Active => WorkflowState::Running,
        _ if automatic => WorkflowState::Integrating,
        IntegrationStatus::Armed
            if status.state() == TaskState::Open
                && status.last_outcome() == Some(&TaskOutcome::Done) =>
        {
            WorkflowState::Integrating
        }
        _ => ordinary_workflow,
    });
    match view.workflow_state {
        Some(WorkflowState::Integrating | WorkflowState::Running | WorkflowState::Queued) => {
            view.review_state = if facts.close_pending {
                ReviewState::ClosePending
            } else {
                ReviewState::NotReviewable
            };
            view.attention = false;
        }
        Some(WorkflowState::Done) if snapshot.state == IntegrationStatus::Integrated => {
            view.review_state = if status.state() == TaskState::Open {
                ReviewState::NotReviewable
            } else {
                ordinary_review
            };
            view.attention = false;
        }
        _ if snapshot.state == IntegrationStatus::Blocked => {
            view.review_state = if facts.close_pending {
                ReviewState::ClosePending
            } else {
                ReviewState::ReadyForFollowUp
            };
            view.attention = true;
        }
        Some(WorkflowState::NeedsYou) if status.state() == TaskState::Lost => {
            view.review_state = ReviewState::ReadyForFollowUp;
        }
        _ => {}
    }
    if matches!(
        snapshot.state,
        IntegrationStatus::Armed | IntegrationStatus::Revoked
    ) && record.abandon_code()
        == Some(IntegrationCode::IntegrationDependencyNotIntegrated.as_str())
    {
        view.workflow_state = Some(WorkflowState::NeedsYou);
        view.review_state = ReviewState::ReadyForFollowUp;
        view.attention = true;
    }
    view.validate()?;
    Ok(view)
}
