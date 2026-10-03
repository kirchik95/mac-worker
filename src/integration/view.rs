//! Pure projections of durable integration state. Reads never drive work.
use super::contracts::*;
use crate::error::WorkerError;
use crate::task::{LocalTaskRecord, TaskOutcome, TaskState};
use crate::task_view::{ReviewState, review_state};

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

pub fn project_integration(
    snapshot: Option<&IntegrationSnapshot>,
    facts: &IntegrationTaskFacts,
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
        integration: snapshot.cloned(),
        workflow_state: None,
        review_state: ordinary_review,
        attention: ordinary_attention,
        requested_close: record.meta().close_policy(),
    };
    let Some(snapshot) = snapshot else {
        return Ok(view);
    };
    snapshot.validate()?;
    let ordinary_workflow = match status.state() {
        TaskState::Queued => WorkflowState::Queued,
        TaskState::Active => WorkflowState::Running,
        TaskState::Open => WorkflowState::NeedsYou,
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
        _ => {}
    }
    view.validate()?;
    Ok(view)
}
