//! Owner coordinator interface. T3 supplies the lifecycle implementation.
use super::contracts::*;
use crate::{
    error::WorkerError,
    redaction::RedactionBoundary,
    task::{TaskId, TaskOutcome, TaskState, TurnId},
};
use sha2::{Digest, Sha256};
pub struct IntegrationCoordinator<'a> {
    state: &'a dyn IntegrationState,
    host: &'a dyn IntegrationHost,
    turns: &'a dyn IntegrationTurns,
    runtime: &'a dyn IntegrationRuntime,
    observer: &'a dyn IntegrationObserver,
}
impl<'a> IntegrationCoordinator<'a> {
    pub fn new(
        state: &'a dyn IntegrationState,
        host: &'a dyn IntegrationHost,
        turns: &'a dyn IntegrationTurns,
        runtime: &'a dyn IntegrationRuntime,
        observer: &'a dyn IntegrationObserver,
    ) -> Self {
        Self {
            state,
            host,
            turns,
            runtime,
            observer,
        }
    }
    pub fn on_terminal(&self, task: TaskId, source: TurnId) -> Result<(), WorkerError> {
        let Some(policy) = self.state.load_policy(task)? else {
            return Ok(());
        };
        let facts = self.observer.facts(task)?;
        let ordinary = &facts.ordinary;
        let status = ordinary.status();
        if status.state() != TaskState::Open
            || status.last_outcome() != Some(&TaskOutcome::Done)
            || status.turns().last().is_none_or(|t| {
                t.turn_id() != source
                    || t.terminal().is_none()
                    || t.outcome() != Some(&TaskOutcome::Done)
            })
            || !facts.result_imported
            || !facts.session_import_complete
            || facts.continuation_pending
            || facts.runner_present
            || facts.stop_requested
            || facts.close_pending
            || facts.submission_pending
            || facts.auxiliary_purpose.is_some()
            || ordinary.runner().is_some()
            || ordinary.close_intent().is_some()
            || ordinary.auto_continue_intent().is_some()
            || ordinary.submission_intent_turn_id().is_some()
            || ordinary.submission_rollback_turn_id().is_some()
            || status.head_oid().is_none()
            || ordinary.fetched_head() != status.head_oid()
        {
            return Ok(());
        }
        let head = status
            .head_oid()
            .cloned()
            .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
        let target_key = policy.target_key()?;
        let id = IntegrationId::derive(task, source, &head, &target_key)?;
        let old = self.state.load(task)?;
        if let Some(old) = &old {
            if old.snapshot.integration_id == id {
                return Ok(());
            }
            if !matches!(
                old.snapshot.state,
                IntegrationStatus::Integrated | IntegrationStatus::Revoked
            ) {
                return Err(WorkerError::task("TASK_BUSY", "INTEGRATION_IN_PROGRESS"));
            }
        }
        let cycle_base = if let Some(parent) = policy.base_task {
            let parent = self
                .state
                .load(parent)?
                .ok_or_else(|| IntegrationCode::IntegrationDependencyBlocked.error())?;
            let receipt = parent
                .receipt
                .as_ref()
                .filter(|r| r.imported)
                .ok_or_else(|| IntegrationCode::IntegrationDependencyBlocked.error())?;
            if parent.snapshot.state != IntegrationStatus::Integrated
                || parent.target_key != target_key
            {
                return Err(IntegrationCode::IntegrationDependencyNotIntegrated.error());
            }
            receipt
                .merge_oid
                .as_ref()
                .unwrap_or(&receipt.target_head)
                .clone()
        } else {
            facts.cycle_base.clone()
        };
        let boundary = RedactionBoundary::from_env();
        let now = self.runtime.now_millis();
        let revision = old
            .as_ref()
            .map_or(IntegrationRevision(0), |r| r.snapshot.revision);
        let mut archived = old
            .as_ref()
            .map_or_else(Vec::new, |r| r.archived_receipts.clone());
        if let Some(receipt) = old.as_ref().and_then(|r| r.receipt.clone()) {
            archived.push(receipt);
        }
        if archived.len() > MAX_ARCHIVED_RECEIPTS {
            archived.drain(..archived.len() - MAX_ARCHIVED_RECEIPTS);
        }
        let checks = status.reported_checks().to_vec();
        let failed = checks.iter().any(|c| {
            matches!(
                c.status(),
                crate::agent::ReportedCheckStatus::Fail | crate::agent::ReportedCheckStatus::Error
            )
        });
        let record = IntegrationRecord {
            schema_version: INTEGRATION_SCHEMA_VERSION,
            task_id: task,
            cycle_base,
            target_key,
            source_revision: format!(
                "{:x}",
                Sha256::digest(
                    ordinary
                        .canonical_bytes()
                        .map_err(|_| IntegrationCode::IntegrationStateInvalid.error())?
                )
            ),
            source_summary: boundary.text(
                status
                    .summary()
                    .unwrap_or("Task completed; see the retained task result."),
                MAX_MESSAGE_SUMMARY_BYTES,
            ),
            source_checks: checks,
            git_identity: ordinary.meta().git_identity().clone(),
            actor: None,
            candidates: vec![],
            auxiliaries: vec![],
            archived_receipts: archived,
            push_intent: None,
            receipt: None,
            tombstone: None,
            pause: None,
            remaining_admission_millis: None,
            admission_deadline_millis: None,
            remaining_backoff_millis: None,
            phase_retries: vec![],
            ready_at_millis: now,
            run_position: 0,
            followups_spent: ordinary.status().turns().len().saturating_sub(1) as u32,
            snapshot: IntegrationSnapshot {
                schema_version: INTEGRATION_SCHEMA_VERSION,
                integration_id: id,
                epoch: 0,
                revision: revision.next()?,
                target: public_target_display(policy.target.as_str(), &boundary),
                state: if failed {
                    IntegrationStatus::Blocked
                } else {
                    IntegrationStatus::Pending
                },
                resume_state: None,
                pause_reason: None,
                source_turn_id: source,
                source_head: head,
                merge_oid: None,
                observed_target_oid: None,
                disposition: None,
                attempts: 0,
                resolve_turns: 0,
                verify_turns: 0,
                blocked_code: failed.then_some(IntegrationCode::IntegrationChecksFailed),
                retry_exhausted: false,
                retry_at_millis: None,
                verification: IntegrationVerification::SourceAgentReportOnly,
                updated_at_millis: now,
            },
            policy,
        };
        self.runtime.reach(IntegrationHook::AfterSourceImport);
        self.runtime.reach(IntegrationHook::AfterRunnerRetirement);
        if self.state.replace(task, revision, &record)? {
            self.runtime.reach(IntegrationHook::AfterIntent);
            self.runtime.reach(IntegrationHook::AfterStateBeforeEvent);
        }
        Ok(())
    }
    pub fn drive_once(&self, _task: TaskId) -> Result<IntegrationSnapshot, WorkerError> {
        Err(integration_unavailable())
    }
    pub fn redrive(
        &self,
        _task: TaskId,
        _expected: IntegrationRevision,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        Err(integration_unavailable())
    }
    pub fn revoke(
        &self,
        _task: TaskId,
        _expected: IntegrationRevision,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        Err(integration_unavailable())
    }
}
impl PreparedIntegrationTurn {
    pub fn prepare(
        _record: &crate::task::LocalTaskRecord,
        _integration: &IntegrationRecord,
        _purpose: IntegrationTurnPurpose,
        _attempt: u8,
        _ordinal: u8,
    ) -> Result<Self, WorkerError> {
        Err(integration_unavailable())
    }
}
