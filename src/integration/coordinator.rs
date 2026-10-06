//! Owner coordinator interface. T3 supplies the lifecycle implementation.
use super::contracts::*;
use crate::{
    error::WorkerError,
    redaction::RedactionBoundary,
    task::{TaskId, TaskOutcome, TaskState, TurnId},
};
use sha2::{Digest, Sha256};
/// Closed settlement has its own driver budget, separate from Open Repair.
/// Every consumer uses this proof before treating a Closed block as terminal.
pub(crate) fn closed_observation_pending(record: &IntegrationRecord) -> bool {
    !matches!(
        record.snapshot.state,
        IntegrationStatus::Integrated | IntegrationStatus::Revoked
    ) && (record.snapshot.state != IntegrationStatus::Blocked
        || !record
            .phase_retries
            .iter()
            .any(|retry| retry.phase == IntegrationPhase::Drive))
}

pub struct IntegrationCoordinator<'a> {
    state: &'a dyn IntegrationState,
    host: &'a dyn IntegrationHost,
    turns: &'a dyn IntegrationTurns,
    runtime: &'a dyn IntegrationRuntime,
    observer: &'a dyn IntegrationObserver,
    source_observer: &'a dyn IntegrationObserver,
    owner_paths: Option<&'a crate::paths::PathLayout>,
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
            source_observer: observer,
            owner_paths: None,
        }
    }
    pub(crate) fn with_owner_gate(mut self, paths: &'a crate::paths::PathLayout) -> Self {
        self.owner_paths = Some(paths);
        self
    }
    pub(crate) fn with_source_observer(mut self, observer: &'a dyn IntegrationObserver) -> Self {
        self.source_observer = observer;
        self
    }
    pub fn on_terminal(&self, task: TaskId, source: TurnId) -> Result<(), WorkerError> {
        let Some(policy) = self.state.load_policy(task)? else {
            return Ok(());
        };
        let old = self.state.load(task)?;
        if let Some(mut record) = old.clone()
            && let Some(index) = record.auxiliaries.iter().position(|a| a.turn_id == source)
        {
            let observation = self.turns.observe(source)?;
            observation.validate()?;
            let auxiliary = &mut record.auxiliaries[index];
            let accepted = observation.accepted && !auxiliary.accepted;
            let completed = observation.completed && !auxiliary.completed;
            if observation.turn_id != source
                || auxiliary
                    .queue_position
                    .is_some_and(|p| observation.queue_position != Some(p))
                || (auxiliary.accepted && !observation.accepted)
                || (auxiliary.completed && !observation.completed)
            {
                return Err(IntegrationCode::IntegrationStateInvalid.error());
            }
            if auxiliary.completed == observation.completed
                && auxiliary.accepted == observation.accepted
                && auxiliary.queue_position == observation.queue_position
            {
                return Ok(());
            }
            auxiliary.queue_position = observation.queue_position;
            auxiliary.accepted = observation.accepted;
            auxiliary.completed = observation.completed;
            if observation.accepted {
                record.admission_deadline_millis = None;
                record.remaining_admission_millis = None;
            }
            record.ready_at_millis = self.runtime.now_millis();
            self.save(&mut record)?;
            if accepted {
                self.runtime.reach(IntegrationHook::AfterAuxAccepted);
            }
            if completed {
                self.runtime.reach(IntegrationHook::AfterAuxCompleted);
            }
            return Ok(());
        }
        // Preparations outlive compact epoch references so a late old auxiliary
        // completion remains distinguishable from an ordinary source turn.
        if self.state.load_prepared(task, source)?.is_some() {
            return Ok(());
        }
        let facts = self.observer.facts(task)?;
        let ordinary = &facts.ordinary;
        let status = ordinary.status();
        if !Self::terminal_source_ready(&facts, source) {
            return Ok(());
        }
        let head = status
            .head_oid()
            .cloned()
            .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
        // Import/repair changes the public head to M/T, while the ordinary
        // source turn stays the same. A repeated terminal wake is that cycle's
        // recovery, never a new source derived from the accepted merge.
        if let Some(old) = &old
            && old.snapshot.source_turn_id == source
        {
            if head != old.snapshot.source_head
                && !old.receipt.as_ref().is_some_and(|receipt| {
                    &head == receipt.merge_oid.as_ref().unwrap_or(&receipt.target_head)
                })
            {
                return Err(IntegrationCode::IntegrationStateInvalid.error());
            }
            return Ok(());
        }
        let target_key = policy.target_key()?;
        let id = IntegrationId::derive(task, source, &head, &target_key)?;
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
        let cycle_base = if let Some(receipt) = old
            .as_ref()
            .filter(|r| r.snapshot.state == IntegrationStatus::Integrated)
            .and_then(|r| r.receipt.as_ref())
            .filter(|r| r.imported)
        {
            receipt
                .merge_oid
                .as_ref()
                .unwrap_or(&receipt.target_head)
                .clone()
        } else if let Some(parent) = policy.base_task {
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
            followups_spent: self.followups_spent(ordinary, old.as_ref())?,
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
        let current = self.source_observer.facts(task)?;
        if !Self::terminal_source_ready(&current, source)
            || current.ordinary.status().head_oid() != Some(&record.snapshot.source_head)
        {
            return Ok(());
        }
        if self.state.replace(task, revision, &record)? {
            self.runtime.reach(IntegrationHook::AfterIntent);
            self.runtime.reach(IntegrationHook::AfterStateBeforeEvent);
        }
        Ok(())
    }
    fn terminal_source_ready(facts: &IntegrationTaskFacts, source: TurnId) -> bool {
        let ordinary = &facts.ordinary;
        let status = ordinary.status();
        status.state() == TaskState::Open
            && status.last_outcome() == Some(&TaskOutcome::Done)
            && status.turns().last().is_some_and(|turn| {
                turn.turn_id() == source
                    && turn.terminal().is_some()
                    && turn.outcome() == Some(&TaskOutcome::Done)
            })
            && facts.result_imported
            && facts.session_import_complete
            && !facts.continuation_pending
            && !facts.runner_present
            && !facts.stop_requested
            && !facts.close_pending
            && !facts.submission_pending
            && facts.auxiliary_purpose.is_none()
            && ordinary.runner().is_none()
            && ordinary.close_intent().is_none()
            && ordinary.auto_continue_intent().is_none()
            && ordinary.submission_intent_turn_id().is_none()
            && ordinary.submission_rollback_turn_id().is_none()
            && status.head_oid().is_some()
            && ordinary.fetched_head() == status.head_oid()
    }
    fn owner_source_is_current(
        &self,
        record: &IntegrationRecord,
        facts: &IntegrationTaskFacts,
    ) -> Result<bool, WorkerError> {
        let ordinary = &facts.ordinary;
        if !self.covers_latest_ordinary_work(ordinary, &record.snapshot)?
            || facts.continuation_pending
            || facts.submission_pending
            || facts.stop_requested
            || facts.close_pending
            || ordinary.auto_continue_intent().is_some()
            || ordinary.close_intent().is_some()
            || ordinary.submission_intent_turn_id().is_some()
            || ordinary.submission_rollback_turn_id().is_some()
        {
            return Ok(false);
        }
        let Some(last) = ordinary.status().turns().last() else {
            return Ok(false);
        };
        if last.turn_id() == record.snapshot.source_turn_id {
            return Ok(!matches!(
                ordinary.status().state(),
                TaskState::Active | TaskState::Queued
            ) && !facts.runner_present
                && ordinary.runner().is_none()
                && facts.auxiliary_purpose.is_none());
        }
        // Only this cycle's persisted preparation permits an active auxiliary;
        // a purpose hint or another epoch's sidecar is not admission evidence.
        let Some(prepared) = self.state.load_prepared(record.task_id, last.turn_id())? else {
            return Ok(false);
        };
        if prepared.validate_for(record).is_ok() {
            return Ok(true);
        }
        // Re-drive retires old epoch references, but their completed sidecars
        // still classify history. They never authorize another auxiliary run.
        prepared.validate()?;
        Ok(prepared.integration_id == record.snapshot.integration_id
            && prepared.epoch < record.snapshot.epoch
            && prepared.followup.task_id() == record.task_id
            && prepared.workspace_binding.head == record.snapshot.source_head
            && last.terminal().is_some()
            && !matches!(
                ordinary.status().state(),
                TaskState::Active | TaskState::Queued
            )
            && !facts.runner_present
            && ordinary.runner().is_none())
    }
    fn revoke_superseded(&self, record: &mut IntegrationRecord) -> Result<bool, WorkerError> {
        let facts = self.source_observer.facts(record.task_id)?;
        if self.owner_source_is_current(record, &facts)? {
            return Ok(false);
        }
        self.revoke_for_stop(record.task_id, &record.snapshot)?;
        *record = self
            .state
            .load(record.task_id)?
            .ok_or_else(integration_unavailable)?;
        Ok(true)
    }
    fn save(&self, record: &mut IntegrationRecord) -> Result<(), WorkerError> {
        if let Some(paths) = self.owner_paths {
            extend_elapsed_pauses(self.state, self.runtime, paths, record)?;
        }
        persist_record(self.state, self.runtime, record)
    }
    fn release(&self, record: &mut IntegrationRecord) -> Result<(), WorkerError> {
        if let Some(actor) = record.actor {
            self.state.release(&TargetReservation {
                key: record.target_key.clone(),
                integration_id: record.snapshot.integration_id,
                epoch: record.snapshot.epoch,
                actor,
            })?;
            record.actor = None;
        }
        Ok(())
    }
    /// Reclaim in the observing parent: a re-exec starts with an empty absence cache.
    pub(crate) fn reclaim_exited_actor(&self, task: TaskId) -> Result<bool, WorkerError> {
        let Some(mut record) = self.state.load(task)? else {
            return Ok(true);
        };
        if let Some(actor) = record.actor {
            if self.runtime.actor_verdict(actor)
                != crate::client_state::RunnerLivenessVerdict::Exited
            {
                return Ok(false);
            }
            self.release(&mut record)?;
            self.save(&mut record)?;
        }
        Ok(true)
    }
    fn release_failed_phase(
        &self,
        task: TaskId,
        reservation: &TargetReservation,
    ) -> Result<(), WorkerError> {
        self.state.release(reservation)?;
        // An application can fail after changing its local copy or losing CAS.
        // Clear only our durable ownership; never publish that partial copy.
        if let Some(mut current) = self.state.load(task)?
            && current.snapshot.integration_id == reservation.integration_id
            && current.snapshot.epoch == reservation.epoch
            && current.actor == Some(reservation.actor)
        {
            current.actor = None;
            self.save(&mut current)?;
        }
        Ok(())
    }
    fn park(
        &self,
        record: &mut IntegrationRecord,
        pause: IntegrationPauseEvidence,
    ) -> Result<(), WorkerError> {
        park_record(self.state, self.runtime, self.owner_paths, record, pause)
    }
    fn key(&self, record: &IntegrationRecord, phase: IntegrationPhase) -> IntegrationPhaseKey {
        IntegrationPhaseKey {
            task: record.task_id,
            intent: record.snapshot.integration_id,
            epoch: record.snapshot.epoch,
            revision: record.snapshot.revision,
            phase,
        }
    }
    fn permit(
        &self,
        record: &mut IntegrationRecord,
        phase: IntegrationPhase,
    ) -> Result<Option<IntegrationPhasePermit>, WorkerError> {
        self.runtime.reach(IntegrationHook::BeforePhasePermit);
        if self.revoke_superseded(record)? {
            return Ok(None);
        }
        match self.runtime.begin_phase(&self.key(record, phase))? {
            IntegrationDriveAdmission::Permit(p) => {
                if let Some(paths) = self.owner_paths {
                    extend_elapsed_pauses(self.state, self.runtime, paths, record)?;
                }
                Ok(Some(p))
            }
            IntegrationDriveAdmission::Park(p) => {
                self.park(record, p)?;
                Ok(None)
            }
        }
    }
    fn block(
        &self,
        record: &mut IntegrationRecord,
        code: IntegrationCode,
    ) -> Result<(), WorkerError> {
        record.snapshot.state = IntegrationStatus::Blocked;
        record.snapshot.blocked_code = Some(code);
        record.snapshot.resume_state = None;
        record.snapshot.pause_reason = None;
        record.pause = None;
        record.snapshot.retry_at_millis = None;
        record.admission_deadline_millis = None;
        self.release(record)?;
        self.save(record)
    }
    fn request(&self, record: &IntegrationRecord, step: IntegrationStep) -> HostIntegrationRequest {
        HostIntegrationRequest {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            task_id: record.task_id,
            integration_id: Some(record.snapshot.integration_id),
            epoch: record.snapshot.epoch,
            revision: record.snapshot.revision,
            action: HostIntegrationAction::Step {
                step,
                record: Box::new(record.clone()),
            },
        }
    }
    fn retry(
        &self,
        record: &mut IntegrationRecord,
        phase: IntegrationPhase,
        code: IntegrationCode,
    ) -> Result<(), WorkerError> {
        if code == IntegrationCode::IntegrationUnavailable {
            return self.park(
                record,
                IntegrationPauseEvidence {
                    reason: IntegrationPauseReason::HelperUnavailable,
                    effective_at_millis: self.runtime.now_millis(),
                },
            );
        }
        if !matches!(
            code,
            IntegrationCode::IntegrationNetwork | IntegrationCode::IntegrationWorkerOffline
        ) {
            return self.block(record, code);
        }
        let retry = record
            .phase_retries
            .iter_mut()
            .find(|r| r.phase == phase)
            .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
        if retry.retries as usize >= TRANSPORT_RETRY_DELAYS_MILLIS.len() {
            record.snapshot.retry_exhausted = true;
            return self.block(record, code);
        }
        let delay = TRANSPORT_RETRY_DELAYS_MILLIS[retry.retries as usize];
        retry.retries += 1;
        retry.code = code;
        retry.due_at_millis = self.runtime.now_millis().saturating_add(delay);
        record.snapshot.updated_at_millis = self.runtime.now_millis();
        record.snapshot.retry_at_millis = Some(retry.due_at_millis);
        record.snapshot.resume_state = Some(if phase == IntegrationPhase::Push {
            IntegrationStatus::Fetching
        } else {
            record.snapshot.state
        });
        record.snapshot.state = IntegrationStatus::RetryWait;
        record.snapshot.blocked_code = Some(code);
        self.release(record)?;
        self.save(record)
    }
    fn resume(&self, record: &mut IntegrationRecord) -> Result<(), WorkerError> {
        resume_record(self.state, self.runtime, self.owner_paths, record)
    }
    pub fn configured(&self, task: TaskId) -> Result<bool, WorkerError> {
        Ok(self.state.load_policy(task)?.is_some())
    }
    pub(crate) fn park_for_runtime(
        &self,
        task: TaskId,
        pause: IntegrationPauseEvidence,
    ) -> Result<(), WorkerError> {
        let mut record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
        self.park(&mut record, pause)
    }
    pub fn snapshot(&self, task: TaskId) -> Result<Option<IntegrationSnapshot>, WorkerError> {
        Ok(self.state.load(task)?.map(|r| r.snapshot))
    }
    pub(crate) fn covers_latest_ordinary_work(
        &self,
        ordinary: &crate::task::LocalTaskRecord,
        snapshot: &IntegrationSnapshot,
    ) -> Result<bool, WorkerError> {
        for turn in ordinary.status().turns().iter().rev() {
            if self
                .state
                .load_prepared(ordinary.meta().task_id(), turn.turn_id())?
                .is_none()
            {
                return Ok(turn.turn_id() == snapshot.source_turn_id);
            }
        }
        Ok(false)
    }
    fn followups_spent(
        &self,
        ordinary: &crate::task::LocalTaskRecord,
        previous: Option<&IntegrationRecord>,
    ) -> Result<u32, WorkerError> {
        let turns = ordinary.status().turns();
        let materialized = u32::try_from(turns.len().saturating_sub(1))
            .map_err(|_| IntegrationCode::IntegrationStateInvalid.error())?;
        let Some(previous) = previous else {
            return Ok(materialized);
        };
        let source = turns
            .iter()
            .position(|t| t.turn_id() == previous.snapshot.source_turn_id)
            .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
        // The prior counter already charged all reserved auxiliaries, including
        // ones absent from task history. Charge only later ordinary turns again.
        let mut spent = previous.followups_spent;
        for turn in &turns[source + 1..] {
            if self
                .state
                .load_prepared(ordinary.meta().task_id(), turn.turn_id())?
                .is_none()
            {
                spent = spent
                    .checked_add(1)
                    .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
            }
        }
        Ok(spent.max(materialized))
    }
    pub(crate) fn check_ordinary_followup_allowance(
        &self,
        ordinary: &crate::task::LocalTaskRecord,
    ) -> Result<(), WorkerError> {
        if ordinary.status().state() != TaskState::Open {
            return Ok(());
        }
        let previous = self.state.load(ordinary.meta().task_id())?;
        if self.followups_spent(ordinary, previous.as_ref())?
            >= ordinary.meta().limits().max_followups
        {
            return Err(WorkerError::task(
                "FOLLOWUP_LIMIT",
                "task follow-up limit has been reached",
            ));
        }
        Ok(())
    }
    pub(crate) fn set_run_position(&self, task: TaskId, position: u64) -> Result<(), WorkerError> {
        let Some(mut record) = self.state.load(task)? else {
            return Ok(());
        };
        if record.run_position != position {
            record.run_position = position;
            self.save(&mut record)?;
        }
        Ok(())
    }
    pub(crate) fn mark_given_up(&self, task: TaskId) -> Result<(), WorkerError> {
        let Some(record) = self.state.load(task)? else {
            return Ok(());
        };
        self.save_stop_update(record, |record| {
            if record.snapshot.state != IntegrationStatus::Revoked
                || record.tombstone.as_ref().is_none_or(|t| !t.acknowledged)
            {
                return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
            }
            // The DAG gate reads this durable give-up evidence to fail
            // configured children whose parent was not integrated.
            record.snapshot.blocked_code =
                Some(IntegrationCode::IntegrationDependencyNotIntegrated);
            Ok(())
        })?;
        Ok(())
    }
    pub(crate) fn ready_to_drive(&self, task: TaskId) -> Result<bool, WorkerError> {
        let record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
        Ok(!matches!(
            record.snapshot.state,
            IntegrationStatus::Integrated
                | IntegrationStatus::Blocked
                | IntegrationStatus::Revoked
                | IntegrationStatus::Parked
                | IntegrationStatus::RetryWait
        ) && !record.auxiliaries.last().is_some_and(|a| {
            a.attempt == record.snapshot.attempts
                && a.queue_position.is_some()
                && !a.completed
                && matches!(
                    record.snapshot.state,
                    IntegrationStatus::Resolving | IntegrationStatus::Verifying
                )
        }))
    }
    pub fn drive_once(&self, task: TaskId) -> Result<IntegrationSnapshot, WorkerError> {
        let mut record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
        let facts = self.observer.facts(task)?;
        // Observe the refreshed host state before consulting any launch/admission permit.
        if facts.ordinary.status().state() == TaskState::Closed
            && !matches!(
                record.snapshot.state,
                IntegrationStatus::Integrated | IntegrationStatus::Revoked
            )
        {
            if let Some(actor) = record.actor
                && actor != self.runtime.actor()
            {
                if self.runtime.actor_verdict(actor)
                    != crate::client_state::RunnerLivenessVerdict::Exited
                {
                    return Ok(record.snapshot);
                }
                self.release(&mut record)?;
            }
            return self.settle_closed(record);
        }
        if record.tombstone.as_ref().is_some_and(|t| !t.acknowledged)
            || facts.stop_requested
            || facts.close_pending
        {
            return self.revoke_for_stop(task, &record.snapshot);
        }
        if !matches!(
            record.snapshot.state,
            IntegrationStatus::Integrated | IntegrationStatus::Revoked
        ) && !self.owner_source_is_current(&record, &facts)?
        {
            return self.revoke_for_stop(task, &record.snapshot);
        }
        if matches!(
            record.snapshot.state,
            IntegrationStatus::Integrated | IntegrationStatus::Blocked | IntegrationStatus::Revoked
        ) {
            return Ok(record.snapshot);
        }
        if record.source_checks.iter().any(|c| {
            matches!(
                c.status(),
                crate::agent::ReportedCheckStatus::Fail | crate::agent::ReportedCheckStatus::Error
            )
        }) {
            self.block(&mut record, IntegrationCode::IntegrationChecksFailed)?;
            return Ok(record.snapshot);
        }
        if !facts.session_import_complete
            || facts.submission_pending
            || matches!(
                facts.ordinary.status().state(),
                TaskState::Lost | TaskState::Abandoned
            )
        {
            self.block(&mut record, IntegrationCode::IntegrationWorkspaceMissing)?;
            return Ok(record.snapshot);
        }
        if let Some(actor) = record.actor
            && actor != self.runtime.actor()
        {
            if self.runtime.actor_verdict(actor)
                != crate::client_state::RunnerLivenessVerdict::Exited
            {
                return Ok(record.snapshot);
            }
            self.release(&mut record)?;
        }
        if matches!(
            record.snapshot.state,
            IntegrationStatus::Resolving | IntegrationStatus::Verifying | IntegrationStatus::Parked
        ) && let Some(auxiliary) = record
            .auxiliaries
            .last()
            .filter(|a| a.attempt == record.snapshot.attempts)
        {
            // Retained completion is an observation, including while the launch gate is shut.
            self.on_terminal(task, auxiliary.turn_id)?;
            record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
        }
        let Some(permit) = self.permit(&mut record, IntegrationPhase::Drive)? else {
            return Ok(record.snapshot);
        };
        if record.snapshot.state == IntegrationStatus::Parked {
            self.resume(&mut record)?;
        }
        if record.snapshot.state == IntegrationStatus::RetryWait {
            if record
                .snapshot
                .retry_at_millis
                .is_some_and(|due| due > self.runtime.now_millis())
            {
                drop(permit);
                return Ok(record.snapshot);
            }
            self.resume(&mut record)?;
        }
        drop(permit);
        if matches!(
            record.snapshot.state,
            IntegrationStatus::Resolving | IntegrationStatus::Verifying
        ) && record.auxiliaries.last().is_some_and(|a| {
            a.attempt == record.snapshot.attempts
                && a.purpose
                    == if record.snapshot.state == IntegrationStatus::Resolving {
                        IntegrationTurnPurpose::Resolve
                    } else {
                        IntegrationTurnPurpose::Verify
                    }
        }) {
            return self.drive_auxiliary(record, &facts);
        }
        if record.snapshot.state == IntegrationStatus::Published {
            return self.finish_receipt(record, false);
        }
        let step = match record.snapshot.state {
            IntegrationStatus::Pending | IntegrationStatus::Fetching => IntegrationStep::Fetch,
            IntegrationStatus::Resolving | IntegrationStatus::Verifying => IntegrationStep::Prepare,
            IntegrationStatus::CommitReady => {
                if record
                    .candidates
                    .last()
                    .is_some_and(|c| c.merge_oid.is_some())
                {
                    IntegrationStep::Push
                } else {
                    IntegrationStep::Build
                }
            }
            // A lost push answer ALWAYS observes origin before repetition.
            IntegrationStatus::Pushing => IntegrationStep::Fetch,
            _ => return Err(IntegrationCode::IntegrationStateInvalid.error()),
        };
        self.host_phase(record, step)
    }
    fn host_phase(
        &self,
        mut record: IntegrationRecord,
        step: IntegrationStep,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        let phase = match step {
            IntegrationStep::Fetch => IntegrationPhase::Fetch,
            IntegrationStep::Prepare => IntegrationPhase::Prepare,
            IntegrationStep::AcceptTurn => IntegrationPhase::AcceptTurn,
            IntegrationStep::Build => IntegrationPhase::Build,
            IntegrationStep::Push => IntegrationPhase::Push,
            IntegrationStep::Repair => IntegrationPhase::Repair,
        };
        let Some(permit) = self.permit(&mut record, phase)? else {
            return Ok(record.snapshot);
        };
        let actor = self.runtime.actor();
        let Some(reservation) = self.state.reserve(
            &record.target_key,
            record.snapshot.integration_id,
            record.snapshot.epoch,
            actor,
        )?
        else {
            drop(permit);
            return Ok(record.snapshot);
        };
        record.actor = Some(actor);
        if step == IntegrationStep::Fetch {
            record.snapshot.state = IntegrationStatus::Fetching;
            if record.snapshot.attempts == 0 {
                record.snapshot.attempts = 1;
            }
        }
        if step == IntegrationStep::Push {
            let candidate = record
                .candidates
                .last()
                .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
            record.push_intent = Some(IntegrationPushIntent {
                candidate: candidate.id,
                expected_target: candidate.target_head.clone(),
                merge_oid: candidate
                    .merge_oid
                    .clone()
                    .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?,
                started_at_millis: record
                    .push_intent
                    .as_ref()
                    .map_or(self.runtime.now_millis(), |p| p.started_at_millis),
                uncertain: true,
            });
            record.snapshot.state = IntegrationStatus::Pushing;
        }
        if !record.phase_retries.iter().any(|r| r.phase == phase) {
            record.phase_retries.push(IntegrationPhaseRetry {
                phase,
                retries: 0,
                code: IntegrationCode::IntegrationNetwork,
                due_at_millis: self.runtime.now_millis(),
            });
        }
        self.save(&mut record)?;
        let request = self.request(&record, step);
        drop(permit);
        self.runtime.reach(IntegrationHook::TargetReserved);
        self.runtime.reach(IntegrationHook::AfterPhaseAdmission);
        if step == IntegrationStep::Push {
            self.runtime.reach(IntegrationHook::AfterPushIntent);
            self.runtime.reach(IntegrationHook::BeforePush);
        }
        if self.state.load(record.task_id)?.is_none_or(|r| {
            r.snapshot.revision != record.snapshot.revision
                || (r.tombstone.is_some()
                    && !matches!(step, IntegrationStep::Fetch | IntegrationStep::Repair))
        }) {
            self.state.release(&reservation)?;
            return self
                .state
                .load(record.task_id)?
                .map(|r| r.snapshot)
                .ok_or_else(integration_unavailable);
        }
        if self.revoke_superseded(&mut record)? {
            return Ok(record.snapshot);
        }
        let response = self.host.execute(&request);
        // Reservation remains durable on a process crash, and only confirmed absence reclaims it.
        let applied = (|| {
            match response {
                Ok(response) => {
                    response.validate_for(&request)?;
                    if step == IntegrationStep::Fetch {
                        self.runtime.reach(IntegrationHook::AfterFetchBeforePin);
                    }
                    if step == IntegrationStep::Build {
                        self.runtime.reach(IntegrationHook::AfterCommitBeforePin);
                    }
                    if step == IntegrationStep::Push {
                        self.runtime.reach(IntegrationHook::AfterPushBeforeReceipt);
                    }
                    self.apply_response(&mut record, step, response)?;
                }
                Err(error) => {
                    self.apply_response(
                        &mut record,
                        step,
                        HostIntegrationResponse::Blocked {
                            identity: IntegrationResponseIdentity::for_request(&request),
                            code: Self::host_error_code(&error),
                            retry_exhausted: false,
                        },
                    )?;
                }
            }
            Ok(())
        })();
        if let Err(error) = applied {
            self.release_failed_phase(record.task_id, &reservation)?;
            return Err(error);
        }
        if record.actor.is_some() {
            self.release(&mut record)?;
            self.save(&mut record)?;
        }
        if !matches!(
            record.snapshot.state,
            IntegrationStatus::Blocked
                | IntegrationStatus::Integrated
                | IntegrationStatus::Revoked
                | IntegrationStatus::Parked
        ) && let IntegrationDriveAdmission::Park(pause) =
            self.runtime.begin_phase(&self.key(&record, phase))?
        {
            self.park(&mut record, pause)?;
        }
        Ok(record.snapshot)
    }
    fn host_error_code(error: &WorkerError) -> IntegrationCode {
        let code = error.public_code();
        IntegrationCode::ALL
            .iter()
            .copied()
            .find(|known| known.as_str() == code)
            .unwrap_or(IntegrationCode::IntegrationStateInvalid)
    }
    fn record_candidate(
        &self,
        record: &mut IntegrationRecord,
        candidate: IntegrationCandidate,
    ) -> Result<(), WorkerError> {
        if candidate.source_head != record.snapshot.source_head
            || candidate.identity != record.git_identity
            || candidate.id.integration_id != record.snapshot.integration_id
            || candidate.id.epoch != record.snapshot.epoch
        {
            return Err(IntegrationCode::IntegrationStateInvalid.error());
        }
        if let Some(old) = record.candidates.iter_mut().find(|c| c.id == candidate.id) {
            if candidate.id.attempt != record.snapshot.attempts {
                return Err(IntegrationCode::IntegrationStateInvalid.error());
            }
            let mut completed = old.clone();
            if completed.tree_oid.is_none() {
                completed.tree_oid = candidate.tree_oid.clone();
            }
            if completed.merge_oid.is_none() {
                completed.merge_oid = candidate.merge_oid.clone();
            }
            if completed != candidate {
                return Err(IntegrationCode::IntegrationStateInvalid.error());
            }
            *old = candidate;
        } else {
            if candidate.id.attempt < record.snapshot.attempts
                || record
                    .candidates
                    .iter()
                    .any(|old| old.id.attempt >= candidate.id.attempt)
            {
                return Err(IntegrationCode::IntegrationStateInvalid.error());
            }
            if record.candidates.len() >= MAX_CANDIDATES {
                return Err(IntegrationCode::IntegrationTargetMovedExhausted.error());
            }
            if candidate.id.attempt > record.snapshot.attempts {
                Self::invalidate_candidate(record, candidate.id.attempt);
            }
            record.candidates.push(candidate);
        }
        self.runtime.reach(IntegrationHook::AfterTargetPin);
        Ok(())
    }
    fn invalidate_candidate(record: &mut IntegrationRecord, attempt: u8) {
        record.snapshot.attempts = attempt;
        record.snapshot.state = IntegrationStatus::Fetching;
        record.snapshot.merge_oid = None;
        record.snapshot.observed_target_oid = None;
        record.snapshot.verification = IntegrationVerification::SourceAgentReportOnly;
        record.phase_retries.clear();
        record.push_intent = None;
        record.admission_deadline_millis = None;
        record.remaining_admission_millis = None;
    }
    fn apply_response(
        &self,
        record: &mut IntegrationRecord,
        step: IntegrationStep,
        response: HostIntegrationResponse,
    ) -> Result<(), WorkerError> {
        match response {
            HostIntegrationResponse::CandidateReady { candidate, .. } => {
                self.record_candidate(record, *candidate)?;
                record.snapshot.merge_oid =
                    record.candidates.last().and_then(|c| c.merge_oid.clone());
                record.snapshot.observed_target_oid =
                    record.candidates.last().map(|c| c.target_head.clone());
                record.snapshot.state = if step == IntegrationStep::AcceptTurn {
                    IntegrationStatus::Fetching
                } else {
                    IntegrationStatus::CommitReady
                };
                if step == IntegrationStep::AcceptTurn {
                    record.ready_at_millis = self.runtime.now_millis().saturating_add(1);
                }
                if step == IntegrationStep::Fetch
                    && let Some(p) = &mut record.push_intent
                {
                    p.uncertain = false;
                }
                self.save(record)?;
                if step == IntegrationStep::Build {
                    self.runtime.reach(IntegrationHook::AfterMergePin);
                }
            }
            HostIntegrationResponse::NeedTurn {
                candidate, purpose, ..
            } => {
                if purpose == IntegrationTurnPurpose::Verify
                    && record.policy.verify == VerifyPolicy::Never
                {
                    return self.block(record, IntegrationCode::IntegrationStateInvalid);
                }
                self.record_candidate(record, *candidate)?;
                record.snapshot.state = match purpose {
                    IntegrationTurnPurpose::Resolve => IntegrationStatus::Resolving,
                    IntegrationTurnPurpose::Verify => IntegrationStatus::Verifying,
                };
                self.save(record)?;
                if step == IntegrationStep::Prepare {
                    self.runtime.reach(IntegrationHook::AfterWorkspaceManifest);
                    self.runtime.reach(IntegrationHook::DuringWorkspacePrepare);
                    self.prepare_auxiliary(record, purpose)?;
                }
            }
            HostIntegrationResponse::TargetMoved { .. } => {
                if record.snapshot.attempts as usize >= MAX_CANDIDATES {
                    return self.block(record, IntegrationCode::IntegrationTargetMovedExhausted);
                }
                Self::invalidate_candidate(record, record.snapshot.attempts + 1);
                self.save(record)?;
            }
            HostIntegrationResponse::Integrated { receipt, .. } => {
                self.validate_receipt(record, &receipt)?;
                let mut receipt = receipt;
                if let Some(old) = &record.receipt {
                    receipt.imported = old.imported;
                    if &receipt != old {
                        return Err(IntegrationCode::IntegrationStateInvalid.error());
                    }
                }
                record.snapshot.state = IntegrationStatus::Published;
                record.snapshot.merge_oid = receipt.merge_oid.clone();
                record.snapshot.observed_target_oid = Some(receipt.target_head.clone());
                record.snapshot.disposition = Some(receipt.disposition);
                record.receipt = Some(receipt);
                self.save(record)?;
                self.runtime.reach(IntegrationHook::AfterReceipt);
            }
            HostIntegrationResponse::Blocked {
                code,
                retry_exhausted,
                ..
            } => {
                let phase = match step {
                    IntegrationStep::Fetch => IntegrationPhase::Fetch,
                    IntegrationStep::Prepare => IntegrationPhase::Prepare,
                    IntegrationStep::AcceptTurn => IntegrationPhase::AcceptTurn,
                    IntegrationStep::Build => IntegrationPhase::Build,
                    IntegrationStep::Push => IntegrationPhase::Push,
                    IntegrationStep::Repair => IntegrationPhase::Repair,
                };
                // Err and typed Blocked share pause, retry and resolver budgets.
                if let IntegrationDriveAdmission::Park(pause) =
                    self.runtime.begin_phase(&self.key(record, phase))?
                {
                    return self.park(record, pause);
                }
                if code == IntegrationCode::IntegrationResolutionIncomplete
                    && record.snapshot.resolve_turns < MAX_RESOLVE_TURNS
                {
                    self.prepare_auxiliary(record, IntegrationTurnPurpose::Resolve)?;
                } else {
                    record.snapshot.retry_exhausted = retry_exhausted;
                    self.retry(
                        record,
                        phase,
                        if code == IntegrationCode::IntegrationResolutionIncomplete {
                            IntegrationCode::IntegrationConflictBudgetExhausted
                        } else {
                            code
                        },
                    )?;
                }
            }
            HostIntegrationResponse::Progress {
                snapshot: Some(snapshot),
                ..
            } => {
                // Public hints cannot overwrite owner counters, fences or private evidence.
                if snapshot.source_head != record.snapshot.source_head
                    || snapshot.source_turn_id != record.snapshot.source_turn_id
                {
                    return Err(IntegrationCode::IntegrationStateInvalid.error());
                }
            }
            _ => return Err(IntegrationCode::IntegrationStateInvalid.error()),
        }
        Ok(())
    }
    fn prepare_auxiliary(
        &self,
        record: &mut IntegrationRecord,
        purpose: IntegrationTurnPurpose,
    ) -> Result<(), WorkerError> {
        let ordinal = match purpose {
            IntegrationTurnPurpose::Resolve => record.snapshot.resolve_turns.checked_add(1),
            IntegrationTurnPurpose::Verify => record.snapshot.verify_turns.checked_add(1),
        }
        .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
        if ordinal
            > match purpose {
                IntegrationTurnPurpose::Resolve => MAX_RESOLVE_TURNS,
                IntegrationTurnPurpose::Verify => MAX_VERIFY_TURNS,
            }
        {
            return self.block(record, IntegrationCode::IntegrationConflictBudgetExhausted);
        }
        let facts = self.observer.facts(record.task_id)?;
        if !self.owner_source_is_current(record, &facts)? {
            self.revoke_for_stop(record.task_id, &record.snapshot)?;
            *record = self
                .state
                .load(record.task_id)?
                .ok_or_else(integration_unavailable)?;
            return Ok(());
        }
        if record.followups_spent >= facts.ordinary.meta().limits().max_followups {
            return self.block(record, IntegrationCode::IntegrationFollowupLimit);
        }
        let turn = auxiliary_turn_id(
            record.snapshot.integration_id,
            record.snapshot.epoch,
            record.snapshot.attempts,
            purpose,
            ordinal,
        )?;
        let prepared = match self.state.load_prepared(record.task_id, turn)? {
            Some(prepared) => {
                prepared.validate_for(record)?;
                prepared
            }
            None => match PreparedIntegrationTurn::prepare(
                &facts.ordinary,
                record,
                purpose,
                record.snapshot.attempts,
                ordinal,
            ) {
                Ok(p) => p,
                Err(e) if e.public_code() == "FOLLOWUP_LIMIT" => {
                    return self.block(record, IntegrationCode::IntegrationFollowupLimit);
                }
                Err(e) => return Err(e),
            },
        };
        self.state.publish_prepared(record.task_id, &prepared)?;
        let intent = prepared.intent()?;
        if !record
            .auxiliaries
            .iter()
            .any(|a| a.turn_id == intent.turn_id)
        {
            record.auxiliaries.push(intent);
            record.followups_spent = record.followups_spent.saturating_add(1);
            match purpose {
                IntegrationTurnPurpose::Resolve => record.snapshot.resolve_turns = ordinal,
                IntegrationTurnPurpose::Verify => record.snapshot.verify_turns = ordinal,
            }
        }
        record.snapshot.state = match purpose {
            IntegrationTurnPurpose::Resolve => IntegrationStatus::Resolving,
            IntegrationTurnPurpose::Verify => IntegrationStatus::Verifying,
        };
        record.admission_deadline_millis = None;
        record.remaining_admission_millis = None;
        self.release(record)?;
        self.save(record)?;
        self.runtime.reach(IntegrationHook::AfterAuxPrepared);
        Ok(())
    }
    fn drive_auxiliary(
        &self,
        mut record: IntegrationRecord,
        facts: &IntegrationTaskFacts,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        let index = record.auxiliaries.len() - 1;
        let auxiliary = record.auxiliaries[index].clone();
        let prepared = self
            .state
            .load_prepared(record.task_id, auxiliary.turn_id)?
            .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
        prepared.validate_for(&record)?;
        let observation = self.turns.observe(auxiliary.turn_id)?;
        observation.validate()?;
        if observation.turn_id != auxiliary.turn_id
            || auxiliary
                .queue_position
                .is_some_and(|p| observation.queue_position != Some(p))
            || (auxiliary.accepted && !observation.accepted)
            || (auxiliary.completed && !observation.completed)
        {
            return Err(IntegrationCode::IntegrationStateInvalid.error());
        }
        record.auxiliaries[index].queue_position = observation.queue_position;
        record.auxiliaries[index].accepted = observation.accepted;
        record.auxiliaries[index].completed = observation.completed;
        if observation.accepted {
            record.admission_deadline_millis = None;
            record.remaining_admission_millis = None;
        }
        if observation.completed {
            self.save(&mut record)?;
            self.runtime.reach(IntegrationHook::AfterAuxCompleted);
            if facts.runner_present {
                return Ok(record.snapshot);
            }
            if facts.ordinary.status().turns().last().is_none_or(|t| {
                t.turn_id() != auxiliary.turn_id
                    || t.terminal().is_none()
                    || t.outcome() != Some(&TaskOutcome::Done)
            }) {
                self.block(&mut record, IntegrationCode::IntegrationResolveBlocked)?;
                return Ok(record.snapshot);
            }
            if let Some(code) = auxiliary_checks(
                &record.source_checks,
                facts.ordinary.status().reported_checks(),
            ) {
                self.block(&mut record, code)?;
                return Ok(record.snapshot);
            }
            record.snapshot.verification = match auxiliary.purpose {
                IntegrationTurnPurpose::Resolve => IntegrationVerification::ResolveAgentReport,
                IntegrationTurnPurpose::Verify => IntegrationVerification::VerifyAgentReport,
            };
            return self.host_phase(record, IntegrationStep::AcceptTurn);
        }
        self.runtime.reach(IntegrationHook::BeforeAuxAdmission);
        let Some(permit) = self.permit(&mut record, IntegrationPhase::AuxiliaryAdmission)? else {
            return Ok(record.snapshot);
        };
        if record.admission_deadline_millis.is_none() && !observation.accepted {
            record.snapshot.updated_at_millis = self.runtime.now_millis();
            record.admission_deadline_millis = Some(
                self.runtime
                    .now_millis()
                    .saturating_add(AUXILIARY_ADMISSION_MILLIS),
            );
        }
        if record
            .admission_deadline_millis
            .is_some_and(|d| d <= self.runtime.now_millis())
        {
            drop(permit);
            self.block(&mut record, IntegrationCode::IntegrationTurnQueueTimeout)?;
            return Ok(record.snapshot);
        }
        self.release(&mut record)?;
        self.save(&mut record)?;
        drop(permit);
        self.runtime.reach(IntegrationHook::AfterPhaseAdmission);
        if self.revoke_superseded(&mut record)? {
            return Ok(record.snapshot);
        }
        if observation.queue_position.is_none() {
            if self.turns.enqueue(&prepared)? != auxiliary.turn_id {
                return Err(IntegrationCode::IntegrationStateInvalid.error());
            }
            self.runtime.reach(IntegrationHook::AfterAuxPrompt);
            self.runtime.reach(IntegrationHook::AfterAuxCas);
            self.runtime.reach(IntegrationHook::AfterAuxEnqueue);
            // Native publication/launch valves durably record their own gate
            // and budget observations. Do not overwrite that revision.
            record = self
                .state
                .load(record.task_id)?
                .ok_or_else(integration_unavailable)?;
            prepared.validate_for(&record)?;
        }
        let observation = self.turns.observe(auxiliary.turn_id)?;
        observation.validate()?;
        record.auxiliaries[index].queue_position = observation.queue_position;
        record.auxiliaries[index].accepted = observation.accepted;
        record.auxiliaries[index].completed = observation.completed;
        if observation.accepted {
            record.admission_deadline_millis = None;
            self.runtime.reach(IntegrationHook::AfterAuxAccepted);
        }
        self.save(&mut record)?;
        if let IntegrationDriveAdmission::Park(pause) = self
            .runtime
            .begin_phase(&self.key(&record, IntegrationPhase::AuxiliaryAdmission))?
        {
            self.park(&mut record, pause)?;
        }
        Ok(record.snapshot)
    }
    fn validate_receipt(
        &self,
        record: &IntegrationRecord,
        receipt: &IntegrationReceipt,
    ) -> Result<(), WorkerError> {
        receipt.validate()?;
        if receipt.integration_id != record.snapshot.integration_id
            || receipt.epoch != record.snapshot.epoch
            || receipt.source_turn_id != record.snapshot.source_turn_id
            || receipt.source_head != record.snapshot.source_head
        {
            return Err(IntegrationCode::IntegrationStateInvalid.error());
        }
        Ok(())
    }
    fn finish_receipt(
        &self,
        mut record: IntegrationRecord,
        closed: bool,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        let receipt = record
            .receipt
            .clone()
            .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
        let task = record.task_id;
        if closed {
            let Some(response) = self.closed_observation(&mut record, IntegrationStep::Repair)?
            else {
                return Ok(record.snapshot);
            };
            if let HostIntegrationResponse::Integrated {
                receipt: repaired, ..
            } = response
            {
                self.validate_receipt(&record, &repaired)?;
                let mut expected = receipt.clone();
                expected.imported = repaired.imported;
                if repaired != expected {
                    return Err(IntegrationCode::IntegrationStateInvalid.error());
                }
            } else {
                return Err(IntegrationCode::IntegrationStateInvalid.error());
            }
        } else {
            let revision = record.snapshot.revision;
            let snapshot = self.host_phase(record, IntegrationStep::Repair)?;
            if snapshot.state != IntegrationStatus::Published || snapshot.revision == revision {
                return Ok(snapshot);
            }
            record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
            // Repair and owner import are separate admissions. An acknowledged drain
            // during transport permits the outcome, but cannot chain another phase.
            let Some(permit) = self.permit(&mut record, IntegrationPhase::Repair)? else {
                return Ok(record.snapshot);
            };
            self.save(&mut record)?;
            drop(permit);
        }
        let imported = self.turns.import_receipt(record.task_id, &receipt)?;
        self.validate_receipt(&record, &imported)?;
        let mut expected = receipt;
        expected.imported = true;
        if imported != expected {
            return Err(IntegrationCode::IntegrationStateInvalid.error());
        }
        record.receipt = Some(imported.clone());
        self.save(&mut record)?;
        self.runtime.reach(IntegrationHook::AfterOwnerImport);
        if record.policy.requested_close == crate::task::ClosePolicy::Done && !closed {
            let Some(permit) = self.permit(&mut record, IntegrationPhase::Repair)? else {
                return Ok(record.snapshot);
            };
            self.save(&mut record)?;
            drop(permit);
            self.turns.close_integrated(record.task_id, &imported)?;
            self.runtime.reach(IntegrationHook::AfterClose);
        }
        record.snapshot.state = IntegrationStatus::Integrated;
        record.snapshot.resume_state = None;
        record.snapshot.pause_reason = None;
        record.pause = None;
        record.snapshot.blocked_code = None;
        record.snapshot.retry_at_millis = None;
        self.release(&mut record)?;
        self.save(&mut record)?;
        Ok(record.snapshot)
    }
    fn closed_observation(
        &self,
        record: &mut IntegrationRecord,
        step: IntegrationStep,
    ) -> Result<Option<HostIntegrationResponse>, WorkerError> {
        let started = record
            .phase_retries
            .iter()
            .any(|retry| retry.phase == IntegrationPhase::Drive);
        if !closed_observation_pending(record)
            || (started
                && record
                    .snapshot
                    .retry_at_millis
                    .is_some_and(|d| d > self.runtime.now_millis()))
        {
            return Ok(None);
        }
        // Drive is the Closed observation budget. Host Repair keeps its Open
        // transport history, and cannot spend or finish this later settlement.
        let phase = IntegrationPhase::Drive;
        record.snapshot.state = if step == IntegrationStep::Repair {
            IntegrationStatus::Published
        } else {
            IntegrationStatus::Fetching
        };
        record.snapshot.resume_state = None;
        record.snapshot.retry_at_millis = None;
        record.snapshot.blocked_code = None;
        record.snapshot.retry_exhausted = false;
        record.snapshot.pause_reason = None;
        record.pause = None;
        if !record.phase_retries.iter().any(|r| r.phase == phase) {
            record.phase_retries.push(IntegrationPhaseRetry {
                phase,
                retries: 0,
                code: IntegrationCode::IntegrationWorkerOffline,
                due_at_millis: self.runtime.now_millis(),
            });
        }
        self.save(record)?;
        let request = self.request(record, step);
        match self.host.execute(&request) {
            Ok(response) => {
                response.validate_for(&request)?;
                if let HostIntegrationResponse::Blocked {
                    code,
                    retry_exhausted,
                    ..
                } = response
                {
                    record.snapshot.retry_exhausted = retry_exhausted;
                    self.retry(record, phase, code)?;
                    Ok(None)
                } else {
                    Ok(Some(response))
                }
            }
            Err(error) => {
                self.retry(record, phase, Self::host_error_code(&error))?;
                Ok(None)
            }
        }
    }
    fn settle_closed(
        &self,
        mut record: IntegrationRecord,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        let now = self.runtime.now_millis();
        let started = record
            .phase_retries
            .iter()
            .any(|retry| retry.phase == IntegrationPhase::Drive);
        if !closed_observation_pending(&record)
            || (started && record.snapshot.retry_at_millis.is_some_and(|due| due > now))
        {
            return Ok(record.snapshot);
        }
        // D-R9 bypasses launch admission, never target serialization or the
        // shared Git cap. Retain this slot through host Repair and owner import.
        let actor = self.runtime.actor();
        let Some(reservation) = self.state.reserve(
            &record.target_key,
            record.snapshot.integration_id,
            record.snapshot.epoch,
            actor,
        )?
        else {
            let due = now.saturating_add(TRANSPORT_RETRY_DELAYS_MILLIS[0]);
            if !started {
                record.phase_retries.push(IntegrationPhaseRetry {
                    phase: IntegrationPhase::Drive,
                    retries: 0,
                    code: IntegrationCode::IntegrationWorkerOffline,
                    due_at_millis: due,
                });
            }
            record.snapshot.state = IntegrationStatus::RetryWait;
            record.snapshot.resume_state = Some(IntegrationStatus::Published);
            record.snapshot.retry_at_millis = Some(due);
            record.snapshot.pause_reason = None;
            record.pause = None;
            self.save(&mut record)?;
            return Ok(record.snapshot);
        };
        let task = record.task_id;
        record.actor = Some(actor);
        let applied = (|| {
            self.save(&mut record)?;
            self.runtime.reach(IntegrationHook::TargetReserved);
            self.settle_closed_reserved(record)
        })();
        // Release the known reservation even when response validation, import
        // or state persistence fails. Clearing the saved actor is a separate CAS.
        self.state.release(&reservation)?;
        let mut latest = self.state.load(task)?.ok_or_else(integration_unavailable)?;
        if latest.actor == Some(actor)
            && latest.snapshot.integration_id == reservation.integration_id
            && latest.snapshot.epoch == reservation.epoch
        {
            latest.actor = None;
            self.save(&mut latest)?;
        }
        applied.map(|_| latest.snapshot)
    }
    fn settle_closed_reserved(
        &self,
        mut record: IntegrationRecord,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        if record.receipt.is_some() {
            return self.finish_receipt(record, true);
        }
        if let Some(response) = self.closed_observation(&mut record, IntegrationStep::Repair)? {
            if let HostIntegrationResponse::Integrated { receipt, .. } = response {
                self.validate_receipt(&record, &receipt)?;
                record.snapshot.merge_oid = receipt.merge_oid.clone();
                record.snapshot.observed_target_oid = Some(receipt.target_head.clone());
                record.snapshot.disposition = Some(receipt.disposition);
                record.receipt = Some(receipt);
                record.snapshot.state = IntegrationStatus::Published;
                record.snapshot.resume_state = None;
                record.snapshot.pause_reason = None;
                record.pause = None;
                self.save(&mut record)?;
                self.finish_receipt(record, true)
            } else {
                self.block(&mut record, IntegrationCode::IntegrationWorkspaceMissing)?;
                Ok(record.snapshot)
            }
        } else {
            Ok(record.snapshot)
        }
    }
    pub fn redrive(
        &self,
        task: TaskId,
        expected: IntegrationRevision,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        let record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
        if record.snapshot.revision != expected {
            return Err(WorkerError::task(
                "TASK_REVISION_CONFLICT",
                "integration revision changed",
            ));
        }
        if record.snapshot.state == IntegrationStatus::Integrated {
            return Ok(record.snapshot);
        }
        self.redrive_record(record, false)
    }
    pub(crate) fn resume_redrive(
        &self,
        task: TaskId,
        intent: IntegrationId,
        epoch: u32,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        let record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
        if record.snapshot.integration_id != intent || record.snapshot.epoch != epoch {
            return Err(WorkerError::task(
                "TASK_REVISION_CONFLICT",
                "integration cycle changed",
            ));
        }
        self.redrive_record(record, true)
    }
    fn redrive_record(
        &self,
        record: IntegrationRecord,
        resuming: bool,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        let task = record.task_id;
        let facts = self.observer.facts(task)?;
        if facts.ordinary.status().state() != TaskState::Open
            || facts.stop_requested
            || facts.close_pending
            || record.snapshot.blocked_code
                == Some(IntegrationCode::IntegrationDependencyNotIntegrated)
            || !facts.ordinary.status().turns().iter().any(|turn| {
                turn.turn_id() == record.snapshot.source_turn_id
                    && turn.outcome() == Some(&TaskOutcome::Done)
            })
        {
            return Err(IntegrationCode::IntegrationDependencyNotIntegrated.error());
        }
        let stopped = resuming
            && record.snapshot.state == IntegrationStatus::Revoked
            && record.tombstone.as_ref().is_some_and(|t| t.acknowledged);
        if record.snapshot.state != IntegrationStatus::Blocked && !stopped {
            return Err(WorkerError::task("TASK_BUSY", "INTEGRATION_IN_PROGRESS"));
        }
        if !stopped {
            self.revoke_for_stop(task, &record.snapshot)?;
        }
        let mut record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
        if record.snapshot.state != IntegrationStatus::Revoked
            || record.tombstone.as_ref().is_none_or(|t| !t.acknowledged)
        {
            return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
        }
        record.snapshot.epoch = record
            .snapshot
            .epoch
            .checked_add(1)
            .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
        record.snapshot.state = IntegrationStatus::Pending;
        record.snapshot.attempts = 0;
        record.snapshot.resolve_turns = 0;
        record.snapshot.verify_turns = 0;
        record.snapshot.blocked_code = None;
        record.snapshot.retry_exhausted = false;
        record.snapshot.retry_at_millis = None;
        record.snapshot.merge_oid = None;
        record.snapshot.observed_target_oid = None;
        record.snapshot.disposition = None;
        record.candidates.clear();
        record.auxiliaries.clear();
        record.push_intent = None;
        record.receipt = None;
        record.tombstone = None;
        record.phase_retries.clear();
        record.remaining_admission_millis = None;
        record.remaining_backoff_millis = None;
        record.admission_deadline_millis = None;
        record.ready_at_millis = self.runtime.now_millis();
        self.save(&mut record)?;
        Ok(record.snapshot)
    }
    /// A stop targets the observed cycle by its integration id and epoch, not
    /// by its revision or phase. Auxiliary retirement, or a driver admitting
    /// the next phase (Pending to Fetching, say), can move the record after the
    /// caller read it; neither may skip the tombstone or turn the stop into a
    /// CAS error. A new epoch or cycle is a different stop: unconfirmed.
    pub(crate) fn revoke_for_stop(
        &self,
        task: TaskId,
        expected: &IntegrationSnapshot,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        for _ in 0..3 {
            let record = self
                .state
                .load(task)?
                .ok_or_else(|| IntegrationCode::IntegrationStopUnconfirmed.error())?;
            if record.snapshot.integration_id != expected.integration_id
                || record.snapshot.epoch != expected.epoch
            {
                return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
            }
            match self.revoke(task, record.snapshot.revision) {
                Err(error) if error.public_code() == "TASK_REVISION_CONFLICT" => continue,
                result => return result,
            }
        }
        Err(IntegrationCode::IntegrationStopUnconfirmed.error())
    }

    /// Rebase only onto the same durable stop request. In particular, an old
    /// host acknowledgement cannot acknowledge a replacement epoch/tombstone.
    fn save_stop_update(
        &self,
        mut record: IntegrationRecord,
        update: impl Fn(&mut IntegrationRecord) -> Result<(), WorkerError>,
    ) -> Result<IntegrationRecord, WorkerError> {
        let id = record.snapshot.integration_id;
        let tombstone = record
            .tombstone
            .clone()
            .ok_or_else(|| IntegrationCode::IntegrationStopUnconfirmed.error())?;
        for attempt in 0..=3 {
            if record.snapshot.integration_id != id
                || record.snapshot.epoch != tombstone.epoch
                || record.tombstone.as_ref().is_none_or(|current| {
                    current.epoch != tombstone.epoch
                        || current.revision != tombstone.revision
                        || current.requested_at_millis != tombstone.requested_at_millis
                        || (tombstone.acknowledged && !current.acknowledged)
                })
            {
                return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
            }
            let before = record.clone();
            update(&mut record)?;
            if record == before {
                return Ok(record);
            }
            if attempt == 3 {
                break;
            }
            self.release(&mut record)?;
            match self.save(&mut record) {
                Ok(()) => return Ok(record),
                Err(error) if error.public_code() == "TASK_REVISION_CONFLICT" => {
                    record = self
                        .state
                        .load(record.task_id)?
                        .ok_or_else(|| IntegrationCode::IntegrationStopUnconfirmed.error())?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(IntegrationCode::IntegrationStopUnconfirmed.error())
    }

    pub fn revoke(
        &self,
        task: TaskId,
        expected: IntegrationRevision,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        let mut record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
        if record.snapshot.revision != expected {
            return Err(WorkerError::task(
                "TASK_REVISION_CONFLICT",
                "integration revision changed",
            ));
        }
        if record.snapshot.state == IntegrationStatus::Integrated {
            return Err(IntegrationCode::IntegrationAlreadyCommitted.error());
        }
        if record.tombstone.as_ref().is_some_and(|t| t.acknowledged) {
            return Ok(record.snapshot);
        }
        if record.tombstone.is_none() {
            record.tombstone = Some(IntegrationTombstone {
                epoch: record.snapshot.epoch,
                revision: record.snapshot.revision.next()?,
                requested_at_millis: self.runtime.now_millis(),
                acknowledged: false,
            });
            self.save(&mut record)?;
            self.runtime.reach(IntegrationHook::AfterRevoke);
        }
        let tombstone = record
            .tombstone
            .clone()
            .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
        let request = HostIntegrationRequest {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            task_id: task,
            integration_id: Some(record.snapshot.integration_id),
            epoch: tombstone.epoch,
            revision: tombstone.revision,
            action: HostIntegrationAction::Revoke { tombstone },
        };
        let response = match self.host.execute(&request) {
            Ok(response) => response,
            Err(error)
                if error.public_code() == IntegrationCode::IntegrationStateInvalid.as_str() =>
            {
                let facts = self.source_observer.facts(task)?;
                if facts.ordinary.meta().task_id() != task
                    || !facts
                        .ordinary
                        .status()
                        .turns()
                        .iter()
                        .any(|turn| turn.turn_id() == record.snapshot.source_turn_id)
                    || self.covers_latest_ordinary_work(&facts.ordinary, &record.snapshot)?
                {
                    return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
                }
                // The owner durably advanced and the host structurally refuses
                // this obsolete identity. Settle supersession without inventing
                // a host stop acknowledgement or retrying the retired source.
                record.tombstone = None;
                record.snapshot.state = IntegrationStatus::Revoked;
                record.snapshot.blocked_code = Some(IntegrationCode::IntegrationStateInvalid);
                record.snapshot.resume_state = None;
                record.snapshot.pause_reason = None;
                record.pause = None;
                record.admission_deadline_millis = None;
                record.snapshot.retry_at_millis = None;
                self.release(&mut record)?;
                self.save(&mut record)?;
                return Ok(record.snapshot);
            }
            Err(_) => return Err(IntegrationCode::IntegrationStopUnconfirmed.error()),
        };
        response.validate_for(&request)?;
        self.runtime.reach(IntegrationHook::BeforeRevokeAck);
        match response {
            HostIntegrationResponse::Revoked { .. } => {
                let record = self.save_stop_update(record, |record| {
                    if record.snapshot.state == IntegrationStatus::Integrated {
                        return Err(IntegrationCode::IntegrationAlreadyCommitted.error());
                    }
                    record
                        .tombstone
                        .as_mut()
                        .ok_or_else(|| IntegrationCode::IntegrationStopUnconfirmed.error())?
                        .acknowledged = true;
                    record.snapshot.state = IntegrationStatus::Revoked;
                    record.snapshot.resume_state = None;
                    record.snapshot.pause_reason = None;
                    record.pause = None;
                    record.admission_deadline_millis = None;
                    record.snapshot.retry_at_millis = None;
                    Ok(())
                })?;
                self.runtime.reach(IntegrationHook::AfterRevokeAck);
                Ok(record.snapshot)
            }
            HostIntegrationResponse::Integrated { receipt, .. } => {
                self.validate_receipt(&record, &receipt)?;
                record
                    .tombstone
                    .as_mut()
                    .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?
                    .acknowledged = true;
                record.snapshot.merge_oid = receipt.merge_oid.clone();
                record.snapshot.observed_target_oid = Some(receipt.target_head.clone());
                record.snapshot.disposition = Some(receipt.disposition);
                record.receipt = Some(receipt);
                // Origin may already contain this cycle when a newer owner
                // turn supersedes it. Retain that proof without importing over
                // the new work or recursively admitting another Repair.
                if !self.owner_source_is_current(&record, &self.source_observer.facts(task)?)? {
                    record.snapshot.state = IntegrationStatus::Revoked;
                    record.snapshot.resume_state = None;
                    record.snapshot.pause_reason = None;
                    record.pause = None;
                    record.admission_deadline_millis = None;
                    record.snapshot.retry_at_millis = None;
                    self.release(&mut record)?;
                    self.save(&mut record)?;
                    self.runtime.reach(IntegrationHook::AfterRevokeAck);
                    return Ok(record.snapshot);
                }
                record.snapshot.state = IntegrationStatus::Published;
                self.release(&mut record)?;
                self.save(&mut record)?;
                self.finish_receipt(record, false)?;
                Err(IntegrationCode::IntegrationAlreadyCommitted.error())
            }
            _ => Err(IntegrationCode::IntegrationStopUnconfirmed.error()),
        }
    }
}

pub(crate) fn persist_record(
    state: &dyn IntegrationState,
    runtime: &dyn IntegrationRuntime,
    record: &mut IntegrationRecord,
) -> Result<(), WorkerError> {
    let expected = record.snapshot.revision;
    record.snapshot.revision = expected.next()?;
    record.snapshot.updated_at_millis = runtime.now_millis();
    if !state.replace(record.task_id, expected, record)? {
        return Err(WorkerError::task(
            "TASK_REVISION_CONFLICT",
            "integration changed before publication",
        ));
    }
    runtime.reach(IntegrationHook::AfterStateBeforeEvent);
    Ok(())
}
pub(crate) fn extend_elapsed_pauses(
    state: &dyn IntegrationState,
    runtime: &dyn IntegrationRuntime,
    paths: &crate::paths::PathLayout,
    record: &mut IntegrationRecord,
) -> Result<(), WorkerError> {
    extend_elapsed_pauses_through(state, runtime, paths, record, runtime.now_millis())
}
fn extend_elapsed_pauses_through(
    state: &dyn IntegrationState,
    runtime: &dyn IntegrationRuntime,
    paths: &crate::paths::PathLayout,
    record: &mut IntegrationRecord,
    through: u64,
) -> Result<(), WorkerError> {
    if record.pause.is_some()
        || (record.admission_deadline_millis.is_none() && record.snapshot.retry_at_millis.is_none())
    {
        return Ok(());
    }
    let extra = crate::controller::drain::elapsed_pause_time(
        &paths.controller_state_root(),
        record.snapshot.updated_at_millis,
        through,
    )?;
    if extra != 0 {
        record.admission_deadline_millis = record
            .admission_deadline_millis
            .map(|d| d.saturating_add(extra));
        record.snapshot.retry_at_millis = record
            .snapshot
            .retry_at_millis
            .map(|d| d.saturating_add(extra));
        persist_record(state, runtime, record)?;
    }
    Ok(())
}
pub(crate) fn park_record(
    state: &dyn IntegrationState,
    runtime: &dyn IntegrationRuntime,
    paths: Option<&crate::paths::PathLayout>,
    record: &mut IntegrationRecord,
    pause: IntegrationPauseEvidence,
) -> Result<(), WorkerError> {
    if let Some(previous) = record.pause {
        if previous.reason == pause.reason {
            return Ok(());
        }
        if pause.reason == IntegrationPauseReason::HelperUnavailable
            && matches!(
                previous.reason,
                IntegrationPauseReason::ControllerDrained
                    | IntegrationPauseReason::ControllerDisabled
            )
        {
            // The persisted global gate may have reopened before this helper
            // observation. Spend that active interval, then park the remainder.
            let Some(paths) = paths else {
                return Ok(());
            };
            if crate::controller::drain::resumed_at(
                &paths.controller_state_root(),
                previous.effective_at_millis,
            )?
            .is_none()
            {
                return Ok(());
            }
            resume_record(state, runtime, Some(paths), record)?;
        } else {
            // A newly closed global valve supersedes helper unavailability;
            // both intervals stay paused and the saved remainder is unchanged.
            record.pause = Some(pause);
            record.snapshot.pause_reason = Some(pause.reason);
            return persist_record(state, runtime, record);
        }
    }
    if matches!(
        record.snapshot.state,
        IntegrationStatus::Blocked | IntegrationStatus::Integrated | IntegrationStatus::Revoked
    ) {
        return Ok(());
    }
    if let Some(paths) = paths {
        extend_elapsed_pauses_through(state, runtime, paths, record, pause.effective_at_millis)?;
    }
    record.remaining_admission_millis = record
        .admission_deadline_millis
        .take()
        .map(|d| {
            d.saturating_sub(pause.effective_at_millis)
                .min(AUXILIARY_ADMISSION_MILLIS)
        })
        .or(record.remaining_admission_millis);
    record.remaining_backoff_millis = record
        .snapshot
        .retry_at_millis
        .take()
        .map(|d| d.saturating_sub(pause.effective_at_millis).min(30000))
        .or(record.remaining_backoff_millis);
    if record.snapshot.state != IntegrationStatus::RetryWait {
        record.snapshot.resume_state = Some(record.snapshot.state);
    }
    record.snapshot.state = IntegrationStatus::Parked;
    record.snapshot.pause_reason = Some(pause.reason);
    record.pause = Some(pause);
    if let Some(actor) = record.actor.take() {
        state.release(&TargetReservation {
            key: record.target_key.clone(),
            integration_id: record.snapshot.integration_id,
            epoch: record.snapshot.epoch,
            actor,
        })?;
    }
    persist_record(state, runtime, record)?;
    runtime.reach(IntegrationHook::AfterPark);
    Ok(())
}
pub(crate) fn resume_record(
    state: &dyn IntegrationState,
    runtime: &dyn IntegrationRuntime,
    paths: Option<&crate::paths::PathLayout>,
    record: &mut IntegrationRecord,
) -> Result<(), WorkerError> {
    let mut now = runtime.now_millis();
    if let Some(paths) = paths
        && let Some(pause) = &record.pause
        && matches!(
            pause.reason,
            IntegrationPauseReason::ControllerDrained | IntegrationPauseReason::ControllerDisabled
        )
        && let Some(end) = crate::controller::drain::resumed_at(
            &paths.controller_state_root(),
            pause.effective_at_millis,
        )?
    {
        now = end.saturating_add(crate::controller::drain::elapsed_pause_time(
            &paths.controller_state_root(),
            end,
            now,
        )?);
    }
    record.snapshot.state = record
        .snapshot
        .resume_state
        .take()
        .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
    record.pause = None;
    record.snapshot.pause_reason = None;
    if let Some(remaining) = record.remaining_admission_millis.take() {
        record.admission_deadline_millis = Some(now.saturating_add(remaining));
    }
    if let Some(remaining) = record.remaining_backoff_millis.take() {
        record.snapshot.resume_state = Some(record.snapshot.state);
        record.snapshot.state = IntegrationStatus::RetryWait;
        record.snapshot.retry_at_millis = Some(now.saturating_add(remaining));
    } else {
        record.snapshot.retry_at_millis = None;
    }
    persist_record(state, runtime, record)
}

fn auxiliary_checks(
    source: &[crate::agent::ReportedCheck],
    checks: &[crate::agent::ReportedCheck],
) -> Option<IntegrationCode> {
    use crate::agent::ReportedCheckStatus::*;
    if checks.iter().any(|c| matches!(c.status(), Fail | Error)) {
        Some(IntegrationCode::IntegrationChecksFailed)
    } else if !source.is_empty() && (checks.is_empty() || checks.iter().any(|c| c.status() != Pass))
    {
        Some(IntegrationCode::IntegrationChecksNotRun)
    } else {
        None
    }
}

impl PreparedIntegrationTurn {
    pub fn prepare(
        ordinary: &crate::task::LocalTaskRecord,
        integration: &IntegrationRecord,
        purpose: IntegrationTurnPurpose,
        attempt: u8,
        ordinal: u8,
    ) -> Result<Self, WorkerError> {
        integration.validate()?;
        let candidate = integration
            .candidates
            .iter()
            .find(|c| c.id.attempt == attempt)
            .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?;
        if ordinary.meta().task_id() != integration.task_id
            || ordinary.status().head_oid() != Some(&integration.snapshot.source_head)
            || ordinary.auto_continue_intent().is_some()
        {
            return Err(IntegrationCode::IntegrationStateInvalid.error());
        }
        let boundary = RedactionBoundary::from_env();
        let paths = candidate
            .conflict_paths
            .iter()
            .map(|p| {
                serde_json::to_string(&boundary.text(p, MAX_CONFLICT_PATH_BYTES))
                    .map_err(|_| IntegrationCode::IntegrationStateInvalid.error())
            })
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");
        validate_conflict_paths(&candidate.conflict_paths)?;
        let instruction = match purpose {
            IntegrationTurnPurpose::Resolve => format!("Resolve this task's integration into the configured origin branch.\nThe host prepared a merge: HEAD (ours) is the task result; MERGE_HEAD is the updated target.\nConflicted repository-relative paths (JSON strings, redacted):\n{paths}\nInspect Git's unmerged-path list if a displayed path was redacted.\nResolve every unmerged path, including deletions. Remove conflict markers."),
            IntegrationTurnPurpose::Verify => "Verify this task's clean merge onto a moved origin target.\nThis is read-only verification. Do not edit files or Git metadata. The host compares the index and worktree with the pinned candidate before and after this turn.".into(),
        };
        let check_rule = if integration.source_checks.is_empty() {
            "The source reported no checks; an empty checks list is allowed. Any fail/error blocks integration."
        } else {
            "The source reported checks; report at least one, all pass. A missing/not_run check blocks integration. Any fail/error blocks integration."
        };
        let message = format!(
            "{instruction}\nTask: {}; source turn: {}; attempt: {attempt}/3.\nRead-only git status, diff and log commands are fine.\nDo not commit, stage Git metadata, switch branches, rebase, squash or push. The host will stage and commit.\nRun applicable project checks inside this turn and report commands/results truthfully.\n{check_rule}\nReturn done only when the applicable rule succeeds; otherwise return blocked.\nThe host will not run project checks.",
            integration.task_id, integration.snapshot.source_turn_id
        );
        validate_prompt(&message)?;
        let turn = auxiliary_turn_id(
            integration.snapshot.integration_id,
            integration.snapshot.epoch,
            attempt,
            purpose,
            ordinal,
        )?;
        let followup = crate::prepared_followup::PreparedFollowup::prepare(
            ordinary,
            message,
            turn,
            candidate.timestamp_millis,
        )?;
        if followup.composed_prompt().len() > MAX_PROMPT_BYTES {
            return Err(IntegrationCode::IntegrationConflictListTooLarge.error());
        }
        let limits = &ordinary.meta().limits().turn;
        let prepared = Self {
            integration_id: integration.snapshot.integration_id,
            epoch: integration.snapshot.epoch,
            attempt,
            purpose,
            ordinal,
            followup,
            workspace_binding: IntegrationWorkspaceBinding {
                task_id: integration.task_id,
                candidate: candidate.id,
                branch: candidate.clean_h.branch.clone(),
                head: candidate.source_head.clone(),
                merge_head: candidate.target_head.clone(),
                attribute_source: candidate.attribute_source.clone(),
                ours: candidate.ours.clone(),
                theirs: candidate.theirs.clone(),
                pinned_tree: if purpose == IntegrationTurnPurpose::Verify {
                    candidate.tree_oid.clone()
                } else {
                    None
                },
                clean_h: candidate.clean_h.clone(),
            },
            approved_turn_limits: crate::agent::TurnLimits::new(
                limits.timeout_millis.min(AUXILIARY_ADMISSION_MILLIS),
                limits.max_turns,
                limits.max_budget_usd_cents,
            )?,
        };
        prepared.validate_for(integration)?;
        Ok(prepared)
    }
}

#[cfg(test)]
mod stop_race_tests {
    use super::*;
    use crate::{integration::testing::*, job::ProcessIdentity};
    use std::sync::Mutex;

    struct AckRace<'a> {
        clock: ManualIntegrationRuntime,
        at: Option<IntegrationHook>,
        action: Mutex<Option<Box<dyn FnOnce() + Send + 'a>>>,
    }
    impl AckRace<'_> {
        fn run_action(&self) {
            let action = self.action.lock().unwrap().take();
            if let Some(action) = action {
                action();
            }
        }
    }
    impl IntegrationRuntime for AckRace<'_> {
        fn now_millis(&self) -> u64 {
            if self.at.is_none() {
                self.run_action();
            }
            self.clock.now_millis()
        }
        fn actor(&self) -> ProcessIdentity {
            self.clock.actor()
        }
        fn actor_verdict(
            &self,
            actor: ProcessIdentity,
        ) -> crate::client_state::RunnerLivenessVerdict {
            self.clock.actor_verdict(actor)
        }
        fn begin_phase(
            &self,
            key: &IntegrationPhaseKey,
        ) -> Result<IntegrationDriveAdmission, WorkerError> {
            self.clock.begin_phase(key)
        }
        fn reach(&self, hook: IntegrationHook) {
            if self.at == Some(hook) {
                self.run_action();
            }
        }
    }

    struct RevokeHost;
    impl IntegrationHost for RevokeHost {
        fn execute(
            &self,
            request: &HostIntegrationRequest,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            request.validate()?;
            assert!(matches!(
                request.action,
                HostIntegrationAction::Revoke { .. }
            ));
            Ok(HostIntegrationResponse::Revoked {
                identity: IntegrationResponseIdentity::for_request(request),
            })
        }
    }

    fn auxiliary(
        state: &MemoryIntegrationState,
        turns: &FakeIntegrationTurns,
    ) -> IntegrationRecord {
        let mut record = sample_record(fixture_task(), fixture_source(), "main");
        record.snapshot.state = IntegrationStatus::Resolving;
        record.snapshot.attempts = 1;
        record.snapshot.resolve_turns = 1;
        record.followups_spent = 1;
        record.candidates.push(sample_candidate(&record));
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
        let turn = turns.enqueue(&prepared).unwrap();
        let mut intent = prepared.intent().unwrap();
        intent.queue_position = turns.queue_position(turn);
        intent.accepted = true;
        record.auxiliaries.push(intent);
        state
            .publish_policy(record.task_id, &record.policy)
            .unwrap();
        state.publish_prepared(record.task_id, &prepared).unwrap();
        assert!(
            state
                .replace(record.task_id, IntegrationRevision(0), &record)
                .unwrap()
        );
        turns
            .set_observation(IntegrationTurnObservation {
                turn_id: turn,
                queue_position: turns.queue_position(turn),
                accepted: true,
                completed: true,
            })
            .unwrap();
        record
    }

    #[test]
    fn revoke_ack_reloads_after_terminal_auxiliary_save() {
        let state = MemoryIntegrationState::default();
        let turns = FakeIntegrationTurns::default();
        let record = auxiliary(&state, &turns);
        let observer = FakeIntegrationObserver::default();
        let clock = ManualIntegrationRuntime::default();
        let completion =
            IntegrationCoordinator::new(&state, &RevokeHost, &turns, &clock, &observer);
        let runtime = AckRace {
            clock: ManualIntegrationRuntime::default(),
            at: Some(IntegrationHook::BeforeRevokeAck),
            action: Mutex::new(Some(Box::new(|| {
                assert!(
                    state
                        .load(record.task_id)
                        .unwrap()
                        .unwrap()
                        .tombstone
                        .is_some()
                );
                completion
                    .on_terminal(record.task_id, record.auxiliaries[0].turn_id)
                    .unwrap();
            }))),
        };
        let coordinator =
            IntegrationCoordinator::new(&state, &RevokeHost, &turns, &runtime, &observer);
        let stopped = coordinator
            .revoke(record.task_id, record.snapshot.revision)
            .unwrap();
        let saved = state.load(record.task_id).unwrap().unwrap();
        assert_eq!(stopped, saved.snapshot);
        assert_eq!(stopped.state, IntegrationStatus::Revoked);
        assert!(saved.tombstone.unwrap().acknowledged);
        assert!(saved.auxiliaries[0].completed);
    }

    #[test]
    fn revoke_ack_accepts_another_acknowledger_of_the_same_tombstone() {
        let state = MemoryIntegrationState::default();
        let turns = FakeIntegrationTurns::default();
        let record = auxiliary(&state, &turns);
        let observer = FakeIntegrationObserver::default();
        let clock = ManualIntegrationRuntime::default();
        let other = IntegrationCoordinator::new(&state, &RevokeHost, &turns, &clock, &observer);
        let runtime = AckRace {
            clock: ManualIntegrationRuntime::default(),
            at: Some(IntegrationHook::BeforeRevokeAck),
            action: Mutex::new(Some(Box::new(|| {
                let current = state.load(record.task_id).unwrap().unwrap();
                other
                    .revoke(record.task_id, current.snapshot.revision)
                    .unwrap();
            }))),
        };
        let coordinator =
            IntegrationCoordinator::new(&state, &RevokeHost, &turns, &runtime, &observer);
        let stopped = coordinator
            .revoke(record.task_id, record.snapshot.revision)
            .unwrap();
        assert_eq!(
            stopped,
            state.load(record.task_id).unwrap().unwrap().snapshot
        );
        assert_eq!(stopped.state, IntegrationStatus::Revoked);
    }

    #[test]
    fn revoke_ack_refuses_a_removed_or_replaced_tombstone() {
        for replace in [false, true] {
            let state = MemoryIntegrationState::default();
            let turns = FakeIntegrationTurns::default();
            let record = auxiliary(&state, &turns);
            let observer = FakeIntegrationObserver::default();
            let clock = ManualIntegrationRuntime::default();
            let runtime = AckRace {
                clock: ManualIntegrationRuntime::default(),
                at: Some(IntegrationHook::BeforeRevokeAck),
                action: Mutex::new(Some(Box::new(|| {
                    let mut current = state.load(record.task_id).unwrap().unwrap();
                    if replace {
                        current.tombstone.as_mut().unwrap().requested_at_millis += 1;
                    } else {
                        current.tombstone = None;
                    }
                    persist_record(&state, &clock, &mut current).unwrap();
                }))),
            };
            let coordinator =
                IntegrationCoordinator::new(&state, &RevokeHost, &turns, &runtime, &observer);
            let error = coordinator
                .revoke(record.task_id, record.snapshot.revision)
                .unwrap_err();
            assert_eq!(error.public_code(), "INTEGRATION_STOP_UNCONFIRMED");
            assert_eq!(
                state.load(record.task_id).unwrap().unwrap().snapshot.state,
                IntegrationStatus::Resolving
            );
        }
    }

    #[test]
    fn revoke_stop_rebases_only_the_observed_cycle_after_auxiliary_retirement() {
        for changed_epoch in [false, true] {
            let state = MemoryIntegrationState::default();
            let turns = FakeIntegrationTurns::default();
            let record = auxiliary(&state, &turns);
            let observer = FakeIntegrationObserver::default();
            let clock = ManualIntegrationRuntime::default();
            let coordinator =
                IntegrationCoordinator::new(&state, &RevokeHost, &turns, &clock, &observer);
            coordinator
                .on_terminal(record.task_id, record.auxiliaries[0].turn_id)
                .unwrap();
            if changed_epoch {
                let mut next = state.load(record.task_id).unwrap().unwrap();
                next.snapshot.epoch += 1;
                next.auxiliaries.clear();
                next.candidates.clear();
                persist_record(&state, &clock, &mut next).unwrap();
            }
            let result = coordinator.revoke_for_stop(record.task_id, &record.snapshot);
            if changed_epoch {
                let error = result.unwrap_err();
                assert_eq!(error.public_code(), "INTEGRATION_STOP_UNCONFIRMED");
                assert_eq!(error.exit_code(), 69);
                assert!(
                    state
                        .load(record.task_id)
                        .unwrap()
                        .unwrap()
                        .tombstone
                        .is_none()
                );
            } else {
                assert_eq!(result.unwrap().state, IntegrationStatus::Revoked);
                let saved = state.load(record.task_id).unwrap().unwrap();
                assert!(saved.auxiliaries[0].completed);
                assert!(saved.tombstone.unwrap().acknowledged);
            }
        }
    }

    #[test]
    fn stop_after_a_phase_advance_still_revokes_the_observed_cycle() {
        let state = MemoryIntegrationState::default();
        let record = sample_record(fixture_task(), fixture_source(), "main");
        state
            .publish_policy(record.task_id, &record.policy)
            .unwrap();
        assert!(
            state
                .replace(record.task_id, IntegrationRevision(0), &record)
                .unwrap()
        );
        let turns = FakeIntegrationTurns::default();
        let observer = FakeIntegrationObserver::default();
        let clock = ManualIntegrationRuntime::default();
        let coordinator =
            IntegrationCoordinator::new(&state, &RevokeHost, &turns, &clock, &observer);
        // The CLI read Pending; a driver then admitted the Fetch phase.
        let observed = state.load(record.task_id).unwrap().unwrap().snapshot;
        assert_eq!(observed.state, IntegrationStatus::Pending);
        let mut next = state.load(record.task_id).unwrap().unwrap();
        next.snapshot.state = IntegrationStatus::Fetching;
        next.snapshot.attempts = 1;
        persist_record(&state, &clock, &mut next).unwrap();
        let stopped = coordinator
            .revoke_for_stop(record.task_id, &observed)
            .unwrap();
        let saved = state.load(record.task_id).unwrap().unwrap();
        assert_eq!(stopped, saved.snapshot);
        assert_eq!(stopped.state, IntegrationStatus::Revoked);
        assert!(saved.tombstone.unwrap().acknowledged);
    }

    #[test]
    fn cancel_given_up_save_reloads_terminal_auxiliary() {
        let state = MemoryIntegrationState::default();
        let turns = FakeIntegrationTurns::default();
        let record = auxiliary(&state, &turns);
        let observer = FakeIntegrationObserver::default();
        let clock = ManualIntegrationRuntime::default();
        let completion =
            IntegrationCoordinator::new(&state, &RevokeHost, &turns, &clock, &observer);
        completion
            .revoke(record.task_id, record.snapshot.revision)
            .unwrap();
        let mut stopped = state.load(record.task_id).unwrap().unwrap();
        stopped.snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
        persist_record(&state, &clock, &mut stopped).unwrap();
        let runtime = AckRace {
            clock: ManualIntegrationRuntime::default(),
            at: None,
            action: Mutex::new(Some(Box::new(|| {
                completion
                    .on_terminal(record.task_id, record.auxiliaries[0].turn_id)
                    .unwrap();
            }))),
        };
        let coordinator =
            IntegrationCoordinator::new(&state, &RevokeHost, &turns, &runtime, &observer);
        coordinator.mark_given_up(record.task_id).unwrap();
        let saved = state.load(record.task_id).unwrap().unwrap();
        assert!(runtime.action.lock().unwrap().is_none());
        assert!(saved.auxiliaries[0].completed);
        assert!(saved.tombstone.unwrap().acknowledged);
        assert_eq!(
            saved.snapshot.blocked_code,
            Some(IntegrationCode::IntegrationDependencyNotIntegrated)
        );
    }

    #[test]
    fn cancel_given_up_still_blocks_configured_children() {
        use crate::{
            dag::ParentGate, integration::store::RootedIntegrationState, paths::PathLayout,
        };
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let paths = PathLayout {
            config: dir.path().join("config"),
            state: dir.path().join("state"),
            cache: dir.path().join("cache"),
            data: dir.path().join("data"),
        };
        let clock = Arc::new(ManualIntegrationRuntime::default());
        let state = RootedIntegrationState::open(&paths, clock.clone()).unwrap();
        let record = sample_record(fixture_task(), fixture_source(), "main");
        state
            .publish_policy(record.task_id, &record.policy)
            .unwrap();
        assert!(
            state
                .replace(record.task_id, IntegrationRevision(0), &record)
                .unwrap()
        );
        let turns = FakeIntegrationTurns::default();
        let observer = FakeIntegrationObserver::default();
        let coordinator =
            IntegrationCoordinator::new(&state, &RevokeHost, &turns, clock.as_ref(), &observer);
        coordinator
            .revoke(record.task_id, record.snapshot.revision)
            .unwrap();
        let ordinary = sample_ordinary(record.task_id, fixture_source());
        assert_eq!(
            crate::dag::parent_gate_at(&paths.state, &ordinary).unwrap(),
            ParentGate::Waiting
        );
        coordinator.mark_given_up(record.task_id).unwrap();
        // The configured-child gate must distinguish an operator's give-up
        // from the reversible revoke used by a blocked cycle's ordinary say.
        assert_eq!(
            crate::dag::parent_gate_at(&paths.state, &ordinary).unwrap(),
            ParentGate::IntegrationFailed
        );
    }

    #[test]
    fn revoke_ack_retry_exhaustion_is_unconfirmed_after_three_saves() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Churn<'a> {
            clock: ManualIntegrationRuntime,
            state: &'a MemoryIntegrationState,
            writes: AtomicUsize,
        }
        impl IntegrationRuntime for Churn<'_> {
            fn now_millis(&self) -> u64 {
                let mut current = self.state.load(fixture_task()).unwrap().unwrap();
                if current.tombstone.is_some() {
                    current.ready_at_millis += 1;
                    persist_record(self.state, &self.clock, &mut current).unwrap();
                    self.writes.fetch_add(1, Ordering::SeqCst);
                }
                self.clock.now_millis()
            }
            fn actor(&self) -> ProcessIdentity {
                self.clock.actor()
            }
            fn actor_verdict(
                &self,
                actor: ProcessIdentity,
            ) -> crate::client_state::RunnerLivenessVerdict {
                self.clock.actor_verdict(actor)
            }
            fn begin_phase(
                &self,
                key: &IntegrationPhaseKey,
            ) -> Result<IntegrationDriveAdmission, WorkerError> {
                self.clock.begin_phase(key)
            }
            fn reach(&self, _: IntegrationHook) {}
        }
        let state = MemoryIntegrationState::default();
        let turns = FakeIntegrationTurns::default();
        let record = auxiliary(&state, &turns);
        let observer = FakeIntegrationObserver::default();
        let runtime = Churn {
            clock: ManualIntegrationRuntime::default(),
            state: &state,
            writes: AtomicUsize::new(0),
        };
        let coordinator =
            IntegrationCoordinator::new(&state, &RevokeHost, &turns, &runtime, &observer);
        let error = coordinator
            .revoke(record.task_id, record.snapshot.revision)
            .unwrap_err();
        assert_eq!(error.public_code(), "INTEGRATION_STOP_UNCONFIRMED");
        assert_eq!(error.exit_code(), 69);
        assert_eq!(runtime.writes.load(Ordering::SeqCst), 3);
        assert!(
            !state
                .load(record.task_id)
                .unwrap()
                .unwrap()
                .tombstone
                .unwrap()
                .acknowledged
        );
    }
}

#[cfg(test)]
mod source_fence_tests {
    use super::*;
    use crate::integration::testing::*;
    use crate::job::ProcessIdentity;
    use std::sync::Mutex;

    fn facts(ordinary: crate::task::LocalTaskRecord) -> IntegrationTaskFacts {
        IntegrationTaskFacts {
            cycle_base: ordinary.meta().base_oid().clone(),
            ordinary,
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

    #[derive(Default)]
    struct StopOnlyHost(Mutex<Vec<HostIntegrationRequest>>);
    impl IntegrationHost for StopOnlyHost {
        fn execute(
            &self,
            request: &HostIntegrationRequest,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            request.validate()?;
            assert!(
                matches!(request.action, HostIntegrationAction::Revoke { .. }),
                "superseded cycle reached host phase: {request:?}"
            );
            self.0.lock().unwrap().push(request.clone());
            Ok(HostIntegrationResponse::Revoked {
                identity: IntegrationResponseIdentity::for_request(request),
            })
        }
    }

    fn phase_record(step: IntegrationStep) -> IntegrationRecord {
        let mut record = sample_record(fixture_task(), fixture_source(), "main");
        record.snapshot.state = match step {
            IntegrationStep::Fetch => IntegrationStatus::Pending,
            IntegrationStep::Prepare | IntegrationStep::AcceptTurn => IntegrationStatus::Resolving,
            IntegrationStep::Build | IntegrationStep::Push => IntegrationStatus::CommitReady,
            IntegrationStep::Repair => IntegrationStatus::Published,
        };
        record.snapshot.attempts = 1;
        let candidate = sample_candidate(&record);
        if step == IntegrationStep::Repair {
            record.receipt = Some(IntegrationReceipt {
                integration_id: record.snapshot.integration_id,
                epoch: 0,
                source_turn_id: fixture_source(),
                source_head: fixture_head(),
                target_head: candidate.target_head.clone(),
                merge_oid: candidate.merge_oid.clone(),
                disposition: IntegrationDisposition::Merged,
                imported: false,
                recorded_at_millis: 1002,
            });
            record.snapshot.merge_oid = candidate.merge_oid.clone();
            record.snapshot.disposition = Some(IntegrationDisposition::Merged);
        }
        record.candidates.push(candidate);
        record
    }

    fn save_initial(state: &MemoryIntegrationState, record: &IntegrationRecord) {
        state
            .publish_policy(record.task_id, &record.policy)
            .unwrap();
        assert!(
            state
                .replace(record.task_id, IntegrationRevision(0), record)
                .unwrap()
        );
    }

    #[test]
    fn every_phase_refuses_a_newer_or_pending_ordinary_owner_before_io() {
        for step in [
            IntegrationStep::Fetch,
            IntegrationStep::Prepare,
            IntegrationStep::AcceptTurn,
            IntegrationStep::Build,
            IntegrationStep::Push,
            IntegrationStep::Repair,
        ] {
            for reason in ["new_done", "new_running", "queue"] {
                let record = phase_record(step);
                let state = MemoryIntegrationState::default();
                save_initial(&state, &record);
                let host = StopOnlyHost::default();
                let turns = FakeIntegrationTurns::default();
                let runtime = ManualIntegrationRuntime::default();
                let observer = FakeIntegrationObserver::default();
                let ordinary = match reason {
                    "new_done" => sample_ordinary_followup(
                        record.task_id,
                        fixture_source(),
                        Some(TaskOutcome::Done),
                    ),
                    "new_running" => {
                        sample_ordinary_followup(record.task_id, fixture_source(), None)
                    }
                    _ => sample_ordinary(record.task_id, fixture_source()),
                };
                let mut current = facts(ordinary);
                current.runner_present = reason != "new_done";
                observer.insert(current);
                let owner = IntegrationCoordinator::new(&state, &host, &turns, &runtime, &observer);
                let stopped = owner.host_phase(record, step).unwrap();
                assert_eq!(
                    stopped.state,
                    IntegrationStatus::Revoked,
                    "{step:?}/{reason}"
                );
                assert!(
                    state
                        .load(fixture_task())
                        .unwrap()
                        .unwrap()
                        .tombstone
                        .unwrap()
                        .acknowledged
                );
                assert_eq!(owner.drive_once(fixture_task()).unwrap(), stopped);
                assert_eq!(host.0.lock().unwrap().len(), 1);
            }
        }
    }

    struct AdvanceAfterAdmission<'a> {
        inner: ManualIntegrationRuntime,
        observer: &'a FakeIntegrationObserver,
        next: Mutex<Option<IntegrationTaskFacts>>,
    }
    impl IntegrationRuntime for AdvanceAfterAdmission<'_> {
        fn now_millis(&self) -> u64 {
            self.inner.now_millis()
        }
        fn actor(&self) -> ProcessIdentity {
            self.inner.actor()
        }
        fn actor_verdict(
            &self,
            actor: ProcessIdentity,
        ) -> crate::client_state::RunnerLivenessVerdict {
            self.inner.actor_verdict(actor)
        }
        fn begin_phase(
            &self,
            key: &IntegrationPhaseKey,
        ) -> Result<IntegrationDriveAdmission, WorkerError> {
            self.inner.begin_phase(key)
        }
        fn reach(&self, hook: IntegrationHook) {
            if hook == IntegrationHook::AfterPhaseAdmission
                && let Some(next) = self.next.lock().unwrap().take()
            {
                self.observer.insert(next);
            }
        }
    }

    #[test]
    fn refreshed_owner_fences_host_and_auxiliary_after_the_phase_permit() {
        for auxiliary in [false, true] {
            let mut record = phase_record(if auxiliary {
                IntegrationStep::Prepare
            } else {
                IntegrationStep::Push
            });
            let state = MemoryIntegrationState::default();
            let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
            if auxiliary {
                state.publish_prepared(record.task_id, &prepared).unwrap();
                record.auxiliaries.push(prepared.intent().unwrap());
                record.snapshot.resolve_turns = 1;
                record.followups_spent = 1;
            }
            save_initial(&state, &record);
            let host = StopOnlyHost::default();
            let turns = FakeIntegrationTurns::default();
            let observer = FakeIntegrationObserver::default();
            let initial = facts(sample_ordinary(record.task_id, fixture_source()));
            observer.insert(initial.clone());
            let mut next = facts(sample_ordinary_followup(
                record.task_id,
                fixture_source(),
                None,
            ));
            next.runner_present = true;
            let runtime = AdvanceAfterAdmission {
                inner: ManualIntegrationRuntime::default(),
                observer: &observer,
                next: Mutex::new(Some(next)),
            };
            let owner = IntegrationCoordinator::new(&state, &host, &turns, &runtime, &observer);
            let stopped = if auxiliary {
                owner.drive_auxiliary(record, &initial).unwrap()
            } else {
                owner.host_phase(record, IntegrationStep::Push).unwrap()
            };
            assert_eq!(stopped.state, IntegrationStatus::Revoked);
            assert_eq!(turns.enqueue_count(prepared.followup.turn_id()), 0);
            assert_eq!(host.0.lock().unwrap().len(), 1);
            assert_eq!(owner.drive_once(fixture_task()).unwrap(), stopped);
            assert_eq!(host.0.lock().unwrap().len(), 1);
        }
    }

    #[test]
    fn newer_owner_refuses_auxiliary_preparation_before_sidecar_publication() {
        let mut record = phase_record(IntegrationStep::Prepare);
        let state = MemoryIntegrationState::default();
        save_initial(&state, &record);
        let host = StopOnlyHost::default();
        let turns = FakeIntegrationTurns::default();
        let runtime = ManualIntegrationRuntime::default();
        let observer = FakeIntegrationObserver::default();
        observer.insert(facts(sample_ordinary_followup(
            record.task_id,
            fixture_source(),
            Some(TaskOutcome::Done),
        )));
        let owner = IntegrationCoordinator::new(&state, &host, &turns, &runtime, &observer);
        owner
            .prepare_auxiliary(&mut record, IntegrationTurnPurpose::Resolve)
            .unwrap();
        assert_eq!(record.snapshot.state, IntegrationStatus::Revoked);
        let turn = auxiliary_turn_id(
            record.snapshot.integration_id,
            0,
            1,
            IntegrationTurnPurpose::Resolve,
            1,
        )
        .unwrap();
        assert!(state.load_prepared(record.task_id, turn).unwrap().is_none());
        assert_eq!(turns.enqueue_count(turn), 0);
    }

    #[test]
    fn superseded_committed_reply_is_retained_without_import_or_another_phase() {
        struct CommittedStop(IntegrationReceipt, Mutex<usize>);
        impl IntegrationHost for CommittedStop {
            fn execute(
                &self,
                request: &HostIntegrationRequest,
            ) -> Result<HostIntegrationResponse, WorkerError> {
                assert!(
                    matches!(request.action, HostIntegrationAction::Revoke { .. }),
                    "superseded cycle reached another phase"
                );
                *self.1.lock().unwrap() += 1;
                Ok(HostIntegrationResponse::Integrated {
                    identity: IntegrationResponseIdentity::for_request(request),
                    receipt: self.0.clone(),
                })
            }
        }
        let record = phase_record(IntegrationStep::Repair);
        let state = MemoryIntegrationState::default();
        save_initial(&state, &record);
        let receipt = record.receipt.clone().unwrap();
        let host = CommittedStop(receipt.clone(), Mutex::new(0));
        let turns = FakeIntegrationTurns::default();
        let runtime = ManualIntegrationRuntime::default();
        let observer = FakeIntegrationObserver::default();
        let mut newer = facts(sample_ordinary_followup(
            record.task_id,
            fixture_source(),
            None,
        ));
        newer.runner_present = true;
        observer.insert(newer);
        let owner = IntegrationCoordinator::new(&state, &host, &turns, &runtime, &observer);
        let stopped = owner.drive_once(record.task_id).unwrap();
        assert_eq!(stopped.state, IntegrationStatus::Revoked);
        let saved = state.load(record.task_id).unwrap().unwrap();
        assert_eq!(saved.receipt, Some(receipt));
        assert!(saved.tombstone.unwrap().acknowledged);
        assert!(turns.imports(record.task_id).is_empty());
        assert_eq!(owner.drive_once(record.task_id).unwrap(), stopped);
        assert_eq!(*host.1.lock().unwrap(), 1);
    }

    #[test]
    fn active_resolver_and_verifier_sidecars_keep_their_own_cycle_admitted() {
        for purpose in [
            IntegrationTurnPurpose::Resolve,
            IntegrationTurnPurpose::Verify,
        ] {
            let mut record = phase_record(IntegrationStep::Prepare);
            if purpose == IntegrationTurnPurpose::Verify {
                record.policy.verify = VerifyPolicy::MovedTarget;
                record.snapshot.state = IntegrationStatus::Verifying;
                record.snapshot.verify_turns = 1;
            } else {
                record.snapshot.resolve_turns = 1;
            }
            record.followups_spent = 1;
            let prepared = sample_prepared_turn(&record, purpose, 1, 1);
            let state = MemoryIntegrationState::default();
            state.publish_prepared(record.task_id, &prepared).unwrap();
            let turns = FakeIntegrationTurns::default();
            turns.enqueue(&prepared).unwrap();
            let mut intent = prepared.intent().unwrap();
            intent.queue_position = turns.queue_position(intent.turn_id);
            record.auxiliaries.push(intent);
            save_initial(&state, &record);
            let mut current = facts(sample_ordinary(record.task_id, fixture_source()));
            let mut wire = serde_json::to_value(current.ordinary.status()).unwrap();
            wire["state"] = "active".into();
            wire["turns"].as_array_mut().unwrap().push(
                serde_json::to_value(crate::task::TurnSummary::new(
                    2,
                    prepared.followup.turn_id(),
                    None,
                    None,
                    None,
                    false,
                    Some(1002),
                    None,
                ))
                .unwrap(),
            );
            current.ordinary = current
                .ordinary
                .with_status(serde_json::from_value(wire).unwrap())
                .unwrap();
            current.runner_present = true;
            current.auxiliary_purpose = Some(purpose);
            let observer = FakeIntegrationObserver::default();
            observer.insert(current);
            let host = StopOnlyHost::default();
            let runtime = ManualIntegrationRuntime::default();
            let owner = IntegrationCoordinator::new(&state, &host, &turns, &runtime, &observer);
            assert_eq!(
                owner.drive_once(record.task_id).unwrap().state,
                record.snapshot.state
            );
            assert_eq!(turns.enqueue_count(prepared.followup.turn_id()), 1);
            assert!(host.0.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn a_retired_auxiliary_from_the_previous_epoch_does_not_supersede_its_ordinary_source() {
        struct FetchOnly;
        impl IntegrationHost for FetchOnly {
            fn execute(
                &self,
                request: &HostIntegrationRequest,
            ) -> Result<HostIntegrationResponse, WorkerError> {
                let HostIntegrationAction::Step {
                    step: IntegrationStep::Fetch,
                    record,
                } = &request.action
                else {
                    panic!(
                        "retained auxiliary history was treated as a newer ordinary source: {request:?}"
                    );
                };
                Ok(HostIntegrationResponse::CandidateReady {
                    identity: IntegrationResponseIdentity::for_request(request),
                    candidate: Box::new(sample_candidate(record)),
                })
            }
        }
        for purpose in [
            IntegrationTurnPurpose::Resolve,
            IntegrationTurnPurpose::Verify,
        ] {
            let mut previous = phase_record(IntegrationStep::Prepare);
            if purpose == IntegrationTurnPurpose::Verify {
                previous.policy.verify = VerifyPolicy::MovedTarget;
            }
            let prepared = sample_prepared_turn(&previous, purpose, 1, 1);
            let mut record = sample_record(fixture_task(), fixture_source(), "main");
            record.policy = previous.policy.clone();
            record.snapshot.epoch = 1;
            record.followups_spent = 1;
            let state = MemoryIntegrationState::default();
            state.publish_prepared(record.task_id, &prepared).unwrap();
            save_initial(&state, &record);
            let mut current = facts(sample_ordinary(record.task_id, fixture_source()));
            let mut wire = serde_json::to_value(current.ordinary.status()).unwrap();
            wire["turns"].as_array_mut().unwrap().push(
                serde_json::to_value(crate::task::TurnSummary::new(
                    2,
                    prepared.followup.turn_id(),
                    Some(crate::task::TurnTerminal::Succeeded),
                    Some(TaskOutcome::Done),
                    Some(false),
                    false,
                    Some(1002),
                    Some(1003),
                ))
                .unwrap(),
            );
            current.ordinary = current
                .ordinary
                .with_status(serde_json::from_value(wire).unwrap())
                .unwrap();
            current.auxiliary_purpose = Some(purpose);
            let observer = FakeIntegrationObserver::default();
            observer.insert(current);
            let turns = FakeIntegrationTurns::default();
            let runtime = ManualIntegrationRuntime::default();
            let owner =
                IntegrationCoordinator::new(&state, &FetchOnly, &turns, &runtime, &observer);
            let snapshot = owner.drive_once(record.task_id).unwrap();
            assert_eq!(snapshot.state, IntegrationStatus::CommitReady);
            assert_eq!(snapshot.epoch, 1);
            assert_eq!(snapshot.source_turn_id, fixture_source());
            assert_eq!(turns.enqueue_count(prepared.followup.turn_id()), 0);
        }
    }
}
