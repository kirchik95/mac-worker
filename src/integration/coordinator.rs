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
    fn save(&self, record: &mut IntegrationRecord) -> Result<(), WorkerError> {
        let expected = record.snapshot.revision;
        record.snapshot.revision = expected.next()?;
        record.snapshot.updated_at_millis = self.runtime.now_millis();
        if !self.state.replace(record.task_id, expected, record)? {
            return Err(WorkerError::task(
                "TASK_REVISION_CONFLICT",
                "integration changed before publication",
            ));
        }
        self.runtime.reach(IntegrationHook::AfterStateBeforeEvent);
        Ok(())
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
    fn park(
        &self,
        record: &mut IntegrationRecord,
        pause: IntegrationPauseEvidence,
    ) -> Result<(), WorkerError> {
        if record.pause.is_some() {
            return Ok(());
        }
        let resume = record.snapshot.state;
        if matches!(
            resume,
            IntegrationStatus::Blocked | IntegrationStatus::Integrated | IntegrationStatus::Revoked
        ) {
            return Ok(());
        }
        record.remaining_admission_millis = record
            .admission_deadline_millis
            .take()
            .map(|deadline| {
                deadline
                    .saturating_sub(pause.effective_at_millis)
                    .min(AUXILIARY_ADMISSION_MILLIS)
            })
            .or(record.remaining_admission_millis);
        record.remaining_backoff_millis = record
            .snapshot
            .retry_at_millis
            .take()
            .map(|deadline| {
                deadline
                    .saturating_sub(pause.effective_at_millis)
                    .min(30000)
            })
            .or(record.remaining_backoff_millis);
        // RetryWait already has its actual host phase as resume_state.
        if resume != IntegrationStatus::RetryWait {
            record.snapshot.resume_state = Some(resume);
        }
        record.snapshot.state = IntegrationStatus::Parked;
        record.snapshot.pause_reason = Some(pause.reason);
        record.pause = Some(pause);
        self.release(record)?;
        self.save(record)?;
        self.runtime.reach(IntegrationHook::AfterPark);
        Ok(())
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
        match self.runtime.begin_phase(&self.key(record, phase))? {
            IntegrationDriveAdmission::Permit(p) => Ok(Some(p)),
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
        let now = self.runtime.now_millis();
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
        self.save(record)
    }
    pub fn configured(&self, task: TaskId) -> Result<bool, WorkerError> {
        Ok(self.state.load_policy(task)?.is_some())
    }
    pub fn snapshot(&self, task: TaskId) -> Result<Option<IntegrationSnapshot>, WorkerError> {
        Ok(self.state.load(task)?.map(|r| r.snapshot))
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
            return self.settle_closed(record);
        }
        if record.tombstone.as_ref().is_some_and(|t| !t.acknowledged)
            || facts.stop_requested
            || facts.close_pending
        {
            return self.revoke(task, record.snapshot.revision);
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
            r.snapshot.revision != record.snapshot.revision || r.tombstone.is_some()
        }) {
            self.state.release(&reservation)?;
            return self
                .state
                .load(record.task_id)?
                .map(|r| r.snapshot)
                .ok_or_else(integration_unavailable);
        }
        let response = self.host.execute(&request);
        // Reservation remains durable on a process crash, and only confirmed absence reclaims it.
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
                let code = match error.public_code().as_str() {
                    "INTEGRATION_UNAVAILABLE" => IntegrationCode::IntegrationUnavailable,
                    "INTEGRATION_WORKER_OFFLINE" => IntegrationCode::IntegrationWorkerOffline,
                    "INTEGRATION_NETWORK" => IntegrationCode::IntegrationNetwork,
                    _ => IntegrationCode::IntegrationStateInvalid,
                };
                // Consult effective pause evidence before retry/timeout spending.
                if let IntegrationDriveAdmission::Park(pause) =
                    self.runtime.begin_phase(&self.key(&record, phase))?
                {
                    self.park(&mut record, pause)?;
                } else {
                    self.retry(&mut record, phase, code)?;
                }
            }
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
    fn record_candidate(
        &self,
        record: &mut IntegrationRecord,
        candidate: IntegrationCandidate,
    ) -> Result<(), WorkerError> {
        if candidate.source_head != record.snapshot.source_head
            || candidate.identity != record.git_identity
            || candidate.id.attempt != record.snapshot.attempts
        {
            return Err(IntegrationCode::IntegrationStateInvalid.error());
        }
        if let Some(old) = record.candidates.iter_mut().find(|c| c.id == candidate.id) {
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
            if record.candidates.len() >= MAX_CANDIDATES {
                return Err(IntegrationCode::IntegrationTargetMovedExhausted.error());
            }
            record.candidates.push(candidate);
        }
        self.runtime.reach(IntegrationHook::AfterTargetPin);
        Ok(())
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
                record.snapshot.attempts += 1;
                record.snapshot.state = IntegrationStatus::Fetching;
                record.snapshot.merge_oid = None;
                record.snapshot.observed_target_oid = None;
                record.push_intent = None;
                record.admission_deadline_millis = None;
                record.remaining_admission_millis = None;
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
                if code == IntegrationCode::IntegrationResolutionIncomplete
                    && record.snapshot.resolve_turns < MAX_RESOLVE_TURNS
                {
                    self.prepare_auxiliary(record, IntegrationTurnPurpose::Resolve)?;
                } else {
                    record.snapshot.retry_exhausted = retry_exhausted;
                    self.retry(
                        record,
                        match step {
                            IntegrationStep::Fetch => IntegrationPhase::Fetch,
                            IntegrationStep::Prepare => IntegrationPhase::Prepare,
                            IntegrationStep::AcceptTurn => IntegrationPhase::AcceptTurn,
                            IntegrationStep::Build => IntegrationPhase::Build,
                            IntegrationStep::Push => IntegrationPhase::Push,
                            IntegrationStep::Repair => IntegrationPhase::Repair,
                        },
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
        let Some(permit) = self.permit(&mut record, IntegrationPhase::AuxiliaryAdmission)? else {
            return Ok(record.snapshot);
        };
        self.runtime.reach(IntegrationHook::BeforeAuxAdmission);
        if record.admission_deadline_millis.is_none() && !observation.accepted {
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
        if observation.queue_position.is_none() {
            if self.turns.enqueue(&prepared)? != auxiliary.turn_id {
                return Err(IntegrationCode::IntegrationStateInvalid.error());
            }
            self.runtime.reach(IntegrationHook::AfterAuxPrompt);
            self.runtime.reach(IntegrationHook::AfterAuxCas);
            self.runtime.reach(IntegrationHook::AfterAuxEnqueue);
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
            let snapshot = self.host_phase(record, IntegrationStep::Repair)?;
            if snapshot.state != IntegrationStatus::Published {
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
        if record.snapshot.state == IntegrationStatus::Blocked
            || record
                .snapshot
                .retry_at_millis
                .is_some_and(|d| d > self.runtime.now_millis())
        {
            return Ok(None);
        }
        let phase = if step == IntegrationStep::Repair {
            IntegrationPhase::Repair
        } else {
            IntegrationPhase::Fetch
        };
        record.snapshot.state = if step == IntegrationStep::Repair {
            IntegrationStatus::Published
        } else {
            IntegrationStatus::Fetching
        };
        record.snapshot.resume_state = None;
        record.snapshot.retry_at_millis = None;
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
                Ok(Some(response))
            }
            Err(_) => {
                self.retry(record, phase, IntegrationCode::IntegrationWorkerOffline)?;
                Ok(None)
            }
        }
    }
    fn settle_closed(
        &self,
        mut record: IntegrationRecord,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        if record.receipt.is_some() {
            return self.finish_receipt(record, true);
        }
        if let Some(response) = self.closed_observation(&mut record, IntegrationStep::Fetch)? {
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
        let facts = self.observer.facts(task)?;
        if facts.ordinary.status().state() != TaskState::Open
            || facts.stop_requested
            || facts.close_pending
        {
            return Err(IntegrationCode::IntegrationDependencyNotIntegrated.error());
        }
        if record.snapshot.state != IntegrationStatus::Blocked {
            return Err(WorkerError::task("TASK_BUSY", "INTEGRATION_IN_PROGRESS"));
        }
        self.revoke(task, expected)?;
        let mut record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
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
        let response = self
            .host
            .execute(&request)
            .map_err(|_| IntegrationCode::IntegrationStopUnconfirmed.error())?;
        response.validate_for(&request)?;
        self.runtime.reach(IntegrationHook::BeforeRevokeAck);
        match response {
            HostIntegrationResponse::Revoked { .. } => {
                record
                    .tombstone
                    .as_mut()
                    .ok_or_else(|| IntegrationCode::IntegrationStateInvalid.error())?
                    .acknowledged = true;
                record.snapshot.state = IntegrationStatus::Revoked;
                record.snapshot.resume_state = None;
                record.snapshot.pause_reason = None;
                record.pause = None;
                record.admission_deadline_millis = None;
                record.snapshot.retry_at_millis = None;
                self.release(&mut record)?;
                self.save(&mut record)?;
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
                record.snapshot.state = IntegrationStatus::Published;
                self.save(&mut record)?;
                self.finish_receipt(record, false)?;
                Err(IntegrationCode::IntegrationAlreadyCommitted.error())
            }
            _ => Err(IntegrationCode::IntegrationStopUnconfirmed.error()),
        }
    }
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
