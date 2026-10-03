//! One bounded, fenced host phase. The queue owner remains the lifecycle driver.
use super::contracts::*;
use super::{
    git::{IntegrationGit, PushOutcome},
    host_store::{HostIntegrationStore, invalid},
};
use crate::{
    error::WorkerError,
    host_store::HostStore,
    process::ProcessRunner,
    task::{BranchName, ClosePolicy, TaskSource, TaskState},
    task_store::TaskStore,
};
pub struct HostIntegrationService<'a> {
    store: &'a HostStore,
    runner: &'a dyn ProcessRunner,
    runtime: &'a dyn IntegrationRuntime,
}
impl<'a> HostIntegrationService<'a> {
    pub fn new(
        store: &'a HostStore,
        runner: &'a dyn ProcessRunner,
        runtime: &'a dyn IntegrationRuntime,
    ) -> Self {
        Self {
            store,
            runner,
            runtime,
        }
    }
    pub fn execute(
        &self,
        request: &HostIntegrationRequest,
    ) -> Result<HostIntegrationResponse, WorkerError> {
        request.validate()?;
        let capacity = self.store.capacity_lock()?;
        let session = self.store.session_lock()?;
        let sidecars = HostIntegrationStore::new(self.store);
        let stored_policy;
        let policy = match &request.action {
            HostIntegrationAction::Arm { policy } => policy,
            HostIntegrationAction::Step { record, .. } => &record.policy,
            _ => {
                let project = sidecars.find_project(request.task_id)?;
                stored_policy = sidecars
                    .policy(&project, request.task_id)?
                    .ok_or_else(invalid)?;
                &stored_policy
            }
        };
        let _fence = sidecars.lock(&policy.project_id, request.task_id)?;
        let task_store = TaskStore::new(self.store, self.runner);
        let meta = task_store.load_meta(&policy.project_id, request.task_id)?;
        if meta.close_policy() != ClosePolicy::Never {
            return Err(invalid());
        }
        if matches!(meta.source(), TaskSource::Local { wip: true, .. }) {
            return Err(IntegrationCode::IntegrationWipBase.error());
        }
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(b"origin\0");
        digest.update(policy.target_key()?.origin.as_bytes());
        if format!("{:x}", digest.finalize()) != meta.project_id() {
            return Err(invalid());
        }
        if meta.publish_branch() == Some(&policy.target) {
            return Err(IntegrationCode::IntegrationPublishTargetCollision.error());
        }
        sidecars.arm(&policy.project_id, request.task_id, policy)?;
        if matches!(request.action, HostIntegrationAction::Arm { .. })
            && task_store
                .load_status(&policy.project_id, request.task_id)?
                .state()
                .is_terminal()
        {
            return Err(IntegrationCode::IntegrationWorkspaceMissing.error());
        }
        // Policy retention is durable before releasing installation/session guards.
        // Only the per-task integration fence spans bounded Git effects.
        drop(session);
        drop(capacity);
        let identity = IntegrationResponseIdentity::for_request(request);
        let response = match &request.action {
            HostIntegrationAction::Read => {
                let mut snapshot = sidecars
                    .load(&policy.project_id, request.task_id)?
                    .map(|record| record.snapshot);
                if let Some(snapshot) = &mut snapshot {
                    if Some(snapshot.integration_id) != request.integration_id
                        || snapshot.epoch != request.epoch
                        || snapshot.revision.0 > request.revision.0
                    {
                        return Err(invalid());
                    }
                    snapshot.revision = request.revision;
                }
                HostIntegrationResponse::Progress { identity, snapshot }
            }
            HostIntegrationAction::Revoke { tombstone } => {
                let mut record = sidecars
                    .load(&policy.project_id, request.task_id)?
                    .ok_or_else(invalid)?;
                if Some(record.snapshot.integration_id) != request.integration_id
                    || record.snapshot.epoch != request.epoch
                    || record.snapshot.revision.0 > request.revision.0
                {
                    return Err(invalid());
                }
                record.snapshot.revision = request.revision;
                record.tombstone = Some(tombstone.clone());
                sidecars.save(&record)?;
                self.runtime.reach(IntegrationHook::AfterRevoke);
                if task_store
                    .load_status(&policy.project_id, request.task_id)?
                    .state()
                    == TaskState::Active
                    || crate::lease::LeaseService::new(self.store)
                        .task_scope_is_live(&policy.project_id, request.task_id)?
                {
                    return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
                }
                let git = IntegrationGit::new(self.store, self.runner, self.runtime);
                if record.receipt.is_none() && record.push_intent.is_some() {
                    let candidate = record.candidates.last().ok_or_else(invalid)?;
                    record.receipt = git
                        .settle(&record, candidate)
                        .map_err(|_| IntegrationCode::IntegrationStopUnconfirmed.error())?;
                }
                if let Some(receipt) = record.receipt.clone() {
                    sidecars.save(&record)?;
                    HostIntegrationResponse::Integrated { identity, receipt }
                } else {
                    if let Some(candidate) = record.candidates.last() {
                        git.restore_source(&record, candidate)?;
                    }
                    self.runtime.reach(IntegrationHook::BeforeRevokeAck);
                    record.tombstone.as_mut().ok_or_else(invalid)?.acknowledged = true;
                    if let Some(push) = &mut record.push_intent {
                        push.uncertain = false;
                    }
                    record.snapshot.state = IntegrationStatus::Revoked;
                    record.snapshot.resume_state = None;
                    record.snapshot.pause_reason = None;
                    record.pause = None;
                    sidecars.save(&record)?;
                    self.runtime.reach(IntegrationHook::AfterRevokeAck);
                    HostIntegrationResponse::Revoked { identity }
                }
            }
            HostIntegrationAction::Arm { .. } => {
                self.runtime.reach(IntegrationHook::AfterPolicy);
                HostIntegrationResponse::Progress {
                    identity,
                    snapshot: None,
                }
            }
            HostIntegrationAction::Step { step, record } => {
                let status = task_store.load_status(&policy.project_id, request.task_id)?;
                if status.state() != TaskState::Open && *step != IntegrationStep::Repair {
                    return Err(IntegrationCode::IntegrationWorkspaceMissing.error());
                }
                if crate::lease::LeaseService::new(self.store)
                    .task_scope_is_live(&policy.project_id, request.task_id)?
                {
                    return Err(invalid());
                }
                if status.head_oid() != Some(&record.snapshot.source_head)
                    && *step != IntegrationStep::Repair
                {
                    return Err(invalid());
                }
                let mut next = (**record).clone();
                if let Some(old) = sidecars.load(&policy.project_id, request.task_id)? {
                    if old.snapshot.integration_id != next.snapshot.integration_id
                        || old.snapshot.epoch != next.snapshot.epoch
                        || old.snapshot.revision.0 > next.snapshot.revision.0
                        || old.policy != next.policy
                        || (old.tombstone.is_some() && *step != IntegrationStep::Repair)
                    {
                        return Err(invalid());
                    }
                    next.candidates = old.candidates;
                    next.snapshot.verification = old.snapshot.verification;
                    for auxiliary in old.auxiliaries {
                        if let Some(incoming) = next
                            .auxiliaries
                            .iter_mut()
                            .find(|aux| aux.turn_id == auxiliary.turn_id)
                        {
                            if incoming.prepared_binding != auxiliary.prepared_binding {
                                return Err(invalid());
                            }
                            if incoming.queue_position.is_some()
                                && auxiliary.queue_position.is_some()
                                && incoming.queue_position != auxiliary.queue_position
                            {
                                return Err(invalid());
                            }
                            if incoming.queue_position.is_none() {
                                incoming.queue_position = auxiliary.queue_position;
                            }
                            incoming.accepted |= auxiliary.accepted;
                            incoming.completed |= auxiliary.completed;
                        } else {
                            next.auxiliaries.push(auxiliary);
                        }
                    }
                    next.push_intent = old.push_intent;
                    next.receipt = old.receipt;
                    next.tombstone = old.tombstone;
                }
                sidecars.save(&next)?;
                self.runtime.reach(IntegrationHook::AfterIntent);
                let git = IntegrationGit::new(self.store, self.runner, self.runtime);
                match step {
                    IntegrationStep::Fetch | IntegrationStep::Prepare => {
                        let target = git.fetch_target(&next)?;
                        let mirror = git.mirror(policy)?;
                        if !git.is_ancestor(&mirror, &next.cycle_base, &target)? {
                            return Err(IntegrationCode::IntegrationBaseNotOnTarget.error());
                        }
                        if git.is_ancestor(&mirror, &next.snapshot.source_head, &target)? {
                            let receipt = IntegrationReceipt {
                                integration_id: next.snapshot.integration_id,
                                epoch: next.snapshot.epoch,
                                source_turn_id: next.snapshot.source_turn_id,
                                source_head: next.snapshot.source_head.clone(),
                                target_head: target,
                                merge_oid: None,
                                disposition: IntegrationDisposition::AlreadyIntegrated,
                                imported: false,
                                recorded_at_millis: self.runtime.now_millis(),
                            };
                            next.receipt = Some(receipt.clone());
                            sidecars.save(&next)?;
                            HostIntegrationResponse::Integrated { identity, receipt }
                        } else {
                            if next
                                .candidates
                                .last()
                                .is_some_and(|candidate| candidate.target_head != target)
                            {
                                if next.candidates.len() >= MAX_CANDIDATES {
                                    return Err(
                                        IntegrationCode::IntegrationTargetMovedExhausted.error()
                                    );
                                }
                                let previous = next.candidates.last().ok_or_else(invalid)?;
                                git.restore_source(&next, previous)?;
                                next.push_intent = None;
                            }
                            if next
                                .candidates
                                .last()
                                .is_none_or(|candidate| candidate.target_head != target)
                            {
                                let head = next.snapshot.source_head.clone();
                                let candidate = IntegrationCandidate {
                                    id: IntegrationCandidateId {
                                        integration_id: next.snapshot.integration_id,
                                        epoch: next.snapshot.epoch,
                                        attempt: u8::try_from(next.candidates.len() + 1)
                                            .map_err(|_| invalid())?,
                                    },
                                    target_head: target.clone(),
                                    source_head: head.clone(),
                                    tree_oid: None,
                                    merge_oid: None,
                                    message: format!(
                                        "{}\n\n{}\n\nMac-Worker-Task: {}\nMac-Worker-Turn: {}\nMac-Worker-Integration: {}",
                                        meta.title().as_str(),
                                        next.source_summary,
                                        next.task_id,
                                        next.snapshot.source_turn_id,
                                        next.snapshot.integration_id
                                    ),
                                    identity: next.git_identity.clone(),
                                    timestamp_millis: self.runtime.now_millis(),
                                    attribute_source: head.clone(),
                                    ours: head.clone(),
                                    theirs: target,
                                    clean_h: CleanHManifest {
                                        branch: BranchName::for_task(next.task_id),
                                        head: head.clone(),
                                        untracked_files: git
                                            .untracked(&git.workspace(&next)?, &head)?,
                                    },
                                    conflict_paths: vec![],
                                };
                                next.candidates.push(candidate);
                                next.snapshot.attempts = next.candidates.len() as u8;
                                sidecars.save(&next)?;
                                self.runtime.reach(IntegrationHook::AfterWorkspaceManifest);
                            }
                            let mut candidate = next.candidates.last().ok_or_else(invalid)?.clone();
                            if candidate.merge_oid.is_none() {
                                git.merge(&next, &mut candidate)?;
                            }
                            *next.candidates.last_mut().ok_or_else(invalid)? = candidate.clone();
                            next.snapshot.observed_target_oid = Some(candidate.target_head.clone());
                            next.snapshot.merge_oid = candidate.merge_oid.clone();
                            next.snapshot.state = IntegrationStatus::CommitReady;
                            let purpose = if !candidate.conflict_paths.is_empty()
                                && candidate.merge_oid.is_none()
                            {
                                Some(IntegrationTurnPurpose::Resolve)
                            } else if next.policy.verify == VerifyPolicy::MovedTarget
                                && next.snapshot.verification
                                    != IntegrationVerification::VerifyAgentReport
                                && next.snapshot.verification
                                    != IntegrationVerification::ResolveAgentReport
                                && candidate.target_head != next.cycle_base
                                && git.query(
                                    &mirror,
                                    None,
                                    &["rev-parse", &format!("{}^{{tree}}", candidate.source_head)],
                                )? != candidate.tree_oid.as_ref().ok_or_else(invalid)?.as_str()
                            {
                                Some(IntegrationTurnPurpose::Verify)
                            } else {
                                None
                            };
                            if let Some(purpose) = purpose {
                                next.snapshot.state = match purpose {
                                    IntegrationTurnPurpose::Resolve => IntegrationStatus::Resolving,
                                    IntegrationTurnPurpose::Verify => IntegrationStatus::Verifying,
                                };
                                sidecars.save(&next)?;
                                git.prepare_workspace(&next, &candidate, purpose)?;
                                HostIntegrationResponse::NeedTurn {
                                    identity,
                                    candidate: Box::new(candidate),
                                    purpose,
                                }
                            } else {
                                sidecars.save(&next)?;
                                HostIntegrationResponse::CandidateReady {
                                    identity,
                                    candidate: Box::new(candidate),
                                }
                            }
                        }
                    }
                    IntegrationStep::Push => {
                        let candidate = next.candidates.last().ok_or_else(invalid)?.clone();
                        next.push_intent = Some(IntegrationPushIntent {
                            candidate: candidate.id,
                            expected_target: candidate.target_head.clone(),
                            merge_oid: candidate.merge_oid.clone().ok_or_else(invalid)?,
                            started_at_millis: self.runtime.now_millis(),
                            uncertain: true,
                        });
                        sidecars.save(&next)?;
                        self.runtime.reach(IntegrationHook::AfterPushIntent);
                        match git.push(&next, &candidate)? {
                            PushOutcome::Integrated(receipt) => {
                                next.receipt = Some(receipt.clone());
                                next.push_intent.as_mut().ok_or_else(invalid)?.uncertain = false;
                                next.snapshot.state = IntegrationStatus::Published;
                                sidecars.save(&next)?;
                                self.runtime.reach(IntegrationHook::AfterReceipt);
                                HostIntegrationResponse::Integrated { identity, receipt }
                            }
                            PushOutcome::Moved(observed_target) => {
                                HostIntegrationResponse::TargetMoved {
                                    identity,
                                    observed_target,
                                }
                            }
                        }
                    }
                    IntegrationStep::AcceptTurn | IntegrationStep::Build => {
                        let mut candidate = next.candidates.last().ok_or_else(invalid)?.clone();
                        if *step == IntegrationStep::Build
                            && candidate.merge_oid.is_some()
                            && (next.snapshot.verification
                                == IntegrationVerification::VerifyAgentReport
                                || next.snapshot.verification
                                    == IntegrationVerification::ResolveAgentReport
                                || (candidate.conflict_paths.is_empty()
                                    && (next.policy.verify == VerifyPolicy::Never
                                        || candidate.target_head == next.cycle_base)))
                        {
                            let response = HostIntegrationResponse::CandidateReady {
                                identity,
                                candidate: Box::new(candidate),
                            };
                            response.validate_for(request)?;
                            return Ok(response);
                        }
                        let auxiliary = next
                            .auxiliaries
                            .iter()
                            .rev()
                            .find(|aux| {
                                aux.attempt == candidate.id.attempt && aux.completed && aux.accepted
                            })
                            .ok_or_else(invalid)?;
                        let bytes = sidecars
                            .read(
                                &policy.project_id,
                                request.task_id,
                                &format!("turn-{}.json", auxiliary.turn_id),
                                MAX_PREPARED_TURN_BYTES,
                            )?
                            .ok_or_else(invalid)?;
                        let prepared = decode_prepared_turn(&bytes)?;
                        prepared.validate_for(&next)?;
                        prepared.followup.validate_self_consistency()?;
                        if prepared.binding()? != auxiliary.prepared_binding
                            || prepared.followup.turn_id() != auxiliary.turn_id
                            || status.turns().last().is_none_or(|last| {
                                last.turn_id() != auxiliary.turn_id
                                    || last.terminal() != Some(crate::task::TurnTerminal::Succeeded)
                            })
                            || status.last_outcome() != Some(&crate::task::TaskOutcome::Done)
                        {
                            return Err(IntegrationCode::IntegrationResolveBlocked.error());
                        }
                        validate_checks(&next.source_checks, status.reported_checks())?;
                        let purpose = prepared.purpose;
                        git.accept_workspace(&next, &mut candidate, purpose)?;
                        *next.candidates.last_mut().ok_or_else(invalid)? = candidate.clone();
                        next.snapshot.state = IntegrationStatus::CommitReady;
                        next.snapshot.merge_oid = candidate.merge_oid.clone();
                        next.snapshot.verification = match purpose {
                            IntegrationTurnPurpose::Resolve => {
                                IntegrationVerification::ResolveAgentReport
                            }
                            IntegrationTurnPurpose::Verify => {
                                IntegrationVerification::VerifyAgentReport
                            }
                        };
                        sidecars.save(&next)?;
                        self.runtime.reach(IntegrationHook::AfterAuxCompleted);
                        HostIntegrationResponse::CandidateReady {
                            identity,
                            candidate: Box::new(candidate),
                        }
                    }
                    IntegrationStep::Repair => {
                        if next.receipt.is_none() {
                            let candidate = next.candidates.last().ok_or_else(invalid)?;
                            next.receipt = git.settle(&next, candidate)?;
                        }
                        let receipt = next
                            .receipt
                            .clone()
                            .ok_or_else(|| IntegrationCode::IntegrationWorkspaceMissing.error())?;
                        sidecars.save(&next)?;
                        self.runtime.reach(IntegrationHook::AfterReceipt);
                        git.repair(&next, &receipt)?;
                        next.snapshot.state = IntegrationStatus::Published;
                        sidecars.save(&next)?;
                        HostIntegrationResponse::Integrated { identity, receipt }
                    }
                }
            }
        };
        response.validate_for(request)?;
        Ok(response)
    }
}
fn validate_checks(
    source: &[crate::agent::ReportedCheck],
    auxiliary: &[crate::agent::ReportedCheck],
) -> Result<(), WorkerError> {
    use crate::agent::ReportedCheckStatus;
    if source.iter().chain(auxiliary).any(|check| {
        matches!(
            check.status(),
            ReportedCheckStatus::Fail | ReportedCheckStatus::Error
        )
    }) {
        return Err(IntegrationCode::IntegrationChecksFailed.error());
    }
    if !source.is_empty()
        && (auxiliary.is_empty()
            || auxiliary
                .iter()
                .any(|check| check.status() != ReportedCheckStatus::Pass))
    {
        return Err(IntegrationCode::IntegrationChecksNotRun.error());
    }
    Ok(())
}
impl IntegrationHost for HostIntegrationService<'_> {
    fn execute(
        &self,
        request: &HostIntegrationRequest,
    ) -> Result<HostIntegrationResponse, WorkerError> {
        HostIntegrationService::execute(self, request)
    }
}
