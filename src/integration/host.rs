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
        let sidecars = HostIntegrationStore::new(self.store);
        let policy = match &request.action {
            HostIntegrationAction::Arm { policy } => policy,
            HostIntegrationAction::Step { record, .. } => &record.policy,
            _ => return Err(integration_unavailable()),
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
        let identity = IntegrationResponseIdentity::for_request(request);
        let response = match &request.action {
            HostIntegrationAction::Arm { .. } => {
                self.runtime.reach(IntegrationHook::AfterPolicy);
                HostIntegrationResponse::Progress {
                    identity,
                    snapshot: None,
                }
            }
            HostIntegrationAction::Step { step, record } => {
                let status = task_store.load_status(&policy.project_id, request.task_id)?;
                if status.state() != TaskState::Open {
                    return Err(IntegrationCode::IntegrationWorkspaceMissing.error());
                }
                if crate::lease::LeaseService::new(self.store)
                    .task_scope_is_live(&policy.project_id, request.task_id)?
                {
                    return Err(invalid());
                }
                if status.head_oid() != Some(&record.snapshot.source_head) {
                    return Err(invalid());
                }
                let mut next = (**record).clone();
                if let Some(old) = sidecars.load(&policy.project_id, request.task_id)? {
                    if old.snapshot.integration_id != next.snapshot.integration_id
                        || old.snapshot.epoch != next.snapshot.epoch
                        || old.snapshot.revision.0 > next.snapshot.revision.0
                        || old.policy != next.policy
                        || old.tombstone.is_some()
                    {
                        return Err(invalid());
                    }
                    next.candidates = old.candidates;
                    next.push_intent = old.push_intent;
                    next.receipt = old.receipt;
                }
                sidecars.save(&next)?;
                self.runtime.reach(IntegrationHook::AfterIntent);
                let git = IntegrationGit::new(self.store, self.runner, self.runtime);
                match step {
                    IntegrationStep::Fetch | IntegrationStep::Prepare | IntegrationStep::Build => {
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
                            if next.candidates.is_empty() {
                                let head = next.snapshot.source_head.clone();
                                let candidate = IntegrationCandidate {
                                    id: IntegrationCandidateId {
                                        integration_id: next.snapshot.integration_id,
                                        epoch: next.snapshot.epoch,
                                        attempt: 1,
                                    },
                                    target_head: target.clone(),
                                    source_head: head.clone(),
                                    tree_oid: None,
                                    merge_oid: None,
                                    message: format!(
                                        "Integration fixture\n\n{}\n\nMac-Worker-Task: {}\nMac-Worker-Turn: {}\nMac-Worker-Integration: {}",
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
                                next.snapshot.attempts = 1;
                                sidecars.save(&next)?;
                                self.runtime.reach(IntegrationHook::AfterWorkspaceManifest);
                            }
                            let mut candidate = next.candidates.last().ok_or_else(invalid)?.clone();
                            git.merge(&next, &mut candidate)?;
                            *next.candidates.last_mut().ok_or_else(invalid)? = candidate.clone();
                            next.snapshot.observed_target_oid = Some(candidate.target_head.clone());
                            next.snapshot.merge_oid = candidate.merge_oid.clone();
                            next.snapshot.state = IntegrationStatus::CommitReady;
                            let purpose = if !candidate.conflict_paths.is_empty() {
                                Some(IntegrationTurnPurpose::Resolve)
                            } else if next.policy.verify == VerifyPolicy::MovedTarget
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
                                HostIntegrationResponse::Progress {
                                    identity,
                                    snapshot: Some(next.snapshot),
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
                    _ => return Err(integration_unavailable()),
                }
            }
            _ => return Err(integration_unavailable()),
        };
        response.validate_for(request)?;
        Ok(response)
    }
}
impl IntegrationHost for HostIntegrationService<'_> {
    fn execute(
        &self,
        request: &HostIntegrationRequest,
    ) -> Result<HostIntegrationResponse, WorkerError> {
        HostIntegrationService::execute(self, request)
    }
}
