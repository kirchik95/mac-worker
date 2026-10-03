//! Deterministic doubles only. These never claim real Git or RPC behavior.
use super::contracts::*;
use crate::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    client_state::RunnerLivenessVerdict,
    error::WorkerError,
    job::ProcessIdentity,
    prepared_followup::PreparedFollowup,
    task::{
        BaseOid, BranchName, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId,
        TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus,
        TurnId, TurnSummary, TurnTerminal,
    },
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

pub fn fixture_task() -> TaskId {
    TaskId::new(uuid::Uuid::from_u128(2))
}
pub fn fixture_source() -> TurnId {
    TurnId::new(uuid::Uuid::from_u128(3))
}
pub fn fixture_head() -> BaseOid {
    "a".repeat(40).parse().expect("fixture OID")
}
pub fn sample_policy(branch: &str) -> FrozenIntegrationPolicy {
    FrozenIntegrationPolicy {
        schema_version: 1,
        origin: "https://example.test/repo.git".into(),
        target: validate_integration_target(branch).expect("fixture branch"),
        verify: VerifyPolicy::Never,
        requested_close: ClosePolicy::Never,
        base_kind: IntegrationBaseKind::Committed,
        base_oid: Some("b".repeat(40).parse().expect("fixture base")),
        base_task: None,
        base_preflight: IntegrationBasePreflight::Pass,
        project_id: "a".repeat(64),
    }
}
pub fn sample_ordinary(task: TaskId, source: TurnId) -> LocalTaskRecord {
    let meta = TaskMeta::new(TaskMetaInput {
        session_import: None,
        task_id: task,
        run_id: None,
        project_id: "a".repeat(64),
        worktree_id: "b".repeat(64),
        agent: AgentKind::Codex,
        model: Some("fixture-model".into()),
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: "b".repeat(40).parse().expect("fixture base"),
        limits: TaskLimits::new(
            TurnLimits::new(2_700_000, None, None).expect("fixture limits"),
            10,
        )
        .expect("fixture limits"),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("mac-worker", "mac-worker@localhost")
            .expect("fixture identity"),
        title: Some("Integration fixture".into()),
        prompt: "Do the fixture work".into(),
        created_at_millis: 1000,
    })
    .expect("fixture meta");
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        Some("fixture-worker".into()),
        true,
        Some(fixture_head()),
        Some("Fixture work completed".into()),
        vec![],
        vec![],
        None,
        vec![TurnSummary::new(
            1,
            source,
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            Some(true),
            false,
            Some(1000),
            Some(1001),
        )],
        1001,
    )
    .expect("fixture status");
    LocalTaskRecord::new(
        meta,
        status,
        Some(1001),
        None,
        Some(fixture_head()),
        "c".repeat(64),
        Some("fixture-worker".into()),
        true,
        None,
    )
    .expect("fixture ordinary record")
}
/// New ordinary work after an integrated Open/Never cycle; None is running.
pub fn sample_ordinary_followup(
    task: TaskId,
    source: TurnId,
    outcome: Option<TaskOutcome>,
) -> LocalTaskRecord {
    let prior = sample_ordinary(task, source);
    let terminal = outcome.as_ref().map(|outcome| match outcome {
        TaskOutcome::Cancelled => TurnTerminal::Cancelled,
        TaskOutcome::Failed { .. } => TurnTerminal::Failed,
        _ => TurnTerminal::Succeeded,
    });
    let mut turns = prior.status().turns().to_vec();
    turns.push(TurnSummary::new(
        2,
        TurnId::new(uuid::Uuid::from_u128(8)),
        terminal,
        outcome.clone(),
        terminal.map(|_| false),
        false,
        Some(1002),
        terminal.map(|_| 1003),
    ));
    let questions = if outcome == Some(TaskOutcome::NeedsInput) {
        vec!["Which follow-up option?".into()]
    } else {
        vec![]
    };
    let head = "e".repeat(40).parse().expect("fixture integrated head");
    let status = TaskStatus::new(
        if outcome.is_none() {
            TaskState::Active
        } else {
            TaskState::Open
        },
        outcome,
        Some("fixture-worker".into()),
        true,
        Some(head),
        Some("New ordinary follow-up".into()),
        questions,
        vec![],
        None,
        turns,
        1003,
    )
    .expect("fixture follow-up status");
    prior
        .with_status(status)
        .unwrap()
        .with_fetched_head(Some("e".repeat(40).parse().unwrap()))
        .unwrap()
}
pub fn sample_record(task: TaskId, source: TurnId, branch: &str) -> IntegrationRecord {
    let policy = sample_policy(branch);
    let target_key = policy.target_key().expect("fixture target");
    let id = IntegrationId::derive(task, source, &fixture_head(), &target_key).expect("fixture id");
    IntegrationRecord {
        schema_version: 1,
        task_id: task,
        cycle_base: policy.base_oid.clone().expect("fixture base"),
        policy,
        target_key,
        snapshot: IntegrationSnapshot {
            schema_version: 1,
            integration_id: id,
            epoch: 0,
            revision: IntegrationRevision(1),
            target: public_target_display(
                branch,
                &crate::redaction::RedactionBoundary::new("/fixture/home"),
            ),
            state: IntegrationStatus::Pending,
            resume_state: None,
            pause_reason: None,
            source_turn_id: source,
            source_head: fixture_head(),
            merge_oid: None,
            observed_target_oid: None,
            disposition: None,
            attempts: 0,
            resolve_turns: 0,
            verify_turns: 0,
            blocked_code: None,
            retry_exhausted: false,
            retry_at_millis: None,
            verification: IntegrationVerification::SourceAgentReportOnly,
            updated_at_millis: 1001,
        },
        source_revision: "d".repeat(64),
        source_summary: "Fixture work completed".into(),
        source_checks: vec![],
        git_identity: GitIdentity::new("mac-worker", "mac-worker@localhost")
            .expect("fixture identity"),
        actor: None,
        candidates: vec![],
        auxiliaries: vec![],
        archived_receipts: vec![],
        push_intent: None,
        receipt: None,
        tombstone: None,
        pause: None,
        remaining_admission_millis: None,
        admission_deadline_millis: None,
        remaining_backoff_millis: None,
        phase_retries: vec![],
        ready_at_millis: 1001,
        run_position: 0,
        followups_spent: 0,
    }
}
pub fn sample_candidate(record: &IntegrationRecord) -> IntegrationCandidate {
    IntegrationCandidate {
        id: IntegrationCandidateId {
            integration_id: record.snapshot.integration_id,
            epoch: record.snapshot.epoch,
            attempt: 1,
        },
        target_head: record.cycle_base.clone(),
        source_head: record.snapshot.source_head.clone(),
        tree_oid: Some("c".repeat(40).parse().expect("fixture tree")),
        merge_oid: Some("e".repeat(40).parse().expect("fixture merge")),
        message: "Fixture integration".into(),
        identity: record.git_identity.clone(),
        timestamp_millis: 1001,
        attribute_source: record.snapshot.source_head.clone(),
        ours: record.snapshot.source_head.clone(),
        theirs: record.cycle_base.clone(),
        clean_h: CleanHManifest {
            branch: BranchName::for_task(record.task_id),
            head: record.snapshot.source_head.clone(),
            untracked_files: vec![],
        },
        conflict_paths: vec![],
    }
}
pub fn sample_prepared_turn(
    record: &IntegrationRecord,
    purpose: IntegrationTurnPurpose,
    attempt: u8,
    ordinal: u8,
) -> PreparedIntegrationTurn {
    let ordinary = sample_ordinary(record.task_id, record.snapshot.source_turn_id);
    let turn = auxiliary_turn_id(
        record.snapshot.integration_id,
        record.snapshot.epoch,
        attempt,
        purpose,
        ordinal,
    )
    .expect("fixture auxiliary");
    let followup =
        PreparedFollowup::prepare(&ordinary, "Resolve fixture integration".into(), turn, 1002)
            .expect("fixture preparation");
    PreparedIntegrationTurn {
        integration_id: record.snapshot.integration_id,
        epoch: record.snapshot.epoch,
        attempt,
        purpose,
        ordinal,
        followup,
        workspace_binding: IntegrationWorkspaceBinding {
            task_id: record.task_id,
            candidate: IntegrationCandidateId {
                integration_id: record.snapshot.integration_id,
                epoch: record.snapshot.epoch,
                attempt,
            },
            branch: BranchName::for_task(record.task_id),
            head: fixture_head(),
            merge_head: record.cycle_base.clone(),
            attribute_source: fixture_head(),
            ours: fixture_head(),
            theirs: record.cycle_base.clone(),
            pinned_tree: Some("c".repeat(40).parse().expect("fixture tree")),
            clean_h: CleanHManifest {
                branch: BranchName::for_task(record.task_id),
                head: fixture_head(),
                untracked_files: vec![],
            },
        },
        approved_turn_limits: TurnLimits::new(600_000, None, None).expect("fixture limits"),
    }
}

#[derive(Default)]
struct MemoryStateData {
    policies: HashMap<TaskId, FrozenIntegrationPolicy>,
    records: HashMap<TaskId, IntegrationRecord>,
    preparations: HashMap<(TaskId, TurnId), PreparedIntegrationTurn>,
    reservations: HashMap<Vec<u8>, TargetReservation>,
}
#[derive(Default)]
pub struct MemoryIntegrationState {
    data: Mutex<MemoryStateData>,
}
impl IntegrationState for MemoryIntegrationState {
    fn publish_prepared(
        &self,
        task: TaskId,
        prepared: &PreparedIntegrationTurn,
    ) -> Result<(), WorkerError> {
        prepared.validate()?;
        if task != prepared.followup.task_id() {
            return Err(integration_error("INTEGRATION_STATE_INVALID"));
        }
        let turn = prepared.followup.turn_id();
        let mut data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        if let Some(old) = data.preparations.get(&(task, turn)) {
            if old != prepared {
                return Err(integration_error("INTEGRATION_STATE_INVALID"));
            }
            return Ok(());
        }
        if let Some(record) = data.records.get(&task) {
            prepared.validate_for(record)?;
            if let Some(intent) = record
                .auxiliaries
                .iter()
                .find(|intent| intent.turn_id == turn)
            {
                validate_prepared_reference(prepared, intent, record)?;
            }
        }
        data.preparations.insert((task, turn), prepared.clone());
        Ok(())
    }
    fn load_prepared(
        &self,
        task: TaskId,
        turn: TurnId,
    ) -> Result<Option<PreparedIntegrationTurn>, WorkerError> {
        let data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        let Some(prepared) = data.preparations.get(&(task, turn)) else {
            return Ok(None);
        };
        prepared.validate()?;
        if let Some(record) = data.records.get(&task)
            && let Some(intent) = record
                .auxiliaries
                .iter()
                .find(|intent| intent.turn_id == turn)
        {
            validate_prepared_reference(prepared, intent, record)?;
        }
        Ok(Some(prepared.clone()))
    }
    fn load(&self, task: TaskId) -> Result<Option<IntegrationRecord>, WorkerError> {
        Ok(self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?
            .records
            .get(&task)
            .cloned())
    }
    fn load_policy(&self, task: TaskId) -> Result<Option<FrozenIntegrationPolicy>, WorkerError> {
        Ok(self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?
            .policies
            .get(&task)
            .cloned())
    }
    fn publish_policy(
        &self,
        task: TaskId,
        policy: &FrozenIntegrationPolicy,
    ) -> Result<(), WorkerError> {
        policy.validate()?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        if let Some(old) = data.policies.get(&task) {
            if old != policy {
                return Err(integration_error("INTEGRATION_STATE_INVALID"));
            }
        } else {
            data.policies.insert(task, policy.clone());
        }
        Ok(())
    }
    fn replace(
        &self,
        task: TaskId,
        expected: IntegrationRevision,
        next: &IntegrationRecord,
    ) -> Result<bool, WorkerError> {
        next.validate()?;
        if next.task_id != task || next.snapshot.revision != expected.next()? {
            return Err(integration_error("INTEGRATION_STATE_INVALID"));
        }
        let mut data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        if data
            .records
            .get(&task)
            .map_or(IntegrationRevision(0), |r| r.snapshot.revision)
            != expected
        {
            return Ok(false);
        }
        if data.policies.get(&task).is_some_and(|p| p != &next.policy) {
            return Err(integration_error("INTEGRATION_STATE_INVALID"));
        }
        for intent in &next.auxiliaries {
            if let Some(prepared) = data.preparations.get(&(task, intent.turn_id)) {
                validate_prepared_reference(prepared, intent, next)?;
            }
        }
        data.records.insert(task, next.clone());
        Ok(true)
    }
    fn reserve(
        &self,
        key: &TargetKey,
        id: IntegrationId,
        epoch: u32,
        actor: ProcessIdentity,
    ) -> Result<Option<TargetReservation>, WorkerError> {
        let reservation = TargetReservation {
            key: key.clone(),
            integration_id: id,
            epoch,
            actor,
        };
        reservation.validate()?;
        let key = key.canonical_bytes()?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        if let Some(existing) = data.reservations.get(&key) {
            return Ok((existing == &reservation).then(|| existing.clone()));
        }
        if data.reservations.len() >= MAX_GIT_DRIVERS {
            return Ok(None);
        }
        data.reservations.insert(key, reservation.clone());
        Ok(Some(reservation))
    }
    fn release(&self, reservation: &TargetReservation) -> Result<(), WorkerError> {
        reservation.validate()?;
        let key = reservation.key.canonical_bytes()?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        if data
            .reservations
            .get(&key)
            .is_some_and(|r| r != reservation)
        {
            return Err(integration_error("INTEGRATION_STATE_INVALID"));
        }
        data.reservations.remove(&key);
        Ok(())
    }
    fn due(&self, now_millis: u64, limit: usize) -> Result<Vec<TaskId>, WorkerError> {
        let data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        let mut rows: Vec<_> = data
            .records
            .values()
            .filter(|r| {
                !matches!(
                    r.snapshot.state,
                    IntegrationStatus::Armed
                        | IntegrationStatus::Integrated
                        | IntegrationStatus::Blocked
                        | IntegrationStatus::Revoked
                ) && r.ready_at_millis <= now_millis
                    && r.snapshot
                        .retry_at_millis
                        .is_none_or(|due| due <= now_millis)
            })
            .collect();
        rows.sort_by_key(|r| (r.ready_at_millis, r.run_position, r.task_id.to_string()));
        Ok(rows.into_iter().take(limit).map(|r| r.task_id).collect())
    }
}
fn validate_prepared_reference(
    prepared: &PreparedIntegrationTurn,
    intent: &IntegrationAuxiliaryIntent,
    record: &IntegrationRecord,
) -> Result<(), WorkerError> {
    prepared.validate_for(record)?;
    let mut frozen = intent.clone();
    frozen.queue_position = None;
    frozen.accepted = false;
    frozen.completed = false;
    if frozen != prepared.intent()? {
        return Err(integration_error("INTEGRATION_STATE_INVALID"));
    }
    Ok(())
}
#[derive(Default)]
struct TurnsData {
    entries: HashMap<TurnId, PreparedIntegrationTurn>,
    observations: HashMap<TurnId, IntegrationTurnObservation>,
    imports: HashMap<TaskId, Vec<IntegrationReceipt>>,
    closes: HashMap<TaskId, IntegrationReceipt>,
    accepted_heads: HashMap<TaskId, BaseOid>,
    sequence: u64,
}
#[derive(Default)]
pub struct FakeIntegrationTurns {
    data: Mutex<TurnsData>,
}
impl FakeIntegrationTurns {
    /// Inject retained runner evidence without changing the queue ticket or undoing completion.
    pub fn set_observation(
        &self,
        observation: IntegrationTurnObservation,
    ) -> Result<(), WorkerError> {
        observation.validate()?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        let old = data
            .observations
            .get(&observation.turn_id)
            .ok_or_else(|| integration_error("INTEGRATION_STATE_INVALID"))?;
        if old.queue_position != observation.queue_position
            || (old.accepted && !observation.accepted)
            || (old.completed && !observation.completed)
        {
            return Err(integration_error("INTEGRATION_STATE_INVALID"));
        }
        data.observations.insert(observation.turn_id, observation);
        Ok(())
    }
    pub fn observations(&self) -> Vec<IntegrationTurnObservation> {
        let mut observations: Vec<_> = self
            .data
            .lock()
            .expect("fixture queue")
            .observations
            .values()
            .cloned()
            .collect();
        observations.sort_by_key(|entry| (entry.queue_position, entry.turn_id.to_string()));
        observations
    }
    pub fn imports(&self, task: TaskId) -> Vec<IntegrationReceipt> {
        self.data
            .lock()
            .expect("fixture task effects")
            .imports
            .get(&task)
            .cloned()
            .unwrap_or_default()
    }
    pub fn closes(&self, task: TaskId) -> Vec<IntegrationReceipt> {
        self.data
            .lock()
            .expect("fixture task effects")
            .closes
            .get(&task)
            .cloned()
            .into_iter()
            .collect()
    }
    pub fn accepted_head(&self, task: TaskId) -> Option<BaseOid> {
        self.data
            .lock()
            .expect("fixture task effects")
            .accepted_heads
            .get(&task)
            .cloned()
    }
    pub fn enqueue_count(&self, turn: TurnId) -> usize {
        usize::from(
            self.data
                .lock()
                .expect("fixture queue")
                .entries
                .contains_key(&turn),
        )
    }
    pub fn queue_position(&self, turn: TurnId) -> Option<u64> {
        self.data
            .lock()
            .expect("fixture queue")
            .observations
            .get(&turn)
            .and_then(|observation| observation.queue_position)
    }
    pub fn prepared(&self, turn: TurnId) -> Option<PreparedIntegrationTurn> {
        self.data
            .lock()
            .expect("fixture queue")
            .entries
            .get(&turn)
            .cloned()
    }
}
impl IntegrationTurns for FakeIntegrationTurns {
    fn observe(&self, turn: TurnId) -> Result<IntegrationTurnObservation, WorkerError> {
        let data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        Ok(data
            .observations
            .get(&turn)
            .cloned()
            .unwrap_or(IntegrationTurnObservation {
                turn_id: turn,
                queue_position: None,
                accepted: false,
                completed: false,
            }))
    }
    fn import_receipt(
        &self,
        task: TaskId,
        receipt: &IntegrationReceipt,
    ) -> Result<IntegrationReceipt, WorkerError> {
        receipt.validate()?;
        let mut imported = receipt.clone();
        imported.imported = true;
        let mut data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        if data.imports.iter().any(|(owner, receipts)| {
            *owner != task
                && receipts
                    .iter()
                    .any(|old| old.integration_id == receipt.integration_id)
        }) {
            return Err(integration_error("INTEGRATION_STATE_INVALID"));
        }
        if let Some(old) = data.imports.get(&task).and_then(|receipts| {
            receipts.iter().find(|old| {
                old.integration_id == receipt.integration_id && old.epoch == receipt.epoch
            })
        }) {
            if old != &imported {
                return Err(integration_error("INTEGRATION_STATE_INVALID"));
            }
            return Ok(old.clone());
        }
        let head = imported
            .merge_oid
            .as_ref()
            .unwrap_or(&imported.target_head)
            .clone();
        data.accepted_heads.insert(task, head);
        data.imports.entry(task).or_default().push(imported.clone());
        Ok(imported)
    }
    fn close_integrated(
        &self,
        task: TaskId,
        receipt: &IntegrationReceipt,
    ) -> Result<(), WorkerError> {
        receipt.validate()?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        if !receipt.imported
            || !data
                .imports
                .get(&task)
                .is_some_and(|receipts| receipts.contains(receipt))
        {
            return Err(integration_error("INTEGRATION_STATE_INVALID"));
        }
        data.closes.entry(task).or_insert_with(|| receipt.clone());
        Ok(())
    }
    fn enqueue(&self, prepared: &PreparedIntegrationTurn) -> Result<TurnId, WorkerError> {
        prepared.validate()?;
        let turn = prepared.followup.turn_id();
        let mut data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        if let Some(old) = data.entries.get(&turn) {
            if old != prepared {
                return Err(integration_error("INTEGRATION_STATE_INVALID"));
            }
        } else {
            data.sequence = data
                .sequence
                .checked_add(1)
                .ok_or_else(|| integration_error("INTEGRATION_STATE_INVALID"))?;
            let position = data.sequence;
            data.observations.insert(
                turn,
                IntegrationTurnObservation {
                    turn_id: turn,
                    queue_position: Some(position),
                    accepted: false,
                    completed: false,
                },
            );
            data.entries.insert(turn, prepared.clone());
        }
        Ok(turn)
    }
}
pub struct ManualIntegrationRuntime {
    now: AtomicU64,
    generation: AtomicU64,
    gate: Mutex<Option<IntegrationPauseEvidence>>,
    actors: Mutex<HashMap<(u32, u64), RunnerLivenessVerdict>>,
    hooks: Mutex<Vec<IntegrationHook>>,
    crash: Mutex<Option<IntegrationHook>>,
}
impl Default for ManualIntegrationRuntime {
    fn default() -> Self {
        let mut actors = HashMap::new();
        actors.insert((5_000_001, 1), RunnerLivenessVerdict::Live);
        Self {
            now: AtomicU64::new(1000),
            generation: AtomicU64::new(0),
            gate: Mutex::new(None),
            actors: Mutex::new(actors),
            hooks: Mutex::new(vec![]),
            crash: Mutex::new(None),
        }
    }
}
impl ManualIntegrationRuntime {
    pub fn advance(&self, duration: Duration) {
        let millis = u64::try_from(duration.as_millis()).expect("fixture duration");
        self.now
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |now| {
                now.checked_add(millis)
            })
            .expect("fixture clock overflow");
    }
    pub fn set_drive_gate(&self, reason: Option<IntegrationPauseReason>) {
        let mut gate = self.gate.lock().expect("fixture gate");
        if gate.as_ref().map(|g| g.reason) != reason {
            *gate = reason.map(|reason| IntegrationPauseEvidence {
                reason,
                effective_at_millis: self.now_millis(),
            });
        }
    }
    pub fn pause_evidence(&self) -> Option<IntegrationPauseEvidence> {
        *self.gate.lock().expect("fixture gate")
    }
    pub fn mark_actor_absent(&self) {
        self.set_actor_verdict(self.actor(), RunnerLivenessVerdict::Exited);
    }
    pub fn set_actor_verdict(&self, actor: ProcessIdentity, verdict: RunnerLivenessVerdict) {
        self.actors
            .lock()
            .expect("fixture actors")
            .insert((actor.pid(), actor.start_time_micros()), verdict);
    }
    pub fn restart(&self) {
        self.mark_actor_absent();
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.set_actor_verdict(self.actor(), RunnerLivenessVerdict::Live);
        *self.crash.lock().expect("fixture hook") = None;
    }
    pub fn crash_at(&self, point: IntegrationHook) {
        *self.crash.lock().expect("fixture hook") = Some(point);
    }
    pub fn hooks(&self) -> Vec<IntegrationHook> {
        self.hooks.lock().expect("fixture hooks").clone()
    }
}
impl IntegrationRuntime for ManualIntegrationRuntime {
    fn now_millis(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
    fn actor(&self) -> ProcessIdentity {
        let generation = self.generation.load(Ordering::SeqCst);
        let pid = 5_000_001 + u32::try_from(generation).expect("fixture generation");
        ProcessIdentity::new(pid, 1 + generation).expect("fixture actor")
    }
    fn actor_verdict(&self, actor: ProcessIdentity) -> RunnerLivenessVerdict {
        self.actors
            .lock()
            .expect("fixture actors")
            .get(&(actor.pid(), actor.start_time_micros()))
            .copied()
            .unwrap_or(RunnerLivenessVerdict::Unverifiable)
    }
    fn begin_phase(
        &self,
        key: &IntegrationPhaseKey,
    ) -> Result<IntegrationDriveAdmission, WorkerError> {
        if key.revision.0 == 0 {
            return Err(integration_error("INTEGRATION_STATE_INVALID"));
        }
        Ok(match self.pause_evidence() {
            Some(pause) => IntegrationDriveAdmission::Park(pause),
            None => IntegrationDriveAdmission::Permit(IntegrationPhasePermit::new(key.clone())),
        })
    }
    fn reach(&self, point: IntegrationHook) {
        self.hooks.lock().expect("fixture hooks").push(point);
        let crash = {
            let mut hook = self.crash.lock().expect("fixture hook");
            if *hook == Some(point) {
                *hook = None;
                true
            } else {
                false
            }
        };
        if crash {
            self.mark_actor_absent();
            panic!("injected integration crash at {point:?}");
        }
    }
}
#[derive(Default)]
pub struct FakeIntegrationObserver {
    facts: Mutex<HashMap<TaskId, IntegrationTaskFacts>>,
}
impl FakeIntegrationObserver {
    pub fn insert(&self, facts: IntegrationTaskFacts) {
        self.facts
            .lock()
            .expect("fixture facts")
            .insert(facts.ordinary.meta().task_id(), facts);
    }
}
impl IntegrationObserver for FakeIntegrationObserver {
    fn facts(&self, task: TaskId) -> Result<IntegrationTaskFacts, WorkerError> {
        self.facts
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?
            .get(&task)
            .cloned()
            .ok_or_else(|| WorkerError::task("TASK_NOT_FOUND", "fixture task is absent"))
    }
}
#[derive(Default)]
struct HostData {
    calls: Vec<HostIntegrationRequest>,
    response: Option<HostIntegrationResponse>,
    responses: std::collections::VecDeque<HostIntegrationResponse>,
}
#[derive(Default)]
pub struct FakeIntegrationHost {
    data: Mutex<HostData>,
}
impl FakeIntegrationHost {
    pub fn set_response(&self, response: HostIntegrationResponse) {
        self.data.lock().expect("fixture host").response = Some(response);
    }
    pub fn push_response(&self, response: HostIntegrationResponse) {
        self.data
            .lock()
            .expect("fixture host")
            .responses
            .push_back(response);
    }
    pub fn calls(&self) -> Vec<HostIntegrationRequest> {
        self.data.lock().expect("fixture host").calls.clone()
    }
}
impl IntegrationHost for FakeIntegrationHost {
    fn execute(
        &self,
        request: &HostIntegrationRequest,
    ) -> Result<HostIntegrationResponse, WorkerError> {
        request.validate()?;
        let mut data = self
            .data
            .lock()
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?;
        data.calls.push(request.clone());
        let response = data
            .responses
            .pop_front()
            .or_else(|| data.response.clone())
            .ok_or_else(integration_unavailable)?;
        drop(data);
        response.validate_for(request)?;
        Ok(response)
    }
}
/// Isolated fixture plus borrowed real coordinator seam. T1's drive fails closed.
pub struct IntegrationFixture {
    root: std::path::PathBuf,
    state: Arc<MemoryIntegrationState>,
    host: Arc<FakeIntegrationHost>,
    turns: Arc<FakeIntegrationTurns>,
    runtime: Arc<ManualIntegrationRuntime>,
    observer: Arc<FakeIntegrationObserver>,
}
impl Default for IntegrationFixture {
    fn default() -> Self {
        Self::new()
    }
}
impl IntegrationFixture {
    pub fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/integration-fixtures")
            .join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&root).expect("isolated integration root");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        Self {
            root,
            state: Arc::new(MemoryIntegrationState::default()),
            host: Arc::new(FakeIntegrationHost::default()),
            turns: Arc::new(FakeIntegrationTurns::default()),
            runtime: Arc::new(ManualIntegrationRuntime::default()),
            observer: Arc::new(FakeIntegrationObserver::default()),
        }
    }
    pub fn root(&self) -> &std::path::Path {
        &self.root
    }
    pub fn task(&self) -> TaskId {
        fixture_task()
    }
    pub fn source(&self) -> TurnId {
        fixture_source()
    }
    pub fn state(&self) -> &MemoryIntegrationState {
        &self.state
    }
    pub fn host(&self) -> &FakeIntegrationHost {
        &self.host
    }
    pub fn turns(&self) -> &FakeIntegrationTurns {
        &self.turns
    }
    pub fn runtime(&self) -> &ManualIntegrationRuntime {
        &self.runtime
    }
    pub fn observer(&self) -> &FakeIntegrationObserver {
        &self.observer
    }
    pub fn coordinator(&self) -> super::coordinator::IntegrationCoordinator<'_> {
        super::coordinator::IntegrationCoordinator::new(
            self.state(),
            self.host(),
            self.turns(),
            self.runtime(),
            self.observer(),
        )
    }
    pub fn enable(&self, task: TaskId, branch: &str) -> Result<(), WorkerError> {
        let mut policy = sample_policy("main");
        policy.target = validate_integration_target(branch)?;
        policy.origin = url::Url::from_file_path(self.root.join("origin.git"))
            .map_err(|_| integration_error("INTEGRATION_STATE_INVALID"))?
            .to_string();
        self.state.publish_policy(task, &policy)?;
        let ordinary = sample_ordinary(task, self.source()).with_fetched_head(None)?;
        self.observer.insert(IntegrationTaskFacts {
            cycle_base: ordinary.meta().base_oid().clone(),
            ordinary,
            result_imported: false,
            session_import_complete: true,
            continuation_pending: false,
            runner_present: false,
            stop_requested: false,
            close_pending: false,
            submission_pending: false,
            auxiliary_purpose: None,
        });
        Ok(())
    }
    pub fn complete_source(&self, task: TaskId) -> Result<(), WorkerError> {
        let mut facts = self.observer.facts(task)?;
        facts.ordinary = sample_ordinary(task, self.source());
        facts.result_imported = true;
        self.observer.insert(facts);
        self.coordinator().on_terminal(task, self.source())
    }
    pub fn drive(&self, task: TaskId) -> Result<IntegrationSnapshot, WorkerError> {
        self.coordinator().drive_once(task)
    }
    pub fn load(&self, task: TaskId) -> Result<Option<IntegrationRecord>, WorkerError> {
        self.state.load(task)
    }
    pub fn host_calls(&self) -> Vec<HostIntegrationRequest> {
        self.host.calls()
    }
    pub fn advance(&self, duration: Duration) {
        self.runtime.advance(duration);
    }
    pub fn crash_at(&self, point: IntegrationHook) {
        self.runtime.crash_at(point);
    }
    pub fn restart(&self) {
        self.runtime.restart();
    }
    pub fn mark_actor_absent(&self) {
        self.runtime.mark_actor_absent();
    }
    pub fn set_host_response(&self, response: HostIntegrationResponse) {
        self.host.set_response(response);
    }
    pub fn set_drive_gate(&self, reason: Option<IntegrationPauseReason>) {
        self.runtime.set_drive_gate(reason);
    }
    pub fn enqueue_count(&self, turn: TurnId) -> usize {
        self.turns.enqueue_count(turn)
    }
    pub fn stored_preparation(
        &self,
        task: TaskId,
        turn: TurnId,
    ) -> Result<Option<PreparedIntegrationTurn>, WorkerError> {
        self.state.load_prepared(task, turn)
    }
    pub fn turn_observation(
        &self,
        turn: TurnId,
    ) -> Result<IntegrationTurnObservation, WorkerError> {
        self.turns.observe(turn)
    }
    pub fn observations(&self) -> Vec<IntegrationTurnObservation> {
        self.turns.observations()
    }
    pub fn imports(&self, task: TaskId) -> Vec<IntegrationReceipt> {
        self.turns.imports(task)
    }
    pub fn closes(&self, task: TaskId) -> Vec<IntegrationReceipt> {
        self.turns.closes(task)
    }
    pub fn accepted_head(&self, task: TaskId) -> Option<BaseOid> {
        self.turns.accepted_head(task)
    }
    pub fn admission_remaining(&self, task: TaskId) -> Option<Duration> {
        let record = self.state.load(task).ok().flatten()?;
        record
            .remaining_admission_millis
            .or_else(|| {
                record.admission_deadline_millis.map(|deadline| {
                    let effective = record
                        .pause
                        .or_else(|| self.runtime.pause_evidence())
                        .map_or(self.runtime.now_millis(), |p| p.effective_at_millis);
                    deadline
                        .saturating_sub(effective)
                        .min(AUXILIARY_ADMISSION_MILLIS)
                })
            })
            .map(Duration::from_millis)
    }
    pub fn queue_position(&self, turn: TurnId) -> Option<u64> {
        self.turns.queue_position(turn)
    }
    pub fn followups_spent(&self, task: TaskId) -> u32 {
        self.state
            .load(task)
            .ok()
            .flatten()
            .map_or(0, |record| record.followups_spent)
    }
}
impl Drop for IntegrationFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_state_replays_the_authoritative_snapshot_and_refuses_rebinding() {
        let state = MemoryIntegrationState::default();
        let record = sample_record(fixture_task(), fixture_source(), "main");
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
        let task = record.task_id;
        let turn = prepared.followup.turn_id();
        let port: &dyn IntegrationState = &state;
        assert_eq!(port.load_prepared(task, turn).unwrap(), None);
        port.publish_prepared(task, &prepared).unwrap();
        port.publish_prepared(task, &prepared).unwrap();
        let loaded = port.load_prepared(task, turn).unwrap().unwrap();
        assert_eq!(
            encode_prepared_turn(&loaded).unwrap(),
            encode_prepared_turn(&prepared).unwrap()
        );
        let mut changed = prepared.clone();
        changed.followup = PreparedFollowup::prepare(
            prepared.followup.expected(),
            "Changed prompt".into(),
            turn,
            1003,
        )
        .unwrap();
        assert_eq!(
            port.publish_prepared(task, &changed)
                .unwrap_err()
                .public_code(),
            "INTEGRATION_STATE_INVALID"
        );
        let wrong_task = TaskId::new(uuid::Uuid::from_u128(9));
        assert!(port.publish_prepared(wrong_task, &prepared).is_err());
        assert_eq!(port.load_prepared(wrong_task, turn).unwrap(), None);
        changed = prepared.clone();
        changed.workspace_binding.clean_h.untracked_files = vec!["a".repeat(1024); 256];
        assert!(serde_json::to_vec(&changed).unwrap().len() > MAX_PREPARED_TURN_BYTES);
        assert!(port.publish_prepared(task, &changed).is_err());
        assert_eq!(port.load_prepared(task, turn).unwrap(), Some(prepared));
    }

    #[test]
    fn prepared_state_checks_compact_binding_in_both_publication_orders() {
        let mut record = sample_record(fixture_task(), fixture_source(), "main");
        record.candidates.push(sample_candidate(&record));
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Verify, 1, 1);
        record.auxiliaries.push(prepared.intent().unwrap());
        record.auxiliaries[0].prepared_binding = "f".repeat(64);
        let state = MemoryIntegrationState::default();
        state
            .replace(record.task_id, IntegrationRevision(0), &record)
            .unwrap();
        assert_eq!(
            state
                .publish_prepared(record.task_id, &prepared)
                .unwrap_err()
                .public_code(),
            "INTEGRATION_STATE_INVALID"
        );
        let other = MemoryIntegrationState::default();
        other.publish_prepared(record.task_id, &prepared).unwrap();
        assert!(
            other
                .replace(record.task_id, IntegrationRevision(0), &record)
                .is_err()
        );
        record.auxiliaries[0] = prepared.intent().unwrap();
        assert!(
            other
                .replace(record.task_id, IntegrationRevision(0), &record)
                .unwrap()
        );
        other.publish_prepared(record.task_id, &prepared).unwrap();
    }

    #[test]
    fn prepared_codec_refuses_inconsistent_frozen_followup_fields() {
        let record = sample_record(fixture_task(), fixture_source(), "main");
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
        let mut wire = serde_json::to_value(&prepared).unwrap();
        wire["followup"]["message"] = serde_json::json!("Different from composed prompt");
        assert!(serde_json::from_value::<PreparedIntegrationTurn>(wire).is_err());
    }

    #[test]
    fn auxiliary_observation_and_intent_share_strict_completion_invariants() {
        let record = sample_record(fixture_task(), fixture_source(), "main");
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
        for position in [None, Some(0), Some(1)] {
            for accepted in [false, true] {
                for completed in [false, true] {
                    let observation = IntegrationTurnObservation {
                        turn_id: prepared.followup.turn_id(),
                        queue_position: position,
                        accepted,
                        completed,
                    };
                    let mut intent = prepared.intent().unwrap();
                    intent.queue_position = position;
                    intent.accepted = accepted;
                    intent.completed = completed;
                    let valid = (!completed || accepted) && (!accepted || position.is_some());
                    assert_eq!(observation.validate().is_ok(), valid);
                    assert_eq!(intent.validate().is_ok(), valid);
                    let wire = serde_json::to_value(&observation).unwrap();
                    assert_eq!(
                        serde_json::from_value::<IntegrationTurnObservation>(wire.clone()).is_ok(),
                        valid
                    );
                    let mut unknown = wire;
                    unknown["extra"] = serde_json::json!(true);
                    assert!(serde_json::from_value::<IntegrationTurnObservation>(unknown).is_err());
                }
            }
        }
    }

    #[test]
    fn owner_queue_observes_retained_evidence_without_readmitting_a_completed_turn() {
        let turns = FakeIntegrationTurns::default();
        let record = sample_record(fixture_task(), fixture_source(), "main");
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
        let turn = prepared.followup.turn_id();
        let port: &dyn IntegrationTurns = &turns;
        assert_eq!(
            port.observe(turn).unwrap(),
            IntegrationTurnObservation {
                turn_id: turn,
                queue_position: None,
                accepted: false,
                completed: false,
            }
        );
        assert!(turns.observations().is_empty());
        port.enqueue(&prepared).unwrap();
        let queued = port.observe(turn).unwrap();
        assert_eq!(queued.queue_position, Some(1));
        assert!(!queued.accepted);
        let completed = IntegrationTurnObservation {
            accepted: true,
            completed: true,
            ..queued.clone()
        };
        turns.set_observation(completed.clone()).unwrap();
        assert!(turns.set_observation(queued).is_err());
        let moved = IntegrationTurnObservation {
            queue_position: Some(2),
            ..completed.clone()
        };
        assert!(turns.set_observation(moved).is_err());
        port.enqueue(&prepared).unwrap();
        assert_eq!(port.observe(turn).unwrap(), completed);
        assert_eq!(turns.observations(), vec![completed]);
        assert_eq!(turns.enqueue_count(turn), 1);
    }

    #[test]
    fn owner_import_and_close_are_receipt_bound_and_idempotent_for_m_and_t() {
        for disposition in [
            IntegrationDisposition::Merged,
            IntegrationDisposition::AlreadyIntegrated,
        ] {
            let turns = FakeIntegrationTurns::default();
            let record = sample_record(fixture_task(), fixture_source(), "main");
            let receipt = IntegrationReceipt {
                integration_id: record.snapshot.integration_id,
                epoch: 0,
                source_turn_id: fixture_source(),
                source_head: fixture_head(),
                target_head: record.cycle_base.clone(),
                merge_oid: (disposition == IntegrationDisposition::Merged)
                    .then(|| "e".repeat(40).parse().unwrap()),
                disposition,
                imported: false,
                recorded_at_millis: 1004,
            };
            let port: &dyn IntegrationTurns = &turns;
            assert!(port.close_integrated(record.task_id, &receipt).is_err());
            let imported = port.import_receipt(record.task_id, &receipt).unwrap();
            assert!(imported.imported);
            assert_eq!(
                port.import_receipt(record.task_id, &receipt).unwrap(),
                imported
            );
            assert_eq!(
                port.import_receipt(record.task_id, &imported).unwrap(),
                imported
            );
            let head = receipt.merge_oid.as_ref().unwrap_or(&receipt.target_head);
            assert_eq!(turns.accepted_head(record.task_id).as_ref(), Some(head));
            port.close_integrated(record.task_id, &imported).unwrap();
            port.close_integrated(record.task_id, &imported).unwrap();
            assert_eq!(turns.imports(record.task_id), vec![imported.clone()]);
            assert_eq!(turns.closes(record.task_id), vec![imported.clone()]);
            let mut changed = imported.clone();
            changed.target_head = "f".repeat(40).parse().unwrap();
            assert!(port.import_receipt(record.task_id, &changed).is_err());
            assert!(port.close_integrated(record.task_id, &changed).is_err());
            assert!(
                port.import_receipt(TaskId::new(uuid::Uuid::from_u128(9)), &receipt)
                    .is_err()
            );
            assert_eq!(turns.accepted_head(record.task_id).as_ref(), Some(head));
            let mut later = receipt.clone();
            later.epoch = 1;
            later.target_head = "f".repeat(40).parse().unwrap();
            if disposition == IntegrationDisposition::Merged {
                later.merge_oid = Some("d".repeat(40).parse().unwrap());
            }
            let later = port.import_receipt(record.task_id, &later).unwrap();
            port.import_receipt(record.task_id, &receipt).unwrap();
            assert_eq!(
                turns.accepted_head(record.task_id).as_ref(),
                Some(later.merge_oid.as_ref().unwrap_or(&later.target_head))
            );
            port.close_integrated(record.task_id, &later).unwrap();
            assert_eq!(turns.closes(record.task_id), vec![imported]);
        }
    }

    #[test]
    fn memory_state_enforces_policy_replay_revision_cas_and_target_ownership() {
        let state = MemoryIntegrationState::default();
        let record = sample_record(fixture_task(), fixture_source(), "main");
        state
            .publish_policy(record.task_id, &record.policy)
            .unwrap();
        state
            .publish_policy(record.task_id, &record.policy)
            .unwrap();
        assert!(
            state
                .publish_policy(record.task_id, &sample_policy("other"))
                .is_err()
        );
        assert!(
            state
                .replace(record.task_id, IntegrationRevision(0), &record)
                .unwrap()
        );
        assert!(
            !state
                .replace(record.task_id, IntegrationRevision(0), &record)
                .unwrap()
        );
        assert_eq!(state.load(record.task_id).unwrap(), Some(record.clone()));
        let actor = ManualIntegrationRuntime::default().actor();
        let reservation = state
            .reserve(&record.target_key, record.snapshot.integration_id, 0, actor)
            .unwrap()
            .unwrap();
        assert_eq!(
            state
                .reserve(&record.target_key, record.snapshot.integration_id, 0, actor)
                .unwrap(),
            Some(reservation.clone())
        );
        assert!(
            state
                .reserve(&record.target_key, record.snapshot.integration_id, 1, actor)
                .unwrap()
                .is_none()
        );
        let mut wrong = reservation.clone();
        wrong.epoch = 1;
        assert!(state.release(&wrong).is_err());
        state.release(&reservation).unwrap();
        assert!(
            state
                .reserve(&record.target_key, record.snapshot.integration_id, 1, actor)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn record_codec_refuses_rebound_identity_and_preserves_private_bound() {
        let mut record = sample_record(fixture_task(), fixture_source(), "main");
        assert_eq!(
            decode_record(&encode_record(&record).unwrap()).unwrap(),
            record
        );
        record.snapshot.integration_id = "00000000-0000-8000-8000-000000000009".parse().unwrap();
        assert!(record.validate().is_err());
        assert!(decode_record(&vec![b' '; MAX_PRIVATE_RECORD_BYTES + 1]).is_err());
    }

    #[test]
    fn prepared_codec_refuses_rebound_turn_and_verify_tree_or_limit_drift() {
        let record = sample_record(fixture_task(), fixture_source(), "main");
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Verify, 1, 1);
        assert_eq!(
            decode_prepared_turn(&encode_prepared_turn(&prepared).unwrap()).unwrap(),
            prepared
        );
        let mut wrong = prepared.clone();
        wrong.followup = PreparedFollowup::prepare(
            prepared.followup.expected(),
            "Resolve fixture integration".into(),
            TurnId::new(uuid::Uuid::from_u128(9)),
            1002,
        )
        .unwrap();
        assert!(wrong.validate().is_err());
        let mut wrong = prepared.clone();
        wrong.workspace_binding.pinned_tree = None;
        assert!(wrong.validate().is_err());
        let mut wrong = prepared;
        wrong.approved_turn_limits.timeout_millis = 600_001;
        assert!(wrong.validate().is_err());
    }

    #[test]
    fn auxiliary_queue_replays_one_id_and_refuses_a_changed_preparation() {
        let turns = FakeIntegrationTurns::default();
        let record = sample_record(fixture_task(), fixture_source(), "main");
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
        let turn = turns.enqueue(&prepared).unwrap();
        assert_eq!(turns.enqueue(&prepared).unwrap(), turn);
        assert_eq!(turns.enqueue_count(turn), 1);
        assert_eq!(turns.queue_position(turn), Some(1));
        let mut changed = prepared;
        changed.followup = PreparedFollowup::prepare(
            changed.followup.expected(),
            "Different input".into(),
            turn,
            1002,
        )
        .unwrap();
        assert!(turns.enqueue(&changed).is_err());
    }

    #[test]
    fn gate_evidence_keeps_effective_transition_time_across_late_observation() {
        let runtime = ManualIntegrationRuntime::default();
        runtime.advance(Duration::from_secs(120));
        runtime.set_drive_gate(Some(IntegrationPauseReason::ControllerDrained));
        runtime.advance(Duration::from_secs(900));
        runtime.set_drive_gate(Some(IntegrationPauseReason::ControllerDrained));
        let record = sample_record(fixture_task(), fixture_source(), "main");
        let key = IntegrationPhaseKey {
            task: record.task_id,
            intent: record.snapshot.integration_id,
            epoch: 0,
            revision: IntegrationRevision(1),
            phase: IntegrationPhase::Push,
        };
        match runtime.begin_phase(&key).unwrap() {
            IntegrationDriveAdmission::Park(evidence) => {
                assert_eq!(evidence.effective_at_millis, 121_000)
            }
            IntegrationDriveAdmission::Permit(_) => panic!("drained phase admitted"),
        }
        runtime.set_drive_gate(None);
        assert!(matches!(
            runtime.begin_phase(&key).unwrap(),
            IntegrationDriveAdmission::Permit(_)
        ));
    }
    #[test]
    fn private_record_keeps_compact_auxiliary_refs_outside_prepared_sidecars() {
        let record = sample_record(fixture_task(), fixture_source(), "main");
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
        let mut wire = serde_json::to_value(&record).unwrap();
        let auxiliary = serde_json::json!({
            "turn_id":prepared.followup.turn_id(),"integration_id":prepared.integration_id,
            "epoch":0,"attempt":1,"purpose":"resolve","ordinal":1,
            "prepared_binding":prepared.binding().unwrap(),"created_at_millis":1002,
            "queue_position":1,"accepted":false,"completed":false,
        });
        wire["auxiliaries"] = serde_json::json!([auxiliary.clone()]);
        let decoded: IntegrationRecord = serde_json::from_value(wire).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap()["auxiliaries"][0],
            auxiliary
        );
        assert!(auxiliary.get("followup").is_none());
    }

    #[test]
    fn shared_fixture_is_isolated_and_preserves_runtime_evidence_across_restart() {
        let fixture = IntegrationFixture::new();
        let other = IntegrationFixture::new();
        assert_ne!(fixture.root(), other.root());
        fixture.enable(fixture.task(), "main").unwrap();
        assert!(
            fixture
                .state()
                .load_policy(fixture.task())
                .unwrap()
                .is_some()
        );
        assert!(
            !fixture
                .observer()
                .facts(fixture.task())
                .unwrap()
                .result_imported
        );
        assert!(fixture.host_calls().is_empty());
        assert!(fixture.load(fixture.task()).unwrap().is_none());
        fixture.advance(Duration::from_secs(120));
        fixture.set_drive_gate(Some(IntegrationPauseReason::ControllerDisabled));
        let actor = fixture.runtime().actor();
        fixture.mark_actor_absent();
        assert_eq!(
            fixture.runtime().actor_verdict(actor),
            RunnerLivenessVerdict::Exited
        );
        fixture.restart();
        assert_eq!(fixture.runtime().now_millis(), 121_000);
        assert_eq!(
            fixture
                .runtime()
                .pause_evidence()
                .unwrap()
                .effective_at_millis,
            121_000
        );
        assert_ne!(fixture.runtime().actor(), actor);
        fixture.crash_at(IntegrationHook::AfterStateBeforeEvent);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fixture
                .runtime()
                .reach(IntegrationHook::AfterStateBeforeEvent)))
            .is_err()
        );
        fixture.restart();
        fixture
            .runtime()
            .reach(IntegrationHook::AfterStateBeforeEvent);
    }

    #[test]
    fn fake_host_refuses_wrong_revision_before_claiming_a_reply() {
        let record = sample_record(fixture_task(), fixture_source(), "main");
        let request = HostIntegrationRequest {
            protocol_version: 7,
            task_id: record.task_id,
            integration_id: Some(record.snapshot.integration_id),
            epoch: 0,
            revision: IntegrationRevision(1),
            action: HostIntegrationAction::Read,
        };
        let host = FakeIntegrationHost::default();
        let mut identity = IntegrationResponseIdentity::for_request(&request);
        identity.revision = IntegrationRevision(2);
        host.set_response(HostIntegrationResponse::TargetMoved {
            identity,
            observed_target: record.cycle_base.clone(),
        });
        assert_eq!(
            host.execute(&request).unwrap_err().public_code(),
            "INTEGRATION_STATE_INVALID"
        );
        host.set_response(HostIntegrationResponse::TargetMoved {
            identity: IntegrationResponseIdentity::for_request(&request),
            observed_target: record.cycle_base,
        });
        assert!(host.execute(&request).is_ok());
        assert_eq!(host.calls().len(), 2);
    }
}
