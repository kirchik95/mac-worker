//! Laptop event/reconciliation facade. Transport and reconciliation are T4.
pub use super::contracts::{
    AttentionSummary, BaselineKind, ChangeCause, DerivedTaskChange, EventReconciler, EventSource,
    EventSupport, PreviousProjection, ReconcileInput, Reconciliation, RepairProgress,
    TaskEligibilitySignature,
};

use super::contracts::*;
use crate::{
    config::ControllerConfig,
    controller::{
        ControllerRequest, controller_rpc_ssh_request, decode_frame, encode_json_frame,
        parse_request,
        read::{ControllerReadIdentity, ControllerReadReply},
    },
    error::WorkerError,
    job::HostControlError,
    process::ProcessRunner,
};
use serde::de::DeserializeOwned;
use std::{collections::BTreeSet, sync::Arc, time::Duration};

const LEGACY_SELECTOR_REJECTION: &str = "task.list body contained unexpected key controller_events";

pub struct ControllerEventClient {
    runner: Arc<dyn ProcessRunner>,
    controller: ControllerConfig,
    runtime: Arc<dyn EventRuntime>,
}
impl ControllerEventClient {
    pub fn new(
        runner: Arc<dyn ProcessRunner>,
        controller: ControllerConfig,
        runtime: Arc<dyn EventRuntime>,
    ) -> Self {
        Self {
            runner,
            controller,
            runtime,
        }
    }
    fn budget(&self, deadline: Duration) -> Result<Duration, WorkerError> {
        check(self.runtime.as_ref(), deadline)?;
        Ok(deadline.min(self.runtime.now().saturating_add(RPC_BUDGET)))
    }
    fn exchange<T: DeserializeOwned>(
        &self,
        request: &ControllerRequest,
        deadline: Duration,
    ) -> Result<T, WorkerError> {
        let deadline = self.budget(deadline)?;
        let mut process = controller_rpc_ssh_request(&self.controller)?;
        process.policy.deadline = deadline.saturating_sub(self.runtime.now()).min(RPC_BUDGET);
        process.stdin = Some(encode_json_frame(&serde_json::json!({
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "request_id": request.request_id(), "command": request.command(), "body": request.body()
        }))?);
        let result = self.runner.run_interruptible(&process, &|| {
            self.runtime.cancelled() || self.runtime.now() >= deadline
        });
        check(self.runtime.as_ref(), deadline)?;
        let result = result.map_err(|_| unavailable())?;
        let bytes = decode_frame(&result.stdout).map_err(|_| unavailable())?;
        if let Ok(error) = serde_json::from_slice::<HostControlError>(bytes) {
            if request.body().get("controller_events").is_some()
                && error.error().code() == "INVALID_REQUEST"
                && error.error().message() == LEGACY_SELECTOR_REJECTION
            {
                return Err(WorkerError::Unavailable(
                    "CONTROLLER_EVENTS_UNSUPPORTED: legacy selector rejection".into(),
                ));
            }
            return Err(unavailable());
        }
        if !result.status.success() {
            return Err(unavailable());
        }
        let reply: ControllerReadReply<T> =
            serde_json::from_slice(bytes).map_err(|_| unavailable())?;
        reply.verify_envelope(request).map_err(|_| unavailable())?;
        Ok(reply.into_result())
    }
}
fn selector_request(selector: &EventSelector) -> Result<ControllerRequest, WorkerError> {
    request_body(selector.request_body()?)
}
fn request_body(body: serde_json::Value) -> Result<ControllerRequest, WorkerError> {
    parse_request(
        &serde_json::to_vec(&serde_json::json!({
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "request_id": format!("{:x}",uuid::Uuid::new_v4().simple()),
            "command":"task.list", "body": body,
        }))
        .map_err(|_| unavailable())?,
    )
}
fn unavailable() -> WorkerError {
    WorkerError::Unavailable("CONTROLLER_EVENTS_UNAVAILABLE: event read unavailable".into())
}
fn check(runtime: &dyn EventRuntime, deadline: Duration) -> Result<(), WorkerError> {
    if runtime.cancelled() {
        return Err(WorkerError::Unavailable(
            "CONTROLLER_EVENTS_CANCELLED: cancelled".into(),
        ));
    }
    if runtime.now() >= deadline {
        return Err(unavailable());
    }
    Ok(())
}

pub(crate) fn validate_addressed(
    query: &TaskAddressQuery,
    result: &TaskFactsBatch,
) -> Result<(), WorkerError> {
    query.validate()?;
    result.validate()?;
    let requested: BTreeSet<_> = query.task_ids.iter().copied().collect();
    let returned: BTreeSet<_> = result
        .rows
        .iter()
        .map(|row| row.task_id)
        .chain(result.missing.iter().copied())
        .collect();
    if requested != returned
        || (!query.include_titles && result.rows.iter().any(|row| row.title.is_some()))
    {
        return Err(unavailable());
    }
    Ok(())
}
fn validate_read(query: &ReadQuery, result: &EventReadResult) -> Result<(), WorkerError> {
    result.validate()?;
    if let EventReadResult::Batch(batch) = result {
        let after = query.after.ok_or_else(unavailable)?;
        if after.journal_id != batch.journal_id
            || batch.events.len() > query.limit
            || batch
                .events
                .first()
                .is_some_and(|event| after.seq.checked_increment() != Some(event.seq))
            || (batch.events.is_empty() && batch.next_after != after)
        {
            return Err(unavailable());
        }
    }
    Ok(())
}
impl EventSource for ControllerEventClient {
    fn discover(&self, deadline: Duration) -> Result<EventSupport, WorkerError> {
        let deadline = self.budget(deadline)?;
        let request = request_body(serde_json::json!({"controller_health":true}))?;
        let status: crate::controller::health_read::ControllerHealthStatus =
            self.exchange(&request, deadline)?;
        status.verify_payload(&request)?;
        Ok(
            if status.features.as_ref().is_some_and(|features| {
                features
                    .iter()
                    .any(|feature| feature == "controller.events")
            }) {
                EventSupport::Supported
            } else {
                EventSupport::Unsupported
            },
        )
    }
    fn read(&self, query: ReadQuery, deadline: Duration) -> Result<EventReadResult, WorkerError> {
        let query = query.normalized();
        let result = self.exchange(
            &selector_request(&EventSelector::Read(query.clone()))?,
            deadline,
        )?;
        validate_read(&query, &result)?;
        Ok(result)
    }
    fn tasks(
        &self,
        query: TaskAddressQuery,
        deadline: Duration,
    ) -> Result<TaskFactsBatch, WorkerError> {
        query.validate()?;
        let result = self.exchange(
            &selector_request(&EventSelector::Tasks(query.clone()))?,
            deadline,
        )?;
        validate_addressed(&query, &result)?;
        Ok(result)
    }
    fn repair(
        &self,
        query: TaskRepairQuery,
        deadline: Duration,
    ) -> Result<TaskRepairPage, WorkerError> {
        let query = query.normalized();
        let result: TaskRepairPage = self.exchange(
            &selector_request(&EventSelector::Repair(query.clone()))?,
            deadline,
        )?;
        result.validate()?;
        if result.rows.len() > query.limit || result.baseline_after != query.baseline_after {
            return Err(unavailable());
        }
        Ok(result)
    }
}

use crate::task::TaskId;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};

struct Candidate {
    previous: Option<TaskFacts>,
    warm: bool,
    cause: Option<ChangeCause>,
}
struct Sweep {
    rows: BTreeMap<TaskId, TaskFacts>,
    after: Option<OpaqueCursor>,
    baseline: Option<EventCursor>,
    complete: bool,
    replay_after: Option<EventCursor>,
    replay_done: bool,
}
struct Comparison {
    old: PreviousProjection,
    ids: VecDeque<TaskId>,
    attention: Sha256,
    attention_count: usize,
}
/// Consumer-owned projections and bounded confirmation work. Complete maps
/// scale with the admitted registry; pending IDs and emitted chunks do not.
pub struct TaskReconciler {
    previous: PreviousProjection,
    cursor: Option<EventCursor>,
    runtime: Arc<dyn EventRuntime>,
    candidates: BTreeMap<TaskId, Candidate>,
    order: VecDeque<TaskId>,
    proof: Option<TaskAddressQuery>,
    sweep: Option<Sweep>,
    comparison: Option<Comparison>,
    last_started: Option<Duration>,
    repair_needed: bool,
    changes: VecDeque<DerivedTaskChange>,
    confirmed: BTreeMap<TaskId, TaskFacts>,
}
impl TaskReconciler {
    pub fn new(
        previous: PreviousProjection,
        cursor: Option<EventCursor>,
        pending: Vec<TaskId>,
        runtime: Arc<dyn EventRuntime>,
    ) -> Self {
        let mut reconciler = Self {
            previous,
            cursor,
            runtime,
            candidates: BTreeMap::new(),
            order: VecDeque::new(),
            proof: None,
            sweep: None,
            comparison: None,
            last_started: None,
            repair_needed: false,
            changes: VecDeque::new(),
            confirmed: BTreeMap::new(),
        };
        for id in pending {
            reconciler.enqueue(id, None, None);
        }
        reconciler
    }
    pub fn previous_projection(&self) -> &PreviousProjection {
        &self.previous
    }
    pub fn cursor(&self) -> Option<EventCursor> {
        self.cursor
    }
    fn prior(&self, id: TaskId) -> Option<TaskFacts> {
        match &self.previous {
            PreviousProjection::Absent => None,
            PreviousProjection::Present(rows) => rows.get(&id).cloned(),
        }
    }
    fn enqueue(
        &mut self,
        id: TaskId,
        cause: Option<ChangeCause>,
        prior: Option<(Option<TaskFacts>, bool)>,
    ) -> bool {
        if let Some(candidate) = self.candidates.get_mut(&id) {
            if cause.is_some() {
                candidate.cause = cause;
            }
            return true;
        }
        if self.candidates.len() == NOTIFY_PENDING_CAPACITY {
            self.repair_needed = true;
            return false;
        }
        let (previous, warm) = prior.unwrap_or_else(|| {
            (
                self.prior(id),
                matches!(self.previous, PreviousProjection::Present(_)),
            )
        });
        self.candidates.insert(
            id,
            Candidate {
                previous,
                warm,
                cause,
            },
        );
        self.order.push_back(id);
        true
    }
    fn consume_batch(
        &mut self,
        batch: &ReadBatch,
        replay: bool,
    ) -> Result<(bool, Option<EventCursor>), WorkerError> {
        batch.validate()?;
        let mut consumed = None;
        for event in &batch.events {
            if !replay
                && self.cursor.is_some_and(|cursor| {
                    cursor.journal_id == event.journal_id && event.seq <= cursor.seq
                })
            {
                continue;
            }
            if let Some(id) = event.affected_task() {
                let cause = match event.kind.as_str() {
                    "turn.finished" | "turn.outcome_changed" => {
                        let hint: TurnHint = serde_json::from_value(event.data.clone())
                            .map_err(|_| unavailable())?;
                        Some(ChangeCause::ReplayTerminal {
                            turn_id: hint.turn_id,
                            outcome: hint.outcome,
                        })
                    }
                    "task.abandoned" => Some(ChangeCause::ReplayAbandoned),
                    _ => None,
                };
                if !self.enqueue(id, cause, None) {
                    return Ok((false, consumed));
                }
            } else {
                // Unknown kinds/versions and global hints never borrow a raw task_id.
                self.repair_needed = true;
            }
            let cursor = EventCursor {
                journal_id: event.journal_id,
                seq: event.seq,
            };
            consumed = Some(cursor);
            if self
                .cursor
                .is_none_or(|old| old.journal_id == cursor.journal_id && old.seq < cursor.seq)
            {
                self.cursor = Some(cursor);
            }
        }
        Ok((true, consumed))
    }
    fn confirm_pending(
        &mut self,
        source: &dyn EventSource,
        titles: bool,
        deadline: Duration,
    ) -> Result<(), WorkerError> {
        if self.candidates.is_empty()
            || self.changes.len() + ADDRESSED_MAX_TASKS > MAX_RECONCILIATION_ROWS
        {
            return Ok(());
        }
        let mut query = match &self.proof {
            Some(query) => query.clone(),
            None => TaskAddressQuery::try_new(
                self.order
                    .iter()
                    .copied()
                    .take(ADDRESSED_MAX_TASKS)
                    .collect(),
                titles,
                None,
            )?,
        };
        query.include_titles = titles;
        let result = source.tasks(query.clone(), deadline)?;
        validate_addressed(&query, &result)?;
        check(self.runtime.as_ref(), deadline)?;
        for row in result.rows {
            let id = row.task_id;
            if let Some(sweep) = &mut self.sweep {
                if sweep.complete {
                    sweep.rows.insert(id, row.clone());
                }
            }
            if self.confirmed.len() < MAX_RECONCILIATION_ROWS || self.confirmed.contains_key(&id) {
                self.confirmed.insert(id, row.clone());
            }
            if let Some(candidate) = self.candidates.get(&id) {
                let signature = row.eligibility_signature();
                if signature.quiescent == Some(true) {
                    let unchanged = candidate.warm
                        && candidate
                            .previous
                            .as_ref()
                            .is_some_and(|old| old.eligibility_signature() == signature);
                    let cause = if unchanged {
                        None
                    } else {
                        match &candidate.cause {
                            Some(ChangeCause::ReplayTerminal { turn_id, outcome })
                                if row.latest_turn_id == Some(*turn_id)
                                    && row.outcome == Some(*outcome) =>
                            {
                                candidate.cause.clone()
                            }
                            Some(ChangeCause::ReplayAbandoned)
                                if signature.abandoned_without_turn =>
                            {
                                candidate.cause.clone()
                            }
                            None if candidate.warm => Some(ChangeCause::RepairDifference),
                            _ => None,
                        }
                    };
                    if let Some(cause) = cause {
                        self.changes.push_back(DerivedTaskChange {
                            task_id: id,
                            previous: candidate.previous.clone(),
                            current: Some(row.clone()),
                            cause,
                        });
                    }
                    self.candidates.remove(&id);
                }
            }
            if let PreviousProjection::Present(rows) = &mut self.previous {
                rows.insert(id, row);
            }
        }
        for id in result.missing {
            self.candidates.remove(&id);
            if let Some(sweep) = &mut self.sweep {
                if sweep.complete {
                    sweep.rows.remove(&id);
                }
            }
        }
        self.order.retain(|id| self.candidates.contains_key(id));
        if result.proof_after.is_some() {
            query.proof_after = result.proof_after;
            self.proof = Some(query);
        } else {
            self.proof = None;
            // Busy rows stay pending, but cannot monopolize every addressed group.
            for id in query.task_ids {
                if self.candidates.contains_key(&id) {
                    self.order.retain(|other| *other != id);
                    self.order.push_back(id);
                }
            }
        }
        Ok(())
    }
    fn start_sweep(
        &mut self,
        source: &dyn EventSource,
        baseline: Option<EventCursor>,
        deadline: Duration,
    ) -> Result<(), WorkerError> {
        check(self.runtime.as_ref(), deadline)?;
        let baseline = if baseline.is_some() {
            baseline
        } else {
            // H is observed before state enumeration. Journal failure leaves a
            // state-only sweep; cancellation/deadline still stops the request.
            let read = source.read(ReadQuery::default(), deadline);
            check(self.runtime.as_ref(), deadline)?;
            match read {
                Ok(result) => {
                    result.validate()?;
                    Some(match result {
                        EventReadResult::SnapshotRequired(control) => control.window.cursor(),
                        EventReadResult::Batch(batch) => EventCursor {
                            journal_id: batch.journal_id,
                            seq: batch.head_seq,
                        },
                    })
                }
                Err(_) => None,
            }
        };
        self.last_started = Some(self.runtime.now());
        self.repair_needed = false;
        self.sweep = Some(Sweep {
            rows: BTreeMap::new(),
            after: None,
            baseline,
            complete: false,
            replay_after: baseline,
            replay_done: false,
        });
        Ok(())
    }
    fn advance_sweep(
        &mut self,
        source: &dyn EventSource,
        deadline: Duration,
    ) -> Result<RepairProgress, WorkerError> {
        let Some(mut sweep) = self.sweep.take() else {
            return Ok(RepairProgress::NotStarted);
        };
        let advance = (|| {
            if !sweep.complete {
                let query = TaskRepairQuery {
                    after: sweep.after.clone(),
                    limit: REPAIR_DEFAULT_LIMIT,
                    baseline_after: sweep.baseline,
                };
                let page = source.repair(query, deadline)?;
                page.validate()?;
                check(self.runtime.as_ref(), deadline)?;
                if page.baseline_after != sweep.baseline {
                    return Err(unavailable());
                }
                if page.restart {
                    return Ok(RepairProgress::Restarted);
                }
                if !page.complete && page.next == sweep.after {
                    return Err(unavailable());
                }
                for row in page.rows {
                    sweep.rows.insert(row.task_id, row);
                }
                sweep.after = page.next;
                sweep.complete = page.complete;
                if !sweep.complete {
                    return Ok(RepairProgress::InProgress);
                }
            }
            if !sweep.replay_done {
                if let Some(after) = sweep.replay_after {
                    let query = ReadQuery {
                        after: Some(after),
                        limit: READ_DEFAULT_LIMIT,
                        wait_ms: 0,
                    };
                    let read = source.read(query.clone(), deadline)?;
                    validate_read(&query, &read)?;
                    check(self.runtime.as_ref(), deadline)?;
                    match read {
                        EventReadResult::SnapshotRequired(_) => {
                            return Ok(RepairProgress::Restarted);
                        }
                        EventReadResult::Batch(batch) => {
                            let (all, consumed) = self.consume_batch(&batch, true)?;
                            sweep.replay_after = consumed.or(Some(after));
                            sweep.replay_done = all && !batch.has_more;
                            if !sweep.replay_done || !batch.events.is_empty() {
                                return Ok(RepairProgress::InProgress);
                            }
                        }
                    }
                } else {
                    sweep.replay_done = true;
                }
            }
            Ok(RepairProgress::Complete)
        })();
        match advance {
            Err(error) => {
                self.sweep = Some(sweep);
                Err(error)
            }
            Ok(RepairProgress::Restarted) => {
                self.repair_needed = true;
                Ok(RepairProgress::Restarted)
            }
            Ok(RepairProgress::Complete) => {
                if let Some(baseline) = sweep.baseline {
                    if self.cursor.is_none_or(|old| {
                        old.journal_id != baseline.journal_id || old.seq < baseline.seq
                    }) {
                        self.cursor = Some(baseline);
                    }
                }
                let old =
                    std::mem::replace(&mut self.previous, PreviousProjection::Present(sweep.rows));
                let mut ids = BTreeSet::new();
                if let PreviousProjection::Present(rows) = &old {
                    ids.extend(rows.keys().copied());
                }
                if let PreviousProjection::Present(rows) = &self.previous {
                    ids.extend(rows.keys().copied());
                }
                self.comparison = Some(Comparison {
                    old,
                    ids: ids.into_iter().collect(),
                    attention: Sha256::new(),
                    attention_count: 0,
                });
                Ok(RepairProgress::InProgress)
            }
            Ok(progress) => {
                self.sweep = Some(sweep);
                Ok(progress)
            }
        }
    }
    fn compare_page(&mut self) -> Result<Option<AttentionSummary>, WorkerError> {
        let Some(mut comparison) = self.comparison.take() else {
            return Ok(None);
        };
        for _ in 0..REPAIR_MAX_LIMIT {
            let Some(id) = comparison.ids.front().copied() else {
                break;
            };
            let old = match &comparison.old {
                PreviousProjection::Absent => None,
                PreviousProjection::Present(rows) => rows.get(&id).cloned(),
            };
            let current = self.prior(id);
            let warm = matches!(comparison.old, PreviousProjection::Present(_));
            let changed = warm
                && old.as_ref().map(TaskFacts::eligibility_signature)
                    != current.as_ref().map(TaskFacts::eligibility_signature);
            if let Some(facts) = &current {
                let signature = facts.eligibility_signature();
                if (changed || signature.current_attention || signature.abandoned_without_turn)
                    && !self.enqueue(id, None, Some((old.clone(), warm)))
                {
                    break;
                }
                if signature.current_attention || signature.abandoned_without_turn {
                    comparison.attention.update(
                        serde_json::to_vec(&(
                            id,
                            signature.latest_turn_id,
                            signature.outcome,
                            &signature.code,
                        ))
                        .map_err(|_| unavailable())?,
                    );
                    comparison.attention.update(b"\n");
                    comparison.attention_count += 1;
                }
                if !warm && self.confirmed.len() < MAX_RECONCILIATION_ROWS {
                    self.confirmed.insert(id, facts.clone());
                }
            } else if changed {
                if self.changes.len() == MAX_RECONCILIATION_ROWS {
                    break;
                }
                self.changes.push_back(DerivedTaskChange {
                    task_id: id,
                    previous: old,
                    current: None,
                    cause: ChangeCause::RepairDifference,
                });
            }
            comparison.ids.pop_front();
        }
        if comparison.ids.is_empty() {
            comparison
                .attention
                .update(comparison.attention_count.to_le_bytes());
            Ok(Some(AttentionSummary {
                count: comparison.attention_count,
                fingerprint: format!("{:x}", comparison.attention.finalize()),
            }))
        } else {
            self.comparison = Some(comparison);
            Ok(None)
        }
    }
}
impl EventReconciler for TaskReconciler {
    fn reconcile(
        &mut self,
        source: &dyn EventSource,
        input: ReconcileInput,
        deadline: Duration,
    ) -> Result<Reconciliation, WorkerError> {
        check(self.runtime.as_ref(), deadline)?;
        let baseline = if matches!(self.previous, PreviousProjection::Absent) {
            BaselineKind::Cold
        } else {
            BaselineKind::Warm
        };
        let mut captured = None;
        if let Some(read) = input.read {
            read.validate()?;
            match read {
                EventReadResult::Batch(batch) => {
                    self.consume_batch(&batch, false)?;
                }
                EventReadResult::SnapshotRequired(control) => {
                    captured = Some(control.window.cursor());
                    self.sweep = None;
                    self.repair_needed = true;
                }
            }
        }
        // Pending proof is always ahead of historical enumeration work.
        self.confirm_pending(source, input.include_titles, deadline)?;
        let due = input.repair_due
            || self.repair_needed
            || self.last_started.is_none_or(|started| {
                self.runtime.now().saturating_sub(started) >= REPAIR_INTERVAL
            });
        if self.sweep.is_none() && self.comparison.is_none() && due {
            self.start_sweep(source, captured, deadline)?;
        }
        let mut repair = self.advance_sweep(source, deadline)?;
        let attention = self.compare_page()?;
        if attention.is_some() {
            repair = RepairProgress::Complete;
        } else if self.comparison.is_some() {
            repair = RepairProgress::InProgress;
        }
        check(self.runtime.as_ref(), deadline)?;
        let result = Reconciliation {
            consumed_after: self.cursor,
            baseline,
            changes: self.changes.drain(..).collect(),
            confirmed: std::mem::take(&mut self.confirmed).into_values().collect(),
            pending_ids: self.order.iter().copied().collect(),
            repair,
            attention,
            repair_needed: self.repair_needed,
        };
        result.validate()?;
        Ok(result)
    }
}
