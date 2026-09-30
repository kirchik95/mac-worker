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
            if request.body().get("controller_events").is_some()
                && error.error().code() == CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE
            {
                return Err(WorkerError::Unavailable(format!(
                    "{CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE}: repair unavailable, registry too large"
                )));
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
    cold: bool,
    ids: VecDeque<TaskId>,
    attention: BTreeMap<TaskId, TaskEligibilitySignature>,
    waiting: BTreeSet<TaskId>,
    verified: BTreeSet<TaskId>,
    hash: Sha256,
    hashed_through: Option<TaskId>,
    hashed_count: usize,
}
impl Comparison {
    fn invalidate_attention(&mut self, id: TaskId) {
        self.waiting.insert(id);
        self.verified.remove(&id);
        if self.attention.remove(&id).is_some() {
            self.reset_hash();
        }
    }
    fn reset_hash(&mut self) {
        self.hash = Sha256::new();
        self.hashed_through = None;
        self.hashed_count = 0;
    }
    fn refresh_attention(&mut self) {
        self.attention.clear();
        self.waiting.clear();
        self.verified.clear();
        self.reset_hash();
    }
    fn confirm_attention(&mut self, row: &TaskFacts) {
        let signature = row.eligibility_signature();
        if potential_attention(row) && signature.quiescent.is_none() {
            self.invalidate_attention(row.task_id);
            return;
        }
        self.waiting.remove(&row.task_id);
        self.verified.insert(row.task_id);
        let attention =
            (signature.current_attention || signature.abandoned_without_turn).then_some(signature);
        let changed = match attention {
            Some(signature) => {
                self.attention.insert(row.task_id, signature.clone()) != Some(signature)
            }
            None => self.attention.remove(&row.task_id).is_some(),
        };
        if changed {
            self.reset_hash();
        }
    }
    fn forget_attention(&mut self, id: TaskId) {
        self.waiting.remove(&id);
        self.verified.insert(id);
        if self.attention.remove(&id).is_some() {
            self.reset_hash();
        }
    }
}
// Unknown dispatch proof can still conceal current attention. A positive busy
// fact, a closed attention turn, or an unknown outcome cannot be attention.
fn potential_attention(row: &TaskFacts) -> bool {
    row.eligibility_signature().busy != Some(true)
        && ((row.state == "open"
            && row.latest_turn_id.is_some()
            && matches!(
                row.outcome,
                Some(SafeOutcome::NeedsInput | SafeOutcome::Blocked)
            ))
            || (row.state == "abandoned" && row.latest_turn_id.is_none() && row.outcome.is_none()))
}
struct ColdHistory {
    signature: TaskEligibilitySignature,
    abandoned: bool,
}
impl ColdHistory {
    fn new(row: &TaskFacts) -> Self {
        Self {
            signature: row.eligibility_signature(),
            abandoned: row.state == "abandoned",
        }
    }
    fn matches(&self, row: &TaskFacts) -> bool {
        self.signature.latest_turn_id == row.latest_turn_id
            && self.signature.outcome == row.outcome
            && self.signature.code == row.code
            && self.abandoned == (row.state == "abandoned")
    }
}
/// Consumer-owned projections and bounded confirmation work. Complete maps
/// scale with the admitted registry; pending IDs and emitted chunks do not.
pub struct TaskReconciler {
    previous: PreviousProjection,
    cursor: Option<EventCursor>,
    cursor_validated: bool,
    runtime: Arc<dyn EventRuntime>,
    candidates: BTreeMap<TaskId, Candidate>,
    order: VecDeque<TaskId>,
    proof: Option<TaskAddressQuery>,
    sweep: Option<Sweep>,
    comparison: Option<Comparison>,
    cold_unresolved: BTreeMap<TaskId, ColdHistory>,
    last_started: Option<Duration>,
    repair_needed: bool,
    cursor_recovery: bool,
    attention_refresh_required: bool,
    unaccounted_feed: bool,
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
            cursor_validated: false,
            runtime,
            candidates: BTreeMap::new(),
            order: VecDeque::new(),
            proof: None,
            sweep: None,
            comparison: None,
            cold_unresolved: BTreeMap::new(),
            last_started: None,
            repair_needed: false,
            cursor_recovery: false,
            attention_refresh_required: false,
            unaccounted_feed: false,
            changes: VecDeque::new(),
            confirmed: BTreeMap::new(),
        };
        for id in pending {
            reconciler.enqueue(id, None, None, None);
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
        completed_rows: Option<&BTreeMap<TaskId, TaskFacts>>,
    ) -> bool {
        let had_cold_history = self.cold_unresolved.contains_key(&id);
        let cold = self.cold_unresolved.get(&id).is_some_and(|history| {
            self.prior(id)
                .as_ref()
                .is_some_and(|row| history.matches(row))
        });
        if !cold {
            self.cold_unresolved.remove(&id);
        }
        if let Some(candidate) = self.candidates.get_mut(&id) {
            if cold {
                candidate.warm = false;
            } else if had_cold_history {
                candidate.warm = true;
                if let Some((previous, _)) = prior {
                    candidate.previous = previous;
                }
            }
            if cause.is_some() {
                candidate.cause = cause;
            }
            return true;
        }
        if self.candidates.len() == NOTIFY_PENDING_CAPACITY {
            // Positively ineligible rows stay in the full projection and are
            // revisited by repair. They must not fence eligible overflow forever.
            // Completed staged rows also supply this proof during cold replay,
            // before the first complete projection can be installed.
            let deferred = self.order.iter().copied().find(|other| {
                self.proof
                    .as_ref()
                    .is_none_or(|query| !query.task_ids.contains(other))
                    && completed_rows
                        .and_then(|rows| rows.get(other))
                        .cloned()
                        .or_else(|| self.prior(*other))
                        .is_some_and(|row| {
                            row.eligibility_signature().quiescent == Some(false)
                                || (row.latest_turn_id.is_some() && row.outcome.is_none())
                        })
            });
            self.repair_needed = true;
            if let Some(deferred) = deferred {
                self.candidates.remove(&deferred);
                self.order.retain(|other| *other != deferred);
            } else {
                return false;
            }
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
                warm: warm && !cold,
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
        completed_rows: Option<&BTreeMap<TaskId, TaskFacts>>,
    ) -> Result<(bool, Option<EventCursor>), WorkerError> {
        batch.validate()?;
        if !replay && let Some(cursor) = self.cursor {
            let first_new = batch.events.iter().find(|event| event.seq > cursor.seq);
            if batch.journal_id != cursor.journal_id
                || first_new.is_some_and(|event| cursor.seq.checked_increment() != Some(event.seq))
                || (batch.events.is_empty() && batch.next_after.seq > cursor.seq)
            {
                self.repair_needed = true;
                return Err(unavailable());
            }
        }
        let mut consumed = None;
        for event in &batch.events {
            let cursor = EventCursor {
                journal_id: event.journal_id,
                seq: event.seq,
            };
            if self.cursor.is_some_and(|cursor| {
                cursor.journal_id == event.journal_id && event.seq <= cursor.seq
            }) {
                if replay {
                    // Already-accounted live evidence is not new cold evidence.
                    // Still advance this sweep's validated replay position.
                    consumed = Some(cursor);
                }
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
                if !self.enqueue(id, cause, None, completed_rows) {
                    self.attention_refresh_required = true;
                    if let Some(comparison) = &mut self.comparison
                        && (comparison.verified.contains(&id) || comparison.waiting.contains(&id))
                    {
                        comparison.invalidate_attention(id);
                    }
                    if !replay {
                        self.unaccounted_feed = true;
                    }
                    return Ok((false, consumed));
                }
                if let Some(comparison) = &mut self.comparison {
                    comparison.invalidate_attention(id);
                }
            } else {
                // Unknown kinds/versions and global hints never borrow a raw task_id.
                self.repair_needed = true;
                self.attention_refresh_required = true;
            }
            consumed = Some(cursor);
            if self
                .cursor
                .is_none_or(|old| old.journal_id == cursor.journal_id && old.seq < cursor.seq)
            {
                self.cursor = Some(cursor);
                self.cursor_validated = true;
            }
        }
        if !replay {
            self.unaccounted_feed = false;
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
            if let Some(sweep) = &mut self.sweep
                && (sweep.complete || sweep.rows.contains_key(&id))
            {
                sweep.rows.insert(id, row.clone());
            }
            if let Some(comparison) = &mut self.comparison {
                comparison.confirm_attention(&row);
            }
            let signature = row.eligibility_signature();
            if let Some(history) = self.cold_unresolved.get(&id) {
                let same_history = history.matches(&row);
                if let Some(candidate) = self.candidates.get_mut(&id) {
                    candidate.warm = !same_history;
                }
                if !same_history || signature.busy == Some(true) || signature.quiescent.is_some() {
                    self.cold_unresolved.remove(&id);
                }
            }
            if self.confirmed.len() < MAX_RECONCILIATION_ROWS || self.confirmed.contains_key(&id) {
                self.confirmed.insert(id, row.clone());
            }
            if let Some(candidate) = self.candidates.get(&id)
                && signature.quiescent == Some(true)
            {
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
                        Some(ChangeCause::ReplayAbandoned) if signature.abandoned_without_turn => {
                            candidate.cause.clone()
                        }
                        _ if candidate.warm => Some(ChangeCause::RepairDifference),
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
            if signature.quiescent == Some(false)
                && let Some(candidate) = self.candidates.get_mut(&id)
            {
                candidate.previous = Some(row.clone());
                candidate.warm = matches!(self.previous, PreviousProjection::Present(_));
            }
            if let PreviousProjection::Present(rows) = &mut self.previous {
                rows.insert(id, row);
            }
        }
        for id in result.missing {
            self.candidates.remove(&id);
            self.cold_unresolved.remove(&id);
            if let Some(comparison) = &mut self.comparison {
                comparison.forget_attention(id);
            }
            if let Some(sweep) = &mut self.sweep
                && sweep.complete
            {
                sweep.rows.remove(&id);
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
        captured: Option<JournalWindow>,
        deadline: Duration,
    ) -> Result<(), WorkerError> {
        check(self.runtime.as_ref(), deadline)?;
        let window = if captured.is_some() {
            captured
        } else {
            let read = source.read(ReadQuery::default(), deadline);
            check(self.runtime.as_ref(), deadline)?;
            match read {
                Ok(result) => {
                    validate_read(&ReadQuery::default(), &result)?;
                    let EventReadResult::SnapshotRequired(control) = result else {
                        return Err(unavailable());
                    };
                    Some(control.window)
                }
                Err(_) => None,
            }
        };
        let baseline = window.as_ref().map(JournalWindow::cursor);
        // A valid saved cursor also owns retained pre-H hints. H stays pinned
        // for every task page; replay traverses both that backlog and >H.
        let replay_after = self
            .cursor
            .filter(|cursor| {
                window.as_ref().is_some_and(|window| {
                    cursor.journal_id == window.journal_id
                        && cursor.seq <= window.head_seq
                        && cursor.seq.as_u64() >= window.oldest_seq.as_u64().saturating_sub(1)
                })
            })
            .or(baseline);
        // Retention can expire a cursor after validation. Recovery decisions
        // still need coalescing even when the consumed cursor stays pinned.
        self.cursor_recovery |=
            window.is_some() && self.cursor.is_some() && replay_after != self.cursor;
        if window.as_ref().is_some_and(|window| {
            replay_after != self.cursor
                && (!self.cursor_validated
                    || self
                        .cursor
                        .is_some_and(|cursor| cursor.journal_id != window.journal_id))
        }) {
            // Ahead, expired and replaced-epoch saved cursors are not consumed
            // evidence in this window. Adopt H only after the full repair.
            self.cursor = None;
            self.cursor_validated = false;
        } else if self.cursor.is_some() && replay_after == self.cursor {
            self.cursor_validated = true;
        }
        if self.attention_refresh_required {
            if let Some(comparison) = &mut self.comparison {
                comparison.refresh_attention();
            }
            self.attention_refresh_required = false;
        }
        self.last_started = Some(self.runtime.now());
        self.repair_needed = false;
        self.sweep = Some(Sweep {
            rows: BTreeMap::new(),
            after: None,
            baseline,
            complete: false,
            replay_after,
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
                            let (all, consumed) =
                                self.consume_batch(&batch, true, Some(&sweep.rows))?;
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
                if let Some(baseline) = sweep.baseline
                    && !self.unaccounted_feed
                    && self.cursor.is_none_or(|old| {
                        old.journal_id != baseline.journal_id || old.seq < baseline.seq
                    })
                {
                    self.cursor = Some(baseline);
                    self.cursor_validated = true;
                }
                self.cold_unresolved
                    .retain(|id, _| sweep.rows.contains_key(id));
                // A known non-quiescent cold baseline becomes warm once complete.
                // Unknown proof of historical completion remains cold until confirmed.
                if matches!(self.previous, PreviousProjection::Absent) {
                    // This provenance scales with the complete projection, not
                    // the bounded candidate cache. Unknown overflow is history too.
                    for (id, row) in &sweep.rows {
                        let signature = row.eligibility_signature();
                        if signature.busy.is_none() || signature.quiescent.is_none() {
                            self.cold_unresolved.insert(*id, ColdHistory::new(row));
                        }
                    }
                    for (id, candidate) in &mut self.candidates {
                        if let Some(facts) = sweep.rows.get(id)
                            && facts.eligibility_signature().quiescent == Some(false)
                        {
                            candidate.previous = Some(facts.clone());
                            candidate.warm = true;
                        }
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
                self.comparison = Some(match self.comparison.take() {
                    Some(mut comparison) => {
                        // Timer sweeps refresh the complete projection while
                        // outstanding addressed verification keeps progressing.
                        comparison.old = old;
                        comparison.ids = ids.into_iter().collect();
                        comparison
                    }
                    None => Comparison {
                        cold: matches!(old, PreviousProjection::Absent),
                        old,
                        ids: ids.into_iter().collect(),
                        attention: BTreeMap::new(),
                        waiting: BTreeSet::new(),
                        verified: BTreeSet::new(),
                        hash: Sha256::new(),
                        hashed_through: None,
                        hashed_count: 0,
                    },
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
            if changed {
                comparison.invalidate_attention(id);
            }
            if let Some(facts) = &current {
                let signature = facts.eligibility_signature();
                let attention = potential_attention(facts) && !comparison.verified.contains(&id);
                if attention {
                    comparison.waiting.insert(id);
                } else if !potential_attention(facts) {
                    comparison.forget_attention(id);
                }
                let needs_confirmation = attention
                    || (changed
                        && (signature.quiescent == Some(true)
                            || (signature.busy.is_none()
                                && (signature.outcome.is_some() || facts.state == "abandoned"))));
                if needs_confirmation && !self.enqueue(id, None, Some((old.clone(), warm)), None) {
                    // Keep unadmitted warm differences in the full baseline so
                    // the next sweep can rediscover them. Advance this page even
                    // when unrelated busy/unknown candidates fill the cache.
                    if warm && let PreviousProjection::Present(rows) = &mut self.previous {
                        if let Some(old) = old.clone() {
                            rows.insert(id, old);
                        } else {
                            rows.remove(&id);
                        }
                    }
                }
            } else if changed {
                comparison.forget_attention(id);
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
        // Overflow attention is still part of the complete set, but only 256
        // IDs may occupy the individual confirmation queue at once.
        for id in comparison.waiting.iter().copied().take(REPAIR_MAX_LIMIT) {
            let old = match &comparison.old {
                PreviousProjection::Absent => None,
                PreviousProjection::Present(rows) => rows.get(&id).cloned(),
            };
            let warm = matches!(comparison.old, PreviousProjection::Present(_));
            self.enqueue(id, None, Some((old, warm)), None);
        }
        if comparison.ids.is_empty()
            && comparison.waiting.is_empty()
            && self.sweep.is_none()
            && !self.attention_refresh_required
        {
            use std::ops::Bound::{Excluded, Unbounded};
            let start = comparison.hashed_through.map_or(Unbounded, Excluded);
            for (id, signature) in comparison
                .attention
                .range((start, Unbounded))
                .take(REPAIR_MAX_LIMIT)
            {
                comparison.hash.update(
                    serde_json::to_vec(&(
                        id,
                        signature.latest_turn_id,
                        signature.outcome,
                        signature.outcome.is_none().then_some(&signature.code),
                    ))
                    .map_err(|_| unavailable())?,
                );
                comparison.hash.update(b"\n");
                comparison.hashed_through = Some(*id);
                comparison.hashed_count += 1;
            }
        }
        if comparison.ids.is_empty()
            && comparison.waiting.is_empty()
            && comparison.hashed_count == comparison.attention.len()
            && self.sweep.is_none()
            && !self.attention_refresh_required
            // A startup summary needs a cold chunk, but actual warm changes
            // must keep their provenance. Drain those changes first and retain
            // the verified set for the next chunk.
            && !(comparison.cold && self.has_warm_changes())
            // Keep recovery attached to every fresh decision chunk, including
            // delayed proof. Complete in a later settled chunk so consumers can
            // both coalesce decisions and observe recovery finishing.
            && !(self.cursor_recovery && (!self.changes.is_empty() || self.recovery_pending()))
        {
            comparison
                .hash
                .update((comparison.hashed_count as u64).to_le_bytes());
            Ok(Some(AttentionSummary {
                count: comparison.hashed_count,
                fingerprint: format!("{:x}", comparison.hash.finalize()),
            }))
        } else {
            self.comparison = Some(comparison);
            Ok(None)
        }
    }
    fn has_warm_changes(&self) -> bool {
        self.changes
            .iter()
            .any(|change| change.cause == ChangeCause::RepairDifference)
    }
    fn recovery_pending(&self) -> bool {
        self.candidates.keys().any(|id| match &self.previous {
            PreviousProjection::Absent => true,
            PreviousProjection::Present(rows) => rows
                .get(id)
                .is_none_or(|row| row.eligibility_signature().quiescent != Some(false)),
        })
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
        let cold = matches!(self.previous, PreviousProjection::Absent)
            || self
                .comparison
                .as_ref()
                .is_some_and(|comparison| comparison.cold);
        let mut captured = None;
        if let Some(read) = input.read {
            read.validate()?;
            match read {
                EventReadResult::Batch(batch) => {
                    self.consume_batch(&batch, false, None)?;
                }
                EventReadResult::SnapshotRequired(control) => {
                    self.cursor_recovery |= control.reason != "bootstrap";
                    let compatible = self.sweep.as_ref().is_some_and(|sweep| {
                        sweep.baseline.is_some_and(|baseline| {
                            baseline.journal_id == control.window.journal_id
                                && baseline.seq <= control.window.head_seq
                                && baseline.seq.as_u64()
                                    >= control.window.oldest_seq.as_u64().saturating_sub(1)
                        })
                    });
                    if !compatible {
                        captured = Some(control.window);
                        self.sweep = None;
                        self.unaccounted_feed = false;
                        self.repair_needed = true;
                    }
                }
            }
        }
        // Pending proof is always ahead of historical enumeration work.
        self.confirm_pending(source, input.include_titles, deadline)?;
        let timer_due = self
            .last_started
            .is_none_or(|started| self.runtime.now().saturating_sub(started) >= REPAIR_INTERVAL);
        let due = input.repair_due || self.repair_needed || timer_due;
        let can_start = self.comparison.is_none()
            || self.comparison.as_ref().is_some_and(|comparison| {
                comparison.ids.is_empty()
                    && (!comparison.waiting.is_empty()
                        || self.attention_refresh_required
                        || self.cursor_recovery
                        || captured.is_some())
            }) && (input.repair_due
                || captured.is_some()
                || timer_due
                || self.attention_refresh_required);
        if self.sweep.is_none() && can_start && due {
            self.start_sweep(source, captured, deadline)?;
        }
        let mut repair = self.advance_sweep(source, deadline)?;
        let attention = self.compare_page()?;
        if attention.is_some() {
            repair = RepairProgress::Complete;
            if !self.repair_needed {
                self.cursor_recovery = false;
            }
        } else if self.comparison.is_some() {
            repair = RepairProgress::InProgress;
        }
        check(self.runtime.as_ref(), deadline)?;
        let result = Reconciliation {
            consumed_after: self.cursor,
            baseline: if cold && !self.has_warm_changes() {
                BaselineKind::Cold
            } else {
                BaselineKind::Warm
            },
            changes: self.changes.drain(..).collect(),
            confirmed: std::mem::take(&mut self.confirmed).into_values().collect(),
            pending_ids: self.order.iter().copied().collect(),
            repair,
            attention,
            repair_needed: self.repair_needed || self.cursor_recovery,
        };
        result.validate()?;
        Ok(result)
    }
}
