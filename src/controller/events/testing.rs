//! Deterministic, bounded in-memory seams for component tests.
//!
//! These are public so integration test binaries can share them, but are never
//! production adapters. No files, processes, RPC, notifications or real sleeps.
//! Script/capture queues are capped at 256; recording sink at 128 batches.

use super::contracts::*;
use crate::{
    error::WorkerError,
    task::{TaskId, TurnId},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

type SleepHook = dyn Fn(Duration) + Send + Sync;

/// Sleep advances monotonic time, then invokes the hook without a held mutex.
/// Hooks may append to a journal or cancel; they never wait for wall time.
#[derive(Clone, Default)]
pub struct ManualEventRuntime {
    inner: Arc<Mutex<ManualClock>>,
}
#[derive(Default)]
struct ManualClock {
    now: Duration,
    cancelled: bool,
    hook: Option<Arc<SleepHook>>,
    sleeps: VecDeque<Duration>,
}
impl ManualEventRuntime {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn advance(&self, duration: Duration) {
        let mut clock = self.inner.lock().unwrap();
        clock.now = clock.now.saturating_add(duration);
    }
    pub fn cancel(&self) {
        self.inner.lock().unwrap().cancelled = true;
    }
    pub fn on_sleep(&self, hook: impl Fn(Duration) + Send + Sync + 'static) {
        self.inner.lock().unwrap().hook = Some(Arc::new(hook));
    }
    pub fn clear_sleep_hook(&self) {
        self.inner.lock().unwrap().hook = None;
    }
    pub fn sleeps(&self) -> Vec<Duration> {
        self.inner.lock().unwrap().sleeps.iter().copied().collect()
    }
}
impl EventRuntime for ManualEventRuntime {
    fn now(&self) -> Duration {
        self.inner.lock().unwrap().now
    }
    fn sleep(&self, duration: Duration) {
        let hook = {
            let mut clock = self.inner.lock().unwrap();
            clock.now = clock.now.saturating_add(duration);
            if clock.sleeps.len() == MAX_RECONCILIATION_ROWS {
                clock.sleeps.pop_front();
            }
            clock.sleeps.push_back(duration);
            clock.hook.clone()
        };
        if let Some(hook) = hook {
            hook(duration);
        }
    }
    fn cancelled(&self) -> bool {
        self.inner.lock().unwrap().cancelled
    }
}

/// Deadline/cancellation checks shared by memory readers/writers.
fn check(runtime: &dyn EventRuntime, deadline: Duration) -> Result<(), WorkerError> {
    if runtime.cancelled() {
        return Err(WorkerError::Unavailable(format!(
            "{CONTROLLER_EVENTS_CANCELLED}: cancelled"
        )));
    }
    if runtime.now() >= deadline {
        return Err(unavailable("deadline exhausted"));
    }
    Ok(())
}

#[derive(Clone, Default)]
pub struct RecordingSink {
    inner: Arc<Mutex<Vec<EventBatch>>>,
    drop_mode: Arc<AtomicBool>,
}
impl RecordingSink {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set_drop_mode(&self, dropped: bool) {
        self.drop_mode.store(dropped, Ordering::SeqCst);
    }
    pub fn batches(&self) -> Vec<EventBatch> {
        self.inner.lock().unwrap().clone()
    }
    pub fn take_batches(&self) -> Vec<EventBatch> {
        std::mem::take(&mut *self.inner.lock().unwrap())
    }
}
impl EventSink for RecordingSink {
    fn try_publish(&self, batch: EventBatch) -> PublishAttempt {
        if self.drop_mode.load(Ordering::SeqCst) {
            return PublishAttempt::Dropped;
        }
        let Ok(mut batches) = self.inner.try_lock() else {
            return PublishAttempt::Dropped;
        };
        if batches.len() == PUBLISHER_CAPACITY {
            return PublishAttempt::Dropped;
        }
        batches.push(batch);
        PublishAttempt::Queued
    }
}

/// A bounded committed-prefix journal. new() uses deterministic UUID 1 and a
/// manual runtime at zero; with_runtime permits wake hooks shared with readers.
#[derive(Clone)]
pub struct MemoryJournal {
    inner: Arc<Mutex<MemoryJournalState>>,
    runtime: Arc<dyn EventRuntime>,
}
struct MemoryJournalState {
    window: JournalWindow,
    events: VecDeque<WireEvent>,
    bytes: usize,
}
impl Default for MemoryJournal {
    fn default() -> Self {
        Self::new()
    }
}
impl MemoryJournal {
    pub fn new() -> Self {
        Self::with_runtime(Arc::new(ManualEventRuntime::new()))
    }
    pub fn with_runtime(runtime: Arc<dyn EventRuntime>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryJournalState {
                window: JournalWindow {
                    journal_id: uuid::Uuid::from_u128(1),
                    oldest_seq: Seq::new(1),
                    head_seq: Seq::ZERO,
                },
                events: VecDeque::new(),
                bytes: 0,
            })),
            runtime,
        }
    }
    pub fn replace_epoch(&self, journal_id: uuid::Uuid) -> Result<(), WorkerError> {
        if journal_id.is_nil() {
            return Err(invalid("nil replacement epoch"));
        }
        let mut state = self.inner.lock().unwrap();
        state.window = JournalWindow {
            journal_id,
            oldest_seq: Seq::new(1),
            head_seq: Seq::ZERO,
        };
        state.events.clear();
        state.bytes = 0;
        Ok(())
    }
    /// Keep records from oldest onward; oldest=head+1 can empty retention.
    pub fn trim_to(&self, oldest: Seq) -> Result<(), WorkerError> {
        let mut state = self.inner.lock().unwrap();
        if oldest < state.window.oldest_seq
            || oldest.as_u64().saturating_sub(1) > state.window.head_seq.as_u64()
        {
            return Err(invalid("invalid trim boundary"));
        }
        while state.events.front().is_some_and(|event| event.seq < oldest) {
            let event = state.events.pop_front().unwrap();
            state.bytes -= event.encoded_len()?;
        }
        state.window.oldest_seq = oldest;
        Ok(())
    }
}
impl JournalReader for MemoryJournal {
    fn window(&self, deadline: Duration) -> Result<JournalWindow, WorkerError> {
        check(self.runtime.as_ref(), deadline)?;
        Ok(self.inner.lock().unwrap().window.clone())
    }
    fn read(&self, query: ReadQuery, deadline: Duration) -> Result<EventReadResult, WorkerError> {
        let query = query.normalized();
        let until = self
            .runtime
            .now()
            .saturating_add(Duration::from_millis(query.wait_ms))
            .min(deadline);
        loop {
            check(self.runtime.as_ref(), deadline)?;
            // This scope releases the mutex before any runtime hook runs.
            let result = {
                let state = self.inner.lock().unwrap();
                let window = state.window.clone();
                let reason = match query.after {
                    None => Some("bootstrap"),
                    Some(cursor) if cursor.journal_id != window.journal_id => {
                        Some("journal_changed")
                    }
                    Some(cursor)
                        if cursor.seq.as_u64() < window.oldest_seq.as_u64().saturating_sub(1) =>
                    {
                        Some("cursor_expired")
                    }
                    Some(cursor) if cursor.seq > window.head_seq => Some("cursor_ahead"),
                    _ => None,
                };
                if let Some(reason) = reason {
                    return Ok(EventReadResult::SnapshotRequired(SnapshotRequired {
                        reason: reason.into(),
                        window,
                    }));
                }
                let after = query.after.unwrap();
                let events: Vec<_> = state
                    .events
                    .iter()
                    .filter(|event| event.seq > after.seq)
                    .take(query.limit)
                    .cloned()
                    .collect();
                let next_after = events.last().map_or(after, |event| EventCursor {
                    journal_id: event.journal_id,
                    seq: event.seq,
                });
                ReadBatch {
                    schema_version: SCHEMA_VERSION,
                    journal_id: window.journal_id,
                    oldest_seq: window.oldest_seq,
                    head_seq: window.head_seq,
                    next_after,
                    has_more: next_after.seq < window.head_seq,
                    events,
                }
            };
            result.validate()?;
            if !result.events.is_empty() || self.runtime.now() >= until {
                return Ok(EventReadResult::Batch(result));
            }
            self.runtime
                .sleep(JOURNAL_CHECK_INTERVAL.min(until.saturating_sub(self.runtime.now())));
        }
    }
}
impl JournalWriter for MemoryJournal {
    fn append(&self, batch: EventBatch, deadline: Duration) -> Result<EventCursor, WorkerError> {
        check(self.runtime.as_ref(), deadline)?;
        let mut state = self.inner.lock().unwrap();
        let head = state
            .window
            .head_seq
            .as_u64()
            .checked_add(batch.len() as u64)
            .ok_or_else(|| unavailable("sequence exhausted"))?;
        let millis = u64::try_from(self.runtime.now().as_millis()).unwrap_or(u64::MAX);
        let mut events = Vec::with_capacity(batch.len());
        let mut bytes = 0;
        for (index, event) in batch.events().iter().enumerate() {
            let event = event.to_wire(
                state.window.journal_id,
                Seq::new(state.window.head_seq.as_u64() + index as u64 + 1),
                millis,
            )?;
            bytes += event.encoded_len()?;
            events.push(event);
        }
        // Validate/encode the entire batch before making its head visible.
        state.events.extend(events);
        state.bytes += bytes;
        state.window.head_seq = Seq::new(head);
        while state.bytes > MAX_RETAINED_BYTES {
            let removed = state.events.pop_front().unwrap();
            state.bytes -= removed.encoded_len()?;
            state.window.oldest_seq = removed
                .seq
                .checked_increment()
                .ok_or_else(|| unavailable("sequence exhausted"))?;
        }
        Ok(state.window.cursor())
    }
}

#[derive(Clone)]
pub struct FakeJournalProvider {
    reader: Option<Arc<dyn JournalReader>>,
    error: Option<String>,
    opens: Arc<AtomicUsize>,
}
impl FakeJournalProvider {
    pub fn present(reader: Arc<dyn JournalReader>) -> Self {
        Self {
            reader: Some(reader),
            error: None,
            opens: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn absent() -> Self {
        Self {
            reader: None,
            error: None,
            opens: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            reader: None,
            error: Some(message.into()),
            opens: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn open_count(&self) -> usize {
        self.opens.load(Ordering::SeqCst)
    }
}
impl JournalProvider for FakeJournalProvider {
    fn open_existing(
        &self,
        _deadline: Duration,
    ) -> Result<Option<Arc<dyn JournalReader>>, WorkerError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = &self.error {
            return Err(unavailable(error));
        }
        Ok(self.reader.clone())
    }
}

/// Bounded scripts fail explicitly when exhausted instead of inventing reads.
struct Script<T> {
    queue: Mutex<VecDeque<Result<T, WorkerError>>>,
}
impl<T> Default for Script<T> {
    fn default() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
        }
    }
}
impl<T> Script<T> {
    fn push(&self, result: Result<T, WorkerError>) -> Result<(), WorkerError> {
        let mut queue = self.queue.lock().unwrap();
        if queue.len() == MAX_RECONCILIATION_ROWS {
            return Err(unavailable("script queue full"));
        }
        queue.push_back(result);
        Ok(())
    }
    fn pop(&self) -> Result<T, WorkerError> {
        self.queue
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(unavailable("script exhausted")))
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedEventRequest {
    pub selector: EventSelector,
    pub body: serde_json::Value,
    pub deadline: Duration,
}
fn capture(
    requests: &Mutex<Vec<CapturedEventRequest>>,
    selector: EventSelector,
    deadline: Duration,
) -> Result<(), WorkerError> {
    let body = selector.request_body()?;
    let mut requests = requests.lock().unwrap();
    if requests.len() == MAX_RECONCILIATION_ROWS {
        return Err(unavailable("request capture full"));
    }
    requests.push(CapturedEventRequest {
        selector,
        body,
        deadline,
    });
    Ok(())
}
#[derive(Default)]
pub struct ScriptedEventSource {
    discovery: Script<EventSupport>,
    reads: Script<EventReadResult>,
    tasks: Script<TaskFactsBatch>,
    repair: Script<TaskRepairPage>,
    requests: Mutex<Vec<CapturedEventRequest>>,
    discovery_deadlines: Mutex<Vec<Duration>>,
}
impl ScriptedEventSource {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn queue_discovery(
        &self,
        result: Result<EventSupport, WorkerError>,
    ) -> Result<(), WorkerError> {
        self.discovery.push(result)
    }
    pub fn queue_read(
        &self,
        result: Result<EventReadResult, WorkerError>,
    ) -> Result<(), WorkerError> {
        self.reads.push(result)
    }
    pub fn queue_tasks(
        &self,
        result: Result<TaskFactsBatch, WorkerError>,
    ) -> Result<(), WorkerError> {
        self.tasks.push(result)
    }
    pub fn queue_repair(
        &self,
        result: Result<TaskRepairPage, WorkerError>,
    ) -> Result<(), WorkerError> {
        self.repair.push(result)
    }
    pub fn requests(&self) -> Vec<CapturedEventRequest> {
        self.requests.lock().unwrap().clone()
    }
    pub fn discovery_deadlines(&self) -> Vec<Duration> {
        self.discovery_deadlines.lock().unwrap().clone()
    }
    pub fn clear_requests(&self) {
        self.requests.lock().unwrap().clear();
        self.discovery_deadlines.lock().unwrap().clear();
    }
}
impl EventSource for ScriptedEventSource {
    fn discover(&self, deadline: Duration) -> Result<EventSupport, WorkerError> {
        let mut deadlines = self.discovery_deadlines.lock().unwrap();
        if deadlines.len() == MAX_RECONCILIATION_ROWS {
            return Err(unavailable("discovery capture full"));
        }
        deadlines.push(deadline);
        drop(deadlines);
        self.discovery.pop()
    }
    fn read(&self, query: ReadQuery, deadline: Duration) -> Result<EventReadResult, WorkerError> {
        capture(&self.requests, EventSelector::Read(query), deadline)?;
        self.reads.pop()
    }
    fn tasks(
        &self,
        query: TaskAddressQuery,
        deadline: Duration,
    ) -> Result<TaskFactsBatch, WorkerError> {
        capture(&self.requests, EventSelector::Tasks(query), deadline)?;
        self.tasks.pop()
    }
    fn repair(
        &self,
        query: TaskRepairQuery,
        deadline: Duration,
    ) -> Result<TaskRepairPage, WorkerError> {
        capture(&self.requests, EventSelector::Repair(query), deadline)?;
        self.repair.pop()
    }
}
/// Standalone scripted JournalReader including batches and reset controls.
pub struct FakeJournalReader {
    pub window: JournalWindow,
    reads: Script<EventReadResult>,
    requests: Mutex<Vec<(ReadQuery, Duration)>>,
}
impl FakeJournalReader {
    pub fn new(window: JournalWindow) -> Self {
        Self {
            window,
            reads: Script::default(),
            requests: Mutex::new(Vec::new()),
        }
    }
    pub fn queue_read(
        &self,
        result: Result<EventReadResult, WorkerError>,
    ) -> Result<(), WorkerError> {
        self.reads.push(result)
    }
    pub fn requests(&self) -> Vec<(ReadQuery, Duration)> {
        self.requests.lock().unwrap().clone()
    }
}
impl JournalReader for FakeJournalReader {
    fn window(&self, _deadline: Duration) -> Result<JournalWindow, WorkerError> {
        self.window.validate()?;
        Ok(self.window.clone())
    }
    fn read(&self, query: ReadQuery, deadline: Duration) -> Result<EventReadResult, WorkerError> {
        let mut requests = self.requests.lock().unwrap();
        if requests.len() == MAX_RECONCILIATION_ROWS {
            return Err(unavailable("read capture full"));
        }
        requests.push((query, deadline));
        drop(requests);
        self.reads.pop()
    }
}

#[derive(Clone)]
pub struct MemoryTaskReader {
    inner: Arc<Mutex<TaskRows>>,
    runtime: Arc<dyn EventRuntime>,
}
struct TaskRows {
    rows: BTreeMap<TaskId, TaskFacts>,
    proof_work: BTreeMap<TaskId, usize>,
    binding: u64,
    generation: u64,
    baseline: Option<EventCursor>,
    page_budget: usize,
    proof_budget: usize,
    directory_entries: usize,
    addressed: Vec<TaskAddressQuery>,
    repairs: Vec<TaskRepairQuery>,
}
impl Default for MemoryTaskReader {
    fn default() -> Self {
        Self::new()
    }
}
impl MemoryTaskReader {
    pub fn new() -> Self {
        Self::with_runtime(Arc::new(ManualEventRuntime::new()))
    }
    pub fn with_runtime(runtime: Arc<dyn EventRuntime>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TaskRows {
                rows: BTreeMap::new(),
                proof_work: BTreeMap::new(),
                binding: 1,
                generation: 0,
                baseline: None,
                page_budget: REPAIR_MAX_LIMIT,
                proof_budget: MAX_DISPATCH_ASSOCIATIONS,
                directory_entries: 0,
                addressed: Vec::new(),
                repairs: Vec::new(),
            })),
            runtime,
        }
    }
    pub fn insert(&self, facts: TaskFacts) -> Result<(), WorkerError> {
        facts.validate()?;
        let mut state = self.inner.lock().unwrap();
        if !state.rows.contains_key(&facts.task_id)
            && state.rows.len() == REPAIR_MAX_DIRECTORY_ENTRIES
        {
            return Err(unavailable("memory registry full"));
        }
        state.rows.insert(facts.task_id, facts);
        state.generation += 1;
        Ok(())
    }
    pub fn remove(&self, id: TaskId) {
        let mut state = self.inner.lock().unwrap();
        state.rows.remove(&id);
        state.proof_work.remove(&id);
        state.generation += 1;
    }
    pub fn set_page_budget(&self, rows: usize) {
        self.inner.lock().unwrap().page_budget = rows.clamp(1, REPAIR_MAX_LIMIT);
    }
    pub fn set_proof_budget(&self, checks: usize) {
        self.inner.lock().unwrap().proof_budget = checks.clamp(1, MAX_DISPATCH_ASSOCIATIONS);
    }
    pub fn set_proof_work(&self, id: TaskId, checks: usize) {
        let mut state = self.inner.lock().unwrap();
        state.proof_work.insert(id, checks);
        state.generation += 1;
    }
    pub fn set_baseline(&self, baseline: Option<EventCursor>) {
        self.inner.lock().unwrap().baseline = baseline;
    }
    pub fn set_binding(&self, binding: u64) {
        self.inner.lock().unwrap().binding = binding;
    }
    pub fn set_directory_entries(&self, count: usize) {
        self.inner.lock().unwrap().directory_entries = count;
    }
    pub fn addressed_requests(&self) -> Vec<TaskAddressQuery> {
        self.inner.lock().unwrap().addressed.clone()
    }
    pub fn repair_requests(&self) -> Vec<TaskRepairQuery> {
        self.inner.lock().unwrap().repairs.clone()
    }
}
impl TaskProjectionReader for MemoryTaskReader {
    fn addressed(
        &self,
        query: TaskAddressQuery,
        deadline: Duration,
    ) -> Result<TaskFactsBatch, WorkerError> {
        check(self.runtime.as_ref(), deadline)?;
        query.validate()?;
        let mut state = self.inner.lock().unwrap();
        if state.addressed.len() == MAX_RECONCILIATION_ROWS {
            return Err(unavailable("addressed capture full"));
        }
        state.addressed.push(query.clone());
        let mut offset = 0;
        if let Some(cursor) = &query.proof_after {
            let token: MemoryProofToken = decode_token(cursor)?;
            if token.version != SCHEMA_VERSION
                || token.binding != state.binding
                || token.task_ids != query.task_ids
            {
                return Err(invalid("proof token binding/query mismatch"));
            }
            if token.generation == state.generation {
                offset = token.offset;
            }
        }
        let end = offset.saturating_add(state.proof_budget);
        let mut total = 0usize;
        let mut rows = Vec::new();
        let mut missing = Vec::new();
        for id in &query.task_ids {
            let Some(mut row) = state.rows.get(id).cloned() else {
                missing.push(*id);
                continue;
            };
            let proof = row.eligibility_signature();
            let work = if proof.busy == Some(true) {
                0
            } else {
                state.proof_work.get(id).copied().unwrap_or(0)
            };
            total = total.saturating_add(work);
            if total > end {
                row.busy = None;
                row.quiescent = None;
                row.queue_dispatching = None;
            } else if work > 0 {
                row.queue_dispatching = Some(false);
                row.busy = Some(false);
                row.quiescent = Some(matches!(
                    row.state.as_str(),
                    "open" | "closed" | "abandoned" | "lost"
                ));
            } else {
                row.busy = proof.busy;
                row.quiescent = proof.quiescent;
            }
            if !query.include_titles {
                row.title = None;
            }
            rows.push(row);
        }
        if offset > total {
            return Err(invalid("proof token position is ahead"));
        }
        let proof_after = if end < total {
            Some(OpaqueCursor::encode(&MemoryProofToken {
                version: SCHEMA_VERSION,
                binding: state.binding,
                generation: state.generation,
                task_ids: query.task_ids,
                offset: end,
            })?)
        } else {
            None
        };
        let batch = TaskFactsBatch {
            rows,
            missing,
            proof_after,
            baseline_after: state.baseline,
        };
        batch.validate()?;
        Ok(batch)
    }
    fn repair(
        &self,
        query: TaskRepairQuery,
        deadline: Duration,
    ) -> Result<TaskRepairPage, WorkerError> {
        check(self.runtime.as_ref(), deadline)?;
        let query = query.normalized();
        let mut state = self.inner.lock().unwrap();
        if state.repairs.len() == MAX_RECONCILIATION_ROWS {
            return Err(unavailable("repair capture full"));
        }
        state.repairs.push(query.clone());
        if state.directory_entries.max(state.rows.len()) > REPAIR_MAX_DIRECTORY_ENTRIES {
            return Err(WorkerError::Unavailable(format!(
                "{CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE}: repair unavailable, registry too large"
            )));
        }
        let after = if let Some(cursor) = &query.after {
            let token: MemoryRepairToken = decode_token(cursor)?;
            if token.version != SCHEMA_VERSION || token.binding != state.binding {
                return Err(invalid("repair token binding mismatch"));
            }
            Some(token.after_task_id)
        } else {
            None
        };
        let baseline_after = if query.after.is_some() {
            query.baseline_after
        } else {
            query.baseline_after.or(state.baseline)
        };
        let limit = query.limit.min(state.page_budget);
        let mut proof_left = state.proof_budget;
        // Names-only fake registry; key cursor deliberately ignores generation.
        let mut remaining = state
            .rows
            .iter()
            .filter(|(id, _)| after.is_none_or(|after| **id > after));
        let mut rows = Vec::new();
        for (id, row) in remaining.by_ref().take(limit) {
            let mut row = row.clone();
            row.title = None;
            let signature = row.eligibility_signature();
            let work = if signature.busy == Some(true) {
                0
            } else {
                state.proof_work.get(id).copied().unwrap_or(0)
            };
            if work > proof_left {
                row.queue_dispatching = None;
                row.busy = None;
                row.quiescent = None;
                proof_left = 0;
            } else {
                proof_left -= work;
                row.busy = signature.busy;
                row.quiescent = signature.quiescent;
            }
            rows.push(row);
        }
        let complete = remaining.next().is_none();
        let next = if complete {
            None
        } else {
            Some(OpaqueCursor::encode(&MemoryRepairToken {
                version: SCHEMA_VERSION,
                binding: state.binding,
                after_task_id: rows.last().unwrap().task_id,
            })?)
        };
        let page = TaskRepairPage {
            rows,
            next,
            complete,
            restart: false,
            baseline_after,
        };
        page.validate()?;
        Ok(page)
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryRepairToken {
    version: u32,
    binding: u64,
    after_task_id: TaskId,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryProofToken {
    version: u32,
    binding: u64,
    generation: u64,
    task_ids: Vec<TaskId>,
    offset: usize,
}
fn decode_token<T: serde::de::DeserializeOwned>(cursor: &OpaqueCursor) -> Result<T, WorkerError> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor.as_str())
        .map_err(|_| invalid("invalid memory token"))?;
    serde_json::from_slice(&bytes).map_err(|_| invalid("invalid memory token"))
}

#[derive(Clone)]
pub struct FakeTaskProjectionProvider {
    reader: Option<Arc<dyn TaskProjectionReader>>,
    error: Option<String>,
    opens: Arc<AtomicUsize>,
}
impl FakeTaskProjectionProvider {
    pub fn new(reader: Arc<dyn TaskProjectionReader>) -> Self {
        Self {
            reader: Some(reader),
            error: None,
            opens: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            reader: None,
            error: Some(message.into()),
            opens: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn open_count(&self) -> usize {
        self.opens.load(Ordering::SeqCst)
    }
}
impl TaskProjectionProvider for FakeTaskProjectionProvider {
    fn open_existing(
        &self,
        _deadline: Duration,
    ) -> Result<Arc<dyn TaskProjectionReader>, WorkerError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.reader
            .clone()
            .ok_or_else(|| unavailable(self.error.as_deref().unwrap_or("missing task reader")))
    }
}

#[derive(Default)]
pub struct FakeEventReconciler {
    results: Script<Reconciliation>,
    inputs: Vec<(ReconcileInput, Duration)>,
}
impl FakeEventReconciler {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn queue(&self, result: Result<Reconciliation, WorkerError>) -> Result<(), WorkerError> {
        if let Ok(result) = &result {
            result.validate()?;
        }
        self.results.push(result)
    }
    pub fn inputs(&self) -> &[(ReconcileInput, Duration)] {
        &self.inputs
    }
}
impl EventReconciler for FakeEventReconciler {
    fn reconcile(
        &mut self,
        _source: &dyn EventSource,
        input: ReconcileInput,
        deadline: Duration,
    ) -> Result<Reconciliation, WorkerError> {
        if self.inputs.len() == MAX_RECONCILIATION_ROWS {
            return Err(unavailable("reconcile capture full"));
        }
        self.inputs.push((input, deadline));
        self.results.pop()
    }
}

#[derive(Default)]
pub struct MemoryViewerEventSource {
    inner: Mutex<ViewerSubscribers>,
}
#[derive(Default)]
struct ViewerSubscribers {
    senders: Vec<tokio::sync::mpsc::Sender<ViewerMessage>>,
    after: Vec<Option<EventCursor>>,
    stopped: bool,
}
impl MemoryViewerEventSource {
    pub fn new() -> Self {
        Self::default()
    }
    /// Try-only fanout; a full subscriber is disconnected, never skips records.
    pub fn push(&self, message: ViewerMessage) -> usize {
        let mut state = self.inner.lock().unwrap();
        if state.stopped {
            return 0;
        }
        let mut delivered = 0;
        state.senders.retain(|sender| {
            if sender.try_send(message.clone()).is_ok() {
                delivered += 1;
                true
            } else {
                false
            }
        });
        delivered
    }
    pub fn subscriptions(&self) -> Vec<Option<EventCursor>> {
        self.inner.lock().unwrap().after.clone()
    }
}
impl ViewerEventSource for MemoryViewerEventSource {
    fn subscribe(
        &self,
        after: Option<EventCursor>,
    ) -> Result<tokio::sync::mpsc::Receiver<ViewerMessage>, WorkerError> {
        let mut state = self.inner.lock().unwrap();
        state.senders.retain(|s| !s.is_closed());
        if state.stopped
            || state.senders.len() == SSE_MAX_STREAMS
            || state.after.len() == MAX_RECONCILIATION_ROWS
        {
            return Err(unavailable("viewer subscription unavailable"));
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(SSE_CAPACITY);
        state.senders.push(sender);
        state.after.push(after);
        Ok(receiver)
    }
    fn stop(&self) {
        let mut state = self.inner.lock().unwrap();
        state.stopped = true;
        state.senders.clear();
    }
}
pub struct FakeLocalProjectionRefresh {
    requests: AtomicUsize,
    publications: tokio::sync::broadcast::Sender<u64>,
}
impl Default for FakeLocalProjectionRefresh {
    fn default() -> Self {
        Self::new()
    }
}
impl FakeLocalProjectionRefresh {
    pub fn new() -> Self {
        let (publications, _) = tokio::sync::broadcast::channel(SSE_CAPACITY);
        Self {
            requests: AtomicUsize::new(0),
            publications,
        }
    }
    pub fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
    pub fn publish(&self, revision: u64) -> usize {
        self.publications.send(revision).unwrap_or(0)
    }
}
impl LocalProjectionRefresh for FakeLocalProjectionRefresh {
    fn request_refresh(&self) {
        self.requests.fetch_add(1, Ordering::SeqCst);
    }
    fn subscribe_publications(&self) -> tokio::sync::broadcast::Receiver<u64> {
        self.publications.subscribe()
    }
}
#[derive(Default)]
pub struct RecordingNoticeChannel {
    records: Mutex<Vec<(Notice, Duration)>>,
    error: Mutex<Option<String>>,
}
impl RecordingNoticeChannel {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set_error(&self, error: Option<String>) {
        *self.error.lock().unwrap() = error;
    }
    pub fn records(&self) -> Vec<(Notice, Duration)> {
        self.records.lock().unwrap().clone()
    }
}
impl NoticeChannel for RecordingNoticeChannel {
    fn deliver(&self, notice: &Notice, deadline: Duration) -> Result<(), WorkerError> {
        let mut records = self.records.lock().unwrap();
        if records.len() == MAX_RECONCILIATION_ROWS {
            return Err(unavailable("notice capture full"));
        }
        records.push((notice.clone(), deadline));
        drop(records);
        if let Some(error) = self.error.lock().unwrap().as_deref() {
            return Err(unavailable(error));
        }
        Ok(())
    }
}

impl TaskFacts {
    /// Contract test builder, never a production proof source.
    #[doc(hidden)]
    pub fn test_terminal(
        task_id: TaskId,
        turn_id: TurnId,
        outcome: SafeOutcome,
        quiescent: bool,
    ) -> Self {
        let mut facts = Self {
            task_id,
            run_id: None,
            state: "open".into(),
            latest_turn_id: Some(turn_id),
            outcome: Some(outcome),
            code: None,
            runner_present: !quiescent,
            close_intent: false,
            auto_continue_intent: false,
            queue_dispatching: Some(false),
            result_imported: true,
            busy: Some(!quiescent),
            quiescent: Some(quiescent),
            fact_digest: String::new(),
            title: None,
        };
        facts.fact_digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&facts).unwrap()));
        facts
    }
}
impl Reconciliation {
    /// A cold baseline has no historical changes, even with a saved cursor.
    #[doc(hidden)]
    pub fn test_cold(consumed_after: Option<EventCursor>, confirmed: Vec<TaskFacts>) -> Self {
        Self {
            consumed_after,
            baseline: BaselineKind::Cold,
            changes: Vec::new(),
            confirmed,
            pending_ids: Vec::new(),
            repair: RepairProgress::Complete,
            attention: None,
            repair_needed: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn deadline() -> Duration {
        Duration::from_secs(60)
    }
    fn id(value: u128) -> TaskId {
        TaskId::new(uuid::Uuid::from_u128(value))
    }
    fn terminal(value: u128) -> TaskFacts {
        TaskFacts::test_terminal(
            id(value),
            TurnId::new(uuid::Uuid::from_u128(value + 100)),
            SafeOutcome::Done,
            true,
        )
    }
    fn batch(count: usize) -> EventBatch {
        EventBatch::try_new(vec![
            NewEvent::ControllerDrainChanged { drained: true };
            count
        ])
        .unwrap()
    }
    fn snapshot() -> EventReadResult {
        EventReadResult::SnapshotRequired(SnapshotRequired {
            reason: "bootstrap".into(),
            window: JournalWindow {
                journal_id: uuid::Uuid::from_u128(1),
                oldest_seq: Seq::new(1),
                head_seq: Seq::ZERO,
            },
        })
    }

    #[test]
    fn memory_append_exposes_only_delivered_cursor() {
        let runtime = ManualEventRuntime::new();
        let journal = MemoryJournal::with_runtime(Arc::new(runtime.clone()));
        let zero = journal.window(deadline()).unwrap().cursor();
        let head = journal.append(batch(3), deadline()).unwrap();
        assert_eq!(head.seq, Seq::new(3));
        let EventReadResult::Batch(read) = journal
            .read(
                ReadQuery {
                    after: Some(zero),
                    limit: 1,
                    wait_ms: 0,
                },
                deadline(),
            )
            .unwrap()
        else {
            panic!("expected batch")
        };
        assert_eq!(read.events.len(), 1);
        assert_eq!(read.next_after.seq, Seq::new(1));
        assert!(read.has_more);
        read.validate().unwrap();
        let EventReadResult::Batch(empty) = journal
            .read(
                ReadQuery {
                    after: Some(head),
                    limit: 1,
                    wait_ms: 500,
                },
                deadline(),
            )
            .unwrap()
        else {
            panic!("expected batch")
        };
        assert!(empty.events.is_empty());
        assert_eq!(empty.next_after, head);
        assert_eq!(runtime.now(), Duration::from_millis(500));
    }

    #[test]
    fn long_poll_wakes_without_locks_and_observes_cancellation() {
        let runtime = ManualEventRuntime::new();
        let journal = MemoryJournal::with_runtime(Arc::new(runtime.clone()));
        let after = journal.window(deadline()).unwrap().cursor();
        let publisher = journal.clone();
        runtime.on_sleep(move |_| {
            publisher.append(batch(1), deadline()).unwrap();
        });
        let EventReadResult::Batch(read) =
            journal.read(ReadQuery::follow(after), deadline()).unwrap()
        else {
            panic!("expected batch")
        };
        assert_eq!(read.events.len(), 1);
        assert_eq!(runtime.sleeps(), vec![Duration::from_millis(200)]);
        let cancel = runtime.clone();
        runtime.on_sleep(move |_| cancel.cancel());
        let error = journal
            .read(ReadQuery::follow(read.next_after), deadline())
            .unwrap_err();
        assert_eq!(error.public_code(), "CONTROLLER_EVENTS_CANCELLED");
    }

    #[test]
    fn memory_journal_repairs_bootstrap_epoch_expired_and_ahead() {
        let journal = MemoryJournal::new();
        assert_eq!(
            journal.read(ReadQuery::default(), deadline()).unwrap(),
            snapshot()
        );
        let head = journal.append(batch(3), deadline()).unwrap();
        journal.trim_to(Seq::new(3)).unwrap();
        for (seq, want) in [(1, "cursor_expired"), (4, "cursor_ahead")] {
            let EventReadResult::SnapshotRequired(control) = journal
                .read(
                    ReadQuery {
                        after: Some(EventCursor {
                            seq: Seq::new(seq),
                            ..head
                        }),
                        limit: 128,
                        wait_ms: 0,
                    },
                    deadline(),
                )
                .unwrap()
            else {
                panic!("expected repair")
            };
            assert_eq!(control.reason, want);
        }
        journal.replace_epoch(uuid::Uuid::from_u128(2)).unwrap();
        let EventReadResult::SnapshotRequired(control) =
            journal.read(ReadQuery::follow(head), deadline()).unwrap()
        else {
            panic!("expected repair")
        };
        assert_eq!(control.reason, "journal_changed");
        assert_eq!(control.window.head_seq, Seq::ZERO);
    }

    #[test]
    fn task_pages_resume_by_key_and_addressed_proof_is_independent() {
        let tasks = MemoryTaskReader::new();
        for value in [2, 4, 6] {
            tasks.insert(terminal(value)).unwrap();
        }
        tasks.set_page_budget(1);
        let baseline = EventCursor {
            journal_id: uuid::Uuid::from_u128(1),
            seq: Seq::new(20),
        };
        tasks.set_baseline(Some(baseline));
        let first = tasks
            .repair(TaskRepairQuery::default(), deadline())
            .unwrap();
        assert_eq!(first.rows[0].task_id, id(2));
        assert!(!first.complete);
        tasks.insert(terminal(1)).unwrap();
        let second = tasks
            .repair(
                TaskRepairQuery {
                    after: first.next,
                    limit: 128,
                    baseline_after: first.baseline_after,
                },
                deadline(),
            )
            .unwrap();
        assert_eq!(second.rows[0].task_id, id(4));
        let third = tasks
            .repair(
                TaskRepairQuery {
                    after: second.next,
                    limit: 128,
                    baseline_after: second.baseline_after,
                },
                deadline(),
            )
            .unwrap();
        assert_eq!(third.rows[0].task_id, id(6));
        assert!(third.complete);
        assert_eq!(third.baseline_after, Some(baseline));
        assert_eq!(
            tasks
                .repair(TaskRepairQuery::default(), deadline())
                .unwrap()
                .rows[0]
                .task_id,
            id(1)
        );
        tasks.set_proof_work(id(4), 40);
        tasks.set_proof_budget(16);
        let mut query = TaskAddressQuery::try_new(vec![id(4), id(9)], false, None).unwrap();
        for index in 0..3 {
            let read = tasks.addressed(query.clone(), deadline()).unwrap();
            assert_eq!(read.missing, vec![id(9)]);
            assert_eq!(read.rows[0].quiescent == Some(true), index == 2);
            query.proof_after = read.proof_after;
        }
        assert!(query.proof_after.is_none());
        tasks.set_directory_entries(100_001);
        assert_eq!(
            tasks
                .repair(TaskRepairQuery::default(), deadline())
                .unwrap_err()
                .public_code(),
            "CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE"
        );
        assert!(
            tasks
                .addressed(
                    TaskAddressQuery::try_new(vec![id(4)], false, None).unwrap(),
                    deadline()
                )
                .is_ok()
        );
    }

    #[test]
    fn scripts_capture_safe_selectors_and_providers_remain_independent() {
        let source = ScriptedEventSource::new();
        source.queue_discovery(Ok(EventSupport::Supported)).unwrap();
        assert_eq!(
            source.discover(deadline()).unwrap(),
            EventSupport::Supported
        );
        source.queue_read(Ok(snapshot())).unwrap();
        assert_eq!(
            source.read(ReadQuery::default(), deadline()).unwrap(),
            snapshot()
        );
        source
            .queue_tasks(Ok(TaskFactsBatch {
                rows: vec![terminal(2)],
                missing: Vec::new(),
                proof_after: None,
                baseline_after: None,
            }))
            .unwrap();
        source
            .tasks(
                TaskAddressQuery::try_new(vec![id(2)], false, None).unwrap(),
                deadline(),
            )
            .unwrap();
        source
            .queue_repair(Ok(TaskRepairPage {
                rows: Vec::new(),
                next: None,
                complete: true,
                restart: false,
                baseline_after: None,
            }))
            .unwrap();
        source
            .repair(TaskRepairQuery::default(), deadline())
            .unwrap();
        let requests = source.requests();
        for (request, op) in requests.iter().zip(["read", "tasks", "repair"]) {
            assert_eq!(request.body.as_object().unwrap().len(), 1);
            assert_eq!(request.body["controller_events"]["op"], op);
            assert_eq!(request.deadline, deadline());
        }
        assert!(source.read(ReadQuery::default(), deadline()).is_err());
        let reader = Arc::new(FakeJournalReader::new(JournalWindow {
            journal_id: uuid::Uuid::from_u128(1),
            oldest_seq: Seq::new(1),
            head_seq: Seq::ZERO,
        }));
        reader.queue_read(Ok(snapshot())).unwrap();
        assert_eq!(
            reader.read(ReadQuery::default(), deadline()).unwrap(),
            snapshot()
        );
        let absent = FakeJournalProvider::absent();
        assert!(absent.open_existing(deadline()).unwrap().is_none());
        assert_eq!(absent.open_count(), 1);
        assert!(
            FakeJournalProvider::present(reader)
                .open_existing(deadline())
                .unwrap()
                .is_some()
        );
        assert!(
            FakeJournalProvider::error("unsafe binding")
                .open_existing(deadline())
                .is_err()
        );
        let provider = FakeTaskProjectionProvider::new(Arc::new(MemoryTaskReader::new()));
        provider.open_existing(deadline()).unwrap();
        assert_eq!(provider.open_count(), 1);
        assert!(
            FakeTaskProjectionProvider::error("state unavailable")
                .open_existing(deadline())
                .is_err()
        );
        let mut reconciler = FakeEventReconciler::new();
        let cold = Reconciliation::test_cold(None, vec![terminal(2)]);
        reconciler.queue(Ok(cold.clone())).unwrap();
        assert_eq!(
            reconciler
                .reconcile(
                    &source,
                    ReconcileInput {
                        read: None,
                        repair_due: true,
                        include_titles: false
                    },
                    deadline()
                )
                .unwrap(),
            cold
        );
        let mut bad = cold;
        bad.changes.push(DerivedTaskChange {
            task_id: id(2),
            previous: None,
            current: Some(terminal(2)),
            cause: ChangeCause::RepairDifference,
        });
        assert!(reconciler.queue(Ok(bad)).is_err());
        for _ in 0..256 {
            source.queue_read(Ok(snapshot())).unwrap();
        }
        assert!(source.queue_read(Ok(snapshot())).is_err());
    }

    #[test]
    fn sinks_channels_and_viewer_queues_are_bounded() {
        let sink = RecordingSink::new();
        let two = batch(2);
        sink.set_drop_mode(true);
        assert_eq!(sink.try_publish(two.clone()), PublishAttempt::Dropped);
        assert!(sink.batches().is_empty());
        sink.set_drop_mode(false);
        for _ in 0..128 {
            assert_eq!(sink.try_publish(two.clone()), PublishAttempt::Queued);
        }
        assert_eq!(sink.try_publish(two), PublishAttempt::Dropped);
        assert_eq!(sink.take_batches().len(), 128);
        let viewer = MemoryViewerEventSource::new();
        let mut receivers = Vec::new();
        for _ in 0..8 {
            receivers.push(viewer.subscribe(None).unwrap());
        }
        assert!(viewer.subscribe(None).is_err());
        for _ in 0..256 {
            assert_eq!(viewer.push(ViewerMessage::Heartbeat), 8);
        }
        assert_eq!(viewer.push(ViewerMessage::Heartbeat), 0);
        for receiver in &mut receivers {
            for _ in 0..256 {
                assert_eq!(receiver.try_recv().unwrap(), ViewerMessage::Heartbeat);
            }
            assert!(receiver.is_closed());
        }
        viewer.stop();
        assert!(viewer.subscribe(None).is_err());
        let refresh = FakeLocalProjectionRefresh::new();
        let mut revisions = refresh.subscribe_publications();
        refresh.request_refresh();
        assert_eq!(refresh.request_count(), 1);
        assert_eq!(refresh.publish(42), 1);
        assert_eq!(revisions.try_recv().unwrap(), 42);
        let channel = RecordingNoticeChannel::new();
        let notice = Notice {
            fingerprint: "a".repeat(64),
            title: "task".into(),
            body: "done".into(),
            sound: NoticeSound::Done,
        };
        channel.deliver(&notice, deadline()).unwrap();
        channel.set_error(Some("failed delivery".into()));
        assert!(channel.deliver(&notice, deadline()).is_err());
        assert_eq!(
            channel.records(),
            vec![(notice.clone(), deadline()), (notice, deadline())]
        );
    }
}
