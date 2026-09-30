//! Durable controller request kernel.
//!
//! Owns the original frozen request/envelope, stable operation identity, the
//! typed durable result, and the bounded active/pending index. Business body
//! validation belongs to the handler (`prepare`); the server digest identity
//! stays the original immutable command+body.
//!
//! Lock rules: the global `requests.lock` file is legacy-compat only and is
//! NEVER held across executor/Git/SSH/TaskClient work (the hot path takes no
//! global lock at all). Same-request coordination is a blocking `flock` on
//! `req-<id>.lock` plus atomic CAS; unrelated requests never block each
//! other. The tick path uses nonblocking try-locks so one busy/poisoned
//! request never starves the rest.
//!
//! Fake executor proof in tests is kernel-only: it proves crash/replay/lock
//! semantics of this store, not a real TaskClient submit (FLOW CP2 owns that).

use std::{fs::File, io, os::fd::AsRawFd, path::Path};

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{
    controller::{
        leader::{lock_exclusive, now_millis, open_controller_root, store_io},
        protocol::{
            ControllerRequest, MAX_STORED_REQUEST_BYTES, canonical_request_sha256,
            validate_request_id,
        },
    },
    error::WorkerError,
    inputs::RelativePath,
    prepared_submit::FrozenSubmitBody,
    protocol::PROTOCOL_VERSION,
    rooted_fs::RootedDir,
    task::{TaskId, TurnId},
};

/// Legacy global lock file. Retained for compatibility; it is NOT the
/// cross-process authority and is never held across executor work.
const REQUESTS_LOCK: &str = "requests.lock";

/// Dedicated active/pending index directory: `active/<request_id>.json`.
/// Enumerated WITHOUT traversing historical req-* root entries.
const ACTIVE_DIR: &str = "active";

/// Fair scheduling cursor receipt at the controller root.
const CURSOR_FILE: &str = "active-cursor.json";

/// One-time bootstrap guard for pre-kernel stores. Deliberately outside the
/// `pending-*` namespace so idle ticks never mistake it for a request.
const BOOTSTRAP_RECEIPT: &str = "active-index-bootstrap-v1.json";

/// Per-request coordination lock. Cross-process authority for the same
/// request; unrelated requests use different files and never block.
const LOCK_PREFIX: &str = "req-";
const LOCK_SUFFIX: &str = ".lock";

/// Test-only publication boundary. Production RPC never stops after publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerFault {
    None,
    /// Durable published row exists; fake ACK has not started.
    StopAfterPublish,
    /// Executor ran; the result was NOT persisted. Resume must re-execute the
    /// same prepared operation/identity.
    StopAfterExecuteBeforeResult,
    /// Result is durable; ACK has not started. Resume must reuse the saved
    /// result without re-executing.
    StopAfterResultBeforeAck,
    /// Pending receipt exists; the request row was NOT published. The next
    /// `handle` for the same request heals; bootstrap keeps the orphan.
    StopAfterActiveReceiptBeforePublish,
    /// Replacement staging is complete; the live name still holds the full
    /// published record. Process loss must resume from that record.
    CrashBeforeAckExchange,
    /// Live name already holds the ACK record; directory sync has not finished.
    CrashAfterAckExchangeBeforeSync,
    /// ACK is durable; the pending receipt was NOT retired. The next tick
    /// retires it idempotently without re-executing.
    CrashAfterAckBeforeRetire,
}

/// Stable operation identity frozen BEFORE executor effects. `task_id` /
/// `turn_id` are `None` for non-task operations (batch/global); handlers
/// supply authoritative identities and must not fabricate task/turn IDs.
/// `prepared` is the handler-produced server preparation (e.g. the frozen
/// expected record/turn/revision snapshot for say/cancel/close); the default
/// CP1 adapters use null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationMeta {
    pub task_id: Option<String>,
    pub turn_id: Option<String>,
    pub created_at_millis: u64,
    pub prepared: Value,
}

/// Checkpoint-1 fake executor. It does not call `TaskClient`, `create_task`,
/// or enqueue work. Resume reuses this durable row's IDs, frozen envelope,
/// timestamp, and saved result instead of allocating a second task.
/// Kernel-only proof: this never demonstrates real controller runtime.
pub struct FakeControllerExecutor;

pub trait ControllerCommandHandler {
    /// Freeze stable operation identity/time before any executor effect.
    /// The default covers `checkpoint.submit` (minted IDs) and ordinary
    /// `task.submit` (authoritative frozen IDs/time). Any other command must
    /// be supplied by an overriding handler; body validation lives there.
    fn prepare(&self, request: &ControllerRequest) -> Result<OperationMeta, WorkerError> {
        default_prepare_operation(request)
    }

    /// Command-specific typed result. Persisted BEFORE ACK; reconnect/restart
    /// returns exactly the saved value, never regenerated from mutable state.
    /// No exactly-once claim for non-idempotent callbacks: the TaskClient
    /// prepared operation owns side-effect convergence.
    fn execute(&self, record: &DurableRequest) -> Result<Value, WorkerError>;
}

impl ControllerCommandHandler for FakeControllerExecutor {
    fn execute(&self, record: &DurableRequest) -> Result<Value, WorkerError> {
        Ok(serde_json::json!({
            "executor": "fake-checkpoint",
            "request_id": record.request_id,
        }))
    }
}

/// Default preparation for the kernel-owned submit shapes. DAG/global
/// handlers override `prepare` instead of calling this.
pub fn default_prepare_operation(
    request: &ControllerRequest,
) -> Result<OperationMeta, WorkerError> {
    match request.command() {
        "checkpoint.submit" => Ok(OperationMeta {
            task_id: Some(TaskId::generate().to_string()),
            turn_id: Some(TurnId::generate().to_string()),
            created_at_millis: now_millis()?,
            prepared: Value::Null,
        }),
        "task.submit" => {
            let body: FrozenSubmitBody =
                serde_json::from_value(request.body().clone()).map_err(|_| {
                    WorkerError::Protocol(
                        "CONTROLLER_TRANSPORT: task.submit body is not a frozen submit".into(),
                    )
                })?;
            if body.run_id.is_some() {
                return Err(WorkerError::task(
                    "TASK_CONFIG_INVALID",
                    "ordinary controller submit must not carry a run_id",
                ));
            }
            Ok(OperationMeta {
                task_id: Some(body.task_id.to_string()),
                turn_id: Some(body.turn_id.to_string()),
                created_at_millis: body.created_at_millis,
                // The frozen body already carries everything; CP1 uses null.
                prepared: Value::Null,
            })
        }
        other => Err(WorkerError::Protocol(format!(
            "CONTROLLER_TRANSPORT: unsupported controller command {other}"
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestPhase {
    Published,
    Acked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableRequest {
    protocol_version: u32,
    request_id: String,
    command: String,
    body: Value,
    payload_sha256: String,
    created_at_millis: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    turn_id: Option<String>,
    phase: RequestPhase,
    /// Handler-produced server preparation, frozen ONCE at publication.
    /// `execute`/restart consumes this saved value, never a fresh mutable
    /// projection. Missing (legacy rows) reads as null.
    #[serde(default)]
    prepared: Value,
    /// Typed command result, persisted BEFORE ACK. `Published + Some` is the
    /// result-ready window: resume reuses it without re-executing.
    /// Presence-aware: a missing key (legacy rows) reads as None, while an
    /// explicit JSON null reads as Some(Value::Null) and must NOT trigger
    /// re-execution.
    #[serde(
        default,
        deserialize_with = "deserialize_present_result",
        skip_serializing_if = "Option::is_none"
    )]
    result: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerAck {
    protocol_version: u32,
    status: String,
    request_id: String,
    payload_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    turn_id: Option<String>,
    created_at_millis: u64,
    #[serde(
        default,
        deserialize_with = "deserialize_present_result",
        skip_serializing_if = "Option::is_none"
    )]
    result: Option<Value>,
}

/// Presence-aware result deserialization: absent key -> None (legacy),
/// explicit null -> Some(Value::Null), value -> Some(value). A saved null
/// must survive reopen as Some(Null); collapsing it to None would re-run
/// the executor after restart and rewrite history.
fn deserialize_present_result<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Some(
        Option::<Value>::deserialize(deserializer)?.unwrap_or(Value::Null),
    ))
}

/// Pending-index receipt: discoverability written BEFORE publication.
/// Lives in the dedicated `active/` directory, never in the root history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingReceipt {
    request_id: String,
    payload_sha256: String,
    command: String,
    created_at_millis: u64,
}

/// Fair scheduling cursor: durable rotation over the sorted pending set so
/// permanently failing/busy/orphaned early entries cannot starve later work.
/// A scheduling hint only; the pending set stays authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveCursor {
    version: u32,
    last_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapReceipt {
    version: u32,
    created_at_millis: u64,
    rebuilt: Vec<String>,
    corrupt: Vec<String>,
}

/// Bounded tick configuration for the independent leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveResumeConfig {
    pub max_requests_per_tick: usize,
}

impl Default for ActiveResumeConfig {
    fn default() -> Self {
        Self {
            max_requests_per_tick: 32,
        }
    }
}

/// One bounded tick over the pending index. Per-request failures never hide
/// other requests: they are collected in `failed` while the tick continues.
/// The durable cursor rotates the window so early poisoned entries cannot
/// starve later work; `cursor_stale` reports a lost cursor CAS race without
/// invalidating the tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveResumeReport {
    pub completed: Vec<ControllerAck>,
    pub busy_skipped: Vec<String>,
    pub orphan_receipts: Vec<String>,
    pub failed: Vec<(String, String)>,
    pub truncated: bool,
    pub cursor_stale: bool,
}

/// One-time rebuild report for pre-kernel stores. Corrupt rows are listed,
/// never silently hidden.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveBootstrapReport {
    pub already_bootstrapped: bool,
    pub rebuilt: Vec<String>,
    pub corrupt: Vec<String>,
}

pub struct ControllerStore {
    root: RootedDir,
    active: RootedDir,
}

impl DurableRequest {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn payload_sha256(&self) -> &str {
        &self.payload_sha256
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn body(&self) -> &Value {
        &self.body
    }

    pub fn task_id(&self) -> Option<&str> {
        self.task_id.as_deref()
    }

    pub fn turn_id(&self) -> Option<&str> {
        self.turn_id.as_deref()
    }

    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }

    pub fn phase(&self) -> RequestPhase {
        self.phase
    }

    pub fn result(&self) -> Option<&Value> {
        self.result.as_ref()
    }

    pub fn prepared(&self) -> &Value {
        &self.prepared
    }
}

impl ControllerAck {
    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn payload_sha256(&self) -> &str {
        &self.payload_sha256
    }

    pub fn task_id(&self) -> Option<&str> {
        self.task_id.as_deref()
    }

    pub fn turn_id(&self) -> Option<&str> {
        self.turn_id.as_deref()
    }

    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }

    pub fn result(&self) -> Option<&Value> {
        self.result.as_ref()
    }

    pub fn set_result(&mut self, result: Value) {
        self.result = Some(result);
    }
}

impl ControllerStore {
    /// Health reads only the pending index, never the historical request rows.
    /// A concurrent retire or malformed receipt makes the age incomplete, not zero.
    pub fn pending_health(
        &self,
        now_millis: u64,
    ) -> Result<crate::controller::health::PendingHealth, WorkerError> {
        let names = self.active_names()?;
        let mut health = crate::controller::health::PendingHealth {
            active_count: names.len() as u64,
            oldest_pending_age_millis: None,
            age_incomplete: false,
        };
        for name in names {
            match self.read_pending(&name) {
                Ok(receipt) => {
                    let age = now_millis.saturating_sub(receipt.created_at_millis);
                    health.oldest_pending_age_millis =
                        Some(health.oldest_pending_age_millis.unwrap_or(0).max(age));
                }
                Err(_) => health.age_incomplete = true,
            }
        }
        Ok(health)
    }

    pub fn open(state_root: &Path) -> Result<Self, WorkerError> {
        let root = open_controller_root(state_root)?;
        super::drain::initialize(&root)?;
        let _requests = root
            .open_private_lock(REQUESTS_LOCK)
            .map_err(|error| op_io("open-lock", REQUESTS_LOCK, error))?;
        let active = root
            .open_child_directory(&active_relative()?, true)
            .map_err(|error| op_io("open-active", ACTIVE_DIR, error))?;
        Ok(Self { root, active })
    }

    /// Legacy row count (scans history). Kept for tests/compat; the leader
    /// tick path must use `resume_active_bounded`, never this.
    pub fn request_count(&self) -> Result<usize, WorkerError> {
        Ok(self.request_names()?.len())
    }

    /// Read under the same request lock as result and ACK publication, so a
    /// concurrent publisher cannot retire this read's opened record inode.
    pub fn load(&self, request_id: &str) -> Result<Option<DurableRequest>, WorkerError> {
        self.load_with_hook(request_id, || {})
    }

    fn load_with_hook(
        &self,
        request_id: &str,
        after_open: impl FnOnce(),
    ) -> Result<Option<DurableRequest>, WorkerError> {
        let _per_request = self.lock_request(request_id)?;
        self.load_locked(request_id, after_open)
    }

    fn load_locked(
        &self,
        request_id: &str,
        after_open: impl FnOnce(),
    ) -> Result<Option<DurableRequest>, WorkerError> {
        let name = request_file_name(request_id)?;
        if !self
            .root
            .entry_exists(&name)
            .map_err(|error| op_io("probe-row", &name, error))?
        {
            return Ok(None);
        }
        Ok(Some(self.read_record_with_hook(&name, after_open)?))
    }

    /// Persist or resume. Holds ONLY the per-request lock across executor
    /// work; no global lock is taken on this path. RPC must not take the
    /// leader lock.
    pub fn handle(
        &self,
        request: &ControllerRequest,
        fault: ControllerFault,
    ) -> Result<ControllerAck, WorkerError> {
        self.handle_with(request, &FakeControllerExecutor, fault)
    }

    pub fn handle_with(
        &self,
        request: &ControllerRequest,
        executor: &dyn ControllerCommandHandler,
        fault: ControllerFault,
    ) -> Result<ControllerAck, WorkerError> {
        let _per_request = self.lock_request(request.request_id())?;
        let result = if let Some(existing) = self.load_locked(request.request_id(), || {})? {
            check_replay(&existing, request)?;
            self.drive_to_ack(existing, executor, fault, 0)
        } else {
            // Discoverability BEFORE publication: a crash from here on leaves a
            // pending receipt that bootstrap keeps and the next `handle` heals.
            self.ensure_pending_receipt(request)?;
            if fault == ControllerFault::StopAfterActiveReceiptBeforePublish {
                return Err(store_io(injected_fault()));
            }
            // Stable identity/time frozen BEFORE any executor effect.
            let meta = executor.prepare(request)?;
            let record = self.publish_with(request, &meta)?;
            if fault == ControllerFault::StopAfterPublish {
                return Ok(ack_from(&record));
            }
            self.drive_to_ack(record, executor, fault, 0)
        };
        result.map_err(|error| {
            // Inspect the durable outcome while still holding the request lock.
            // Preparation and replay-identity failures return before this point.
            // A saved success still needs recovery if ACK/receipt cleanup fails.
            // An unreadable row cannot prove a definitive rejection either.
            if self
                .load_locked(request.request_id(), || {})
                .map_or(true, |row| {
                    row.is_some_and(|row| matches!(saved_rejection(row.result.as_ref()), Ok(None)))
                })
            {
                WorkerError::ControllerResumable(Box::new(error))
            } else {
                error
            }
        })
    }

    /// Bounded idle tick over the pending index ONLY: enumerates the
    /// dedicated `active/` directory and NEVER traverses historical `req-*`
    /// root entries. Rotates past the durable cursor so early failing/busy/
    /// orphaned entries cannot starve later work. Intended for the
    /// independent FLOW leader.
    pub fn resume_active_bounded(
        &self,
        executor: &dyn ControllerCommandHandler,
        config: &ActiveResumeConfig,
    ) -> Result<ActiveResumeReport, WorkerError> {
        let bound = config.max_requests_per_tick.max(1);
        let sorted = self.active_names()?;
        let rotated = self.rotate_past_cursor(&sorted)?;
        let truncated = rotated.len() > bound;
        let window: Vec<&String> = rotated.into_iter().take(bound).collect();
        let mut report = ActiveResumeReport {
            completed: Vec::new(),
            busy_skipped: Vec::new(),
            orphan_receipts: Vec::new(),
            failed: Vec::new(),
            truncated,
            cursor_stale: false,
        };
        let mut last_attempted: Option<&str> = None;
        for name in window {
            // Every attempted name advances rotation, including per-entry
            // failures: a poisoned prefix must never pin the cursor and
            // starve later work. Only a global inability to enumerate the
            // active store aborts the tick (active_names above).
            last_attempted = Some(name);
            let request_id = match active_request_id(name) {
                Ok(id) => id,
                Err(error) => {
                    report.failed.push((name.clone(), display_error(&error)));
                    continue;
                }
            };
            let _per_request = match self.try_lock_request(&request_id) {
                Ok(Some(lock)) => lock,
                Ok(None) => {
                    report.busy_skipped.push(request_id);
                    continue;
                }
                Err(error) => {
                    report.failed.push((request_id, display_error(&error)));
                    continue;
                }
            };
            let receipt = match self.read_pending(name) {
                Ok(receipt) => receipt,
                Err(error) => {
                    report.failed.push((request_id, display_error(&error)));
                    continue;
                }
            };
            let record = match self.load_locked(&request_id, || {}) {
                Ok(Some(record)) => record,
                Ok(None) => {
                    // Receipt without a row: publication never finished.
                    // Kept for the next `handle` retry; never deleted here.
                    report.orphan_receipts.push(request_id);
                    continue;
                }
                // A corrupt/unreadable row fails THIS entry only. The bad
                // evidence is preserved untouched (fail-closed); rotation
                // still advances so later valid work progresses.
                Err(error) => {
                    report.failed.push((request_id, display_error(&error)));
                    continue;
                }
            };
            if let Err(error) = check_row_against_receipt(&record, &receipt) {
                report.failed.push((request_id, display_error(&error)));
                continue;
            }
            match self.drive_to_ack(record, executor, ControllerFault::None, 0) {
                Ok(ack) => report.completed.push(ack),
                Err(error) => report.failed.push((request_id, display_error(&error))),
            }
        }
        if let Some(last) = last_attempted {
            // A lost cursor race (or persist failure) only sets the stale
            // flag; the tick results stand and the next tick retries rotation.
            match self.persist_cursor(last) {
                Ok(stale) => report.cursor_stale = !stale,
                Err(_) => report.cursor_stale = true,
            }
        }
        Ok(report)
    }

    /// One-time rebuild of the pending index for pre-kernel stores.
    /// Guarded by a durable receipt; corrupt rows are reported, never hidden.
    /// A healthy pre-existing receipt is read-back-validated and counts as
    /// rebuilt, never as corrupt. A transient index-write failure for a
    /// VALID row aborts without persisting the completed marker, so a later
    /// bootstrap can finish; only genuine row corruption is reported.
    pub fn bootstrap_active_index(&self) -> Result<ActiveBootstrapReport, WorkerError> {
        if self
            .root
            .entry_exists(BOOTSTRAP_RECEIPT)
            .map_err(|error| op_io("probe-bootstrap", BOOTSTRAP_RECEIPT, error))?
        {
            return self.read_bootstrap_receipt();
        }
        let mut rebuilt = Vec::new();
        let mut corrupt = Vec::new();
        for name in self.request_names()? {
            let record: Result<DurableRequest, WorkerError> = (|| {
                let bytes = self
                    .root
                    .read_private_regular(&name, MAX_STORED_REQUEST_BYTES as u64)
                    .map_err(|error| op_io("read-row", &name, error))?;
                ensure_stored_size(&bytes)?;
                let record: DurableRequest = serde_json::from_slice(&bytes).map_err(|_| {
                    WorkerError::Protocol("CONTROLLER_TRANSPORT: durable request is invalid".into())
                })?;
                validate_record(&name, &record)?;
                Ok(record)
            })();
            match record {
                Ok(record) => {
                    if record.phase == RequestPhase::Published {
                        let receipt = PendingReceipt {
                            request_id: record.request_id.clone(),
                            payload_sha256: record.payload_sha256.clone(),
                            command: record.command.clone(),
                            created_at_millis: record.created_at_millis,
                        };
                        match self.write_pending_receipt(&receipt) {
                            Ok(()) => rebuilt.push(record.request_id.clone()),
                            // Healthy pre-existing receipt (normal restart or
                            // overlap with a live handle): read back and
                            // validate identity; a match counts as rebuilt.
                            Err(error) if is_already_exists(&error) => {
                                match self.read_pending_for_bootstrap(&receipt) {
                                    Ok(()) => rebuilt.push(record.request_id.clone()),
                                    Err(recheck) => {
                                        corrupt.push(format!("{name}: {}", display_error(&recheck)))
                                    }
                                }
                            }
                            // Transient index-write failure for a VALID row:
                            // not corruption. Propagate retryable I/O without
                            // persisting the completed marker.
                            Err(error) => return Err(error),
                        }
                    }
                }
                Err(error) => corrupt.push(format!("{name}: {}", display_error(&error))),
            }
        }
        rebuilt.sort();
        corrupt.sort();
        let receipt = BootstrapReceipt {
            version: 1,
            created_at_millis: now_millis()?,
            rebuilt: rebuilt.clone(),
            corrupt: corrupt.clone(),
        };
        let bytes = serde_json::to_vec(&receipt).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: bootstrap receipt is invalid".into())
        })?;
        match self
            .root
            .write_private_atomic_no_replace(BOOTSTRAP_RECEIPT, &bytes)
        {
            Ok(()) => Ok(ActiveBootstrapReport {
                already_bootstrapped: false,
                rebuilt,
                corrupt,
            }),
            // Lost the marker race: return the WINNER's validated arrays,
            // never this process's partial ones. Invalid winner fails closed.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                self.read_bootstrap_receipt()
            }
            Err(error) => Err(op_io("write-bootstrap", BOOTSTRAP_RECEIPT, error)),
        }
    }

    /// Validated read-back of the durable bootstrap marker. Shared by the
    /// entry path and the marker-race loser path. Invalid marker fails
    /// closed; it is never treated as success.
    fn read_bootstrap_receipt(&self) -> Result<ActiveBootstrapReport, WorkerError> {
        let bytes = self
            .root
            .read_private_regular(BOOTSTRAP_RECEIPT, MAX_STORED_REQUEST_BYTES as u64)
            .map_err(|error| op_io("read-bootstrap", BOOTSTRAP_RECEIPT, error))?;
        let receipt: BootstrapReceipt = serde_json::from_slice(&bytes).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: bootstrap receipt is invalid".into())
        })?;
        if receipt.version != 1 {
            return Err(WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: bootstrap receipt is invalid".into(),
            ));
        }
        Ok(ActiveBootstrapReport {
            already_bootstrapped: true,
            rebuilt: receipt.rebuilt,
            corrupt: receipt.corrupt,
        })
    }

    /// Read-back validation for a pre-existing pending receipt during
    /// bootstrap: the stored identity must match the row being indexed.
    /// A mismatch/malformed receipt is honestly rejected, never overwritten
    /// or silently accepted.
    fn read_pending_for_bootstrap(&self, expected: &PendingReceipt) -> Result<(), WorkerError> {
        let name = active_file_name(&expected.request_id)?;
        let existing = self.read_pending(&name)?;
        check_replay_by_parts(
            &existing.request_id,
            &existing.payload_sha256,
            &existing.command,
            &expected.request_id,
            &expected.payload_sha256,
            &expected.command,
        )
    }

    /// Legacy full-history resume. Kept for compat/tests; the leader tick
    /// path must use `resume_active_bounded`. No global lock is held across
    /// executor work here either: each row advances under its own
    /// per-request lock.
    pub fn resume_incomplete(&self) -> Result<Vec<ControllerAck>, WorkerError> {
        self.resume_incomplete_with(&FakeControllerExecutor)
    }

    pub fn resume_incomplete_with(
        &self,
        executor: &dyn ControllerCommandHandler,
    ) -> Result<Vec<ControllerAck>, WorkerError> {
        let mut acks = Vec::new();
        for name in self.request_names()? {
            let request_id = match request_id_from_name(&name) {
                Some(id) => id,
                None => continue,
            };
            let _per_request = self.lock_request(&request_id)?;
            let record = self.read_record(&name)?;
            if record.phase == RequestPhase::Published {
                acks.push(self.drive_to_ack(record, executor, ControllerFault::None, 0)?);
            }
        }
        Ok(acks)
    }

    /// Advance one row to durable ACK. Caller holds the per-request lock.
    /// `depth` bounds CAS-contention retries.
    ///
    /// Compatibility policy: an `Acked` row WITHOUT a saved typed result
    /// (legacy) returns the saved `None` as-is. The kernel NEVER invents a
    /// fresh result on retry.
    fn drive_to_ack(
        &self,
        record: DurableRequest,
        executor: &dyn ControllerCommandHandler,
        fault: ControllerFault,
        depth: u32,
    ) -> Result<ControllerAck, WorkerError> {
        if depth > 3 {
            return Err(WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: request CAS contention did not converge".into(),
            ));
        }
        let mut current = record;
        if current.phase == RequestPhase::Acked {
            // Compatibility policy: an `Acked` row WITHOUT a saved typed
            // result (legacy) returns the saved `None` as-is. Never invent a
            // fresh result.
            self.retire_receipt(&current.request_id, fault)?;
            return settled(&current);
        }
        // Step A: durable typed result BEFORE ACK.
        if current.result.is_none() {
            // A definitive business rejection is saved through this same
            // result window, so the row settles instead of staying pending for
            // the leader tick to execute once the rejection stops applying.
            // Every other error keeps today's retry semantics untouched.
            let value = match executor.execute(&current) {
                Ok(value) => value,
                Err(error) if terminal_rejection_code(current.command(), &error).is_some() => {
                    // Keep saved diagnostics static for mutation conflicts;
                    // execution details must not become durable public text.
                    let message = match current.command() {
                        "task.say" => Some("task changed before follow-up"),
                        "task.cancel" => Some("task changed before cancel"),
                        "task.close" => Some("task changed before close"),
                        _ => None,
                    };
                    match message {
                        Some(message) => {
                            rejection_result(&WorkerError::task("TASK_REVISION_CONFLICT", message))
                        }
                        None => rejection_result(&error),
                    }
                }
                Err(error) => return Err(error),
            };
            if fault == ControllerFault::StopAfterExecuteBeforeResult {
                return Ok(ack_from(&current));
            }
            let with_result = DurableRequest {
                result: Some(value),
                ..current.clone()
            };
            let name = request_file_name(with_result.request_id())?;
            let expected = encode_record(&current)?;
            let next = encode_record(&with_result)?;
            ensure_stored_size(&next)?;
            match self
                .root
                .replace_private_regular_exact(&name, &expected, &next)
            {
                Ok(()) => current = with_result,
                Err(error) if is_cas_contention(&error) => {
                    // A peer won the race; adopt the saved result instead of
                    // asserting our own execution was exactly-once.
                    let adopted = self.read_record(&name)?;
                    if adopted.result.is_none() || adopted.phase != RequestPhase::Published {
                        return self.drive_to_ack(adopted, executor, fault, depth + 1);
                    }
                    current = adopted;
                }
                Err(error) => return Err(op_io("cas-result", &name, error)),
            }
        }
        if fault == ControllerFault::StopAfterResultBeforeAck {
            return Ok(ack_from(&current));
        }
        // Step B: ACK exchange. Reconnect/restart from here reuses the saved
        // result without re-executing.
        if current.phase == RequestPhase::Published {
            let acked = DurableRequest {
                phase: RequestPhase::Acked,
                ..current.clone()
            };
            let name = request_file_name(acked.request_id())?;
            let expected = encode_record(&current)?;
            let next = encode_record(&acked)?;
            ensure_stored_size(&next)?;
            match self.root.replace_private_regular_exact_with_sync_hooks(
                &name,
                &expected,
                &next,
                || fault_before_exchange(fault),
                || fault_after_exchange(fault),
                || Ok(()),
            ) {
                Ok(()) => current = acked,
                Err(error) if is_cas_contention(&error) => {
                    let adopted = self.read_record(&name)?;
                    return self.drive_to_ack(adopted, executor, fault, depth + 1);
                }
                Err(error) => return Err(op_io("cas-ack", &name, error)),
            }
        }
        // Step C: retire the pending receipt only after durable result/ACK.
        // The definitive rejection reaches the caller only from here, once it
        // is durable and the receipt is gone.
        self.retire_receipt(&current.request_id, fault)?;
        settled(&current)
    }

    fn retire_receipt(&self, request_id: &str, fault: ControllerFault) -> Result<(), WorkerError> {
        if fault == ControllerFault::CrashAfterAckBeforeRetire {
            return Err(store_io(injected_fault()));
        }
        let name = active_file_name(request_id)?;
        // Idempotent: already-absent counts as retired (heals the
        // crash-after-ACK window on the next tick). A peer retiring the same
        // receipt at the same time (an RPC handler and the leader tick, or a
        // retried request) makes the removal's identity checks report ESTALE.
        // Once the entry is gone that is the same outcome; otherwise retry.
        let mut attempts = 0;
        loop {
            match self.active.remove_owned_regular(&name) {
                Ok(()) => return Ok(()),
                Err(error) if error.raw_os_error() == Some(libc::ESTALE) && attempts < 3 => {
                    attempts += 1;
                    if !self
                        .active
                        .entry_exists(&name)
                        .map_err(|error| op_io("retire-receipt", &name, error))?
                    {
                        return Ok(());
                    }
                }
                Err(error) => return Err(op_io("retire-receipt", &name, error)),
            }
        }
    }

    fn lock_request(&self, request_id: &str) -> Result<File, WorkerError> {
        let name = request_lock_name(request_id)?;
        let file = self
            .root
            .open_private_lock(&name)
            .map_err(|error| op_io("open-req-lock", &name, error))?;
        lock_exclusive(&file).map_err(|error| match error {
            WorkerError::Io(inner) => op_io("flock-req-lock", &name, inner),
            other => other,
        })?;
        Ok(file)
    }

    fn try_lock_request(&self, request_id: &str) -> Result<Option<File>, WorkerError> {
        let name = request_lock_name(request_id)?;
        let file = self
            .root
            .open_private_lock(&name)
            .map_err(|error| op_io("open-req-lock", &name, error))?;
        if try_lock_exclusive(&file).map_err(|error| match error {
            WorkerError::Io(inner) => op_io("flock-req-lock", &name, inner),
            other => other,
        })? {
            Ok(Some(file))
        } else {
            Ok(None)
        }
    }

    fn ensure_pending_receipt(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        let receipt = PendingReceipt {
            request_id: request.request_id().to_owned(),
            payload_sha256: request.payload_sha256().to_owned(),
            command: request.command().to_owned(),
            created_at_millis: now_millis()?,
        };
        match self.write_pending_receipt(&receipt) {
            Ok(()) => Ok(()),
            Err(error) => {
                let name = active_file_name(request.request_id())?;
                let existing = self.read_pending(&name).map_err(|_| error)?;
                check_replay_by_parts(
                    &existing.request_id,
                    &existing.payload_sha256,
                    &existing.command,
                    request.request_id(),
                    request.payload_sha256(),
                    request.command(),
                )?;
                Ok(())
            }
        }
    }

    fn write_pending_receipt(&self, receipt: &PendingReceipt) -> Result<(), WorkerError> {
        let name = active_file_name(&receipt.request_id)?;
        let bytes = serde_json::to_vec(receipt).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: pending receipt is invalid".into())
        })?;
        ensure_stored_size(&bytes)?;
        self.active
            .write_private_atomic_no_replace(&name, &bytes)
            .map_err(|error| op_io("write-receipt", &name, error))?;
        Ok(())
    }

    fn read_pending(&self, name: &str) -> Result<PendingReceipt, WorkerError> {
        let bytes = self
            .active
            .read_private_regular(name, MAX_STORED_REQUEST_BYTES as u64)
            .map_err(|error| op_io("read-receipt", name, error))?;
        ensure_stored_size(&bytes)?;
        let receipt: PendingReceipt = serde_json::from_slice(&bytes).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: pending receipt is invalid".into())
        })?;
        if active_file_name(&receipt.request_id)? != name {
            return Err(WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: pending receipt id does not match its filename".into(),
            ));
        }
        // The receipt binds identity only (no body to re-hash); the digest is
        // cross-checked against the request row on every tick and against the
        // incoming request on every publish.
        Ok(receipt)
    }

    /// Rotate the sorted pending set past the durable cursor. A missing or
    /// corrupt cursor safely resets rotation; the pending set itself stays
    /// authoritative, so no request is ever hidden by cursor state.
    fn rotate_past_cursor<'a>(&self, sorted: &'a [String]) -> Result<Vec<&'a String>, WorkerError> {
        let last = self.read_cursor()?;
        let Some(last) = last else {
            return Ok(sorted.iter().collect());
        };
        let pos = sorted.iter().position(|name| name.as_str() > last.as_str());
        match pos {
            Some(pos) => Ok(sorted[pos..].iter().chain(sorted[..pos].iter()).collect()),
            None => Ok(sorted.iter().collect()),
        }
    }

    fn read_cursor(&self) -> Result<Option<String>, WorkerError> {
        if !self
            .root
            .entry_exists(CURSOR_FILE)
            .map_err(|error| op_io("probe-cursor", CURSOR_FILE, error))?
        {
            return Ok(None);
        }
        let bytes = self
            .root
            .read_private_regular(CURSOR_FILE, MAX_STORED_REQUEST_BYTES as u64)
            .map_err(|error| op_io("read-cursor", CURSOR_FILE, error))?;
        let cursor: ActiveCursor = match serde_json::from_slice(&bytes) {
            Ok(cursor) => cursor,
            Err(_) => return Ok(None),
        };
        if cursor.version != 1 {
            return Ok(None);
        }
        Ok(Some(cursor.last_key))
    }

    /// Best-effort cursor persist (one CAS retry). Returns false when a
    /// concurrent tick won the race; the tick itself stays valid.
    fn persist_cursor(&self, last_key: &str) -> Result<bool, WorkerError> {
        let next = ActiveCursor {
            version: 1,
            last_key: last_key.to_owned(),
        };
        let bytes = serde_json::to_vec(&next).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: cursor receipt is invalid".into())
        })?;
        if !self
            .root
            .entry_exists(CURSOR_FILE)
            .map_err(|error| op_io("probe-cursor", CURSOR_FILE, error))?
        {
            match self
                .root
                .write_private_atomic_no_replace(CURSOR_FILE, &bytes)
            {
                Ok(()) => return Ok(true),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(op_io("write-cursor", CURSOR_FILE, error)),
            }
        }
        for _ in 0..2 {
            let expected = self
                .root
                .read_private_regular(CURSOR_FILE, MAX_STORED_REQUEST_BYTES as u64)
                .map_err(|error| op_io("read-cursor", CURSOR_FILE, error))?;
            match self
                .root
                .replace_private_regular_exact(CURSOR_FILE, &expected, &bytes)
            {
                Ok(()) => return Ok(true),
                Err(error) if is_cas_contention(&error) => continue,
                Err(error) => return Err(op_io("cas-cursor", CURSOR_FILE, error)),
            }
        }
        Ok(false)
    }

    fn publish_with(
        &self,
        request: &ControllerRequest,
        meta: &OperationMeta,
    ) -> Result<DurableRequest, WorkerError> {
        let record = DurableRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: request.request_id().to_owned(),
            command: request.command().to_owned(),
            body: request.body().clone(),
            payload_sha256: request.payload_sha256().to_owned(),
            created_at_millis: meta.created_at_millis,
            task_id: meta.task_id.clone(),
            turn_id: meta.turn_id.clone(),
            phase: RequestPhase::Published,
            prepared: meta.prepared.clone(),
            result: None,
        };
        let name = request_file_name(record.request_id())?;
        let bytes = encode_record(&record)?;
        ensure_stored_size(&bytes)?;
        match self.root.write_private_atomic_no_replace(&name, &bytes) {
            Ok(()) => Ok(record),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let existing = self.read_record(&name)?;
                check_replay(&existing, request)?;
                Ok(existing)
            }
            Err(error) => Err(op_io("publish-row", &name, error)),
        }
    }

    fn read_record(&self, name: &str) -> Result<DurableRequest, WorkerError> {
        self.read_record_with_hook(name, || {})
    }

    fn read_record_with_hook(
        &self,
        name: &str,
        after_open: impl FnOnce(),
    ) -> Result<DurableRequest, WorkerError> {
        let bytes = self
            .root
            .read_private_regular_with_hook(name, MAX_STORED_REQUEST_BYTES as u64, after_open)
            .map_err(|error| op_io("read-row", name, error))?;
        ensure_stored_size(&bytes)?;
        let record: DurableRequest = serde_json::from_slice(&bytes).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: durable request is invalid".into())
        })?;
        validate_record(name, &record)?;
        Ok(record)
    }

    fn request_names(&self) -> Result<Vec<String>, WorkerError> {
        let mut names = Vec::new();
        for raw in self
            .root
            .list_names()
            .map_err(|error| op_io("list-rows", "controller-root", error))?
        {
            let Ok(name) = String::from_utf8(raw) else {
                continue;
            };
            if name.starts_with("req-") && name.ends_with(".json") {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }

    /// Enumerate ONLY the dedicated `active/` directory. Historical `req-*`
    /// root entries are never traversed here.
    fn active_names(&self) -> Result<Vec<String>, WorkerError> {
        let mut names = Vec::new();
        for raw in self
            .active
            .list_names()
            .map_err(|error| op_io("list-active", ACTIVE_DIR, error))?
        {
            let Ok(name) = String::from_utf8(raw) else {
                continue;
            };
            if name.ends_with(".json") {
                // `active_request_id` validation happens per entry on the tick
                // so one bad name cannot hide the rest.
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }
}

fn check_replay(existing: &DurableRequest, request: &ControllerRequest) -> Result<(), WorkerError> {
    check_replay_by_parts(
        existing.request_id(),
        existing.payload_sha256(),
        existing.command(),
        request.request_id(),
        request.payload_sha256(),
        request.command(),
    )
}

fn check_row_against_receipt(
    record: &DurableRequest,
    receipt: &PendingReceipt,
) -> Result<(), WorkerError> {
    check_replay_by_parts(
        record.request_id(),
        record.payload_sha256(),
        record.command(),
        &receipt.request_id,
        &receipt.payload_sha256,
        &receipt.command,
    )
}

fn check_replay_by_parts(
    existing_id: &str,
    existing_digest: &str,
    existing_command: &str,
    request_id: &str,
    request_digest: &str,
    request_command: &str,
) -> Result<(), WorkerError> {
    if existing_id != request_id {
        return Err(WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: durable request_id does not match its filename".into(),
        ));
    }
    if existing_digest != request_digest || existing_command != request_command {
        return Err(conflict());
    }
    Ok(())
}

pub(crate) fn ensure_stored_size(bytes: &[u8]) -> Result<(), WorkerError> {
    if bytes.len() > MAX_STORED_REQUEST_BYTES {
        return Err(WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: durable controller request exceeds the stored size limit".into(),
        ));
    }
    Ok(())
}

fn request_file_name(request_id: &str) -> Result<String, WorkerError> {
    validate_request_id(request_id)?;
    Ok(format!("req-{request_id}.json"))
}

fn request_id_from_name(name: &str) -> Option<String> {
    let id = name.strip_prefix("req-")?.strip_suffix(".json")?;
    validate_request_id(id).ok()?;
    Some(id.to_owned())
}

fn active_relative() -> Result<RelativePath, WorkerError> {
    RelativePath::parse(ACTIVE_DIR.as_bytes())
        .map_err(|_| invalid_stored("active index path is not a safe relative path"))
}

fn active_file_name(request_id: &str) -> Result<String, WorkerError> {
    validate_request_id(request_id)?;
    Ok(format!("{request_id}.json"))
}

fn active_request_id(name: &str) -> Result<String, WorkerError> {
    let id = name.strip_suffix(".json").ok_or_else(|| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: pending receipt name is invalid".into())
    })?;
    validate_request_id(id)?;
    Ok(id.to_owned())
}

fn request_lock_name(request_id: &str) -> Result<String, WorkerError> {
    validate_request_id(request_id)?;
    Ok(format!("{LOCK_PREFIX}{request_id}{LOCK_SUFFIX}"))
}

fn try_lock_exclusive(file: &File) -> Result<bool, WorkerError> {
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EWOULDBLOCK) || error.raw_os_error() == Some(libc::EAGAIN)
    {
        return Ok(false);
    }
    Err(WorkerError::Io(error))
}

fn is_cas_contention(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::ESTALE)
}

fn encode_record(record: &DurableRequest) -> Result<Vec<u8>, WorkerError> {
    serde_json::to_vec(record).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: durable request could not be encoded".into())
    })
}

fn validate_record(name: &str, record: &DurableRequest) -> Result<(), WorkerError> {
    if record.protocol_version != PROTOCOL_VERSION {
        return Err(WorkerError::Protocol(
            "INCOMPATIBLE_PROTOCOL: durable controller request requires protocol 7".into(),
        ));
    }
    if request_file_name(&record.request_id)? != name {
        return Err(WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: durable request_id does not match its filename".into(),
        ));
    }
    // Generalized metadata: task/turn IDs are optional (batch/global ops
    // carry none). When present they must parse.
    if let Some(task_id) = record.task_id.as_deref() {
        task_id
            .parse::<TaskId>()
            .map_err(|_| invalid_stored("durable task_id is invalid"))?;
    }
    if let Some(turn_id) = record.turn_id.as_deref() {
        turn_id
            .parse::<TurnId>()
            .map_err(|_| invalid_stored("durable turn_id is invalid"))?;
    }
    if !record.body.is_object() {
        return Err(invalid_stored("durable request body must be a JSON object"));
    }
    let digest = canonical_request_sha256(record.protocol_version, &record.command, &record.body)?;
    if digest != record.payload_sha256 {
        return Err(invalid_stored(
            "durable request digest does not match command and body",
        ));
    }
    Ok(())
}

/// Reserved key for a saved terminal business rejection. No command result
/// uses it: `task.submit` saves `{task_id, turn_id}`, `task.batch` saves
/// `{run_id, task_ids}`, and successful mutations save a task status projection.
const REJECTION_KEY: &str = "controller_rejection";
const REJECTION_VERSION: u64 = 1;

/// Classify definitive execution rejections. Frozen mutations cannot be
/// retargeted after a revision conflict; TASK_BUSY and ambiguous failures keep
/// their retry semantics. Unfinished close/cancel effects use TASK_BUSY so
/// cleanup is not lost; the handler recovers already-completed say results.
/// Admission rejections remain deliberately narrow:
///
/// * only `task.submit`, the one command whose rejection sites are proven to
///   precede any task row (`TaskClient::submit_with_ids` returns
///   `capacity_busy()` / `capability_missing()` from its admission branches,
///   all of which run before `LocalTaskRecord::new`);
/// * only the `Capacity` variant with `public: true`, so the `Protocol`-variant
///   `CAPACITY_BUSY` raised during worker-host lease acquisition, and any
///   redacted capacity error, keep today's retry behaviour.
///
/// Infrastructure and ambiguous failures are never settled here: the executor
/// may have completed a side effect before failing.
fn terminal_rejection_code(command: &str, error: &WorkerError) -> Option<&'static str> {
    match (command, error) {
        (
            "task.say" | "task.cancel" | "task.close",
            WorkerError::Task {
                code: "TASK_REVISION_CONFLICT",
                ..
            },
        ) => Some("TASK_REVISION_CONFLICT"),
        (
            "task.submit",
            WorkerError::Capacity {
                code, public: true, ..
            },
        ) if matches!(*code, "CAPACITY_BUSY" | "CAPABILITY_MISSING") => Some(code),
        _ => None,
    }
}

pub(super) fn rejection_result(error: &WorkerError) -> Value {
    serde_json::json!({
        REJECTION_KEY: {
            "version": REJECTION_VERSION,
            "code": error.public_code(),
            "message": error.public_message(),
        }
    })
}

/// Reads a saved rejection back. A result without the reserved key is an
/// ordinary result (including legacy `None` and an explicit saved `null`).
/// Once the reserved key is present the value must be exactly the shape this
/// kernel writes: anything else is stored corruption and fails closed rather
/// than being handed back as a successful ACK.
fn saved_rejection(result: Option<&Value>) -> Result<Option<WorkerError>, WorkerError> {
    let Some(rejection) = result.and_then(|value| value.get(REJECTION_KEY)) else {
        return Ok(None);
    };
    let fields = rejection.as_object().ok_or_else(invalid_rejection)?;
    if fields.len() != 3 {
        return Err(invalid_rejection());
    }
    if fields.get("version").and_then(Value::as_u64) != Some(REJECTION_VERSION) {
        return Err(invalid_rejection());
    }
    let code = fields
        .get("code")
        .and_then(Value::as_str)
        .and_then(known_rejection_code)
        .ok_or_else(invalid_rejection)?;
    let message = fields
        .get("message")
        .and_then(Value::as_str)
        .filter(|message| !message.is_empty())
        .ok_or_else(invalid_rejection)?;
    let error = match code {
        "TASK_REVISION_CONFLICT" if message == "task changed before close" => {
            WorkerError::task(code, "task changed before close")
        }
        "TASK_REVISION_CONFLICT" if message == "task changed before follow-up" => {
            WorkerError::task(code, "task changed before follow-up")
        }
        "TASK_REVISION_CONFLICT" if message == "task changed before cancel" => {
            WorkerError::task(code, "task changed before cancel")
        }
        "TASK_CLOSED" if message == "task is terminal" => {
            WorkerError::task(code, "task is terminal")
        }
        "CAPACITY_BUSY" | "CAPABILITY_MISSING" => {
            WorkerError::capacity_public(code, message.to_owned())
        }
        // Mutation rejection messages are static literals; do not turn arbitrary
        // stored task text into a public diagnostic or change its exit kind.
        _ => return Err(invalid_rejection()),
    };
    Ok(Some(error))
}

fn known_rejection_code(code: &str) -> Option<&'static str> {
    match code {
        "CAPACITY_BUSY" => Some("CAPACITY_BUSY"),
        "CAPABILITY_MISSING" => Some("CAPABILITY_MISSING"),
        "TASK_REVISION_CONFLICT" => Some("TASK_REVISION_CONFLICT"),
        "TASK_CLOSED" => Some("TASK_CLOSED"),
        _ => None,
    }
}

fn invalid_rejection() -> WorkerError {
    WorkerError::Protocol("CONTROLLER_TRANSPORT: saved controller rejection is invalid".into())
}

/// Final answer for a row that has reached its durable outcome: the saved
/// rejection when there is one, otherwise the ordinary ACK.
fn settled(record: &DurableRequest) -> Result<ControllerAck, WorkerError> {
    match saved_rejection(record.result.as_ref())? {
        Some(error) => Err(error),
        None => Ok(ack_from(record)),
    }
}

fn ack_from(record: &DurableRequest) -> ControllerAck {
    ControllerAck {
        protocol_version: record.protocol_version,
        status: match record.phase {
            RequestPhase::Published => "published".into(),
            RequestPhase::Acked => "acked".into(),
        },
        request_id: record.request_id.clone(),
        payload_sha256: record.payload_sha256.clone(),
        task_id: record.task_id.clone(),
        turn_id: record.turn_id.clone(),
        created_at_millis: record.created_at_millis,
        // Reconnect/restart returns exactly the saved result, never a fresh
        // projection of mutable task state.
        result: record.result.clone(),
    }
}

fn display_error(error: &WorkerError) -> String {
    format!("{}: {error}", error.public_code())
}

/// AlreadyExists through the WorkerError wrapper: a lost atomic-create race
/// (healthy pre-existing file), not corruption.
fn is_already_exists(error: &WorkerError) -> bool {
    matches!(error, WorkerError::Io(error) if error.kind() == io::ErrorKind::AlreadyExists)
}

/// Attach operation/file context to a RootedDir io error. Kind is preserved;
/// request IDs are not secrets; owner-only checks are never relaxed. This
/// makes concurrent failures self-identify the exact call site.
fn op_io(op: &'static str, name: &str, error: io::Error) -> WorkerError {
    let kind = error.kind();
    store_io(io::Error::new(
        kind,
        format!("controller store {op} {name}: {error}"),
    ))
}

fn conflict() -> WorkerError {
    WorkerError::Protocol(
        "CONTROLLER_REQUEST_CONFLICT: request_id is bound to a different payload".into(),
    )
}

fn invalid_stored(message: &str) -> WorkerError {
    WorkerError::Protocol(format!("CONTROLLER_TRANSPORT: {message}"))
}

fn fault_before_exchange(fault: ControllerFault) -> io::Result<()> {
    if fault == ControllerFault::CrashBeforeAckExchange {
        Err(injected_fault())
    } else {
        Ok(())
    }
}

fn fault_after_exchange(fault: ControllerFault) -> io::Result<()> {
    if fault == ControllerFault::CrashAfterAckExchangeBeforeSync {
        Err(injected_fault())
    } else {
        Ok(())
    }
}

fn injected_fault() -> io::Error {
    io::Error::other("injected controller replacement fault")
}

pub fn serve_rpc(
    state_root: &Path,
    stdin: &mut dyn io::Read,
    stdout: &mut dyn io::Write,
    fault: ControllerFault,
) -> Result<ControllerAck, WorkerError> {
    let payload = crate::controller::protocol::read_frame(stdin)?;
    let request = crate::controller::protocol::parse_request(&payload)?;
    let store = ControllerStore::open(state_root)?;
    let ack = store.handle(&request, fault)?;
    let frame = crate::controller::protocol::encode_json_frame(&ack)?;
    stdout.write_all(&frame).map_err(WorkerError::Io)?;
    stdout.flush().map_err(WorkerError::Io)?;
    Ok(ack)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::protocol::MAX_STORED_REQUEST_BYTES;
    use serde_json::json;

    struct FailingHandler {
        prepare_fails: bool,
        terminal: bool,
    }

    impl ControllerCommandHandler for FailingHandler {
        fn prepare(&self, _: &ControllerRequest) -> Result<OperationMeta, WorkerError> {
            if self.prepare_fails {
                return Err(WorkerError::task("TASK_NOT_FOUND", "task was not found"));
            }
            Ok(OperationMeta {
                task_id: None,
                turn_id: None,
                created_at_millis: 1,
                prepared: Value::Null,
            })
        }

        fn execute(&self, _: &DurableRequest) -> Result<Value, WorkerError> {
            if self.terminal {
                Err(WorkerError::capacity_public(
                    "CAPACITY_BUSY",
                    "all slots occupied".into(),
                ))
            } else {
                Err(io::Error::other("PRIVATE executor failure").into())
            }
        }
    }

    fn submit_request(body: Value) -> ControllerRequest {
        crate::controller::parse_request(
            &serde_json::to_vec(&json!({
                "protocol_version": PROTOCOL_VERSION,
                "request_id": "018f0f4a6b5c7d8e9f00112233445566",
                "command": "task.submit",
                "body": body,
            }))
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn request_load_excludes_ack_replacement_while_its_record_is_open() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("controller");
        let store = ControllerStore::open(&root).unwrap();
        let peer = ControllerStore::open(&root).unwrap();
        let request = submit_request(json!({}));
        let prepare = FailingHandler {
            prepare_fails: false,
            terminal: false,
        };
        store
            .handle_with(&request, &prepare, ControllerFault::StopAfterPublish)
            .unwrap();
        let published = store.load(request.request_id()).unwrap().unwrap();
        let mut peer_acquired = false;

        // A cooperative publisher can replace the opened inode only if this
        // public read failed to retain the same request's authority lock.
        let observed = store
            .load_with_hook(request.request_id(), || {
                if let Some(_lock) = peer.try_lock_request(request.request_id()).unwrap() {
                    peer_acquired = true;
                    peer.drive_to_ack(
                        published.clone(),
                        &FakeControllerExecutor,
                        ControllerFault::None,
                        0,
                    )
                    .unwrap();
                }
            })
            .unwrap()
            .unwrap();

        assert_eq!(observed, published);
        assert!(
            !peer_acquired,
            "ACK replacement must wait for the bound read"
        );
        let ack = peer.handle(&request, ControllerFault::None).unwrap();
        assert_eq!(ack.status(), "acked");
        assert_eq!(
            store.load(request.request_id()).unwrap().unwrap().phase(),
            RequestPhase::Acked
        );
    }

    #[test]
    fn resumable_errors_are_flagged_on_the_wire_and_legacy_laptops_ignore_the_flag() {
        // Wire shapes copied from the 772e83e HostControlError decoders:
        // the outer object is strict; its error detail accepts unknown fields.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyError {
            protocol_version: u32,
            error: LegacyDetail,
        }
        #[derive(Deserialize)]
        struct LegacyDetail {
            code: String,
            message: String,
            #[serde(default)]
            category: Option<String>,
        }

        let temp = tempfile::tempdir().unwrap();
        let request = submit_request(json!({}));
        let handler = FailingHandler {
            prepare_fails: false,
            terminal: false,
        };
        for _ in 0..2 {
            let store = ControllerStore::open(&temp.path().join("controller")).unwrap();
            let error = store
                .handle_with(&request, &handler, ControllerFault::None)
                .unwrap_err();
            assert_eq!(error.exit_code(), 74);
            assert_eq!(error.public_code(), "IO");
            assert_eq!(error.public_message(), "I/O error");
            let row = store.load(request.request_id()).unwrap().unwrap();
            assert_eq!(row.phase(), RequestPhase::Published);
            assert!(row.result().is_none());
            let frame =
                crate::controller::encode_json_frame(&crate::versioned_host_error(&error)).unwrap();
            let bytes = crate::controller::decode_frame(&frame).unwrap();
            let value: Value = serde_json::from_slice(bytes).unwrap();
            assert_eq!(value["error"]["resumable"], true);
            assert!(!String::from_utf8_lossy(bytes).contains("PRIVATE"));
            let legacy: LegacyError = serde_json::from_slice(bytes).unwrap();
            assert_eq!(legacy.protocol_version, 7);
            assert_eq!(legacy.error.code, "HOST_IO");
            assert_eq!(legacy.error.message, "host state operation failed");
            assert_eq!(legacy.error.category, None);
            let current: crate::job::HostControlError = serde_json::from_slice(bytes).unwrap();
            assert_eq!(
                serde_json::to_value(current).unwrap()["error"]["resumable"],
                true
            );
        }
        let store = ControllerStore::open(&temp.path().join("controller")).unwrap();
        let report = store
            .resume_active_bounded(&FakeControllerExecutor, &ActiveResumeConfig::default())
            .unwrap();
        assert_eq!(report.completed.len(), 1);
        assert!(report.failed.is_empty());
        let replay = store
            .handle_with(&request, &handler, ControllerFault::None)
            .unwrap();
        assert_eq!(replay, report.completed[0]);
        assert_eq!(store.request_count().unwrap(), 1);
    }

    #[test]
    fn prepare_failures_saved_rejections_and_payload_conflicts_are_not_resumable() {
        for (prepare_fails, terminal, code) in [
            (true, false, "TASK_NOT_FOUND"),
            (false, true, "CAPACITY_BUSY"),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let store = ControllerStore::open(&temp.path().join("controller")).unwrap();
            let request = submit_request(json!({}));
            let handler = FailingHandler {
                prepare_fails,
                terminal,
            };
            for _ in 0..2 {
                let error = store
                    .handle_with(&request, &handler, ControllerFault::None)
                    .unwrap_err();
                let wire = serde_json::to_value(crate::versioned_host_error(&error)).unwrap();
                assert_eq!(wire["error"]["code"], code);
                assert!(wire["error"].get("resumable").is_none());
            }
            let row = store.load(request.request_id()).unwrap();
            if prepare_fails {
                assert!(row.is_none());
            } else {
                assert_eq!(row.unwrap().phase(), RequestPhase::Acked);
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let store = ControllerStore::open(&temp.path().join("controller")).unwrap();
        let handler = FailingHandler {
            prepare_fails: false,
            terminal: false,
        };
        store
            .handle_with(&submit_request(json!({})), &handler, ControllerFault::None)
            .unwrap_err();
        let error = store
            .handle_with(
                &submit_request(json!({"changed": true})),
                &handler,
                ControllerFault::None,
            )
            .unwrap_err();
        let wire = serde_json::to_value(crate::versioned_host_error(&error)).unwrap();
        assert_eq!(wire["error"]["code"], "CONTROLLER_REQUEST_CONFLICT");
        assert!(wire["error"].get("resumable").is_none());
    }

    #[test]
    fn saved_success_is_resumable_after_ack_or_retirement_io_failure() {
        struct SuccessfulHandler(std::cell::Cell<usize>);
        impl ControllerCommandHandler for SuccessfulHandler {
            fn prepare(&self, request: &ControllerRequest) -> Result<OperationMeta, WorkerError> {
                FailingHandler {
                    prepare_fails: false,
                    terminal: false,
                }
                .prepare(request)
            }
            fn execute(&self, _: &DurableRequest) -> Result<Value, WorkerError> {
                self.0.set(self.0.get() + 1);
                Ok(json!({"saved": "success"}))
            }
        }
        for (fault, phase) in [
            (
                ControllerFault::CrashBeforeAckExchange,
                RequestPhase::Published,
            ),
            (
                ControllerFault::CrashAfterAckExchangeBeforeSync,
                RequestPhase::Acked,
            ),
            (
                ControllerFault::CrashAfterAckBeforeRetire,
                RequestPhase::Acked,
            ),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("controller");
            let store = ControllerStore::open(&root).unwrap();
            let request = submit_request(json!({}));
            let handler = SuccessfulHandler(std::cell::Cell::new(0));
            // A replay of an Acked row can also fail during receipt retirement.
            for _ in 0..2 {
                let error = store.handle_with(&request, &handler, fault).unwrap_err();
                assert_eq!(error.public_code(), "IO");
                assert_eq!(error.exit_code(), 74);
                let row = store.load(request.request_id()).unwrap().unwrap();
                assert_eq!(row.phase(), phase);
                assert_eq!(row.result(), Some(&json!({"saved": "success"})));
                let wire = serde_json::to_value(crate::versioned_host_error(&error)).unwrap();
                assert_eq!(wire["error"]["resumable"], true, "{fault:?}");
                // Only retirement faults recur once the ACK exchange has succeeded.
                if fault == ControllerFault::CrashAfterAckExchangeBeforeSync {
                    break;
                }
            }
            let store = ControllerStore::open(&root).unwrap();
            let ack = store
                .handle_with(&request, &handler, ControllerFault::None)
                .unwrap();
            assert_eq!(ack.result(), Some(&json!({"saved": "success"})));
            assert_eq!(handler.0.get(), 1, "saved success must not execute again");
            assert_eq!(store.request_count().unwrap(), 1);
        }
    }

    #[test]
    fn unreadable_row_after_execution_keeps_the_original_error_resumable() {
        use std::os::unix::fs::PermissionsExt;

        struct UnreadableRow(std::path::PathBuf);
        impl ControllerCommandHandler for UnreadableRow {
            fn prepare(&self, request: &ControllerRequest) -> Result<OperationMeta, WorkerError> {
                FailingHandler {
                    prepare_fails: false,
                    terminal: false,
                }
                .prepare(request)
            }
            fn execute(&self, record: &DurableRequest) -> Result<Value, WorkerError> {
                std::fs::set_permissions(
                    self.0.join(format!("req-{}.json", record.request_id())),
                    std::fs::Permissions::from_mode(0o644),
                )
                .unwrap();
                Err(WorkerError::task("TASK_BUSY", "task is busy"))
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("controller");
        let store = ControllerStore::open(&root).unwrap();
        let request = submit_request(json!({}));
        let error = store
            .handle_with(
                &request,
                &UnreadableRow(root.clone()),
                ControllerFault::None,
            )
            .unwrap_err();
        assert!(store.load(request.request_id()).is_err());
        assert_eq!(error.public_code(), "TASK_BUSY");
        assert_eq!(error.public_message(), "task is busy");
        assert_eq!(error.exit_code(), 64);
        let wire = serde_json::to_value(crate::versioned_host_error(&error)).unwrap();
        assert_eq!(wire["error"]["resumable"], true);
        std::fs::set_permissions(
            root.join(format!("req-{}.json", request.request_id())),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let row = store.load(request.request_id()).unwrap().unwrap();
        assert_eq!(row.phase(), RequestPhase::Published);
        assert!(row.result().is_none());
    }

    #[test]
    fn saved_rejections_remain_definitive_after_ack_or_retirement_io_failure() {
        for fault in [
            ControllerFault::CrashBeforeAckExchange,
            ControllerFault::CrashAfterAckExchangeBeforeSync,
            ControllerFault::CrashAfterAckBeforeRetire,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let store = ControllerStore::open(&temp.path().join("controller")).unwrap();
            let request = submit_request(json!({}));
            let handler = FailingHandler {
                prepare_fails: false,
                terminal: true,
            };
            let error = store.handle_with(&request, &handler, fault).unwrap_err();
            let row = store.load(request.request_id()).unwrap().unwrap();
            assert_eq!(
                saved_rejection(row.result())
                    .unwrap()
                    .unwrap()
                    .public_code(),
                "CAPACITY_BUSY"
            );
            let wire = serde_json::to_value(crate::versioned_host_error(&error)).unwrap();
            assert!(wire["error"].get("resumable").is_none(), "{fault:?}");
            let rejection = store
                .handle_with(&request, &handler, ControllerFault::None)
                .unwrap_err();
            assert_eq!(rejection.public_code(), "CAPACITY_BUSY");
            assert_eq!(rejection.exit_code(), 75);
        }
    }

    #[test]
    fn corrupt_saved_rejections_remain_definitive_after_ack_or_retirement_io_failure() {
        for fault in [
            ControllerFault::CrashBeforeAckExchange,
            ControllerFault::CrashAfterAckExchangeBeforeSync,
            ControllerFault::CrashAfterAckBeforeRetire,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let store = ControllerStore::open(&temp.path().join("controller")).unwrap();
            let request = submit_request(json!({}));
            let handler = FailingHandler {
                prepare_fails: false,
                terminal: true,
            };
            store
                .handle_with(
                    &request,
                    &handler,
                    ControllerFault::StopAfterResultBeforeAck,
                )
                .unwrap();
            let mut row = store.load(request.request_id()).unwrap().unwrap();
            let expected = encode_record(&row).unwrap();
            row.result.as_mut().unwrap()["controller_rejection"]["version"] = json!(0);
            store
                .root
                .replace_private_regular_exact(
                    &request_file_name(request.request_id()).unwrap(),
                    &expected,
                    &encode_record(&row).unwrap(),
                )
                .unwrap();

            let error = store.handle_with(&request, &handler, fault).unwrap_err();
            assert_eq!(error.public_code(), "IO");
            assert_eq!(error.exit_code(), 74);
            let wire = serde_json::to_value(crate::versioned_host_error(&error)).unwrap();
            assert!(wire["error"].get("resumable").is_none(), "{fault:?}");

            let error = store
                .handle_with(&request, &handler, ControllerFault::None)
                .unwrap_err();
            assert_eq!(error.public_code(), "CONTROLLER_TRANSPORT");
            assert_eq!(error.exit_code(), 70);
            let wire = serde_json::to_value(crate::versioned_host_error(&error)).unwrap();
            assert!(
                wire["error"].get("resumable").is_none(),
                "{fault:?}: corrupted rejection must fail closed"
            );
            assert_eq!(
                store.load(request.request_id()).unwrap().unwrap().result(),
                row.result()
            );
        }
    }

    #[test]
    fn oversized_encoded_records_are_rejected_before_publication() {
        let too_big = vec![0u8; MAX_STORED_REQUEST_BYTES + 1];
        assert!(ensure_stored_size(&too_big).is_err());
        assert!(ensure_stored_size(&vec![0u8; MAX_STORED_REQUEST_BYTES]).is_ok());
    }
}
