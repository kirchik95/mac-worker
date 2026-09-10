//! Durable request KERNEL tests (kernel-only proof, not real runtime).
//!
//! The executor below is a counting fake: it proves crash/replay/lock/index
//! semantics of `ControllerStore` (publish/result/ACK windows, saved typed
//! replay, per-request coordination, bounded ticks). It does NOT prove a real
//! TaskClient submit, transfer, or CLI path; FLOW CP2 owns those.

use std::{
    os::unix::fs::PermissionsExt,
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

use mac_worker::{
    controller::{
        ActiveResumeConfig, ControllerCommandHandler, ControllerFault, ControllerRequest,
        ControllerStore, DurableRequest, OperationMeta, RequestPhase, default_prepare_operation,
        parse_request,
    },
    error::WorkerError,
    protocol::PROTOCOL_VERSION,
};
use serde_json::{Value, json};

const ID_A: &str = "018f0f4a6b5c7d8e9f00112233445560";
const ID_B: &str = "018f0f4a6b5c7d8e9f00112233445561";
const ID_C: &str = "018f0f4a6b5c7d8e9f00112233445562";
const ID_D: &str = "018f0f4a6b5c7d8e9f00112233445563";
const ID_E: &str = "018f0f4a6b5c7d8e9f00112233445564";
const ID_F: &str = "018f0f4a6b5c7d8e9f00112233445565";

/// Counting fake executor. `next_result` is the mutable stand-in for task
/// state: whatever it returns LATER must never overwrite a saved result.
struct KernelTestExecutor {
    calls: Mutex<Vec<String>>,
    next_result: Mutex<Value>,
    fixed_meta: Option<OperationMeta>,
    gate: Mutex<Option<mpsc::Receiver<()>>>,
    seen_prepared: Mutex<Vec<Value>>,
    derive_result_from_prepared: bool,
}

impl KernelTestExecutor {
    fn checkpoint(result: Value) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            next_result: Mutex::new(result),
            fixed_meta: None,
            gate: Mutex::new(None),
            seen_prepared: Mutex::new(Vec::new()),
            derive_result_from_prepared: false,
        }
    }

    fn with_meta(meta: OperationMeta, result: Value) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            next_result: Mutex::new(result),
            fixed_meta: Some(meta),
            gate: Mutex::new(None),
            seen_prepared: Mutex::new(Vec::new()),
            derive_result_from_prepared: false,
        }
    }

    fn echoing_prepared(meta: OperationMeta) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            next_result: Mutex::new(Value::Null),
            fixed_meta: Some(meta),
            gate: Mutex::new(None),
            seen_prepared: Mutex::new(Vec::new()),
            derive_result_from_prepared: true,
        }
    }

    fn calls_for(&self, request_id: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|id| id.as_str() == request_id)
            .count()
    }

    fn set_result(&self, value: Value) {
        *self.next_result.lock().unwrap() = value;
    }
}

impl ControllerCommandHandler for KernelTestExecutor {
    fn prepare(&self, request: &ControllerRequest) -> Result<OperationMeta, WorkerError> {
        if let Some(meta) = &self.fixed_meta {
            return Ok(meta.clone());
        }
        default_prepare_operation(request)
    }

    fn execute(&self, record: &DurableRequest) -> Result<Value, WorkerError> {
        // Record the call BEFORE the gate so observers can rendezvous with a
        // blocked execution; the gate itself only pauses ID_F.
        self.calls
            .lock()
            .unwrap()
            .push(record.request_id().to_owned());
        self.seen_prepared
            .lock()
            .unwrap()
            .push(record.prepared().clone());
        if self.derive_result_from_prepared {
            return Ok(json!({
                "rev": record.prepared().get("expected_revision").cloned().unwrap_or(Value::Null),
            }));
        }
        if record.request_id() == ID_F && self.gate.lock().unwrap().is_some() {
            let guard = self.gate.lock().unwrap();
            guard
                .as_ref()
                .unwrap()
                .recv_timeout(Duration::from_secs(20))
                .unwrap();
        }
        Ok(self.next_result.lock().unwrap().clone())
    }
}

fn request_for(request_id: &str, command: &str, body: Value) -> ControllerRequest {
    parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": request_id,
            "command": command,
            "body": body,
        }))
        .unwrap(),
    )
    .unwrap()
}

fn checkpoint_request(request_id: &str) -> ControllerRequest {
    request_for(
        request_id,
        "checkpoint.submit",
        json!({"prompt": "freeze this snapshot"}),
    )
}

fn open_store(temp: &tempfile::TempDir) -> (std::path::PathBuf, ControllerStore) {
    let state = temp.path().join("mac-worker-controller");
    let store = ControllerStore::open(&state).unwrap();
    (state, store)
}

fn public_code(error: WorkerError) -> String {
    error.public_code()
}

#[test]
fn happy_path_persists_the_typed_result_before_ack() {
    let temp = tempfile::tempdir().unwrap();
    let (_, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    let ack = store
        .handle_with(&checkpoint_request(ID_A), &executor, ControllerFault::None)
        .unwrap();
    assert_eq!(ack.status(), "acked");
    assert_eq!(ack.result(), Some(&json!({"n": 1})));
    assert_eq!(executor.calls_for(ID_A), 1);
    let loaded = store.load(ID_A).unwrap().unwrap();
    assert_eq!(loaded.phase(), RequestPhase::Acked);
    assert_eq!(loaded.result(), Some(&json!({"n": 1})));
    assert!(loaded.task_id().is_some());
    assert!(loaded.turn_id().is_some());
    // Receipt retired only after durable result/ACK.
    let tick = store
        .resume_active_bounded(&executor, &ActiveResumeConfig::default())
        .unwrap();
    assert!(tick.completed.is_empty());
    assert!(!tick.truncated);
}

#[test]
fn crash_after_publish_resumes_the_same_identity() {
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    let published = store
        .handle_with(
            &checkpoint_request(ID_A),
            &executor,
            ControllerFault::StopAfterPublish,
        )
        .unwrap();
    assert_eq!(published.status(), "published");
    assert_eq!(executor.calls_for(ID_A), 0);
    assert_eq!(
        store.load(ID_A).unwrap().unwrap().phase(),
        RequestPhase::Published
    );

    drop(store);
    let store = ControllerStore::open(&state).unwrap();
    let ack = store
        .handle_with(&checkpoint_request(ID_A), &executor, ControllerFault::None)
        .unwrap();
    assert_eq!(ack.status(), "acked");
    assert_eq!(ack.task_id(), published.task_id());
    assert_eq!(ack.turn_id(), published.turn_id());
    assert_eq!(ack.created_at_millis(), published.created_at_millis());
    assert_eq!(ack.result(), Some(&json!({"n": 1})));
    assert_eq!(executor.calls_for(ID_A), 1);
}

#[test]
fn crash_after_execute_before_result_retries_the_same_identity() {
    let temp = tempfile::tempdir().unwrap();
    let (_, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    let published = store
        .handle_with(
            &checkpoint_request(ID_A),
            &executor,
            ControllerFault::StopAfterExecuteBeforeResult,
        )
        .unwrap();
    assert_eq!(published.status(), "published");
    assert_eq!(executor.calls_for(ID_A), 1);
    // Nothing persisted: the row is still result-less Published.
    let loaded = store.load(ID_A).unwrap().unwrap();
    assert_eq!(loaded.phase(), RequestPhase::Published);
    assert_eq!(loaded.result(), None);

    executor.set_result(json!({"n": 2}));
    let ack = store
        .handle_with(&checkpoint_request(ID_A), &executor, ControllerFault::None)
        .unwrap();
    assert_eq!(ack.status(), "acked");
    assert_eq!(ack.task_id(), published.task_id());
    assert_eq!(ack.result(), Some(&json!({"n": 2})));
    assert_eq!(executor.calls_for(ID_A), 2);
}

#[test]
fn crash_after_result_before_ack_reuses_the_saved_result() {
    let temp = tempfile::tempdir().unwrap();
    let (_, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    let published = store
        .handle_with(
            &checkpoint_request(ID_A),
            &executor,
            ControllerFault::StopAfterResultBeforeAck,
        )
        .unwrap();
    assert_eq!(executor.calls_for(ID_A), 1);
    let loaded = store.load(ID_A).unwrap().unwrap();
    assert_eq!(loaded.phase(), RequestPhase::Published);
    assert_eq!(loaded.result(), Some(&json!({"n": 1})));

    // Later mutable state must never overwrite the saved result.
    executor.set_result(json!({"n": 2}));
    let ack = store
        .handle_with(&checkpoint_request(ID_A), &executor, ControllerFault::None)
        .unwrap();
    assert_eq!(ack.status(), "acked");
    assert_eq!(ack.task_id(), published.task_id());
    assert_eq!(ack.result(), Some(&json!({"n": 1})));
    assert_eq!(executor.calls_for(ID_A), 1);
}

#[test]
fn restart_after_ack_returns_the_saved_result_without_reexecuting() {
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    let first = store
        .handle_with(&checkpoint_request(ID_A), &executor, ControllerFault::None)
        .unwrap();
    assert_eq!(executor.calls_for(ID_A), 1);

    executor.set_result(json!({"n": 99}));
    drop(store);
    let store = ControllerStore::open(&state).unwrap();
    let second = store
        .handle_with(&checkpoint_request(ID_A), &executor, ControllerFault::None)
        .unwrap();
    assert_eq!(second.status(), "acked");
    assert_eq!(second.result(), Some(&json!({"n": 1})));
    assert_eq!(second.task_id(), first.task_id());
    assert_eq!(executor.calls_for(ID_A), 1);
}

#[test]
fn conflicting_body_and_command_are_rejected_and_leave_the_row() {
    let temp = tempfile::tempdir().unwrap();
    let (_, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    let first = store
        .handle_with(&checkpoint_request(ID_A), &executor, ControllerFault::None)
        .unwrap();

    let changed = request_for(ID_A, "checkpoint.submit", json!({"prompt": "different"}));
    assert_eq!(
        public_code(
            store
                .handle_with(&changed, &executor, ControllerFault::None)
                .unwrap_err()
        ),
        "CONTROLLER_REQUEST_CONFLICT"
    );
    let other_command = request_for(ID_A, "task.close", json!({}));
    assert_eq!(
        public_code(
            store
                .handle_with(&other_command, &executor, ControllerFault::None)
                .unwrap_err()
        ),
        "CONTROLLER_REQUEST_CONFLICT"
    );
    let loaded = store.load(ID_A).unwrap().unwrap();
    assert_eq!(loaded.task_id(), first.task_id());
    assert_eq!(loaded.result(), Some(&json!({"n": 1})));
    assert_eq!(executor.calls_for(ID_A), 1);
}

#[test]
fn generalized_operations_carry_no_fabricated_task_ids() {
    let temp = tempfile::tempdir().unwrap();
    let (_, store) = open_store(&temp);
    // Authoritative handler identity for an existing-task op: no task/turn,
    // plus a frozen expected snapshot that restart must consume.
    let executor = KernelTestExecutor::with_meta(
        OperationMeta {
            task_id: None,
            turn_id: None,
            created_at_millis: 1_700_000_000_000,
            prepared: json!({"expected_revision": 3, "expected_turn": 7}),
        },
        json!({"say": "stored"}),
    );
    let request = request_for(ID_A, "task.say", json!({"task_id": ID_B, "text": "hello"}));
    let ack = store
        .handle_with(&request, &executor, ControllerFault::None)
        .unwrap();
    assert_eq!(ack.status(), "acked");
    assert_eq!(ack.task_id(), None);
    assert_eq!(ack.turn_id(), None);
    assert_eq!(ack.result(), Some(&json!({"say": "stored"})));
    let loaded = store.load(ID_A).unwrap().unwrap();
    assert_eq!(loaded.task_id(), None);
    assert_eq!(loaded.turn_id(), None);
    // Exact original frozen body preserved.
    assert_eq!(loaded.body(), &json!({"task_id": ID_B, "text": "hello"}));
    // Saved server preparation frozen once at publication.
    assert_eq!(
        loaded.prepared(),
        &json!({"expected_revision": 3, "expected_turn": 7})
    );
    // Same request+digest replays with the saved result.
    let replay = store
        .handle_with(&request, &executor, ControllerFault::None)
        .unwrap();
    assert_eq!(replay.result(), Some(&json!({"say": "stored"})));
    assert_eq!(executor.calls_for(ID_A), 1);
}

#[test]
fn restart_consumes_the_saved_preparation_not_a_fresh_one() {
    let temp = tempfile::tempdir().unwrap();
    let (_, store) = open_store(&temp);
    let first_meta = OperationMeta {
        task_id: None,
        turn_id: None,
        created_at_millis: 1_700_000_000_000,
        prepared: json!({"expected_revision": 3}),
    };
    let first = KernelTestExecutor::echoing_prepared(first_meta);
    let request = request_for(ID_A, "task.say", json!({"text": "hello"}));
    let published = store
        .handle_with(&request, &first, ControllerFault::StopAfterPublish)
        .unwrap();
    assert_eq!(published.status(), "published");

    // A restarted handler offers a DIFFERENT fresh preparation. The kernel
    // must execute from the saved row, never the fresh offer.
    let second_meta = OperationMeta {
        task_id: None,
        turn_id: None,
        created_at_millis: 1_800_000_000_000,
        prepared: json!({"expected_revision": 9}),
    };
    let second = KernelTestExecutor::echoing_prepared(second_meta);
    let ack = store
        .handle_with(&request, &second, ControllerFault::None)
        .unwrap();
    assert_eq!(ack.status(), "acked");
    assert_eq!(ack.result(), Some(&json!({"rev": 3})));
    assert_eq!(second.calls_for(ID_A), 1);
    assert_eq!(
        second.seen_prepared.lock().unwrap().as_slice(),
        &[json!({"expected_revision": 3})]
    );
    assert_eq!(
        store.load(ID_A).unwrap().unwrap().phase(),
        RequestPhase::Acked
    );
}

#[test]
fn crash_after_ack_before_retire_heals_on_the_next_tick() {
    let temp = tempfile::tempdir().unwrap();
    let (_, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    let published = store
        .handle_with(
            &checkpoint_request(ID_A),
            &executor,
            ControllerFault::StopAfterPublish,
        )
        .unwrap();
    assert!(
        store
            .handle_with(
                &checkpoint_request(ID_A),
                &executor,
                ControllerFault::CrashAfterAckBeforeRetire,
            )
            .is_err()
    );
    // ACK is durable; only the receipt retire was lost.
    let loaded = store.load(ID_A).unwrap().unwrap();
    assert_eq!(loaded.phase(), RequestPhase::Acked);
    assert_eq!(loaded.task_id(), published.task_id());

    let tick = store
        .resume_active_bounded(&executor, &ActiveResumeConfig::default())
        .unwrap();
    assert_eq!(tick.completed.len(), 1);
    assert_eq!(tick.completed[0].status(), "acked");
    assert_eq!(tick.completed[0].result(), Some(&json!({"n": 1})));
    assert!(tick.failed.is_empty());
    // No re-execution for the retire window.
    assert_eq!(executor.calls_for(ID_A), 1);
    // Second tick is idle: receipt retired, history untouched.
    let idle = store
        .resume_active_bounded(&executor, &ActiveResumeConfig::default())
        .unwrap();
    assert!(idle.completed.is_empty());
    assert!(!idle.truncated);
}

#[test]
fn concurrent_same_and_distinct_requests_converge() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("mac-worker-controller");
    ControllerStore::open(&state).unwrap();
    let executor = Arc::new(KernelTestExecutor::checkpoint(json!({"n": 1})));
    let distinct = [ID_B, ID_C, ID_D];

    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..4 {
            let state = state.clone();
            let executor = executor.clone();
            handles.push(scope.spawn(move || {
                ControllerStore::open(&state)
                    .unwrap()
                    .handle_with(
                        &checkpoint_request(ID_A),
                        executor.as_ref(),
                        ControllerFault::None,
                    )
                    .unwrap()
            }));
        }
        for id in distinct {
            let state = state.clone();
            let executor = executor.clone();
            handles.push(scope.spawn(move || {
                ControllerStore::open(&state)
                    .unwrap()
                    .handle_with(
                        &checkpoint_request(id),
                        executor.as_ref(),
                        ControllerFault::None,
                    )
                    .unwrap()
            }));
        }
        let mut same = Vec::new();
        let mut other = Vec::new();
        for (index, handle) in handles.into_iter().enumerate() {
            let ack = handle.join().unwrap();
            assert_eq!(ack.status(), "acked");
            if index < 4 {
                same.push(ack);
            } else {
                other.push(ack);
            }
        }
        // Same request: one identity, one result, one execution.
        for ack in &same[1..] {
            assert_eq!(ack.task_id(), same[0].task_id());
            assert_eq!(ack.turn_id(), same[0].turn_id());
            assert_eq!(ack.payload_sha256(), same[0].payload_sha256());
            assert_eq!(ack.result(), same[0].result());
        }
        // Distinct requests: distinct identities.
        let mut ids: Vec<_> = other.iter().map(|ack| ack.task_id()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 3);
        assert!(!ids.contains(&same[0].task_id()));
    });
    assert_eq!(executor.calls_for(ID_A), 1);
    assert_eq!(executor.calls_for(ID_B), 1);
    assert_eq!(executor.calls_for(ID_C), 1);
    assert_eq!(executor.calls_for(ID_D), 1);
    assert_eq!(
        ControllerStore::open(&state)
            .unwrap()
            .request_count()
            .unwrap(),
        4
    );
    // Any receipts left by contended retires heal on the bounded tick
    // without re-executing; afterwards the index is idle.
    let store = ControllerStore::open(&state).unwrap();
    let heal = store
        .resume_active_bounded(executor.as_ref(), &ActiveResumeConfig::default())
        .unwrap();
    assert!(heal.failed.is_empty());
    for ack in &heal.completed {
        assert_eq!(ack.status(), "acked");
    }
    assert_eq!(executor.calls_for(ID_A), 1);
    assert_eq!(executor.calls_for(ID_B), 1);
    assert_eq!(executor.calls_for(ID_C), 1);
    assert_eq!(executor.calls_for(ID_D), 1);
    let idle = store
        .resume_active_bounded(executor.as_ref(), &ActiveResumeConfig::default())
        .unwrap();
    assert!(idle.completed.is_empty());
    assert!(idle.failed.is_empty());
    assert!(!idle.truncated);
}

#[test]
fn bounded_ticks_drain_pending_without_starvation() {
    let temp = tempfile::tempdir().unwrap();
    let (_, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    for id in [ID_A, ID_B, ID_C] {
        store
            .handle_with(
                &checkpoint_request(id),
                &executor,
                ControllerFault::StopAfterPublish,
            )
            .unwrap();
    }
    let tight = ActiveResumeConfig {
        max_requests_per_tick: 2,
    };
    let first = store.resume_active_bounded(&executor, &tight).unwrap();
    assert_eq!(first.completed.len(), 2);
    assert!(first.truncated);
    assert!(first.failed.is_empty());
    let second = store.resume_active_bounded(&executor, &tight).unwrap();
    assert_eq!(second.completed.len(), 1);
    assert!(!second.truncated);
    let idle = store
        .resume_active_bounded(&executor, &ActiveResumeConfig::default())
        .unwrap();
    assert!(idle.completed.is_empty());
    assert!(!idle.truncated);
    assert_eq!(executor.calls_for(ID_A), 1);
    assert_eq!(executor.calls_for(ID_B), 1);
    assert_eq!(executor.calls_for(ID_C), 1);
}

#[test]
fn idle_tick_never_reads_poisoned_retired_history() {
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    for id in [ID_A, ID_B, ID_C] {
        store
            .handle_with(&checkpoint_request(id), &executor, ControllerFault::None)
            .unwrap();
    }
    drop(store);
    // Poison retired history directly: one corrupt row, one garbage file.
    // A history-scanning tick would choke; the index tick must stay idle-clean.
    std::fs::write(state.join(format!("req-{ID_A}.json")), b"{broken").unwrap();
    std::fs::write(state.join("req-deadbeef.json"), b"{broken").unwrap();
    let store = ControllerStore::open(&state).unwrap();
    let bootstrap = store.bootstrap_active_index().unwrap();
    assert!(!bootstrap.already_bootstrapped);
    // Only the corrupt names are surfaced; nothing is silently hidden.
    assert!(bootstrap.corrupt.iter().any(|entry| entry.contains(ID_A)));
    assert!(bootstrap.rebuilt.is_empty());

    let tick = store
        .resume_active_bounded(&executor, &ActiveResumeConfig::default())
        .unwrap();
    assert!(tick.completed.is_empty());
    assert!(tick.failed.is_empty());
    assert!(!tick.truncated);
    // Bootstrap runs once: the guard receipt is durable.
    assert!(store.bootstrap_active_index().unwrap().already_bootstrapped);
}

#[test]
fn index_receipt_crash_heals_on_retry_and_keeps_the_orphan_visible() {
    let temp = tempfile::tempdir().unwrap();
    let (_, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    assert!(
        store
            .handle_with(
                &checkpoint_request(ID_A),
                &executor,
                ControllerFault::StopAfterActiveReceiptBeforePublish,
            )
            .is_err()
    );
    // Row missing, receipt present: bootstrap must keep the orphan, not hide it.
    assert!(store.load(ID_A).unwrap().is_none());
    let tick = store
        .resume_active_bounded(&executor, &ActiveResumeConfig::default())
        .unwrap();
    assert_eq!(tick.orphan_receipts, vec![ID_A.to_owned()]);
    assert!(tick.completed.is_empty());

    // The retry heals: same identity, one execution, durable result.
    let ack = store
        .handle_with(&checkpoint_request(ID_A), &executor, ControllerFault::None)
        .unwrap();
    assert_eq!(ack.status(), "acked");
    assert_eq!(ack.result(), Some(&json!({"n": 1})));
    assert_eq!(executor.calls_for(ID_A), 1);
}

#[test]
fn legacy_row_without_a_receipt_is_rebuilt_once_by_bootstrap() {
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(json!({"n": 7}));
    let published = store
        .handle_with(
            &checkpoint_request(ID_E),
            &executor,
            ControllerFault::StopAfterPublish,
        )
        .unwrap();
    drop(store);
    // Simulate a pre-kernel row: drop its active receipt.
    std::fs::remove_file(state.join("active").join(format!("{ID_E}.json"))).unwrap();
    let store = ControllerStore::open(&state).unwrap();
    let idle_before = store
        .resume_active_bounded(&executor, &ActiveResumeConfig::default())
        .unwrap();
    assert!(idle_before.completed.is_empty());

    let bootstrap = store.bootstrap_active_index().unwrap();
    assert!(!bootstrap.already_bootstrapped);
    assert_eq!(bootstrap.rebuilt, vec![ID_E.to_owned()]);
    assert!(bootstrap.corrupt.is_empty());

    let tick = store
        .resume_active_bounded(&executor, &ActiveResumeConfig::default())
        .unwrap();
    assert_eq!(tick.completed.len(), 1);
    assert_eq!(tick.completed[0].task_id(), published.task_id());
    assert_eq!(tick.completed[0].result(), Some(&json!({"n": 7})));
    assert_eq!(executor.calls_for(ID_E), 1);
}

#[test]
fn poisoned_early_entries_do_not_starve_later_work() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("mac-worker-controller");
    ControllerStore::open(&state).unwrap();
    let executor = Arc::new(KernelTestExecutor::checkpoint(json!({"n": 1})));
    let plain = KernelTestExecutor::checkpoint(json!({"n": 1}));
    // Publish all four without executing; poison the early window afterwards:
    // ID_A will be permanently busy, ID_B permanently failing, while ID_C and
    // ID_D are valid later work. Sorted order puts the poison first.
    for id in [ID_A, ID_B, ID_C, ID_D] {
        ControllerStore::open(&state)
            .unwrap()
            .handle_with(
                &checkpoint_request(id),
                &plain,
                ControllerFault::StopAfterPublish,
            )
            .unwrap();
    }
    std::fs::write(
        state.join("active").join(format!("{ID_B}.json")),
        b"{broken",
    )
    .unwrap();
    let (gate_tx, gate_rx) = mpsc::channel();
    *executor.gate.lock().unwrap() = Some(gate_rx);

    // Background handle occupies ID_A inside its executor (per-request lock held).
    let background_state = state.clone();
    let background_executor = executor.clone();
    let background = std::thread::spawn(move || {
        ControllerStore::open(&background_state)
            .unwrap()
            .handle_with(
                &checkpoint_request(ID_A),
                background_executor.as_ref(),
                ControllerFault::None,
            )
            .unwrap()
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    while executor.calls_for(ID_A) == 0 {
        assert!(
            Instant::now() < deadline,
            "background execute did not start"
        );
        std::thread::yield_now();
    }

    let tight = ActiveResumeConfig {
        max_requests_per_tick: 2,
    };
    // Tick 1 attempts the poisoned head: busy + failed, nothing completed.
    let first = ControllerStore::open(&state)
        .unwrap()
        .resume_active_bounded(executor.as_ref(), &tight)
        .unwrap();
    assert!(first.completed.is_empty());
    assert_eq!(first.busy_skipped, vec![ID_A.to_owned()]);
    assert_eq!(first.failed.len(), 1);
    assert_eq!(first.failed[0].0, ID_B.to_owned());
    assert!(first.truncated);
    assert!(!first.cursor_stale);

    // Tick 2 rotates past the poison and completes the later valid work.
    let second = ControllerStore::open(&state)
        .unwrap()
        .resume_active_bounded(executor.as_ref(), &tight)
        .unwrap();
    assert_eq!(second.completed.len(), 2);
    assert!(
        second
            .completed
            .iter()
            .all(|ack| ack.result() == Some(&json!({"n": 1})))
    );
    assert!(second.failed.is_empty());
    assert!(!second.cursor_stale);
    assert_eq!(executor.calls_for(ID_C), 1);
    assert_eq!(executor.calls_for(ID_D), 1);

    // Release the busy head; it converges without re-driving finished work.
    gate_tx.send(()).unwrap();
    let ack = background.join().unwrap();
    assert_eq!(ack.status(), "acked");
    assert_eq!(executor.calls_for(ID_A), 1);
    let idle = ControllerStore::open(&state)
        .unwrap()
        .resume_active_bounded(executor.as_ref(), &ActiveResumeConfig::default())
        .unwrap();
    assert!(idle.completed.is_empty());
    assert_eq!(idle.failed.len(), 1);
    assert_eq!(idle.failed[0].0, ID_B.to_owned());
    assert!(!idle.truncated);
}

#[test]
fn corrupt_row_and_lock_errors_do_not_starve_later_work() {
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let plain = KernelTestExecutor::checkpoint(json!({"n": 1}));
    // Valid receipts for all four; poison the early window afterwards.
    // ID_A: invalid per-request lock (directory in place of the lock file).
    // ID_B: corrupt durable request row. ID_C/ID_D: later valid work.
    for id in [ID_A, ID_B, ID_C, ID_D] {
        store
            .handle_with(
                &checkpoint_request(id),
                &plain,
                ControllerFault::StopAfterPublish,
            )
            .unwrap();
    }
    drop(store);
    std::fs::remove_file(state.join(format!("req-{ID_A}.lock"))).unwrap();
    std::fs::create_dir(state.join(format!("req-{ID_A}.lock"))).unwrap();
    let row_b = state.join(format!("req-{ID_B}.json"));
    std::fs::write(&row_b, b"{broken").unwrap();

    let store = ControllerStore::open(&state).unwrap();
    let executor = KernelTestExecutor::checkpoint(json!({"n": 1}));
    let tight = ActiveResumeConfig {
        max_requests_per_tick: 2,
    };
    // Tick 1 attempts the poisoned head only: lock + row failures.
    let first = store.resume_active_bounded(&executor, &tight).unwrap();
    assert!(first.completed.is_empty());
    assert!(first.busy_skipped.is_empty());
    assert_eq!(first.failed.len(), 2);
    assert_eq!(first.failed[0].0, ID_A.to_owned());
    assert_eq!(first.failed[1].0, ID_B.to_owned());
    assert!(first.truncated);
    assert!(!first.cursor_stale);

    // Tick 2 rotates past the poison and completes the later valid work.
    let second = store.resume_active_bounded(&executor, &tight).unwrap();
    assert_eq!(second.completed.len(), 2);
    assert!(
        second
            .completed
            .iter()
            .all(|ack| ack.result() == Some(&json!({"n": 1})))
    );
    assert!(second.failed.is_empty());
    assert_eq!(executor.calls_for(ID_A), 0);
    assert_eq!(executor.calls_for(ID_B), 0);
    assert_eq!(executor.calls_for(ID_C), 1);
    assert_eq!(executor.calls_for(ID_D), 1);

    // Bad evidence preserved untouched: no repair-by-deletion, no rewrite.
    assert_eq!(std::fs::read(&row_b).unwrap(), b"{broken");
    assert!(
        std::fs::metadata(state.join(format!("req-{ID_A}.lock")))
            .unwrap()
            .is_dir()
    );
}

#[test]
fn saved_json_null_survives_reopen_without_reexecuting() {
    // A handler may legitimately return JSON null. It must persist as an
    // explicit null and survive reopen as Some(Null) — never collapse to
    // None (which would re-execute) and never rewrite history.
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let executor = KernelTestExecutor::checkpoint(Value::Null);
    let published = store
        .handle_with(
            &checkpoint_request(ID_A),
            &executor,
            ControllerFault::StopAfterResultBeforeAck,
        )
        .unwrap();
    assert_eq!(executor.calls_for(ID_A), 1);
    assert_eq!(published.result(), Some(&Value::Null));
    let raw: Value =
        serde_json::from_slice(&std::fs::read(state.join(format!("req-{ID_A}.json"))).unwrap())
            .unwrap();
    assert_eq!(raw.get("result"), Some(&Value::Null));

    drop(store);
    let store = ControllerStore::open(&state).unwrap();
    let ack = store
        .handle_with(&checkpoint_request(ID_A), &executor, ControllerFault::None)
        .unwrap();
    assert_eq!(ack.status(), "acked");
    assert_eq!(ack.result(), Some(&Value::Null));
    assert_eq!(executor.calls_for(ID_A), 1);

    // Absent legacy result key still reads as None (no key at all).
    let legacy = store
        .handle_with(
            &checkpoint_request(ID_B),
            &executor,
            ControllerFault::StopAfterPublish,
        )
        .unwrap();
    assert_eq!(legacy.result(), None);
    let raw_b: Value =
        serde_json::from_slice(&std::fs::read(state.join(format!("req-{ID_B}.json"))).unwrap())
            .unwrap();
    assert!(raw_b.get("result").is_none());
}

#[test]
fn bootstrap_with_healthy_preexisting_receipts_reports_no_corrupt() {
    // Crash-restart shape: receipts exist, bootstrap marker absent. Healthy
    // pre-existing receipts must read-back-validate as rebuilt, never as
    // corrupt, and the second call must replay the durable marker stably.
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let plain = KernelTestExecutor::checkpoint(json!({"n": 1}));
    for id in [ID_A, ID_B] {
        store
            .handle_with(
                &checkpoint_request(id),
                &plain,
                ControllerFault::StopAfterPublish,
            )
            .unwrap();
    }
    drop(store);
    assert!(!state.join("active-index-bootstrap-v1.json").exists());
    let store = ControllerStore::open(&state).unwrap();
    let first = store.bootstrap_active_index().unwrap();
    assert!(!first.already_bootstrapped);
    assert_eq!(first.rebuilt, vec![ID_A.to_owned(), ID_B.to_owned()]);
    assert!(first.corrupt.is_empty());
    let second = store.bootstrap_active_index().unwrap();
    assert!(second.already_bootstrapped);
    assert_eq!(second.rebuilt, first.rebuilt);
    assert_eq!(second.corrupt, first.corrupt);
}

#[test]
fn concurrent_bootstraps_converge_without_false_corrupt() {
    // Legacy rows without receipts; two bootstraps race receipt creation
    // and the marker write. Every interleaving must converge: no false
    // corrupt, winner arrays shared, marker valid and stable afterwards.
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("mac-worker-controller");
    ControllerStore::open(&state).unwrap();
    let plain = KernelTestExecutor::checkpoint(json!({"n": 1}));
    for id in [ID_A, ID_B] {
        ControllerStore::open(&state)
            .unwrap()
            .handle_with(
                &checkpoint_request(id),
                &plain,
                ControllerFault::StopAfterPublish,
            )
            .unwrap();
        std::fs::remove_file(state.join("active").join(format!("{id}.json"))).unwrap();
    }
    std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            ControllerStore::open(&state)
                .unwrap()
                .bootstrap_active_index()
        });
        let second = scope.spawn(|| {
            ControllerStore::open(&state)
                .unwrap()
                .bootstrap_active_index()
        });
        for report in [first.join().unwrap(), second.join().unwrap()] {
            let report = report.unwrap();
            assert_eq!(report.rebuilt, vec![ID_A.to_owned(), ID_B.to_owned()]);
            assert!(report.corrupt.is_empty());
        }
    });
    let stable = ControllerStore::open(&state)
        .unwrap()
        .bootstrap_active_index()
        .unwrap();
    assert!(stable.already_bootstrapped);
    assert_eq!(stable.rebuilt, vec![ID_A.to_owned(), ID_B.to_owned()]);
    assert!(stable.corrupt.is_empty());
}

#[test]
fn malformed_bootstrap_marker_fails_closed() {
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    for (name, bytes) in [
        ("garbage", b"{broken".as_slice()),
        (
            "wrong-version",
            br#"{"version":2,"created_at_millis":1,"rebuilt":[],"corrupt":[]}"#.as_slice(),
        ),
    ] {
        std::fs::write(state.join("active-index-bootstrap-v1.json"), bytes).unwrap();
        // Private store files must stay owner-only for the read path.
        std::fs::set_permissions(
            state.join("active-index-bootstrap-v1.json"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let error = store.bootstrap_active_index().unwrap_err();
        assert_eq!(
            error.public_code(),
            "CONTROLLER_TRANSPORT",
            "{name} marker must fail closed"
        );
        std::fs::remove_file(state.join("active-index-bootstrap-v1.json")).unwrap();
    }
}

#[test]
fn failed_index_write_leaves_bootstrap_incomplete_for_retry() {
    // A transient index-write failure for a VALID row is not corruption:
    // bootstrap must fail retryably WITHOUT persisting the completed
    // marker, so a later bootstrap can finish the same row.
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let plain = KernelTestExecutor::checkpoint(json!({"n": 1}));
    store
        .handle_with(
            &checkpoint_request(ID_A),
            &plain,
            ControllerFault::StopAfterPublish,
        )
        .unwrap();
    drop(store);
    std::fs::remove_file(state.join("active").join(format!("{ID_A}.json"))).unwrap();
    // Open BEFORE tightening permissions: the live handle stays valid,
    // only the new index write must fail.
    let store = ControllerStore::open(&state).unwrap();
    std::fs::set_permissions(state.join("active"), std::fs::Permissions::from_mode(0o555)).unwrap();
    let error = store.bootstrap_active_index().unwrap_err();
    assert_eq!(error.public_code(), "IO");
    assert!(!state.join("active-index-bootstrap-v1.json").exists());
    assert_eq!(
        store.load(ID_A).unwrap().unwrap().phase(),
        RequestPhase::Published
    );

    std::fs::set_permissions(state.join("active"), std::fs::Permissions::from_mode(0o700)).unwrap();
    let retry = store.bootstrap_active_index().unwrap();
    assert!(!retry.already_bootstrapped);
    assert_eq!(retry.rebuilt, vec![ID_A.to_owned()]);
    assert!(retry.corrupt.is_empty());
    assert!(state.join("active-index-bootstrap-v1.json").exists());
}

#[test]
fn busy_request_is_skipped_while_others_progress() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("mac-worker-controller");
    ControllerStore::open(&state).unwrap();
    let executor = Arc::new(KernelTestExecutor::checkpoint(json!({"n": 1})));
    let (gate_tx, gate_rx) = mpsc::channel();
    *executor.gate.lock().unwrap() = Some(gate_rx);

    // Background handle occupies ID_F inside its executor (per-request lock held).
    let background_state = state.clone();
    let background_executor = executor.clone();
    let background = std::thread::spawn(move || {
        ControllerStore::open(&background_state)
            .unwrap()
            .handle_with(
                &checkpoint_request(ID_F),
                background_executor.as_ref(),
                ControllerFault::None,
            )
            .unwrap()
    });
    // Wait until the background thread is inside execute (bounded, no sleep loop).
    let deadline = Instant::now() + Duration::from_secs(20);
    while executor.calls_for(ID_F) == 0 {
        assert!(
            Instant::now() < deadline,
            "background execute did not start"
        );
        std::thread::yield_now();
    }
    // A second request is pending while the first is busy.
    ControllerStore::open(&state)
        .unwrap()
        .handle_with(
            &checkpoint_request(ID_A),
            executor.as_ref(),
            ControllerFault::StopAfterPublish,
        )
        .unwrap();

    let tick = ControllerStore::open(&state)
        .unwrap()
        .resume_active_bounded(executor.as_ref(), &ActiveResumeConfig::default())
        .unwrap();
    assert_eq!(tick.busy_skipped, vec![ID_F.to_owned()]);
    assert_eq!(tick.completed.len(), 1);
    assert_eq!(tick.completed[0].request_id(), ID_A);
    assert!(tick.failed.is_empty());

    gate_tx.send(()).unwrap();
    let ack = background.join().unwrap();
    assert_eq!(ack.status(), "acked");
    assert_eq!(ack.result(), Some(&json!({"n": 1})));
    assert_eq!(executor.calls_for(ID_F), 1);
    // The tick heals the retire; history is quiet afterwards.
    let idle = ControllerStore::open(&state)
        .unwrap()
        .resume_active_bounded(executor.as_ref(), &ActiveResumeConfig::default())
        .unwrap();
    assert!(idle.completed.is_empty());
    assert!(idle.failed.is_empty());
}
