//! Cancellation must not leave `LEASE_RELEASE_FAILED` on a job whose worker
//! slot is already free. Waiting and dispatching rows never acquire a lease;
//! a running cancel that wins the release must stay clean when a second
//! cancel retries the same identity.

use std::{
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use mac_worker::{
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    error::WorkerError,
    job::{CommandSpec, JobId, ProcessIdentity, QueueEntry, QueueEntryKind, QueueState},
    run::{CancelReport, CancelService, SchedulerRuntime},
    scheduler::WorkerPreference,
};
use support::recording_runner::RecordingRunner;

#[allow(dead_code)]
mod support;

struct TestSchedulerRuntime {
    now_millis: AtomicU64,
    owner: ProcessIdentity,
    sleeps: Mutex<Vec<Duration>>,
}

impl TestSchedulerRuntime {
    fn new(now_millis: u64, owner_seed: u32) -> Self {
        Self {
            now_millis: AtomicU64::new(now_millis),
            owner: ProcessIdentity::new(owner_seed, u64::from(owner_seed) * 10_000 + 7).unwrap(),
            sleeps: Mutex::new(Vec::new()),
        }
    }
}

impl SchedulerRuntime for TestSchedulerRuntime {
    fn now_millis(&self) -> Result<u64, WorkerError> {
        Ok(self.now_millis.load(Ordering::SeqCst))
    }

    fn process_identity(&self) -> Result<ProcessIdentity, WorkerError> {
        Ok(self.owner)
    }

    fn sleep(&self, duration: Duration) {
        self.sleeps.lock().unwrap().push(duration);
        let millis = u64::try_from(duration.as_millis()).unwrap();
        self.now_millis.fetch_add(millis, Ordering::SeqCst);
    }
}

fn config() -> Config {
    Config {
        version: 1,
        workers: vec![WorkerEntry {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            slots: 1,
            capabilities: Vec::new(),
            remote_binary: "~/.local/bin/worker".into(),
        }],
    }
}

fn state_store(temp: &tempfile::TempDir) -> ClientStateStore {
    ClientStateStore::open(&temp.path().canonicalize().unwrap().join("state")).unwrap()
}

fn queued_entry(
    store: &ClientStateStore,
    seed: u128,
    owner: ProcessIdentity,
    enqueued_at_millis: u64,
) -> QueueEntry {
    QueueEntry::new(
        JobId::new(uuid::Uuid::from_u128(seed)),
        store.client_id(),
        "a".repeat(64),
        "b".repeat(64),
        CommandSpec::argv(vec!["queued".into()])
            .unwrap()
            .summary()
            .unwrap(),
        Vec::new(),
        WorkerPreference::Automatic,
        QueueEntryKind::Batch,
        None,
        owner,
        enqueued_at_millis,
    )
    .unwrap()
}

fn assert_no_host_lease(temp: &tempfile::TempDir) {
    assert!(
        !temp.path().join("leases").exists(),
        "queued cancellation must not create a host lease"
    );
}

#[test]
fn cancel_waiting_row_leaves_no_lease_and_no_remote_work() {
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let owner = ProcessIdentity::new(91_001, 910_010_007).unwrap();
    let entry = queued_entry(&store, 91_001, owner, 1);
    let job_id = entry.job_id();
    store.enqueue(entry).unwrap();
    let runner = RecordingRunner::default();
    let runtime = TestSchedulerRuntime::new(2_000, 91_001);
    let cancel_config = config();

    assert_eq!(
        CancelService::new(&runner, &cancel_config, &store)
            .with_scheduler_runtime(&runtime)
            .cancel(job_id)
            .unwrap(),
        CancelReport::QueuedCancelled { job_id }
    );
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert!(runner.requests().is_empty());
    assert_no_host_lease(&temp);
}

#[test]
fn cancel_dispatching_row_requests_cancel_without_acquiring_or_releasing_a_lease() {
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let owner = ProcessIdentity::new(91_101, 911_010_007).unwrap();
    let entry = queued_entry(&store, 91_101, owner, 1);
    let job_id = entry.job_id();
    store.enqueue(entry).unwrap();
    store
        .claim_next(owner, &["mini-1".into()], 2)
        .unwrap()
        .expect("owner must claim its waiting row");
    let runner = RecordingRunner::default();
    let runtime = TestSchedulerRuntime::new(91_102, 91_102);
    let cancel_config = config();

    let error = CancelService::new(&runner, &cancel_config, &store)
        .with_scheduler_runtime(&runtime)
        .cancel(job_id)
        .unwrap_err();
    assert!(matches!(
        error,
        WorkerError::Queue {
            code: "CANCEL_PENDING",
            ..
        }
    ));
    assert!(runner.requests().is_empty());
    assert_eq!(
        runtime.sleeps.lock().unwrap().as_slice(),
        &[Duration::from_secs(1)]
    );
    let queue = store.queue_snapshot().unwrap();
    assert_eq!(queue.entries().len(), 1);
    assert!(matches!(
        queue.entries()[0].state(),
        QueueState::Dispatching { dispatch_owner, .. } if *dispatch_owner == owner
    ));
    assert!(queue.entries()[0].is_cancel_requested());
    assert_no_host_lease(&temp);
}

#[test]
fn cancel_waiting_then_cancel_again_stays_local_and_lease_free() {
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let owner = ProcessIdentity::new(91_201, 912_010_007).unwrap();
    let entry = queued_entry(&store, 91_201, owner, 1);
    let job_id = entry.job_id();
    store.enqueue(entry).unwrap();
    let runner = RecordingRunner::default();
    let runtime = TestSchedulerRuntime::new(3_000, 91_201);
    let cancel_config = config();
    let service =
        CancelService::new(&runner, &cancel_config, &store).with_scheduler_runtime(&runtime);

    assert_eq!(
        service.cancel(job_id).unwrap(),
        CancelReport::QueuedCancelled { job_id }
    );
    let second = service.cancel(job_id).unwrap_err();
    assert!(
        second.to_string().contains("JOB_NOT_FOUND"),
        "second waiting cancel must not invent a remote lease release: {second}"
    );
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert!(runner.requests().is_empty());
    assert_no_host_lease(&temp);
}
