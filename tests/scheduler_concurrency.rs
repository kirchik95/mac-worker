use std::{
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

use mac_worker::{
    client_state::{ClientStateConcurrencyHook, ClientStateConcurrencyPoint, ClientStateStore},
    job::{
        AdmissionObservation, CommandSpec, JobId, ProcessIdentity, QueueEntry, QueueEntryKind,
        QueueRunReference, QueueState, RunId,
    },
    scheduler::{CandidateSlot, WorkerPreference},
};

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn owner(value: u32) -> ProcessIdentity {
    ProcessIdentity::new(value, u64::from(value) * 10_000 + 7).unwrap()
}

fn run_reference(id: &str, max_parallel: u32) -> QueueRunReference {
    QueueRunReference::new(RunId::new(id.into()).unwrap(), max_parallel).unwrap()
}

fn queued(
    store: &ClientStateStore,
    id: u128,
    owner: ProcessIdentity,
    preference: WorkerPreference,
    run: Option<QueueRunReference>,
) -> QueueEntry {
    QueueEntry::new(
        JobId::new(uuid::Uuid::from_u128(id)),
        store.client_id(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        CommandSpec::argv(vec!["safe-command-summary-only".into()])
            .unwrap()
            .summary()
            .unwrap(),
        Vec::new(),
        preference,
        QueueEntryKind::TaskTurn,
        run,
        owner,
        100,
    )
    .unwrap()
}

struct OneShotGate {
    point: ClientStateConcurrencyPoint,
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    used: AtomicBool,
}

impl ClientStateConcurrencyHook for OneShotGate {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        if point == self.point && !self.used.swap(true, Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
    }
}

fn gated_store(
    point: ClientStateConcurrencyPoint,
) -> (
    tempfile::TempDir,
    Arc<ClientStateStore>,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap().join("state");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let hook = Arc::new(OneShotGate {
        point,
        entered: entered_tx,
        release: Mutex::new(release_rx),
        used: AtomicBool::new(false),
    });
    let store = Arc::new(ClientStateStore::open_with_concurrency_hook(&root, hook).unwrap());
    (directory, store, entered_rx, release_tx)
}

#[test]
fn fifty_simultaneous_clients_preserve_sequence_and_never_reserve_two_slots_per_worker() {
    // Break caught: concurrent queue publication loses or duplicates a row,
    // assigns a non-monotonic sequence, or lets two claims reserve one worker.
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap().join("state");
    let store = Arc::new(ClientStateStore::open(&root).unwrap());
    let barrier = Arc::new(Barrier::new(51));
    let mut clients = Vec::new();
    for value in 1..=50_u32 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        clients.push(thread::spawn(move || {
            barrier.wait();
            store.enqueue(queued(
                &store,
                u128::from(value),
                owner(value),
                WorkerPreference::Automatic,
                None,
            ))
        }));
    }
    barrier.wait();
    for client in clients {
        client.join().unwrap().unwrap();
    }

    let snapshot = store.queue_snapshot().unwrap();
    assert_eq!(snapshot.entries().len(), 50);
    assert_eq!(
        snapshot
            .entries()
            .iter()
            .map(|entry| entry.queue_id().value())
            .collect::<Vec<_>>(),
        (1..=50).collect::<Vec<_>>(),
    );

    let mut claimed_workers = Vec::new();
    for entry in snapshot.entries() {
        if let Some(claim) = store
            .claim_next(
                *entry.owner(),
                &["mini-a".into(), "mini-b".into(), "mini-c".into()],
                101,
            )
            .unwrap()
        {
            let QueueState::Dispatching {
                selected_worker, ..
            } = claim.entry().state()
            else {
                panic!("a claim must be dispatching");
            };
            claimed_workers.push(selected_worker.to_owned());
        }
    }
    claimed_workers.sort();
    claimed_workers.dedup();
    assert_eq!(claimed_workers, ["mini-a", "mini-b", "mini-c"]);
}

#[test]
fn run_cap_and_observation_refresh_are_atomic_under_competing_dispatchers() {
    // Break caught: a claim evaluates the final run slot outside the queue
    // lock, or stale readers independently publish more than one refresh.
    for interleaving in 0..100_u128 {
        let (_directory, store, entered, release) =
            gated_store(ClientStateConcurrencyPoint::ClaimRunCapEvaluation);
        let left_owner = owner(10_000 + interleaving as u32 * 2);
        let right_owner = owner(10_001 + interleaving as u32 * 2);
        store
            .enqueue(queued(
                &store,
                1_000 + interleaving * 2,
                left_owner,
                WorkerPreference::Pinned {
                    worker: "mini-a".into(),
                },
                Some(run_reference("final-slot", 1)),
            ))
            .unwrap();
        store
            .enqueue(queued(
                &store,
                1_001 + interleaving * 2,
                right_owner,
                WorkerPreference::Pinned {
                    worker: "mini-b".into(),
                },
                Some(run_reference("final-slot", 1)),
            ))
            .unwrap();

        let left_store = Arc::clone(&store);
        let left =
            thread::spawn(move || left_store.claim_next(left_owner, &["mini-a".into()], 101));
        entered.recv().unwrap();
        let right_store = Arc::clone(&store);
        let right =
            thread::spawn(move || right_store.claim_next(right_owner, &["mini-b".into()], 101));
        release.send(()).unwrap();

        let claims = [
            left.join().unwrap().unwrap(),
            right.join().unwrap().unwrap(),
        ]
        .into_iter()
        .flatten()
        .count();
        assert_eq!(claims, 1, "interleaving {interleaving}");
    }

    let (_directory, store, entered, release) =
        gated_store(ClientStateConcurrencyPoint::ObservationRefreshPublication);
    let refreshes = Arc::new(AtomicUsize::new(0));
    let first_store = Arc::clone(&store);
    let first_refreshes = Arc::clone(&refreshes);
    let first = thread::spawn(move || {
        first_store.admission_observation("mini-a", 10_000, || {
            first_refreshes.fetch_add(1, Ordering::SeqCst);
            AdmissionObservation::new(
                "mini-a".into(),
                true,
                CandidateSlot::Idle,
                Vec::new(),
                Some(10),
                20,
                10_000,
            )
        })
    });
    entered.recv().unwrap();
    let second_store = Arc::clone(&store);
    let second_refreshes = Arc::clone(&refreshes);
    let second = thread::spawn(move || {
        second_store.admission_observation("mini-a", 10_000, || {
            second_refreshes.fetch_add(1, Ordering::SeqCst);
            AdmissionObservation::new(
                "mini-a".into(),
                true,
                CandidateSlot::Idle,
                Vec::new(),
                Some(10),
                20,
                10_000,
            )
        })
    });
    release.send(()).unwrap();
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
}
