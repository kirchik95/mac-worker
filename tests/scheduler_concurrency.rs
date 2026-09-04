use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

use mac_worker::{
    client_state::{ClientStateConcurrencyHook, ClientStateConcurrencyPoint, ClientStateStore},
    host_store::HostStore,
    job::{
        AdmissionObservation, CommandSpec, JobId, LeaseAcquireRequest, LeaseAcquireResponse,
        LeaseToken, ProcessIdentity, QueueEntry, QueueEntryKind, QueueRunReference, QueueState,
        RequestFingerprintMaterial, RunId,
    },
    lease::{AdmissionFacts, LeaseService},
    protocol::MemoryPressure,
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

fn healthy_admission() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 250 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

fn lease_request(entry: &QueueEntry, worker: &str, seed: u128) -> LeaseAcquireRequest {
    let material = RequestFingerprintMaterial::new(
        entry.job_id(),
        entry.client_id(),
        LeaseToken::new(uuid::Uuid::from_u128(seed + 100_000)),
        entry.enqueued_at_millis(),
        worker.into(),
        entry.project_id().into(),
        entry.worktree_id().into(),
        "c".repeat(64),
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::argv(vec!["safe-command-summary-only".into()]).unwrap(),
    )
    .unwrap();
    LeaseAcquireRequest::new(material)
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
fn fifty_simultaneous_clients_preserve_fifo_and_never_start_two_heavy_jobs_per_worker() {
    // Break caught: concurrent queue publication loses or duplicates a row,
    // assigns a non-monotonic sequence, or lets two claims reserve one worker.
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap().join("state");
    let store = Arc::new(ClientStateStore::open(&root).unwrap());
    let barrier = Arc::new(Barrier::new(51));
    let enqueue_events = Arc::new(Mutex::new(Vec::new()));
    let owners = [owner(40_001), owner(40_002), owner(40_003)];
    let pinned_workers = ["mini-a", "mini-b", "mini-c"];
    let mut clients = Vec::new();
    for value in 1..=50_u32 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let enqueue_events = Arc::clone(&enqueue_events);
        let row_owner = owners[((value - 1) as usize) % owners.len()];
        let pinned_worker = pinned_workers[((value - 1) as usize) % pinned_workers.len()];
        clients.push(thread::spawn(move || {
            barrier.wait();
            let entry = store.enqueue(queued(
                &store,
                u128::from(value),
                row_owner,
                WorkerPreference::Pinned {
                    worker: pinned_worker.into(),
                },
                None,
            ))?;
            enqueue_events
                .lock()
                .unwrap()
                .push((value, entry.job_id(), entry.queue_id().value()));
            Ok::<_, mac_worker::error::WorkerError>(entry)
        }));
    }
    barrier.wait();
    let published = clients
        .into_iter()
        .map(|client| client.join().unwrap().unwrap())
        .collect::<Vec<_>>();

    let snapshot = store.queue_snapshot().unwrap();
    assert_eq!(snapshot.entries().len(), 50);
    let enqueue_events = enqueue_events.lock().unwrap().clone();
    assert_eq!(enqueue_events.len(), 50);
    assert_eq!(
        enqueue_events
            .iter()
            .map(|(value, _, _)| *value)
            .collect::<BTreeSet<_>>(),
        (1..=50).collect::<BTreeSet<_>>(),
        "enqueue event trace must retain every client"
    );
    assert_eq!(
        enqueue_events
            .iter()
            .map(|(_, _, queue_id)| *queue_id)
            .collect::<BTreeSet<_>>()
            .len(),
        50,
        "enqueue event trace must retain every canonical publication"
    );
    assert!(enqueue_events.iter().all(|(_, job_id, queue_id)| {
        snapshot
            .entries()
            .iter()
            .any(|entry| entry.job_id() == *job_id && entry.queue_id().value() == *queue_id)
    }));
    let queue_ids = snapshot
        .entries()
        .iter()
        .map(|entry| entry.queue_id().value())
        .collect::<Vec<_>>();
    assert!(
        queue_ids.windows(2).all(|window| window[0] < window[1]),
        "canonical queue sequence must be strictly monotonic"
    );
    assert_eq!(
        published
            .iter()
            .map(|entry| entry.job_id().to_string())
            .collect::<BTreeSet<_>>()
            .len(),
        50,
        "all simultaneous clients retain distinct IDs"
    );

    let claim_barrier = Arc::new(Barrier::new(51));
    let claim_event_counter = Arc::new(AtomicUsize::new(0));
    let (claim_events_tx, claim_events_rx) = mpsc::channel();
    let mut claimers = Vec::new();
    let claim_inputs = enqueue_events
        .iter()
        .map(|(_, job_id, _)| {
            snapshot
                .entries()
                .iter()
                .find(|entry| entry.job_id() == *job_id)
                .unwrap()
                .clone()
        })
        .collect::<Vec<_>>();
    for (attempt, entry) in claim_inputs.into_iter().enumerate() {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&claim_barrier);
        let event_counter = Arc::clone(&claim_event_counter);
        let events = claim_events_tx.clone();
        claimers.push(thread::spawn(move || {
            barrier.wait();
            let result = store.claim_next(
                *entry.owner(),
                &["mini-a".into(), "mini-b".into(), "mini-c".into()],
                101,
            );
            let event = event_counter.fetch_add(1, Ordering::SeqCst);
            events.send((event, attempt, result)).unwrap();
        }));
    }
    claim_barrier.wait();
    for claimer in claimers {
        claimer.join().unwrap();
    }
    drop(claim_events_tx);
    let claim_events = claim_events_rx.into_iter().collect::<Vec<_>>();
    assert_eq!(claim_events.len(), 50);
    assert_eq!(
        claim_events
            .iter()
            .map(|(event, _, _)| *event)
            .collect::<BTreeSet<_>>()
            .len(),
        50,
        "claim event trace must retain every concurrent attempt"
    );
    let reservations = claim_events
        .into_iter()
        .filter_map(|(_, attempt, result)| {
            let claim =
                result.unwrap_or_else(|error| panic!("claim attempt {attempt} failed: {error}"))?;
            Some((
                claim.entry().clone(),
                match claim.entry().state() {
                    QueueState::Dispatching {
                        selected_worker, ..
                    } => selected_worker.to_owned(),
                    QueueState::Waiting { .. } => panic!("a successful claim must be dispatching"),
                },
            ))
        })
        .collect::<Vec<_>>();
    assert_eq!(reservations.len(), 3);
    let oldest_for_owner = owners
        .iter()
        .map(|row_owner| {
            snapshot
                .entries()
                .iter()
                .find(|entry| entry.owner() == row_owner)
                .unwrap()
                .clone()
        })
        .collect::<Vec<_>>();
    for (entry, _) in &reservations {
        assert!(
            oldest_for_owner
                .iter()
                .any(|oldest| oldest.owner() == entry.owner() && oldest.job_id() == entry.job_id()),
            "a younger row bypassed its owner-scoped FIFO head"
        );
    }
    let reservation_counts =
        reservations
            .iter()
            .fold(BTreeMap::new(), |mut counts, (_, worker)| {
                *counts.entry(worker.as_str()).or_insert(0_usize) += 1;
                counts
            });
    assert_eq!(
        reservation_counts,
        BTreeMap::from([("mini-a", 1), ("mini-b", 1), ("mini-c", 1)]),
        "one worker must never hold two queue reservations"
    );
    assert_eq!(
        reservations
            .iter()
            .map(|(entry, _)| entry.job_id().to_string())
            .collect::<BTreeSet<_>>()
            .len(),
        3,
        "claim events must retain three distinct FIFO heads"
    );

    let host_roots = ["mini-a", "mini-b", "mini-c"]
        .into_iter()
        .map(|worker| {
            (
                worker,
                Arc::new(HostStore::open(&directory.path().join("hosts").join(worker)).unwrap()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let lease_threads = reservations
        .into_iter()
        .enumerate()
        .map(|(index, (entry, worker))| {
            let host = host_roots.get(worker.as_str()).unwrap().clone();
            let request = lease_request(&entry, &worker, index as u128);
            thread::spawn(move || {
                let response =
                    LeaseService::new(&host).acquire(&request, &healthy_admission(), 101);
                (worker, entry.job_id(), response)
            })
        })
        .collect::<Vec<_>>();
    let leases = lease_threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    for (worker, job_id, response) in leases {
        let LeaseAcquireResponse::Acquired { lease } = response.unwrap() else {
            panic!("a selected worker must durably acquire one lease");
        };
        assert_eq!(lease.job_id(), job_id);
        assert_eq!(lease.worker_name(), worker);
        let durable = LeaseService::new(host_roots.get(worker.as_str()).unwrap())
            .load()
            .unwrap()
            .expect("selected worker must retain its one lease");
        assert_eq!(durable, lease);
    }
}

#[test]
fn queue_publication_and_claim_interleavings_preserve_fifo_reservations() {
    // Break caught: publication outside the queue lock lets a later claimant
    // observe an unsequenced row or reserve one worker twice.
    for schedule in 0..100_u128 {
        let (_directory, store, entered, release) =
            gated_store(ClientStateConcurrencyPoint::QueuePublication);
        let first_owner = owner(20_000 + schedule as u32 * 2);
        let second_owner = owner(20_001 + schedule as u32 * 2);
        let first_store = Arc::clone(&store);
        let first = thread::spawn(move || {
            first_store.enqueue(queued(
                &first_store,
                10_000 + schedule * 2,
                first_owner,
                WorkerPreference::Automatic,
                None,
            ))
        });
        entered.recv().unwrap();
        let second_store = Arc::clone(&store);
        let second = thread::spawn(move || {
            second_store.enqueue(queued(
                &second_store,
                10_001 + schedule * 2,
                second_owner,
                WorkerPreference::Automatic,
                None,
            ))
        });
        release.send(()).unwrap();
        let first = first.join().unwrap().unwrap();
        let second = second.join().unwrap().unwrap();

        let snapshot = store.queue_snapshot().unwrap();
        assert_eq!(snapshot.entries(), &[first.clone(), second.clone()]);
        let first_claim = store
            .claim_next(first_owner, &["mini-a".into()], 101)
            .unwrap()
            .expect("first published row claims its worker");
        let second_claim = store
            .claim_next(second_owner, &["mini-b".into()], 101)
            .unwrap()
            .expect("second published row claims another worker");
        assert_eq!(first_claim.entry().job_id(), first.job_id());
        assert_eq!(second_claim.entry().job_id(), second.job_id());
    }
}

#[test]
fn enqueue_claim_handoff_matrix_preserves_the_published_fifo_head() {
    // Break caught: a claimant can observe a queue row before its canonical
    // publication, bypass the head, or claim a different worker after the
    // publisher hands off the queue lock.
    let mut schedule_counts = [0usize; 4];
    for case in 0..100_u128 {
        let schedule = (case % 4) as usize;
        schedule_counts[schedule] += 1;
        let (_directory, store, entered, release) =
            gated_store(ClientStateConcurrencyPoint::QueuePublication);
        let row_owner = owner(30_000 + case as u32);
        let entry = queued(
            &store,
            30_000 + case,
            row_owner,
            WorkerPreference::Automatic,
            None,
        );
        let publisher_store = Arc::clone(&store);
        let publisher = thread::spawn(move || publisher_store.enqueue(entry));
        entered.recv().unwrap();

        let ranked_workers: Vec<String> = if case % 2 == 0 {
            vec!["mini-a".into(), "mini-b".into()]
        } else {
            vec!["mini-b".into(), "mini-a".into()]
        };
        let expected_worker = ranked_workers[0].clone();
        let spawn_claim = || {
            let claimant_store = Arc::clone(&store);
            let claimant_workers = ranked_workers.clone();
            thread::spawn(move || claimant_store.claim_next(row_owner, &claimant_workers, 101))
        };
        let mut claimers = Vec::new();
        match schedule {
            0 => {
                claimers.push(spawn_claim());
                release.send(()).unwrap();
            }
            1 => {
                release.send(()).unwrap();
                claimers.push(spawn_claim());
            }
            2 => {
                claimers.push(spawn_claim());
                claimers.push(spawn_claim());
                release.send(()).unwrap();
            }
            3 => {
                release.send(()).unwrap();
                claimers.push(spawn_claim());
                claimers.push(spawn_claim());
            }
            _ => unreachable!(),
        }

        let published = publisher.join().unwrap().unwrap();
        let claims = claimers
            .into_iter()
            .filter_map(|claimant| claimant.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(claims.len(), 1, "case {case} one claimant must win");
        let claim = claims.into_iter().next().unwrap();
        let QueueState::Dispatching {
            selected_worker, ..
        } = claim.entry().state()
        else {
            panic!("published row must be dispatching after the claim");
        };
        assert_eq!(claim.entry().job_id(), published.job_id(), "case {case}");
        assert_eq!(selected_worker, &expected_worker, "case {case}");
        assert_eq!(
            store.queue_snapshot().unwrap().entries()[0].job_id(),
            published.job_id(),
            "case {case} retained the FIFO head"
        );
    }
    assert_eq!(schedule_counts, [25, 25, 25, 25]);
}

#[test]
fn claim_lease_handoff_matrix_persists_exactly_one_authoritative_lease() {
    // Break caught: a claim can be handed to a lease boundary using a stale
    // cap decision, or the lease identity can diverge from the queue winner.
    let mut schedule_counts = [0usize; 4];
    for case in 0..100_u128 {
        let schedule = (case % 4) as usize;
        schedule_counts[schedule] += 1;
        let (directory, store, entered, release) =
            gated_store(ClientStateConcurrencyPoint::ClaimRunCapEvaluation);
        let row_owner = owner(31_000 + case as u32);
        let entry = queued(
            &store,
            31_000 + case,
            row_owner,
            WorkerPreference::Pinned {
                worker: "mini-a".into(),
            },
            None,
        );
        let published = store.enqueue(entry).unwrap();
        let host = Arc::new(HostStore::open(&directory.path().join("host")).unwrap());
        let request = lease_request(&published, "mini-a", case + 1_000);
        let claimant_store = Arc::clone(&store);
        let claimant =
            thread::spawn(move || claimant_store.claim_next(row_owner, &["mini-a".into()], 101));
        entered.recv().unwrap();

        let (start_lease, wait_for_lease) = mpsc::channel();
        let lease_host = Arc::clone(&host);
        let lease = thread::spawn(move || {
            wait_for_lease.recv().unwrap();
            LeaseService::new(&lease_host).acquire(&request, &healthy_admission(), 101)
        });
        let mut lease = Some(lease);
        let mut claimant = Some(claimant);
        let mut claim_result = None;
        let lease_result = match schedule {
            0 => {
                start_lease.send(()).unwrap();
                release.send(()).unwrap();
                None
            }
            1 => {
                release.send(()).unwrap();
                start_lease.send(()).unwrap();
                None
            }
            2 => {
                release.send(()).unwrap();
                claim_result = Some(claimant.take().unwrap().join().unwrap());
                start_lease.send(()).unwrap();
                None
            }
            3 => {
                start_lease.send(()).unwrap();
                release.send(()).unwrap();
                Some(lease.take().unwrap().join().unwrap())
            }
            _ => unreachable!(),
        };
        let claim = claim_result
            .unwrap_or_else(|| claimant.take().unwrap().join().unwrap())
            .unwrap()
            .expect("the pinned row must be claimed");
        let lease_response = lease_result.unwrap_or_else(|| lease.take().unwrap().join().unwrap());
        let LeaseAcquireResponse::Acquired { lease } = lease_response.unwrap() else {
            panic!("the selected worker must accept the exact lease request");
        };
        assert_eq!(claim.entry().job_id(), lease.job_id(), "case {case}");
        assert_eq!(lease.worker_name(), "mini-a", "case {case}");
        assert_eq!(
            LeaseService::new(&host).load().unwrap(),
            Some(lease),
            "case {case} persisted the authoritative lease"
        );
    }
    assert_eq!(schedule_counts, [25, 25, 25, 25]);
}

#[test]
fn cancel_claim_handoff_matrix_has_one_queue_terminal_outcome() {
    // Break caught: cancellation removes a row after it has been claimed,
    // claims a row after cancellation removed it, or loses the exact row
    // identity at the claim/cancel boundary.
    let mut schedule_counts = [0usize; 4];
    for case in 0..100_u128 {
        let schedule = (case % 4) as usize;
        schedule_counts[schedule] += 1;
        let (_directory, store, entered, release) =
            gated_store(ClientStateConcurrencyPoint::ClaimRunCapEvaluation);
        let row_owner = owner(32_000 + case as u32);
        let entry = store
            .enqueue(queued(
                &store,
                32_000 + case,
                row_owner,
                WorkerPreference::Pinned {
                    worker: "mini-a".into(),
                },
                None,
            ))
            .unwrap();
        let job_id = entry.job_id();
        let claimant_store = Arc::clone(&store);
        let claimant =
            thread::spawn(move || claimant_store.claim_next(row_owner, &["mini-a".into()], 101));
        entered.recv().unwrap();

        let (start_cancel, wait_for_cancel) = mpsc::channel();
        let cancel_store = Arc::clone(&store);
        let canceller = thread::spawn(move || {
            wait_for_cancel.recv().unwrap();
            cancel_store.request_queue_cancel(job_id, 102)
        });
        let mut claimant = Some(claimant);
        let mut canceller = Some(canceller);
        let mut claim_result = None;
        let mut cancellation_result = None;
        match schedule {
            0 => {
                start_cancel.send(()).unwrap();
                release.send(()).unwrap();
            }
            1 => {
                release.send(()).unwrap();
                start_cancel.send(()).unwrap();
            }
            2 => {
                start_cancel.send(()).unwrap();
                release.send(()).unwrap();
                cancellation_result = Some(canceller.take().unwrap().join().unwrap());
            }
            3 => {
                release.send(()).unwrap();
                start_cancel.send(()).unwrap();
                claim_result = Some(claimant.take().unwrap().join().unwrap());
            }
            _ => unreachable!(),
        }

        let claim = claim_result
            .unwrap_or_else(|| claimant.take().unwrap().join().unwrap())
            .unwrap();
        let cancellation = cancellation_result
            .unwrap_or_else(|| canceller.take().unwrap().join().unwrap())
            .unwrap();
        match (claim, cancellation) {
            (
                Some(claim),
                Some(mac_worker::job::QueueCancel::RequestedDispatch { job_id: id, .. }),
            ) => {
                assert_eq!(id, job_id, "case {case}");
                assert!(matches!(
                    claim.entry().state(),
                    QueueState::Dispatching { .. }
                ));
            }
            (None, Some(mac_worker::job::QueueCancel::RemovedWaiting { job_id: id })) => {
                assert_eq!(id, job_id, "case {case}");
            }
            (claim, cancellation) => panic!(
                "case {case} produced mismatched claim/cancel outcomes: {claim:?}/{cancellation:?}"
            ),
        }
        assert!(
            store
                .queue_snapshot()
                .unwrap()
                .entries()
                .iter()
                .all(|entry| entry.job_id() != job_id || entry.is_cancel_requested()),
            "case {case} retained an uncancelled live row"
        );
    }
    assert_eq!(schedule_counts, [25, 25, 25, 25]);
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

    for schedule in 0..100 {
        let (directory, store, entered, release) =
            gated_store(ClientStateConcurrencyPoint::ObservationRefreshPublication);
        let refreshes = Arc::new(AtomicUsize::new(0));
        let first_store = Arc::clone(&store);
        let first_refreshes = Arc::clone(&refreshes);
        let first = thread::spawn(move || {
            first_store.admission_observation("mini-a", 10_000 + schedule, || {
                first_refreshes.fetch_add(1, Ordering::SeqCst);
                AdmissionObservation::new(
                    "mini-a".into(),
                    true,
                    CandidateSlot::Idle,
                    Vec::new(),
                    Some(10),
                    20,
                    10_000 + schedule,
                )
            })
        });
        entered.recv().unwrap();
        let second_store = Arc::clone(&store);
        let second_refreshes = Arc::clone(&refreshes);
        let second = thread::spawn(move || {
            second_store.admission_observation("mini-a", 10_000 + schedule, || {
                second_refreshes.fetch_add(1, Ordering::SeqCst);
                AdmissionObservation::new(
                    "mini-a".into(),
                    true,
                    CandidateSlot::Idle,
                    Vec::new(),
                    Some(10),
                    20,
                    10_000 + schedule,
                )
            })
        });
        release.send(()).unwrap();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        assert_eq!(refreshes.load(Ordering::SeqCst), 1, "schedule {schedule}");
        let bytes = std::fs::read(directory.path().join("state/observations/mini-a.json")).unwrap();
        let observation: AdmissionObservation = serde_json::from_slice(&bytes).unwrap();
        let mut canonical = serde_json::to_vec(&observation).unwrap();
        canonical.push(b'\n');
        assert_eq!(
            bytes, canonical,
            "schedule {schedule} publishes canonical bytes"
        );
    }
}
