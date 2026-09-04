#[allow(dead_code)]
mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use mac_worker::{
    client_state::{ClientStateConcurrencyHook, ClientStateConcurrencyPoint, ClientStateStore},
    config::{Config, WorkerEntry},
    error::WorkerError,
    host_store::HostStore,
    job::{
        AdmissionObservation, CommandSpec, HostControlError, JobId, JobMeta, JobStatus,
        LeaseAcquireRequest, LeaseAcquireResponse, LeaseToken, LocalJobRecord, ProcessIdentity,
        QueueEntry, QueueEntryKind, QueueRunReference, QueueState, RequestFingerprintMaterial,
        RunId, StatusResponse, SubmitRequest, SubmitResponse,
    },
    lease::{AdmissionFacts, LeaseService, SlotState},
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    protocol::{CpuCounters, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    remote_snapshot::{SnapshotVerifyRequest, VerifiedSnapshotResponse},
    run::{JobFollower, RunObserver, RunRequest, RunService, RunStage},
    scheduler::{CandidateSlot, WorkerPreference},
    transfer::HostOperation,
};
use support::GitRepo;

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

const PRODUCTION_RSYNC_STATS: &[u8] = b"Number of files: 2\nNumber of files transferred: 1\nTotal file size: 8 B\nTotal transferred file size: 8 B\nUnmatched data: 8 B\nMatched data: 0 B\nFile list size: 64 B\nTotal sent: 128 B\nTotal received: 32 B\n\nsent 128 bytes  received 32 bytes  1000 bytes/sec\ntotal size is 8  speedup is 0.05\n";

fn canonical_process(value: &impl serde::Serialize) -> Result<ProcessResult, WorkerError> {
    let mut stdout = serde_json::to_vec(value).map_err(|error| {
        WorkerError::Protocol(format!("test response serialization failed: {error}"))
    })?;
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })
}

fn host_error_process(code: &'static str, message: String) -> Result<ProcessResult, WorkerError> {
    let mut result = canonical_process(&HostControlError::new(code, message)?)?;
    result.status = ExitStatus::from_raw(23 << 8);
    Ok(result)
}

struct ProductionLeaseRunner {
    host: HostStore,
    lease_acquires: AtomicUsize,
}

impl ProductionLeaseRunner {
    fn new(host: HostStore) -> Self {
        Self {
            host,
            lease_acquires: AtomicUsize::new(0),
        }
    }

    fn lease_acquires(&self) -> usize {
        self.lease_acquires.load(Ordering::SeqCst)
    }
}

impl ProcessRunner for ProductionLeaseRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            return SystemProcessRunner.run(request);
        }
        if request.program == OsStr::new("/usr/bin/rsync") {
            return Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: PRODUCTION_RSYNC_STATS.to_vec(),
                stderr: Vec::new(),
            });
        }

        let operation = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .unwrap_or_default();
        match operation {
            "~/.local/bin/worker host probe" => canonical_process(&ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: SUPERVISION_VERSION,
                hostname: "scheduler-test-host".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 100 * 1024 * 1024 * 1024,
                total_disk_bytes: 250 * 1024 * 1024 * 1024,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(0),
                available_memory_bytes: Some(12 * 1024 * 1024 * 1024),
                cpu_counters: Some(CpuCounters {
                    user_ticks: 10,
                    system_ticks: 20,
                    idle_ticks: 30,
                    nice_ticks: 40,
                }),
                slot_state: SlotState::Idle,
                active_lease: None,
                capabilities: Vec::new(),
            }),
            value if value == HostOperation::LeaseAcquire.command() => {
                let bytes = request.stdin.as_deref().ok_or_else(|| {
                    WorkerError::Protocol(
                        "lease acquire request was missing in test transport".into(),
                    )
                })?;
                let acquire: LeaseAcquireRequest =
                    serde_json::from_slice(bytes).map_err(|error| {
                        WorkerError::Protocol(format!("invalid lease request: {error}"))
                    })?;
                self.lease_acquires.fetch_add(1, Ordering::SeqCst);
                match LeaseService::new(&self.host).acquire(
                    &acquire,
                    &healthy_admission(),
                    acquire.material().created_at_millis(),
                ) {
                    Ok(response) => canonical_process(&response),
                    Err(WorkerError::Capacity { code, message }) => {
                        host_error_process(code, message)
                    }
                    Err(error) => Err(error),
                }
            }
            value if value == HostOperation::SnapshotVerify.command() => {
                let bytes = request.stdin.as_deref().ok_or_else(|| {
                    WorkerError::Protocol(
                        "snapshot verify request was missing in test transport".into(),
                    )
                })?;
                let verify: SnapshotVerifyRequest =
                    serde_json::from_slice(bytes).map_err(|error| {
                        WorkerError::Protocol(format!("invalid snapshot verify request: {error}"))
                    })?;
                canonical_process(
                    &VerifiedSnapshotResponse::new(
                        verify.job_id(),
                        verify.client_id(),
                        verify.project_id().into(),
                        verify.worktree_id().into(),
                        verify.manifest_digest().into(),
                        102,
                        false,
                    )
                    .unwrap(),
                )
            }
            value if value == HostOperation::Submit.command() => {
                let bytes = request.stdin.as_deref().ok_or_else(|| {
                    WorkerError::Protocol("submit request was missing in test transport".into())
                })?;
                let submit: SubmitRequest = serde_json::from_slice(bytes).map_err(|error| {
                    WorkerError::Protocol(format!("invalid submit request: {error}"))
                })?;
                let meta = JobMeta::new(submit.material(), submit.request_fingerprint().clone())?;
                canonical_process(&SubmitResponse::Accepted {
                    meta: Box::new(meta),
                    status: JobStatus::accepted(submit.material().created_at_millis() + 1)?,
                })
            }
            _ => panic!("unexpected production-path test request: {request:?}"),
        }
    }
}

struct DispatchHandoffGate {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    hits: AtomicUsize,
}

impl DispatchHandoffGate {
    fn new(entered: mpsc::Sender<()>, release: mpsc::Receiver<()>) -> Self {
        Self {
            entered,
            release: Mutex::new(release),
            hits: AtomicUsize::new(0),
        }
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

impl RunObserver for DispatchHandoffGate {
    fn observe(&self, stage: RunStage) -> Result<(), WorkerError> {
        if stage == RunStage::DispatchToLeaseHandoff {
            self.hits.fetch_add(1, Ordering::SeqCst);
            self.entered.send(()).map_err(|_| {
                WorkerError::Protocol("production dispatch handoff receiver disappeared".into())
            })?;
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| {
                    WorkerError::Protocol("production dispatch handoff timed out".into())
                })?;
        }
        Ok(())
    }
}

struct SuccessfulRunFollower;

impl JobFollower for SuccessfulRunFollower {
    fn follow(
        &self,
        record: &LocalJobRecord,
        _json: bool,
        _stdout: &mut dyn std::io::Write,
        _stderr: &mut dyn std::io::Write,
    ) -> Result<StatusResponse, WorkerError> {
        StatusResponse::new(
            record.meta().clone(),
            JobStatus::succeeded(record.meta().created_at_millis() + 2, 0, 0)?,
        )
    }
}

fn production_run_repo() -> GitRepo {
    let repo = GitRepo::init();
    repo.write(".worker.toml", b"version = 1\n");
    repo.write("tracked.txt", b"tracked\n");
    repo.commit_all("scheduler production path fixture");
    repo
}

fn production_run_paths(temp: &tempfile::TempDir) -> PathLayout {
    let root = temp.path().canonicalize().unwrap();
    PathLayout {
        config: root.join("config.toml"),
        state: root.join("state"),
        cache: root.join("cache"),
        data: root.join("data"),
    }
}

fn production_run_config() -> Config {
    Config {
        version: 1,
        workers: vec![WorkerEntry {
            name: "mini-a".into(),
            ssh: "mini-a".into(),
            slots: 1,
            capabilities: Vec::new(),
            remote_binary: "~/.local/bin/worker".into(),
        }],
    }
}

fn production_run_request(repo: &GitRepo) -> RunRequest {
    RunRequest {
        preference: WorkerPreference::Pinned {
            worker: "mini-a".into(),
        },
        wait_for_capacity: false,
        project: repo.root().to_path_buf(),
        cli_includes: Vec::new(),
        timeout: None,
        command: CommandSpec::argv(vec!["scheduler-production-path".into()]).unwrap(),
    }
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
    let lease_attempts = reservations
        .into_iter()
        .enumerate()
        .flat_map(|(index, (entry, worker))| {
            let competing = queued(
                &store,
                50_000 + index as u128,
                owner(50_000 + index as u32),
                WorkerPreference::Pinned {
                    worker: worker.clone(),
                },
                None,
            );
            vec![
                (
                    format!("{worker}-queue-head"),
                    worker.clone(),
                    true,
                    entry.job_id(),
                    lease_request(&entry, &worker, index as u128),
                ),
                (
                    format!("{worker}-competing-claimant"),
                    worker.clone(),
                    false,
                    competing.job_id(),
                    lease_request(&competing, &worker, 10_000 + index as u128),
                ),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(lease_attempts.len(), 6);
    let lease_barrier = Arc::new(Barrier::new(lease_attempts.len() + 1));
    let lease_threads = lease_attempts
        .into_iter()
        .map(|(label, worker, from_queue, job_id, request)| {
            let host = host_roots.get(worker.as_str()).unwrap().clone();
            let barrier = Arc::clone(&lease_barrier);
            thread::spawn(move || {
                barrier.wait();
                let response =
                    LeaseService::new(&host).acquire(&request, &healthy_admission(), 101);
                (label, worker, from_queue, job_id, response)
            })
        })
        .collect::<Vec<_>>();
    lease_barrier.wait();
    let lease_results = lease_threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lease_results.len(), 6);
    let attempts_per_worker = lease_results.iter().fold(
        BTreeMap::<&str, usize>::new(),
        |mut counts, (_, worker, _, _, _)| {
            *counts.entry(worker.as_str()).or_insert(0) += 1;
            counts
        },
    );
    assert_eq!(
        attempts_per_worker,
        BTreeMap::from([("mini-a", 2), ("mini-b", 2), ("mini-c", 2)]),
        "every selected worker must have two concurrent authoritative claimants"
    );

    for worker in ["mini-a", "mini-b", "mini-c"] {
        let attempts = lease_results
            .iter()
            .filter(|(_, attempt_worker, _, _, _)| attempt_worker == worker)
            .collect::<Vec<_>>();
        assert_eq!(attempts.len(), 2);
        let acquired = attempts
            .iter()
            .filter_map(|(_, _, _, _, result)| match result {
                Ok(LeaseAcquireResponse::Acquired { lease }) => Some(lease),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            acquired.len(),
            1,
            "worker {worker} must durably grant exactly one lease"
        );
        assert_eq!(
            attempts
                .iter()
                .filter(|(_, _, _, _, result)| matches!(
                    result,
                    Err(WorkerError::Capacity {
                        code: "CAPACITY_BUSY",
                        ..
                    })
                ))
                .count(),
            1,
            "worker {worker} must return typed CAPACITY_BUSY to its losing claimant"
        );
        let lease = acquired[0];
        assert_eq!(lease.worker_name(), worker);
        let durable = LeaseService::new(host_roots.get(worker).unwrap())
            .load()
            .unwrap()
            .expect("selected worker must retain its one authoritative lease");
        assert_eq!(&durable, lease);
        assert!(
            attempts
                .iter()
                .any(|(_, _, _, job_id, _)| *job_id == lease.job_id()),
            "the durable lease must belong to one retained claimant"
        );
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
fn claim_lease_handoff_matrix_uses_production_run_dispatch() {
    // Break caught: a QueueState::Dispatching claim can be decoupled from the
    // RunService lease transfer, allowing a direct test-only lease acquire to
    // pass while production dispatch never reaches the authoritative host.
    let repo = production_run_repo();
    let mut schedule_counts = [0usize; 4];
    for case in 0..100_u128 {
        let schedule = (case % 4) as usize;
        schedule_counts[schedule] += 1;
        let temp = tempfile::tempdir().unwrap();
        let paths = production_run_paths(&temp);
        let store = Arc::new(ClientStateStore::open(&paths.state).unwrap());
        let host = Arc::new(HostStore::open(&paths.data.join("host")).unwrap());
        let runner = Arc::new(ProductionLeaseRunner::new((*host).clone()));
        let config = production_run_config();
        let follower = SuccessfulRunFollower;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let observer = Arc::new(DispatchHandoffGate::new(entered_tx, release_rx));
        let run_store = Arc::clone(&store);
        let run_runner = Arc::clone(&runner);
        let run_observer = Arc::clone(&observer);
        let run_paths = paths.clone();
        let run_config = config.clone();
        let run_request = production_run_request(&repo);
        let run = thread::spawn(move || {
            let service = RunService::with_follower(
                &*run_runner,
                &run_config,
                &run_paths,
                &run_store,
                &follower,
            )
            .with_observer(&*run_observer);
            service.submit_and_follow(run_request, false, &mut Vec::new(), &mut Vec::new())
        });

        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("case {case} never reached dispatch-to-lease handoff"));
        let before = store.queue_snapshot().unwrap();
        assert_eq!(before.entries().len(), 1, "case {case}");
        let entry = &before.entries()[0];
        let job_id = entry.job_id();
        assert!(matches!(
            entry.state(),
            QueueState::Dispatching { selected_worker, .. } if selected_worker == "mini-a"
        ));
        assert!(
            store.load_job(job_id).is_ok(),
            "case {case} local record exists"
        );
        assert_eq!(
            LeaseService::new(&host).load().unwrap(),
            None,
            "case {case}"
        );

        match schedule {
            0 => {
                assert!(matches!(
                    store.queue_snapshot().unwrap().entries()[0].state(),
                    QueueState::Dispatching { .. }
                ));
                release_tx.send(()).unwrap();
            }
            1 => {
                release_tx.send(()).unwrap();
                let _ = store.queue_snapshot().unwrap();
            }
            2 => {
                let reader_store = Arc::clone(&store);
                let reader_host = Arc::clone(&host);
                let (reader_started_tx, reader_started_rx) = mpsc::channel();
                let (reader_done_tx, reader_done_rx) = mpsc::channel();
                let reader = thread::spawn(move || {
                    reader_started_tx.send(()).unwrap();
                    let snapshot = reader_store.queue_snapshot().unwrap();
                    let lease = LeaseService::new(&reader_host).load().unwrap();
                    reader_done_tx.send((snapshot, lease)).unwrap();
                });
                reader_started_rx.recv().unwrap();
                let (snapshot, lease) = reader_done_rx.recv().unwrap();
                assert!(matches!(
                    snapshot.entries()[0].state(),
                    QueueState::Dispatching { .. }
                ));
                assert_eq!(lease, None, "case {case} reader precedes lease transfer");
                release_tx.send(()).unwrap();
                reader.join().unwrap();
            }
            3 => {
                release_tx.send(()).unwrap();
                let reader_store = Arc::clone(&store);
                let reader = thread::spawn(move || reader_store.queue_snapshot().unwrap());
                let _ = reader.join().unwrap();
            }
            _ => unreachable!(),
        }

        let completion = run
            .join()
            .unwrap()
            .unwrap_or_else(|error| panic!("case {case} production dispatch failed: {error}"));
        assert_eq!(
            observer.hits(),
            1,
            "case {case} named handoff is reached once"
        );
        assert_eq!(runner.lease_acquires(), 1, "case {case}");
        assert_eq!(completion.report.job_id, job_id, "case {case}");
        assert_eq!(completion.report.worker, "mini-a", "case {case}");
        assert_eq!(
            completion.report.status.state(),
            mac_worker::job::JobState::Succeeded,
            "case {case} terminal outcome follows authoritative lease"
        );
        let live = LeaseService::new(&host)
            .load()
            .unwrap()
            .expect("case must retain the one authoritative host lease");
        assert_eq!(live.job_id(), job_id, "case {case}");
        assert_eq!(live.worker_name(), "mini-a", "case {case}");
        assert!(
            store.queue_snapshot().unwrap().entries().is_empty(),
            "case {case}"
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
