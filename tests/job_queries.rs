use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Cursor, Write as _},
    os::fd::AsRawFd,
    os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    host_store::{HostStore, HostStoreWritePoint, SupervisorGuard},
    job::{
        ClientId, CommandSpec, HostControlError, JobId, JobState, JobStatus, LeaseAcquireRequest,
        LeaseAcquireResponse, LeaseRecord, LeaseToken, LogChunk, LogChunkRequest, LogChunkResponse,
        LogCursor, LogStream, ProcessIdentity, RequestFingerprintMaterial, ResolveOrAbandonOutcome,
        ResolveOrAbandonRequest, StatusRequest, StatusResponse, SubmitRequest, TerminalLogDrain,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    lease::{AdmissionFacts, LeaseService},
    paths::PathLayout,
    process::SystemProcessRunner,
    protocol::MemoryPressure,
    remote_snapshot::RemoteSnapshotService,
    run_with_stdio_in_context,
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
        ReconciliationRuntime, Supervisor, SystemProcessInspector,
    },
    transfer::HostTransferService,
};
use sha2::{Digest, Sha256};

const JOB_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const CLIENT_ID: &str = "102f0f4a6b5c7d8e9f00112233445566";
const LEASE_TOKEN: &str = "202f0f4a6b5c7d8e9f00112233445566";
const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MANIFEST_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

struct RejectLauncher;

struct HoldingRecordingLauncher {
    launches: Arc<AtomicUsize>,
    job_path: PathBuf,
    identity: ProcessIdentity,
    guard: Mutex<Option<SupervisorGuard>>,
}

struct InlineSupervisorLauncher {
    store: HostStore,
}

struct BlockingLauncher {
    launches: Arc<AtomicUsize>,
    job_path: PathBuf,
    identity: ProcessIdentity,
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

struct CountingRejectLauncher {
    launches: Arc<AtomicUsize>,
}

struct PrelaunchLostLauncher {
    launches: Arc<AtomicUsize>,
    job_path: PathBuf,
    identity: ProcessIdentity,
}

struct SpoofedPrelaunchFailureLauncher {
    launches: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct ScriptedReconciliation {
    inner: Arc<ScriptedReconciliationInner>,
}

struct ScriptedReconciliationInner {
    process: Mutex<VecDeque<ProcessObservation>>,
    groups: Mutex<VecDeque<ProcessGroupObservation>>,
    signals: Mutex<Vec<(u32, i32)>>,
    sleeps: Mutex<Vec<Duration>>,
    now: Mutex<Duration>,
    clock: Mutex<Option<VecDeque<Duration>>>,
    first_observation_gate: Mutex<Option<FirstObservationGate>>,
}

struct FirstObservationGate {
    start_contender: mpsc::Sender<()>,
    contender_queued: mpsc::Receiver<()>,
}

impl ScriptedReconciliation {
    fn new(
        process: impl IntoIterator<Item = ProcessObservation>,
        groups: impl IntoIterator<Item = ProcessGroupObservation>,
    ) -> Self {
        Self {
            inner: Arc::new(ScriptedReconciliationInner {
                process: Mutex::new(process.into_iter().collect()),
                groups: Mutex::new(groups.into_iter().collect()),
                signals: Mutex::new(Vec::new()),
                sleeps: Mutex::new(Vec::new()),
                now: Mutex::new(Duration::ZERO),
                clock: Mutex::new(None),
                first_observation_gate: Mutex::new(None),
            }),
        }
    }

    fn with_clock(self, clock: impl IntoIterator<Item = Duration>) -> Self {
        *self.inner.clock.lock().unwrap() = Some(clock.into_iter().collect());
        self
    }

    fn with_first_observation_gate(self, gate: FirstObservationGate) -> Self {
        *self.inner.first_observation_gate.lock().unwrap() = Some(gate);
        self
    }

    fn signals(&self) -> Vec<(u32, i32)> {
        self.inner.signals.lock().unwrap().clone()
    }

    fn sleeps(&self) -> Vec<Duration> {
        self.inner.sleeps.lock().unwrap().clone()
    }
}

impl ProcessInspector for ScriptedReconciliation {
    fn identity_for_pid(
        &self,
        _pid: u32,
    ) -> Result<ProcessIdentity, mac_worker::error::WorkerError> {
        panic!("orphan reconciliation must never derive a new process identity")
    }

    fn observe(&self, _expected: ProcessIdentity) -> ProcessObservation {
        if let Some(gate) = self.inner.first_observation_gate.lock().unwrap().take() {
            gate.start_contender.send(()).unwrap();
            gate.contender_queued
                .recv_timeout(Duration::from_secs(2))
                .expect("supervisor contender did not queue");
            thread::sleep(Duration::from_millis(50));
        }
        self.inner
            .process
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected process observation")
    }

    fn observe_group(&self, _process_group: u32) -> ProcessGroupObservation {
        self.inner
            .groups
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected process-group observation")
    }

    fn observe_group_members(&self, _leader: u32) -> ProcessGroupMembership {
        panic!("parent-independent reconciliation must not use parent wait anchors")
    }
}

impl ReconciliationRuntime for ScriptedReconciliation {
    fn signal_process_group(
        &self,
        process_group: u32,
        signal: i32,
    ) -> Result<(), mac_worker::error::WorkerError> {
        self.inner
            .signals
            .lock()
            .unwrap()
            .push((process_group, signal));
        Ok(())
    }

    fn monotonic_now(&self) -> Duration {
        if let Some(clock) = self.inner.clock.lock().unwrap().as_mut() {
            return clock.pop_front().expect("unexpected monotonic clock read");
        }
        *self.inner.now.lock().unwrap()
    }

    fn sleep(&self, duration: Duration) {
        self.inner.sleeps.lock().unwrap().push(duration);
        *self.inner.now.lock().unwrap() += duration;
    }
}

fn queue_supervisor_lock_contender(
    root: &Path,
    job_id: JobId,
) -> (
    FirstObservationGate,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
    thread::JoinHandle<()>,
) {
    let (start_tx, start_rx) = mpsc::channel();
    let (queued_tx, queued_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let lock = root
        .join("locks/jobs")
        .join(job_id.to_string())
        .join("supervisor/supervisor.lock");
    let contender = thread::spawn(move || {
        start_rx.recv().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock)
            .unwrap();
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(result, -1, "status did not hold the supervisor lock");
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EWOULDBLOCK)
        );
        queued_tx.send(()).unwrap();
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) }, 0);
        acquired_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) }, 0);
    });
    (
        FirstObservationGate {
            start_contender: start_tx,
            contender_queued: queued_rx,
        },
        acquired_rx,
        release_tx,
        contender,
    )
}

impl SupervisorLauncher for RejectLauncher {
    fn launch(
        &self,
        _job_id: JobId,
        _guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        panic!("a missing job must not launch a supervisor")
    }
}

impl SupervisorLauncher for HoldingRecordingLauncher {
    fn launch(
        &self,
        _job_id: JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        let current: JobStatus =
            serde_json::from_slice(&fs::read(self.job_path.join("status.json"))?)
                .map_err(|error| mac_worker::error::WorkerError::Protocol(error.to_string()))?;
        let enriched = current.with_supervisor(self.identity, current.updated_at_millis() + 1)?;
        replace_json(&self.job_path.join("status.json"), &enriched)?;
        *self.guard.lock().unwrap() = Some(guard);
        Ok(LaunchCandidate::new(self.identity))
    }
}

impl SupervisorLauncher for InlineSupervisorLauncher {
    fn launch(
        &self,
        job_id: JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(std::process::id())?;
        Supervisor::new(&self.store, &inspector).run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

impl SupervisorLauncher for BlockingLauncher {
    fn launch(
        &self,
        _job_id: JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        self.entered.send(()).unwrap();
        self.release.lock().unwrap().recv().unwrap();
        let current: JobStatus =
            serde_json::from_slice(&fs::read(self.job_path.join("status.json"))?)
                .map_err(|error| mac_worker::error::WorkerError::Protocol(error.to_string()))?;
        let enriched = current.with_supervisor(self.identity, current.updated_at_millis() + 1)?;
        replace_json(&self.job_path.join("status.json"), &enriched)?;
        drop(guard);
        Ok(LaunchCandidate::new(self.identity))
    }
}

impl SupervisorLauncher for CountingRejectLauncher {
    fn launch(
        &self,
        _job_id: JobId,
        _guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        Err(mac_worker::error::WorkerError::Protocol(
            "unexpected second launch".into(),
        ))
    }
}

impl SupervisorLauncher for PrelaunchLostLauncher {
    fn launch(
        &self,
        _job_id: JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        let current: JobStatus =
            serde_json::from_slice(&fs::read(self.job_path.join("status.json"))?)
                .map_err(|error| mac_worker::error::WorkerError::Protocol(error.to_string()))?;
        let lost = current
            .with_supervisor(self.identity, current.updated_at_millis() + 1)?
            .into_infrastructure_terminal(
                JobState::Lost,
                current.updated_at_millis() + 2,
                0,
                0,
                "PRELAUNCH_FAILED".into(),
            )?;
        remove_and_sync(&self.job_path.join("execution.json"));
        replace_json(&self.job_path.join("status.json"), &lost)?;
        drop(guard);
        Ok(LaunchCandidate::new(self.identity))
    }
}

impl SupervisorLauncher for SpoofedPrelaunchFailureLauncher {
    fn launch(
        &self,
        _job_id: JobId,
        _guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        let attempt = self.launches.fetch_add(1, Ordering::SeqCst);
        let message = if attempt == 0 {
            "SUPERVISOR_PRELAUNCH_FAILED: synthetic launcher failure"
        } else {
            "SECOND_LAUNCH: status retried an uncommitted launch"
        };
        Err(mac_worker::error::WorkerError::Protocol(message.into()))
    }
}

#[test]
fn status_reports_a_missing_job_without_launching() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let job_id: JobId = JOB_ID.parse().unwrap();

    let error = JobService::new(&store, &RejectLauncher)
        .status(job_id)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_NOT_FOUND"), "{error}");
}

#[test]
fn status_reports_a_permanently_abandoned_job() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let request = lease_request();
    LeaseService::new(&store)
        .acquire(&request, &healthy(), 1)
        .unwrap();
    store.record_abandoned(&request, 2).unwrap();

    let error = JobService::new(&store, &RejectLauncher)
        .status(JOB_ID.parse().unwrap())
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ABANDONED"), "{error}");
}

#[test]
fn resolve_without_a_live_lease_fences_delayed_acquire_and_executes_nothing() {
    // Break caught: resolution treats status/lease absence as an ordinary miss
    // instead of durably fencing the same immutable request before returning.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    let acquire = lease_request();
    let submit = SubmitRequest::new(acquire.material().clone());
    let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
    let synthetic = LeaseRecord::new(
        acquire.material(),
        acquire.request_fingerprint().clone(),
        1,
        30_001,
    )
    .unwrap();
    let (classified_tx, classified_rx) = mpsc::channel();
    let (continue_tx, continue_rx) = mpsc::channel();
    let continue_rx = Arc::new(Mutex::new(continue_rx));
    let resolver_store = store.clone();
    let resolver_continue = Arc::clone(&continue_rx);
    let resolver = thread::spawn(move || {
        JobService::new_with_resolution_before_transfer(
            &resolver_store,
            &RejectLauncher,
            Arc::new(move || {
                classified_tx.send(()).unwrap();
                resolver_continue.lock().unwrap().recv().unwrap();
            }),
        )
        .resolve_or_abandon(request)
    });
    classified_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("resolver did not reach its pre-transfer decision boundary");
    let (attempting_tx, attempting_rx) = mpsc::channel();
    let acquire_store = store.clone();
    let delayed_request = acquire.clone();
    let acquire_attempting = attempting_tx.clone();
    let delayed = thread::spawn(move || {
        acquire_attempting.send("acquire").unwrap();
        LeaseService::new(&acquire_store).acquire(&delayed_request, &healthy(), 2)
    });
    let verify_store = store.clone();
    let verify_attempting = attempting_tx.clone();
    let delayed_verify = thread::spawn(move || {
        verify_attempting.send("verify").unwrap();
        RemoteSnapshotService::new(&verify_store).verify_and_promote_at(
            &synthetic,
            synthetic.manifest_digest(),
            3,
        )
    });
    let submit_store = store.clone();
    let submit_attempting = attempting_tx.clone();
    let submit_request = submit.clone();
    let launches = Arc::new(AtomicUsize::new(0));
    let delayed_launches = Arc::clone(&launches);
    let delayed_submit = thread::spawn(move || {
        submit_attempting.send("submit").unwrap();
        JobService::new(
            &submit_store,
            &CountingRejectLauncher {
                launches: delayed_launches,
            },
        )
        .submit_at(submit_request, 4)
    });
    drop(attempting_tx);
    let attempted = (0..3)
        .map(|_| attempting_rx.recv().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        attempted,
        ["acquire", "submit", "verify"].into_iter().collect()
    );
    continue_tx.send(()).unwrap();
    let response = resolver.join().unwrap().unwrap();

    assert!(
        matches!(response.outcome(), ResolveOrAbandonOutcome::Abandoned),
        "{response:?}"
    );
    assert!(
        store
            .job_index(acquire.material().job_id())
            .unwrap()
            .is_file()
    );
    let delayed = delayed.join().unwrap().unwrap_err();
    assert!(delayed.to_string().contains("JOB_ABANDONED"), "{delayed}");
    let verify_error = delayed_verify.join().unwrap().unwrap_err();
    assert!(
        verify_error.to_string().contains("JOB_ABANDONED")
            || verify_error.to_string().contains("live lease"),
        "{verify_error}"
    );
    let submit_error = delayed_submit.join().unwrap().unwrap_err();
    assert!(submit_error.to_string().contains("LEASE_MISSING"));
    assert_eq!(launches.load(Ordering::SeqCst), 0);
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn resolve_removes_exact_verified_state_keeps_cache_and_retires_the_lease() {
    // Break caught: abandonment proves only incoming cleanup, leaving the
    // exact verified receipt/staging or a live heavy lease behind.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, submit) = prepared_host(&root);
    let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
    let receipt = store.verified_receipt(lease.job_id()).unwrap();
    let cache = store
        .snapshot(
            lease.project_id(),
            lease.worktree_id(),
            lease.manifest_digest(),
        )
        .unwrap();
    assert!(receipt.is_file());
    assert!(cache.is_dir());

    let response = JobService::new(&store, &RejectLauncher)
        .resolve_or_abandon(request.clone())
        .unwrap();

    assert!(
        matches!(response.outcome(), ResolveOrAbandonOutcome::Abandoned),
        "{response:?}"
    );
    assert!(!receipt.exists());
    assert!(cache.is_dir(), "immutable cache must survive abandonment");
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    let retry = JobService::new(&store, &RejectLauncher)
        .resolve_or_abandon(request)
        .unwrap();
    assert!(matches!(
        retry.outcome(),
        ResolveOrAbandonOutcome::Abandoned
    ));
}

#[test]
fn resolve_release_failure_is_cleanup_pending_and_exact_retry_finishes() {
    // Break caught: a post-proof release failure is reported as abandonment,
    // drops the lease, or cannot resume from the durable tombstone.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    let acquire = lease_request();
    LeaseService::new(&store)
        .acquire(&acquire, &healthy(), 1)
        .unwrap();
    let submit = SubmitRequest::new(acquire.material().clone());
    let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
    drop(store);
    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::BeforeJobLeaseRetirement)
            .unwrap();

    let pending = JobService::new(&faulted, &RejectLauncher)
        .resolve_or_abandon(request.clone())
        .unwrap();
    assert!(matches!(
        pending.outcome(),
        ResolveOrAbandonOutcome::CleanupPending { code }
            if code == "LEASE_RELEASE_FAILED"
    ));
    assert!(
        faulted
            .job_index(acquire.material().job_id())
            .unwrap()
            .is_file()
    );
    assert!(LeaseService::new(&faulted).load().unwrap().is_some());
    drop(faulted);

    let reopened = HostStore::open(&root).unwrap();
    let completed = JobService::new(&reopened, &RejectLauncher)
        .resolve_or_abandon(request)
        .unwrap();
    assert!(matches!(
        completed.outcome(),
        ResolveOrAbandonOutcome::Abandoned
    ));
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), None);
}

#[test]
fn complete_preindex_accepted_job_wins_resolution_and_repairs_the_index() {
    // Break caught: a resolver deletes a complete accepted crash-window job
    // or tombstones it instead of repairing its permanent accepted locator.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, submit) = unindexed_identityless_job(&root);
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = HoldingRecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: identity(49_001),
        guard: Mutex::new(None),
    };

    let response = JobService::new(&store, &launcher)
        .resolve_or_abandon(ResolveOrAbandonRequest::from_submit_request(&submit).unwrap())
        .unwrap();

    assert!(
        matches!(response.outcome(), ResolveOrAbandonOutcome::Accepted { .. }),
        "{response:?}"
    );
    assert!(store.job_index(lease.job_id()).unwrap().is_file());
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    drop(launcher.guard.lock().unwrap().take());
}

#[test]
fn submit_and_supervisor_launch_first_make_resolution_accept_without_second_launch() {
    // Break caught: resolution ignores an accepted submit whose elected
    // supervisor is still launching, deletes its final job, or elects a second
    // launcher instead of returning the authoritative Accepted outcome.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, submit) = prepared_host(&root);
    let job_path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let launcher = Arc::new(BlockingLauncher {
        launches: Arc::clone(&launches),
        job_path: job_path.clone(),
        identity: identity(49_101),
        entered: entered_tx,
        release: Mutex::new(release_rx),
    });
    let submit_store = store.clone();
    let submit_request = submit.clone();
    let submit_launcher = Arc::clone(&launcher);
    let submit_thread = thread::spawn(move || {
        JobService::new(&submit_store, submit_launcher.as_ref()).submit_at(submit_request, 10)
    });
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("submit did not enter its elected supervisor launch");

    let resolver_launches = Arc::new(AtomicUsize::new(0));
    let resolution = JobService::new(
        &store,
        &CountingRejectLauncher {
            launches: Arc::clone(&resolver_launches),
        },
    )
    .resolve_or_abandon(ResolveOrAbandonRequest::from_submit_request(&submit).unwrap())
    .unwrap();

    assert!(matches!(
        resolution.outcome(),
        ResolveOrAbandonOutcome::Accepted { .. }
    ));
    assert_eq!(resolver_launches.load(Ordering::SeqCst), 0);
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    assert!(job_path.join("meta.json").is_file());
    let disposition = fs::read_to_string(store.job_index(lease.job_id()).unwrap()).unwrap();
    assert!(disposition.contains("\"disposition\":\"accepted\""));
    assert!(!disposition.contains("\"disposition\":\"abandoned\""));

    release_tx.send(()).unwrap();
    submit_thread.join().unwrap().unwrap();
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    assert!(job_path.join("meta.json").is_file());
}

#[test]
fn exact_identity_bearing_incomplete_final_is_cleaned_before_release() {
    // Break caught: every incomplete final is treated as a conflict even when
    // its durable identity records positively bind it to this exact request.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, submit) = unindexed_identityless_job(&root);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    remove_and_sync(&job.join("status.json"));

    let response = JobService::new(&store, &RejectLauncher)
        .resolve_or_abandon(ResolveOrAbandonRequest::from_submit_request(&submit).unwrap())
        .unwrap();

    assert!(matches!(
        response.outcome(),
        ResolveOrAbandonOutcome::Abandoned
    ));
    for name in ["execution.json", "workspace", "home", "tmp"] {
        assert!(!job.join(name).exists(), "{name} survived exact cleanup");
    }
    assert!(job.join("meta.json").is_file());
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn conflicting_incomplete_final_is_preserved_without_a_tombstone() {
    // Break caught: final-job path ownership is inferred from its name and a
    // resolver deletes mutable state belonging to a conflicting request.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, submit) = unindexed_identityless_job(&root);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    remove_and_sync(&job.join("status.json"));
    let original_meta = fs::read(job.join("meta.json")).unwrap();
    let conflicting = String::from_utf8(original_meta.clone())
        .unwrap()
        .replace(CLIENT_ID, "302f0f4a6b5c7d8e9f00112233445566")
        .into_bytes();
    replace_bytes(&job.join("meta.json"), &conflicting).unwrap();

    let error = JobService::new(&store, &RejectLauncher)
        .resolve_or_abandon(ResolveOrAbandonRequest::from_submit_request(&submit).unwrap())
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert_eq!(fs::read(job.join("meta.json")).unwrap(), conflicting);
    assert!(job.join("workspace").is_dir());
    assert!(job.join("execution.json").is_file());
    assert!(!store.job_index(lease.job_id()).unwrap().exists());
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
}

#[test]
fn every_resolution_cleanup_boundary_is_retryable_and_keeps_the_exact_lease() {
    // Break caught: any durable cleanup boundary retires capacity too early or
    // loses the tombstone needed for an exact idempotent retry.
    for point in [
        HostStoreWritePoint::AfterResolutionTombstone,
        HostStoreWritePoint::AfterResolutionExecutionRemoval,
        HostStoreWritePoint::AfterResolutionIncomingRemoval,
        HostStoreWritePoint::AfterResolutionVerifiedReceiptRemoval,
        HostStoreWritePoint::AfterResolutionVerificationStageRemoval,
        HostStoreWritePoint::AfterResolutionJobMutableRemoval,
        HostStoreWritePoint::AfterResolutionJobStageRemoval,
        HostStoreWritePoint::AfterResolutionAbsenceProof,
        HostStoreWritePoint::AfterResolutionCleanupMarker,
        HostStoreWritePoint::BeforeResolutionLeaseRelease,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("host-{}", point as u8));
        let sentinel = temp.path().join("unrelated");
        fs::write(&sentinel, b"preserve").unwrap();
        let (store, lease, submit) = unindexed_identityless_job(&root);
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        remove_and_sync(&job.join("status.json"));
        let execution = job.join("execution.json");
        let mutable = [job.join("workspace"), job.join("home"), job.join("tmp")];
        let incoming = store
            .incoming_job(lease.job_id(), lease.lease_token())
            .unwrap();
        fs::create_dir_all(&incoming).unwrap();
        fs::set_permissions(&incoming, fs::Permissions::from_mode(0o700)).unwrap();
        let cache_loser = incoming.join("task-6-cache-loser-retained-leaf");
        fs::write(&cache_loser, b"retained-cache-loser").unwrap();
        fs::set_permissions(&cache_loser, fs::Permissions::from_mode(0o600)).unwrap();
        let receipt = store.verified_receipt(lease.job_id()).unwrap();
        let pending = receipt
            .parent()
            .unwrap()
            .join(format!(".verify-{}.json.pending", lease.job_id()));
        fs::copy(&receipt, &pending).unwrap();
        fs::set_permissions(&pending, fs::Permissions::from_mode(0o600)).unwrap();
        File::open(receipt.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
        let job_stage =
            root.join("leases")
                .join(format!(".job-{}-{}", lease.job_id(), "1".repeat(32)));
        fs::create_dir(&job_stage).unwrap();
        fs::set_permissions(&job_stage, fs::Permissions::from_mode(0o700)).unwrap();
        let stage_payload = job_stage.join("retained-stage");
        fs::write(&stage_payload, b"job-stage").unwrap();
        fs::set_permissions(&stage_payload, fs::Permissions::from_mode(0o600)).unwrap();
        File::open(root.join("leases")).unwrap().sync_all().unwrap();
        let cache = store
            .snapshot(
                lease.project_id(),
                lease.worktree_id(),
                lease.manifest_digest(),
            )
            .unwrap();
        drop(store);
        let faulted = HostStore::open_with_write_fault(&root, point).unwrap();
        let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
        let launches = Arc::new(AtomicUsize::new(0));
        let launcher = CountingRejectLauncher {
            launches: Arc::clone(&launches),
        };

        let pending_response = JobService::new(&faulted, &launcher)
            .resolve_or_abandon(request.clone())
            .unwrap();
        let expected = if point == HostStoreWritePoint::BeforeResolutionLeaseRelease {
            "LEASE_RELEASE_FAILED"
        } else {
            "MUTABLE_CLEANUP_FAILED"
        };
        assert!(
            matches!(
                pending_response.outcome(),
                ResolveOrAbandonOutcome::CleanupPending { code } if code == expected
            ),
            "{point:?}: {pending_response:?}"
        );
        let disposition = fs::read_to_string(faulted.job_index(lease.job_id()).unwrap()).unwrap();
        assert!(disposition.contains("\"disposition\":\"abandoned\""));
        assert!(!disposition.contains("\"disposition\":\"accepted\""));
        assert!(faulted.job_index(lease.job_id()).unwrap().is_file());
        assert_eq!(
            LeaseService::new(&faulted).load().unwrap(),
            Some(lease.clone()),
            "{point:?}"
        );
        assert_eq!(launches.load(Ordering::SeqCst), 0, "{point:?}");
        assert!(job.join("meta.json").is_file(), "{point:?}");
        if (point as u8) >= (HostStoreWritePoint::AfterResolutionExecutionRemoval as u8) {
            assert!(!execution.exists(), "{point:?}: execution survived");
        }
        if (point as u8) >= (HostStoreWritePoint::AfterResolutionIncomingRemoval as u8) {
            assert!(!incoming.exists(), "{point:?}: cache loser survived");
        }
        if (point as u8) >= (HostStoreWritePoint::AfterResolutionVerifiedReceiptRemoval as u8) {
            assert!(!receipt.exists(), "{point:?}: receipt survived");
        }
        if (point as u8) >= (HostStoreWritePoint::AfterResolutionVerificationStageRemoval as u8) {
            assert!(!pending.exists(), "{point:?}: pending receipt survived");
        }
        if (point as u8) >= (HostStoreWritePoint::AfterResolutionJobMutableRemoval as u8) {
            for path in &mutable {
                assert!(!path.exists(), "{point:?}: {} survived", path.display());
            }
        }
        if (point as u8) >= (HostStoreWritePoint::AfterResolutionJobStageRemoval as u8) {
            assert!(!job_stage.exists(), "{point:?}: job stage survived");
        }
        assert!(cache.is_dir(), "{point:?}");
        assert_eq!(fs::read(&sentinel).unwrap(), b"preserve", "{point:?}");
        drop(faulted);

        let reopened = HostStore::open(&root).unwrap();
        let completed = JobService::new(&reopened, &RejectLauncher)
            .resolve_or_abandon(request)
            .unwrap();
        assert!(
            matches!(completed.outcome(), ResolveOrAbandonOutcome::Abandoned),
            "{point:?}: {completed:?}"
        );
        assert_eq!(LeaseService::new(&reopened).load().unwrap(), None);
        assert!(!execution.exists(), "{point:?}");
        assert!(!incoming.exists(), "{point:?}");
        assert!(!receipt.exists(), "{point:?}");
        assert!(!pending.exists(), "{point:?}");
        assert!(!job_stage.exists(), "{point:?}");
        for path in &mutable {
            assert!(!path.exists(), "{point:?}: {} resurrected", path.display());
        }
        assert!(job.join("meta.json").is_file(), "{point:?}");
        assert!(cache.is_dir(), "{point:?}");
        assert_eq!(fs::read(&sentinel).unwrap(), b"preserve", "{point:?}");
    }
}

#[test]
fn response_loss_after_durable_retirement_retries_abandoned_without_resurrection() {
    // Break caught: the post-retirement crash seam recreates or wedges the
    // heavy slot, or a retry cannot recognize completed abandonment.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, _lease, submit) = prepared_host(&root);
    drop(store);
    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterJobLeaseRetirement)
            .unwrap();
    let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();

    let first = JobService::new(&faulted, &RejectLauncher)
        .resolve_or_abandon(request.clone())
        .unwrap();
    assert!(matches!(
        first.outcome(),
        ResolveOrAbandonOutcome::Abandoned
    ));
    assert_eq!(LeaseService::new(&faulted).load().unwrap(), None);
    drop(faulted);

    let reopened = HostStore::open(&root).unwrap();
    let retry = JobService::new(&reopened, &RejectLauncher)
        .resolve_or_abandon(request)
        .unwrap();
    assert!(matches!(
        retry.outcome(),
        ResolveOrAbandonOutcome::Abandoned
    ));
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), None);
}

#[test]
fn mismatched_tombstone_retry_is_a_conflict_and_preserves_the_winner() {
    // Break caught: an existing tombstone is treated as job-ID-only authority
    // and a different immutable request receives idempotent abandonment.
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let original = lease_request();
    let original_submit = SubmitRequest::new(original.material().clone());
    let original_resolve = ResolveOrAbandonRequest::from_submit_request(&original_submit).unwrap();
    JobService::new(&store, &RejectLauncher)
        .resolve_or_abandon(original_resolve)
        .unwrap();
    let changed = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            original.material().job_id(),
            ClientId::new(uuid::Uuid::from_u128(77_001)),
            LeaseToken::new(uuid::Uuid::from_u128(77_002)),
            original.material().worker_name().into(),
            original.material().project_id().into(),
            original.material().worktree_id().into(),
            original.material().manifest_digest().into(),
            original.material().relative_working_dir().into(),
            original.material().timeout_millis(),
            original.material().resource_class().into(),
            original.material().command().clone(),
        )
        .unwrap(),
    );
    let changed_submit = SubmitRequest::new(changed.material().clone());

    let error = JobService::new(&store, &RejectLauncher)
        .resolve_or_abandon(ResolveOrAbandonRequest::from_submit_request(&changed_submit).unwrap())
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    assert!(
        store
            .job_index(original.material().job_id())
            .unwrap()
            .is_file()
    );
}

#[test]
fn transfer_resolution_keeps_valid_authoritative_accepted_evidence() {
    // Break caught: removing the transfer fast path accidentally converts a
    // fully validated accepted job into abandonment or launches it again.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, submit) = indexed_identityless_job(&root);
    let acquire = LeaseAcquireRequest::new(submit.material().clone());
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let meta = fs::read(job.join("meta.json")).unwrap();
    let status = fs::read(job.join("status.json")).unwrap();

    let error = HostTransferService::new(&store)
        .abandon(&acquire, 30)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ACCEPTED"), "{error}");
    assert_eq!(fs::read(job.join("meta.json")).unwrap(), meta);
    assert_eq!(fs::read(job.join("status.json")).unwrap(), status);
    assert!(store.job_index(lease.job_id()).unwrap().is_file());
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
}

#[test]
fn mismatched_cleanup_marker_conflicts_before_tombstone_or_mutation() {
    // Break caught: cleanup discovers a foreign durable marker only after
    // publishing Abandoned and removing exact mutable state, then hides the
    // authority conflict behind MUTABLE_CLEANUP_FAILED.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    let acquire = lease_request();
    let lease = match LeaseService::new(&store)
        .acquire(&acquire, &healthy(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    fs::create_dir_all(&incoming).unwrap();
    fs::set_permissions(&incoming, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(incoming.join("sentinel"), b"preserve-exact-state").unwrap();
    fs::set_permissions(incoming.join("sentinel"), fs::Permissions::from_mode(0o600)).unwrap();
    let marker_dir = root.join("locks/jobs").join(lease.job_id().to_string());
    let marker = marker_dir.join("cleanup-complete.json");
    let token_hash = format!(
        "{:x}",
        Sha256::digest(lease.lease_token().to_string().as_bytes())
    );
    let bytes = format!(
        concat!(
            r#"{{"job_id":"{}","client_id":"{}","project_id":"{}","#,
            r#""worktree_id":"{}","manifest_digest":"{}","#,
            r#""request_fingerprint":"{}","lease_token_sha256":"{}","#,
            r#""terminal_or_abandoned":true}}"#
        ),
        lease.job_id(),
        ClientId::new(uuid::Uuid::from_u128(99_001)),
        lease.project_id(),
        lease.worktree_id(),
        lease.manifest_digest(),
        lease.request_fingerprint(),
        token_hash,
    )
    .into_bytes();
    fs::write(&marker, &bytes).unwrap();
    fs::set_permissions(&marker, fs::Permissions::from_mode(0o600)).unwrap();
    File::open(&marker_dir).unwrap().sync_all().unwrap();
    let request = ResolveOrAbandonRequest::from_submit_request(&SubmitRequest::new(
        acquire.material().clone(),
    ))
    .unwrap();

    let error = JobService::new(&store, &RejectLauncher)
        .resolve_or_abandon(request)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert_eq!(fs::read(&marker).unwrap(), bytes);
    assert_eq!(
        fs::read(incoming.join("sentinel")).unwrap(),
        b"preserve-exact-state"
    );
    assert!(!store.job_index(lease.job_id()).unwrap().exists());
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
}

#[test]
fn status_launches_an_identityless_accepted_job_once_and_returns_mutable_status() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let launches = Arc::new(AtomicUsize::new(0));
    let supervisor = identity(41_001);
    let launcher = HoldingRecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: supervisor,
        guard: Mutex::new(None),
    };
    let service = JobService::new(&store, &launcher);

    let first = service.status(lease.job_id()).unwrap();
    let second = service.status(lease.job_id()).unwrap();

    assert_eq!(first.meta().job_id(), lease.job_id());
    assert_eq!(first.status().state(), JobState::Accepted);
    assert_eq!(first.status().supervisor_identity(), Some(supervisor));
    assert_eq!(second.status(), first.status());
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    drop(launcher.guard.lock().unwrap().take());
}

#[test]
fn status_repairs_an_exact_preindex_final_job_and_launches_it_once() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, _request) = unindexed_identityless_job(&root);
    let launches = Arc::new(AtomicUsize::new(0));
    let supervisor = identity(41_002);
    let launcher = HoldingRecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: supervisor,
        guard: Mutex::new(None),
    };
    let service = JobService::new(&store, &launcher);

    let response = service.status(lease.job_id()).unwrap();

    assert_eq!(response.status().supervisor_identity(), Some(supervisor));
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    assert!(store.job_index(lease.job_id()).unwrap().is_file());
    assert_eq!(
        service.status(lease.job_id()).unwrap().status(),
        response.status()
    );
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    drop(launcher.guard.lock().unwrap().take());
}

#[test]
fn status_returns_a_durable_prelaunch_loss_recorded_by_its_elected_supervisor() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("prelaunch-loss-during-election");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let launches = Arc::new(AtomicUsize::new(0));
    let supervisor = identity(41_003);
    let launcher = PrelaunchLostLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: supervisor,
    };
    let service = JobService::new(&store, &launcher);

    let response = service.status(lease.job_id()).unwrap();

    assert_eq!(response.status().state(), JobState::Lost);
    assert_eq!(response.status().supervisor_identity(), Some(supervisor));
    assert_eq!(response.status().child_identity(), None);
    assert_eq!(response.status().error_code(), Some("PRELAUNCH_FAILED"));
    assert_eq!(
        service.status(lease.job_id()).unwrap().status(),
        response.status()
    );
    assert_eq!(launches.load(Ordering::SeqCst), 1);
}

#[test]
fn status_does_not_retry_a_launcher_error_without_durable_progress() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("uncommitted-launch-error");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = SpoofedPrelaunchFailureLauncher {
        launches: Arc::clone(&launches),
    };

    let error = JobService::new(&store, &launcher)
        .status(lease.job_id())
        .unwrap_err();

    assert_error_code(
        error,
        "SUPERVISOR_PRELAUNCH_FAILED",
        "uncommitted launcher failure",
    );
    assert_eq!(launches.load(Ordering::SeqCst), 1);
}

#[test]
fn status_returns_authoritative_nonterminal_states_without_relaunching() {
    let supervisor = identity(51_001);
    let child = identity(51_002);
    let states = [
        JobStatus::accepted(10)
            .unwrap()
            .with_supervisor(supervisor, 11)
            .unwrap(),
        JobStatus::accepted(10)
            .unwrap()
            .with_supervisor(supervisor, 11)
            .unwrap()
            .with_child(child, 12)
            .unwrap(),
        JobStatus::accepted(10)
            .unwrap()
            .with_supervisor(supervisor, 11)
            .unwrap()
            .with_child(child, 12)
            .unwrap()
            .into_running(13)
            .unwrap(),
    ];

    for (index, expected) in states.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("host-{index}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        let launcher = HoldingRecordingLauncher {
            launches: Arc::new(AtomicUsize::new(0)),
            job_path: job.clone(),
            identity: supervisor,
            guard: Mutex::new(None),
        };
        JobService::new(&store, &launcher)
            .status(lease.job_id())
            .unwrap();
        replace_json(&job.join("status.json"), &expected).unwrap();

        let response = JobService::new(&store, &RejectLauncher)
            .status(lease.job_id())
            .unwrap();

        assert_eq!(response.status(), &expected, "state row {index}");
        assert_eq!(response.meta().job_id(), lease.job_id());
        drop(launcher.guard.lock().unwrap().take());
    }
}

#[test]
fn terminal_statuses_remain_authoritative_after_the_lease_is_released() {
    let terminal_builders: [fn(&JobStatus) -> JobStatus; 7] = [
        |running| running.into_succeeded(20, 0, 0).unwrap(),
        |running| {
            running
                .into_succeeded(20, 0, 0)
                .unwrap()
                .with_cleanup_error("CLEANUP_IO".into(), 21)
                .unwrap()
        },
        |running| running.into_failed_exit(20, 7, 0, 0).unwrap(),
        |running| running.into_failed_signal(20, 15, 0, 0).unwrap(),
        |running| {
            running
                .into_infrastructure_terminal(JobState::Cancelled, 20, 0, 0, "CANCELLED".into())
                .unwrap()
        },
        |running| {
            running
                .into_infrastructure_terminal(JobState::TimedOut, 20, 0, 0, "TIMED_OUT".into())
                .unwrap()
        },
        |running| {
            running
                .into_infrastructure_terminal(JobState::Lost, 20, 0, 0, "LOST".into())
                .unwrap()
        },
    ];

    for (index, terminal) in terminal_builders.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("terminal-{index}"));
        let (store, lease, request) = prepared_host(&root);
        let launcher = InlineSupervisorLauncher {
            store: store.clone(),
        };
        let completed = JobService::new(&store, &launcher)
            .submit_at(request, 10)
            .unwrap();
        assert_eq!(LeaseService::new(&store).load().unwrap(), None);
        let supervisor = completed.status().supervisor_identity().unwrap();
        let child = completed.status().child_identity().unwrap();
        let running = JobStatus::accepted(10)
            .unwrap()
            .with_supervisor(supervisor, 11)
            .unwrap()
            .with_child(child, 12)
            .unwrap()
            .into_running(13)
            .unwrap();
        let expected = terminal(&running);
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        replace_json(&job.join("status.json"), &expected).unwrap();

        let response = JobService::new(&store, &RejectLauncher)
            .status(lease.job_id())
            .unwrap();

        assert_eq!(response.status(), &expected, "terminal row {index}");
    }
}

#[test]
fn post_release_success_binds_nonempty_stdout_to_the_recorded_length() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("terminal-output");
    let (store, lease, request) = prepared_host_with_command(
        &root,
        CommandSpec::argv(vec!["/usr/bin/printf".into(), "hello".into()]).unwrap(),
    );
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let submitted = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();
    assert_eq!(submitted.status().final_stdout_bytes(), Some(5));
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);

    let response = JobService::new(&store, &RejectLauncher)
        .status(lease.job_id())
        .unwrap();

    assert_eq!(response.status().final_stdout_bytes(), Some(5));
    assert_eq!(response.status().final_stderr_bytes(), Some(0));
    assert_eq!(
        fs::read(
            store
                .job(lease.project_id(), lease.worktree_id(), lease.job_id())
                .unwrap()
                .join("stdout.log")
        )
        .unwrap(),
        b"hello"
    );
}

#[test]
fn read_log_returns_bounded_binary_chunks_from_independent_fixed_streams() {
    // Catches using a caller-controlled pathname, shared seek state between
    // streams, allocating the raw request limit, or treating arbitrary bytes
    // as text rather than preserving them through LogChunk base64.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("bounded-log-reads");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = HoldingRecordingLauncher {
        launches,
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: identity(51_101),
        guard: Mutex::new(None),
    };
    let service = JobService::new(&store, &launcher);
    service.status(lease.job_id()).unwrap();
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let stdout_bytes = [
        "hello, 世界\n".as_bytes(),
        b"\0\xff\x80binary\n".as_slice(),
        &vec![b'x'; 65_537],
    ]
    .concat();
    let stderr_bytes = b"stderr\0\xfe\n".to_vec();
    OpenOptions::new()
        .append(true)
        .open(job.join("stdout.log"))
        .unwrap()
        .write_all(&stdout_bytes)
        .unwrap();
    OpenOptions::new()
        .append(true)
        .open(job.join("stderr.log"))
        .unwrap()
        .write_all(&stderr_bytes)
        .unwrap();

    let empty = service
        .read_log(lease.job_id(), LogStream::Stdout, 0, 0)
        .unwrap();
    assert_eq!(empty.decoded_bytes().unwrap(), b"");
    assert_eq!(empty.next_offset(), 0);

    for (limit, expected) in [
        (65_535, 65_535usize),
        (65_536, 65_536usize),
        (65_537, 65_536usize),
    ] {
        let chunk = service
            .read_log(lease.job_id(), LogStream::Stdout, 0, limit)
            .unwrap();
        assert_eq!(chunk.stream(), LogStream::Stdout);
        assert_eq!(chunk.offset(), 0);
        assert_eq!(chunk.next_offset(), expected as u64);
        assert_eq!(chunk.decoded_bytes().unwrap(), stdout_bytes[..expected]);
    }

    let stderr = service
        .read_log(lease.job_id(), LogStream::Stderr, 0, 65_537)
        .unwrap();
    assert_eq!(stderr.stream(), LogStream::Stderr);
    assert_eq!(stderr.decoded_bytes().unwrap(), stderr_bytes);
    let stdout_tail = service
        .read_log(lease.job_id(), LogStream::Stdout, 65_536, 65_537)
        .unwrap();
    assert_eq!(stdout_tail.decoded_bytes().unwrap(), stdout_bytes[65_536..]);

    let eof = service
        .read_log(
            lease.job_id(),
            LogStream::Stdout,
            stdout_bytes.len() as u64,
            65_536,
        )
        .unwrap();
    assert_eq!(eof.decoded_bytes().unwrap(), b"");
    assert_eq!(eof.next_offset(), stdout_bytes.len() as u64);
    for offset in [stdout_bytes.len() as u64 + 1, u64::MAX] {
        let error = service
            .read_log(lease.job_id(), LogStream::Stdout, offset, 65_536)
            .unwrap_err();
        assert_error_code(error, "LOG_OFFSET_BEYOND_EOF", "beyond EOF");
    }

    assert_eq!(fs::read(job.join("stdout.log")).unwrap(), stdout_bytes);
    assert_eq!(fs::read(job.join("stderr.log")).unwrap(), stderr_bytes);
    assert_eq!(
        LeaseService::new(&store).load().unwrap(),
        Some(lease.clone())
    );
    drop(launcher.guard.lock().unwrap().take());
}

#[test]
fn read_log_rejects_a_whole_job_directory_swap_and_preserves_all_evidence() {
    // Catches resolving status against one durable job directory and then
    // reopening the textual job path for the log read after that path has
    // been replaced by a different directory.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("whole-job-log-swap");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = HoldingRecordingLauncher {
        launches,
        job_path: job.clone(),
        identity: identity(51_102),
        guard: Mutex::new(None),
    };
    JobService::new(&store, &launcher)
        .status(lease.job_id())
        .unwrap();

    let original_stdout = b"original-private-stdout\n";
    let original_stderr = b"original-private-stderr\n";
    fs::write(job.join("stdout.log"), original_stdout).unwrap();
    fs::write(job.join("stderr.log"), original_stderr).unwrap();
    let original_meta = fs::read(job.join("meta.json")).unwrap();
    let original_status = fs::read(job.join("status.json")).unwrap();
    let unrelated = root.join("unrelated-query-evidence");
    fs::write(&unrelated, b"unrelated-state-must-survive").unwrap();

    let parent = job.parent().unwrap().to_path_buf();
    let replacement = parent.join(".replacement-job-directory");
    fs::create_dir(&replacement).unwrap();
    fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        replacement.join("stdout.log"),
        b"replacement-private-stdout\n",
    )
    .unwrap();
    fs::set_permissions(
        replacement.join("stdout.log"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let detached = parent.join(".detached-authoritative-job");
    let swaps = Arc::new(AtomicUsize::new(0));
    let hook_job = job.clone();
    let hook_replacement = replacement.clone();
    let hook_detached = detached.clone();
    let hook_parent = parent.clone();
    let hook_swaps = Arc::clone(&swaps);
    let service = JobService::new_with_log_read_boundary(
        &store,
        &launcher,
        Arc::new(move || {
            assert_eq!(hook_swaps.fetch_add(1, Ordering::SeqCst), 0);
            fs::rename(&hook_job, &hook_detached).unwrap();
            fs::rename(&hook_replacement, &hook_job).unwrap();
            File::open(&hook_parent).unwrap().sync_all().unwrap();
        }),
    );

    let error = service
        .read_log(lease.job_id(), LogStream::Stdout, 0, 65_536)
        .unwrap_err();

    assert_eq!(swaps.load(Ordering::SeqCst), 1);
    let rendered = error.to_string();
    assert!(rendered.len() <= 512, "unbounded diagnostic: {rendered}");
    for secret in [
        "original-private-stdout",
        "original-private-stderr",
        "replacement-private-stdout",
        "unrelated-state-must-survive",
    ] {
        assert!(!rendered.contains(secret), "diagnostic leaked {secret}");
    }
    assert_eq!(fs::read(detached.join("meta.json")).unwrap(), original_meta);
    assert_eq!(
        fs::read(detached.join("status.json")).unwrap(),
        original_status
    );
    assert_eq!(
        fs::read(detached.join("stdout.log")).unwrap(),
        original_stdout
    );
    assert_eq!(
        fs::read(detached.join("stderr.log")).unwrap(),
        original_stderr
    );
    assert_eq!(
        fs::read(job.join("stdout.log")).unwrap(),
        b"replacement-private-stdout\n"
    );
    assert_eq!(
        fs::read(&unrelated).unwrap(),
        b"unrelated-state-must-survive"
    );
    assert_eq!(
        LeaseService::new(&store).load().unwrap(),
        Some(lease.clone())
    );
    drop(launcher.guard.lock().unwrap().take());
}

#[test]
fn read_log_leaf_failures_preserve_job_evidence_unrelated_state_and_the_exact_lease() {
    for mutation in ["symlink", "replacement", "shrink"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("log-leaf-{mutation}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        let launcher = HoldingRecordingLauncher {
            launches: Arc::new(AtomicUsize::new(0)),
            job_path: job.clone(),
            identity: identity(51_103),
            guard: Mutex::new(None),
        };
        JobService::new(&store, &launcher)
            .status(lease.job_id())
            .unwrap();

        let original_stdout = b"0123456789-original-private-stdout";
        let original_stderr = b"preserved-private-stderr";
        fs::write(job.join("stdout.log"), original_stdout).unwrap();
        fs::write(job.join("stderr.log"), original_stderr).unwrap();
        let original_meta = fs::read(job.join("meta.json")).unwrap();
        let original_status = fs::read(job.join("status.json")).unwrap();
        let unrelated = temp.path().join("unrelated-log-target");
        fs::write(&unrelated, b"unrelated-private-state").unwrap();
        let retained = job.join(".retained-stdout");
        let replacement = job.join(".unsafe-log-replacement");
        if mutation == "replacement" {
            fs::write(&replacement, b"replacement-private-bytes").unwrap();
            fs::set_permissions(&replacement, fs::Permissions::from_mode(0o644)).unwrap();
        }

        let hook_job = job.clone();
        let hook_unrelated = unrelated.clone();
        let hook_retained = retained.clone();
        let hook_replacement = replacement.clone();
        let service = JobService::new_with_log_read_boundary(
            &store,
            &launcher,
            Arc::new(move || {
                let stdout = hook_job.join("stdout.log");
                match mutation {
                    "symlink" => {
                        fs::rename(&stdout, &hook_retained).unwrap();
                        symlink(&hook_unrelated, &stdout).unwrap();
                    }
                    "replacement" => {
                        fs::rename(&stdout, &hook_retained).unwrap();
                        fs::rename(&hook_replacement, &stdout).unwrap();
                    }
                    "shrink" => {
                        let mut file = OpenOptions::new()
                            .write(true)
                            .truncate(true)
                            .open(&stdout)
                            .unwrap();
                        file.write_all(b"tiny").unwrap();
                        file.sync_all().unwrap();
                    }
                    _ => unreachable!(),
                }
                File::open(&hook_job).unwrap().sync_all().unwrap();
            }),
        );

        let error = service
            .read_log(
                lease.job_id(),
                LogStream::Stdout,
                if mutation == "shrink" { 8 } else { 0 },
                65_536,
            )
            .unwrap_err();

        let rendered = error.to_string();
        assert!(
            rendered.len() <= 512,
            "{mutation} produced an unbounded diagnostic: {rendered}"
        );
        for secret in [
            "original-private-stdout",
            "preserved-private-stderr",
            "replacement-private-bytes",
            "unrelated-private-state",
        ] {
            assert!(
                !rendered.contains(secret),
                "{mutation} diagnostic leaked {secret}"
            );
        }
        assert_eq!(fs::read(job.join("meta.json")).unwrap(), original_meta);
        assert_eq!(fs::read(job.join("status.json")).unwrap(), original_status);
        assert_eq!(
            fs::read(job.join("stderr.log")).unwrap(),
            original_stderr,
            "{mutation}"
        );
        assert_eq!(
            fs::read(&unrelated).unwrap(),
            b"unrelated-private-state",
            "{mutation}"
        );
        match mutation {
            "symlink" => {
                assert!(
                    fs::symlink_metadata(job.join("stdout.log"))
                        .unwrap()
                        .is_symlink()
                );
                assert_eq!(fs::read(&retained).unwrap(), original_stdout);
            }
            "replacement" => {
                assert_eq!(
                    fs::read(job.join("stdout.log")).unwrap(),
                    b"replacement-private-bytes"
                );
                assert_eq!(fs::read(&retained).unwrap(), original_stdout);
            }
            "shrink" => assert_eq!(fs::read(job.join("stdout.log")).unwrap(), b"tiny"),
            _ => unreachable!(),
        }
        assert_eq!(
            LeaseService::new(&store).load().unwrap(),
            Some(lease.clone()),
            "{mutation}"
        );
        drop(launcher.guard.lock().unwrap().take());
    }
}

#[test]
fn terminal_log_drain_requires_both_exact_eofs_and_unchanged_status_revalidation() {
    // Catches stopping on an empty preterminal read, coupling stdout/stderr
    // offsets, stopping at the recorded length without the extra EOF probes,
    // or accepting a changed terminal status during final revalidation.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("terminal-log-drain");
    let stdout_length = 65_536 + 17;
    let stderr_length = 65_536 + 9;
    let command = CommandSpec::shell(format!(
        "/usr/bin/head -c {stdout_length} /dev/zero; /usr/bin/head -c {stderr_length} /dev/zero >&2"
    ))
    .unwrap();
    let (store, lease, request) = prepared_host_with_command(&root, command);
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let service = JobService::new(&store, &launcher);
    service.submit_at(request, 10).unwrap();
    let terminal = service.status(lease.job_id()).unwrap();
    assert_eq!(terminal.status().final_stdout_bytes(), Some(stdout_length));
    assert_eq!(terminal.status().final_stderr_bytes(), Some(stderr_length));
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);

    let stdout = LogCursor::new(LogStream::Stdout, 0, 65_537).unwrap();
    let stderr = LogCursor::new(LogStream::Stderr, 0, 65_537).unwrap();
    let mut drain = TerminalLogDrain::new(stdout, stderr).unwrap();

    let preterminal_empty = LogChunk::new(LogStream::Stdout, 0, Vec::new()).unwrap();
    drain.observe_chunk(&preterminal_empty).unwrap();
    assert!(!drain.cursor(LogStream::Stdout).is_drained());
    drain.set_terminal_status(&terminal).unwrap();

    let mut observed_stdout = Vec::new();
    let mut observed_stderr = Vec::new();
    for (stream, observed) in [
        (LogStream::Stdout, &mut observed_stdout),
        (LogStream::Stderr, &mut observed_stderr),
    ] {
        while !drain.cursor(stream).is_drained() {
            let request = drain.cursor(stream).request(lease.job_id());
            let chunk = service
                .read_log(
                    request.job_id(),
                    request.stream(),
                    request.offset(),
                    request.limit(),
                )
                .unwrap();
            observed.extend_from_slice(&chunk.decoded_bytes().unwrap());
            drain.observe_chunk(&chunk).unwrap();
        }
    }
    assert_eq!(observed_stdout, vec![0; stdout_length as usize]);
    assert_eq!(observed_stderr, vec![0; stderr_length as usize]);
    assert_eq!(drain.cursor(LogStream::Stdout).next_offset(), stdout_length);
    assert_eq!(drain.cursor(LogStream::Stderr).next_offset(), stderr_length);
    assert!(!drain.is_complete());

    let changed = StatusResponse::new(
        terminal.meta().clone(),
        terminal
            .status()
            .clone()
            .with_cleanup_error(
                "LATE_CLEANUP_ERROR".into(),
                terminal.status().updated_at_millis() + 1,
            )
            .unwrap(),
    )
    .unwrap();
    assert!(drain.revalidate_terminal_status(&changed).is_err());
    assert!(!drain.is_complete());
    drain.revalidate_terminal_status(&terminal).unwrap();
    assert!(drain.is_complete());
}

#[test]
fn log_cursors_reject_wrong_stream_out_of_order_duplicate_crossing_and_changed_targets() {
    // Catches a cursor accepting any chunk that happens to contain bytes,
    // advancing on rejected input, or treating arrival at a terminal target
    // as EOF without the required empty confirmation.
    let job_id: JobId = JOB_ID.parse().unwrap();
    let mut cursor = LogCursor::new(LogStream::Stdout, 4, 65_537).unwrap();
    assert_eq!(cursor.stream(), LogStream::Stdout);
    assert_eq!(cursor.next_offset(), 4);
    assert_eq!(cursor.request(job_id).limit(), 65_537);
    cursor.set_terminal_target(7).unwrap();

    assert!(
        cursor
            .observe_chunk(&LogChunk::new(LogStream::Stderr, 4, b"a".to_vec()).unwrap())
            .is_err()
    );
    assert!(
        cursor
            .observe_chunk(&LogChunk::new(LogStream::Stdout, 5, b"a".to_vec()).unwrap())
            .is_err()
    );
    assert!(
        cursor
            .observe_chunk(&LogChunk::new(LogStream::Stdout, 4, b"abcd".to_vec()).unwrap())
            .is_err()
    );
    assert_eq!(cursor.next_offset(), 4);

    let accepted = LogChunk::new(LogStream::Stdout, 4, b"abc".to_vec()).unwrap();
    cursor.observe_chunk(&accepted).unwrap();
    assert_eq!(cursor.next_offset(), 7);
    assert!(!cursor.is_drained());
    assert!(cursor.observe_chunk(&accepted).is_err());
    assert!(cursor.set_terminal_target(8).is_err());
    cursor
        .observe_chunk(&LogChunk::new(LogStream::Stdout, 7, Vec::new()).unwrap())
        .unwrap();
    assert!(cursor.is_drained());
    assert!(
        LogCursor::new(LogStream::Stderr, 5, 1)
            .unwrap()
            .set_terminal_target(4)
            .is_err()
    );
    assert!(LogChunk::new(LogStream::Stdout, u64::MAX, vec![1]).is_err());
}

#[test]
fn prelaunch_lost_with_only_a_durable_supervisor_identity_is_queryable() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("prelaunch-lost");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let expected = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(identity(52_001), 11)
        .unwrap()
        .into_infrastructure_terminal(JobState::Lost, 12, 0, 0, "PRELAUNCH_FAILED".into())
        .unwrap();
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    replace_json(&job.join("status.json"), &expected).unwrap();
    remove_and_sync(&job.join("execution.json"));

    let response = JobService::new(&store, &RejectLauncher)
        .status(lease.job_id())
        .unwrap();

    assert_eq!(response.status(), &expected);
    assert_eq!(response.status().child_identity(), None);
}

#[test]
fn status_rejects_preacceptance_timestamp_and_impossible_terminal_shapes() {
    let supervisor = identity(61_001);
    let child = identity(61_002);
    let invalid = [
        JobStatus::new(
            JobState::Uploading,
            10,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap(),
        JobStatus::new(
            JobState::Verified,
            10,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap(),
        JobStatus::accepted(9).unwrap(),
        JobStatus::accepted(11).unwrap(),
        JobStatus::succeeded(20, 0, 0).unwrap(),
        JobStatus::new(
            JobState::Succeeded,
            20,
            Some(supervisor.pid()),
            Some(supervisor.start_time_micros()),
            None,
            None,
            Some(0),
            None,
            Some(0),
            Some(0),
            None,
            None,
        )
        .unwrap(),
        JobStatus::new(
            JobState::Running,
            20,
            Some(supervisor.pid()),
            Some(supervisor.start_time_micros()),
            Some(child.pid()),
            Some(child.start_time_micros()),
            None,
            None,
            None,
            None,
            Some("UNEXPECTED".into()),
            None,
        )
        .unwrap(),
        JobStatus::new(
            JobState::Succeeded,
            20,
            Some(supervisor.pid()),
            Some(supervisor.start_time_micros()),
            Some(child.pid()),
            Some(child.start_time_micros()),
            Some(0),
            None,
            Some(0),
            Some(0),
            Some("UNEXPECTED".into()),
            None,
        )
        .unwrap(),
        JobStatus::new(
            JobState::Lost,
            20,
            Some(supervisor.pid()),
            Some(supervisor.start_time_micros()),
            None,
            None,
            None,
            None,
            Some(0),
            Some(0),
            None,
            None,
        )
        .unwrap(),
    ];

    for (index, status) in invalid.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("invalid-state-{index}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        replace_json(&job.join("status.json"), &status).unwrap();

        let error = JobService::new(&store, &RejectLauncher)
            .status(lease.job_id())
            .unwrap_err();

        assert_error_code(error, "JOB_STATE_INVALID", &format!("state row {index}"));
    }
}

#[test]
fn status_rejects_corrupt_or_missing_indexed_authority_and_preserves_it() {
    for (index, mutation) in [
        "corrupt-index",
        "missing-meta",
        "corrupt-meta",
        "missing-status",
        "corrupt-status",
    ]
    .into_iter()
    .enumerate()
    {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("corrupt-{index}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        match mutation {
            "corrupt-index" => {
                replace_bytes(&store.job_index(lease.job_id()).unwrap(), b"{").unwrap()
            }
            "missing-meta" => remove_and_sync(&job.join("meta.json")),
            "corrupt-meta" => replace_bytes(&job.join("meta.json"), b"{").unwrap(),
            "missing-status" => remove_and_sync(&job.join("status.json")),
            "corrupt-status" => replace_bytes(&job.join("status.json"), b"{").unwrap(),
            _ => unreachable!(),
        }

        let error = JobService::new(&store, &RejectLauncher)
            .status(lease.job_id())
            .unwrap_err();

        assert_error_code(error, "JOB_STATE_INVALID", mutation);
        assert!(job.is_dir(), "{mutation} removed final evidence");
    }
}

#[test]
fn status_rejects_index_meta_lease_and_initial_status_disagreement() {
    for (index, mutation) in ["lease-client", "corrupt-lease", "index-initial"]
        .into_iter()
        .enumerate()
    {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("identity-{index}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        match mutation {
            "lease-client" => replace_ascii_once(
                &root.join("leases/heavy/lease.json"),
                CLIENT_ID,
                "302f0f4a6b5c7d8e9f00112233445566",
            ),
            "corrupt-lease" => replace_bytes(&root.join("leases/heavy/lease.json"), b"{").unwrap(),
            "index-initial" => replace_ascii_once(
                &store.job_index(lease.job_id()).unwrap(),
                "\"updated_at_millis\":10",
                "\"updated_at_millis\":11",
            ),
            _ => unreachable!(),
        }

        let error = JobService::new(&store, &RejectLauncher)
            .status(lease.job_id())
            .unwrap_err();

        let expected = if matches!(mutation, "corrupt-lease" | "index-initial") {
            "JOB_STATE_INVALID"
        } else {
            "JOB_ID_CONFLICT"
        };
        assert_error_code(error, expected, mutation);
    }
}

#[test]
fn status_binds_every_accepted_index_identity_to_canonical_metadata() {
    for (index, mutation) in [
        "index-job",
        "meta-job",
        "meta-client",
        "meta-project",
        "meta-worktree",
        "meta-fingerprint",
    ]
    .into_iter()
    .enumerate()
    {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("index-meta-{index}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        let index_path = store.job_index(lease.job_id()).unwrap();
        match mutation {
            "index-job" => {
                replace_ascii_once(&index_path, JOB_ID, "318f0f4a6b5c7d8e9f00112233445566")
            }
            "meta-job" => replace_ascii_once(
                &job.join("meta.json"),
                JOB_ID,
                "318f0f4a6b5c7d8e9f00112233445566",
            ),
            "meta-client" => replace_ascii_once(
                &job.join("meta.json"),
                CLIENT_ID,
                "302f0f4a6b5c7d8e9f00112233445566",
            ),
            "meta-project" => replace_ascii_once(
                &job.join("meta.json"),
                PROJECT_ID,
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            ),
            "meta-worktree" => replace_ascii_once(
                &job.join("meta.json"),
                WORKTREE_ID,
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            ),
            "meta-fingerprint" => replace_ascii_once(
                &job.join("meta.json"),
                &lease.request_fingerprint().to_string(),
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            ),
            _ => unreachable!(),
        }

        let error = JobService::new(&store, &RejectLauncher)
            .status(lease.job_id())
            .unwrap_err();

        assert_error_code(error, "JOB_ID_CONFLICT", mutation);
    }
}

#[test]
fn terminal_status_rejects_log_length_mismatch_after_release() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("terminal-log-mismatch");
    let (store, lease, request) = prepared_host(&root);
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    OpenOptions::new()
        .append(true)
        .open(job.join("stdout.log"))
        .unwrap()
        .write_all(b"x")
        .unwrap();

    let error = JobService::new(&store, &RejectLauncher)
        .status(lease.job_id())
        .unwrap_err();

    assert_error_code(error, "JOB_STATE_INVALID", "terminal log length");
}

#[test]
fn conflicting_preindex_final_is_preserved_without_index_or_launch() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("preindex-conflict");
    let (store, lease, _request) = unindexed_identityless_job(&root);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    replace_ascii_once(
        &job.join("meta.json"),
        CLIENT_ID,
        "302f0f4a6b5c7d8e9f00112233445566",
    );

    let error = JobService::new(&store, &RejectLauncher)
        .status(lease.job_id())
        .unwrap_err();

    assert_error_code(error, "JOB_ID_CONFLICT", "preindex identity");
    assert!(job.is_dir());
    assert!(!store.job_index(lease.job_id()).unwrap().exists());
}

#[test]
fn status_preserves_prelaunch_jobs_with_nonempty_private_directories() {
    for (index, indexed) in [false, true].into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("nonempty-private-{index}"));
        let (store, lease, _request) = if indexed {
            indexed_identityless_job(&root)
        } else {
            unindexed_identityless_job(&root)
        };
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        let planted = job.join("home/unexpected");
        fs::write(&planted, b"preserve").unwrap();
        fs::set_permissions(&planted, fs::Permissions::from_mode(0o600)).unwrap();
        let launches = Arc::new(AtomicUsize::new(0));
        let launcher = CountingRejectLauncher {
            launches: Arc::clone(&launches),
        };

        let error = JobService::new(&store, &launcher)
            .status(lease.job_id())
            .unwrap_err();

        assert_error_code(error, "JOB_STATE_INVALID", "nonempty prelaunch home");
        assert_eq!(fs::read(&planted).unwrap(), b"preserve");
        assert_eq!(launches.load(Ordering::SeqCst), 0);
        assert_eq!(store.job_index(lease.job_id()).unwrap().exists(), indexed);
    }
}

#[test]
fn a_missing_index_without_the_exact_live_lease_is_not_repaired_from_status_absence() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("post-release-no-index");
    let (store, lease, request) = prepared_host(&root);
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    remove_and_sync(&store.job_index(lease.job_id()).unwrap());

    let error = JobService::new(&store, &RejectLauncher)
        .status(lease.job_id())
        .unwrap_err();

    assert_error_code(error, "JOB_NOT_FOUND", "missing index after release");
    assert!(
        store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap()
            .is_dir()
    );
}

#[test]
fn a_live_lease_without_a_final_job_is_not_inferred_to_be_accepted() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("live-without-final")).unwrap();
    let request = lease_request();
    let lease = match LeaseService::new(&store)
        .acquire(&request, &healthy(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };

    let error = JobService::new(&store, &RejectLauncher)
        .status(lease.job_id())
        .unwrap_err();

    assert_error_code(error, "JOB_NOT_FOUND", "live lease without final job");
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
    assert!(!store.job_index(JOB_ID.parse().unwrap()).unwrap().exists());
}

#[test]
fn status_returns_identityless_accepted_while_an_elected_supervisor_lock_is_busy() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("busy-supervisor");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let job_path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let blocker = Arc::new(BlockingLauncher {
        launches: Arc::clone(&launches),
        job_path,
        identity: identity(71_001),
        entered: entered_tx,
        release: Mutex::new(release_rx),
    });
    let job_id = lease.job_id();
    let first_store = store.clone();
    let first_launcher = Arc::clone(&blocker);
    let first = thread::spawn(move || {
        JobService::new(&first_store, first_launcher.as_ref()).status(job_id)
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let execution = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap()
        .join("execution.json");
    let execution_bytes = fs::read(&execution).unwrap();
    remove_and_sync(&execution);

    let rejected_launches = Arc::new(AtomicUsize::new(0));
    let observer = CountingRejectLauncher {
        launches: Arc::clone(&rejected_launches),
    };
    let observed = JobService::new(&store, &observer).status(job_id).unwrap();

    assert_eq!(observed.status().state(), JobState::Accepted);
    assert_eq!(observed.status().supervisor_identity(), None);
    assert_eq!(rejected_launches.load(Ordering::SeqCst), 0);
    replace_bytes(&execution, &execution_bytes).unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(
        first
            .join()
            .unwrap()
            .unwrap()
            .status()
            .supervisor_identity(),
        Some(identity(71_001))
    );
    assert_eq!(launches.load(Ordering::SeqCst), 1);
}

#[test]
fn concurrent_identityless_status_queries_elect_exactly_one_supervisor() {
    const CONTENDERS: usize = 16;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("concurrent-supervisor");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let launches = Arc::new(AtomicUsize::new(0));
    let supervisor = identity(72_001);
    let launcher = Arc::new(HoldingRecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: supervisor,
        guard: Mutex::new(None),
    });
    let job_id = lease.job_id();
    let barrier = Arc::new(Barrier::new(CONTENDERS));
    let handles = (0..CONTENDERS)
        .map(|_| {
            let store = store.clone();
            let launcher = Arc::clone(&launcher);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                JobService::new(&store, launcher.as_ref()).status(job_id)
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        let response = handle.join().unwrap().unwrap();
        assert_eq!(response.status().state(), JobState::Accepted);
    }
    let final_status = JobService::new(&store, launcher.as_ref())
        .status(job_id)
        .unwrap();
    assert_eq!(
        final_status.status().supervisor_identity(),
        Some(supervisor)
    );
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    drop(launcher.guard.lock().unwrap().take());
}

#[test]
fn reconciliation_fails_closed_for_live_reused_or_ambiguous_supervisor_identity() {
    let observations = [
        ProcessObservation::Matching {
            process_group: 81_001,
        },
        ProcessObservation::Reused,
        ProcessObservation::Ambiguous,
    ];

    for (index, observation) in observations.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("supervisor-identity-{index}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let status = JobStatus::accepted(10)
            .unwrap()
            .with_supervisor(identity(81_001), 11)
            .unwrap();
        install_job_status(&store, &lease, &status, false);
        let runtime = ScriptedReconciliation::new([observation], []);

        let error =
            JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
                .status(lease.job_id())
                .unwrap_err();

        assert_error_code(error, "RECONCILIATION_AMBIGUOUS", "supervisor identity");
        assert_eq!(
            LeaseService::new(&store).load().unwrap(),
            Some(lease.clone())
        );
        assert_eq!(runtime.signals(), Vec::<(u32, i32)>::new());
        assert_eq!(read_job_status(&store, &lease), status);
    }
}

#[test]
fn absent_supervisor_without_child_erases_payload_marks_lost_then_cleans_and_releases() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("absent-supervisor-no-child");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(identity(82_001), 11)
        .unwrap();
    install_job_status(&store, &lease, &status, false);
    let runtime = ScriptedReconciliation::new([ProcessObservation::Absent], []);

    let response =
        JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
            .status(lease.job_id())
            .unwrap();

    assert_eq!(response.status().state(), JobState::Lost);
    assert_eq!(response.status().error_code(), Some("SUPERVISOR_LOST"));
    assert_eq!(response.status().child_identity(), None);
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    assert_eq!(runtime.signals(), Vec::<(u32, i32)>::new());
    assert_mutable_job_scopes_absent(&store, &lease);
}

#[test]
fn surviving_child_gets_exact_term_full_grace_kill_and_absence_proof_before_release() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("orphan-term-kill");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let supervisor = identity(83_001);
    let child = identity(83_002);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(supervisor, 11)
        .unwrap()
        .with_child(child, 12)
        .unwrap()
        .into_running(13)
        .unwrap();
    install_job_status(&store, &lease, &status, true);
    let runtime = ScriptedReconciliation::new(
        [
            ProcessObservation::Absent,
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
            ProcessObservation::Absent,
        ],
        [ProcessGroupObservation::Absent],
    );

    let response =
        JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
            .status(lease.job_id())
            .unwrap();

    assert_eq!(response.status().state(), JobState::Lost);
    assert_eq!(
        runtime.signals(),
        vec![(child.pid(), libc::SIGTERM), (child.pid(), libc::SIGKILL)]
    );
    assert_eq!(runtime.sleeps(), vec![Duration::from_secs(10)]);
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    assert_mutable_job_scopes_absent(&store, &lease);
}

#[test]
fn term_disappearance_still_waits_full_grace_and_skips_kill_only_after_exact_absence() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("orphan-term-only");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let supervisor = identity(84_001);
    let child = identity(84_002);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(supervisor, 11)
        .unwrap()
        .with_child(child, 12)
        .unwrap()
        .into_running(13)
        .unwrap();
    install_job_status(&store, &lease, &status, true);
    let runtime = ScriptedReconciliation::new(
        [
            ProcessObservation::Absent,
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
            ProcessObservation::Absent,
        ],
        [ProcessGroupObservation::Absent],
    );

    JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
        .status(lease.job_id())
        .unwrap();

    assert_eq!(runtime.signals(), vec![(child.pid(), libc::SIGTERM)]);
    assert_eq!(runtime.sleeps(), vec![Duration::from_secs(10)]);
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn kill_proof_deadline_crossing_fails_closed_without_panicking() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("kill-proof-clock-boundary");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let supervisor = identity(84_101);
    let child = identity(84_102);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(supervisor, 11)
        .unwrap()
        .with_child(child, 12)
        .unwrap()
        .into_running(13)
        .unwrap();
    install_job_status(&store, &lease, &status, true);
    let before_scopes = mutable_job_scope_presence(&store, &lease);
    let runtime = ScriptedReconciliation::new(
        [
            ProcessObservation::Absent,
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
        ],
        [
            ProcessGroupObservation::Present,
            ProcessGroupObservation::Present,
        ],
    )
    .with_clock([
        Duration::ZERO,
        Duration::ZERO,
        Duration::from_secs(10),
        Duration::from_secs(20),
        Duration::from_millis(29_999),
        Duration::from_millis(30_001),
    ]);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
            .status(lease.job_id())
    }));

    assert!(result.is_ok(), "KILL proof deadline crossing panicked");
    let error = result.unwrap().unwrap_err();
    assert_error_code(error, "RECONCILIATION_AMBIGUOUS", "KILL proof deadline");
    assert_eq!(
        runtime.signals(),
        vec![(child.pid(), libc::SIGTERM), (child.pid(), libc::SIGKILL)]
    );
    assert_eq!(
        runtime.sleeps(),
        vec![Duration::from_secs(10), Duration::from_millis(1)]
    );
    assert_eq!(read_job_status(&store, &lease), status);
    assert_eq!(mutable_job_scope_presence(&store, &lease), before_scopes);
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
}

#[test]
fn unsafe_post_term_transitions_never_kill_mutate_or_release() {
    let cases = [
        (
            "leader-absent-group-present",
            ProcessObservation::Absent,
            Some(ProcessGroupObservation::Present),
        ),
        (
            "leader-absent-group-ambiguous",
            ProcessObservation::Absent,
            Some(ProcessGroupObservation::Ambiguous),
        ),
        ("reused", ProcessObservation::Reused, None),
        (
            "wrong-pgid",
            ProcessObservation::Matching {
                process_group: 99_998,
            },
            None,
        ),
        ("ambiguous", ProcessObservation::Ambiguous, None),
    ];

    for (index, (label, transition, group)) in cases.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("post-term-transition-{index}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let supervisor = identity(84_201);
        let child = identity(84_202);
        let status = JobStatus::accepted(10)
            .unwrap()
            .with_supervisor(supervisor, 11)
            .unwrap()
            .with_child(child, 12)
            .unwrap()
            .into_running(13)
            .unwrap();
        install_job_status(&store, &lease, &status, true);
        let before_scopes = mutable_job_scope_presence(&store, &lease);
        let runtime = ScriptedReconciliation::new(
            [
                ProcessObservation::Absent,
                ProcessObservation::Matching {
                    process_group: child.pid(),
                },
                transition,
            ],
            group,
        );

        let error =
            JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
                .status(lease.job_id())
                .unwrap_err();

        assert_error_code(error, "RECONCILIATION_AMBIGUOUS", label);
        assert_eq!(
            runtime.signals(),
            vec![(child.pid(), libc::SIGTERM)],
            "{label}"
        );
        assert_eq!(runtime.sleeps(), vec![Duration::from_secs(10)], "{label}");
        assert_eq!(read_job_status(&store, &lease), status, "{label}");
        assert_eq!(
            mutable_job_scope_presence(&store, &lease),
            before_scopes,
            "{label}"
        );
        assert_eq!(
            LeaseService::new(&store).load().unwrap(),
            Some(lease),
            "{label}"
        );
    }
}

#[test]
fn child_identity_or_group_ambiguity_never_signals_cleans_or_releases() {
    let cases = [
        (
            "wrong-pgid",
            ProcessObservation::Matching {
                process_group: 99_999,
            },
            None,
        ),
        ("reused", ProcessObservation::Reused, None),
        ("ambiguous", ProcessObservation::Ambiguous, None),
        (
            "leader-absent-group-present",
            ProcessObservation::Absent,
            Some(ProcessGroupObservation::Present),
        ),
        (
            "leader-absent-group-ambiguous",
            ProcessObservation::Absent,
            Some(ProcessGroupObservation::Ambiguous),
        ),
    ];

    for (index, (label, child_observation, group_observation)) in cases.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("child-ambiguity-{index}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let supervisor = identity(85_001);
        let child = identity(85_002);
        let status = JobStatus::accepted(10)
            .unwrap()
            .with_supervisor(supervisor, 11)
            .unwrap()
            .with_child(child, 12)
            .unwrap()
            .into_running(13)
            .unwrap();
        install_job_status(&store, &lease, &status, true);
        let runtime = ScriptedReconciliation::new(
            [ProcessObservation::Absent, child_observation],
            group_observation,
        );

        let error =
            JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
                .status(lease.job_id())
                .unwrap_err();

        assert_error_code(error, "RECONCILIATION_AMBIGUOUS", label);
        assert_eq!(runtime.signals(), Vec::<(u32, i32)>::new(), "{label}");
        assert_eq!(
            LeaseService::new(&store).load().unwrap(),
            Some(lease.clone())
        );
        assert_eq!(read_job_status(&store, &lease), status);
        assert!(
            store
                .job(lease.project_id(), lease.worktree_id(), lease.job_id())
                .unwrap()
                .join("workspace")
                .is_dir(),
            "{label}"
        );
    }
}

#[test]
fn terminal_status_with_a_retained_exact_lease_resumes_cleanup_without_signalling() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("terminal-cleanup-resume");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(identity(86_001), 11)
        .unwrap()
        .with_child(identity(86_002), 12)
        .unwrap()
        .into_running(13)
        .unwrap()
        .into_succeeded(14, 0, 0)
        .unwrap();
    install_job_status(&store, &lease, &status, true);
    let runtime = ScriptedReconciliation::new([], []);

    let response =
        JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
            .status(lease.job_id())
            .unwrap();

    assert_eq!(response.status(), &status);
    assert_eq!(runtime.signals(), Vec::<(u32, i32)>::new());
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    assert_mutable_job_scopes_absent(&store, &lease);
}

#[test]
fn cleanup_proof_failure_keeps_the_exact_lease_for_idempotent_terminal_retry() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("cleanup-proof-failure");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(identity(87_001), 11)
        .unwrap();
    install_job_status(&store, &lease, &status, false);
    drop(store);
    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterJobCleanupProof).unwrap();
    let runtime = ScriptedReconciliation::new([ProcessObservation::Absent], []);

    let error = JobService::new_with_reconciliation(&faulted, &RejectLauncher, Arc::new(runtime))
        .status(lease.job_id())
        .unwrap_err();

    assert!(error.to_string().contains("cleanup-proof"), "{error}");
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(lease.clone())
    );
    assert_eq!(read_job_status(&faulted, &lease).state(), JobState::Lost);

    let reopened = HostStore::open(&root).unwrap();
    let response = JobService::new_with_reconciliation(
        &reopened,
        &RejectLauncher,
        Arc::new(ScriptedReconciliation::new([], [])),
    )
    .status(lease.job_id())
    .unwrap();
    assert_eq!(response.status().state(), JobState::Lost);
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), None);
}

#[test]
fn cleanup_and_release_failures_return_promptly_under_enrichment_contention() {
    let cases = [
        (
            "cleanup",
            HostStoreWritePoint::AfterJobCleanupProof,
            "injected cleanup-proof crash boundary",
        ),
        (
            "release",
            HostStoreWritePoint::BeforeJobLeaseRetirement,
            "injected lease retirement failure",
        ),
    ];

    for (index, (label, fault, primary_error)) in cases.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp
            .path()
            .join(format!("cleanup-enrichment-contention-{index}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let status = JobStatus::accepted(10)
            .unwrap()
            .with_supervisor(identity(87_101), 11)
            .unwrap();
        install_job_status(&store, &lease, &status, false);
        drop(store);
        let (gate, acquired_rx, release_tx, contender) =
            queue_supervisor_lock_contender(&root, lease.job_id());
        let runtime = ScriptedReconciliation::new([ProcessObservation::Absent], [])
            .with_first_observation_gate(gate);
        let status_root = root.clone();
        let job_id = lease.job_id();
        let (result_tx, result_rx) = mpsc::channel();
        let status_thread = thread::spawn(move || {
            let faulted = HostStore::open_with_write_fault(&status_root, fault).unwrap();
            let result =
                JobService::new_with_reconciliation(&faulted, &RejectLauncher, Arc::new(runtime))
                    .status(job_id);
            result_tx.send(result).unwrap();
        });

        acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("supervisor contender did not acquire after reconciliation");
        let cleanup_marker = root
            .join("locks/jobs")
            .join(lease.job_id().to_string())
            .join("cleanup-complete.json");
        let marker_deadline = Instant::now() + Duration::from_secs(2);
        while !cleanup_marker.exists() {
            assert!(
                Instant::now() < marker_deadline,
                "{label} did not publish its durable cleanup marker"
            );
            thread::sleep(Duration::from_millis(5));
        }
        let started = Instant::now();
        let prompt = result_rx.recv_timeout(Duration::from_millis(500));
        release_tx.send(()).unwrap();
        contender.join().unwrap();
        let result = match prompt {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = result_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("status remained blocked after releasing the contender");
                status_thread.join().unwrap();
                panic!("{label} enrichment blocked on a busy supervisor lock");
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                status_thread.join().unwrap();
                panic!("{label} status thread disconnected without a result");
            }
        };
        status_thread.join().unwrap();

        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{label} did not return within the contention bound"
        );
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains(primary_error),
            "{label} primary error was replaced: {error}"
        );
        let reopened = HostStore::open(&root).unwrap();
        let durable = read_job_status(&reopened, &lease);
        assert_eq!(durable.state(), JobState::Lost, "{label}");
        assert_eq!(durable.error_code(), Some("SUPERVISOR_LOST"), "{label}");
        assert_eq!(durable.cleanup_error_code(), None, "{label}");
        assert_mutable_job_scopes_absent(&reopened, &lease);
        assert_eq!(
            LeaseService::new(&reopened).load().unwrap(),
            Some(lease),
            "{label}"
        );
    }
}

fn assert_primary_error_survives_uncontended_typed_enrichment_failure(
    label: &str,
    primary_fault: HostStoreWritePoint,
    expected_primary: &str,
) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp
        .path()
        .join(format!("primary-error-enrichment-{label}"));
    let (store, lease, _request) = indexed_identityless_job(&root);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(identity(87_201), 11)
        .unwrap()
        .with_child(identity(87_202), 12)
        .unwrap()
        .into_running(13)
        .unwrap()
        .into_succeeded(14, 0, 0)
        .unwrap();
    install_job_status(&store, &lease, &status, true);
    drop(store);
    let faulted = HostStore::open_with_write_faults(
        &root,
        primary_fault,
        HostStoreWritePoint::BeforeJobStatusReplace,
    )
    .unwrap();

    let started = Instant::now();
    let error = JobService::new_with_reconciliation(
        &faulted,
        &RejectLauncher,
        Arc::new(ScriptedReconciliation::new([], [])),
    )
    .status(lease.job_id())
    .unwrap_err();

    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{label} primary failure did not return within the bound"
    );
    let rendered = error.to_string();
    assert_eq!(rendered.as_bytes(), expected_primary.as_bytes(), "{label}");
    assert_eq!(error.exit_code(), 74, "{label}");
    assert!(
        !rendered.contains("injected status replacement failure"),
        "{label} enrichment error replaced the primary error"
    );
    assert_eq!(read_job_status(&faulted, &lease), status, "{label}");
    assert_mutable_job_scopes_absent(&faulted, &lease);
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(lease),
        "{label}"
    );
}

#[test]
fn cleanup_primary_error_survives_uncontended_typed_enrichment_failure() {
    assert_primary_error_survives_uncontended_typed_enrichment_failure(
        "cleanup",
        HostStoreWritePoint::AfterJobCleanupProof,
        "I/O error: injected cleanup-proof crash boundary",
    );
}

#[test]
fn release_primary_error_survives_uncontended_typed_enrichment_failure() {
    assert_primary_error_survives_uncontended_typed_enrichment_failure(
        "release",
        HostStoreWritePoint::BeforeJobLeaseRetirement,
        "I/O error: injected lease retirement failure",
    );
}

#[test]
fn reconciliation_status_publication_failure_retains_the_lease_and_unmodified_status() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("status-publication-failure");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(identity(88_001), 11)
        .unwrap();
    install_job_status(&store, &lease, &status, false);
    drop(store);
    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::BeforeJobStatusReplace)
            .unwrap();
    let runtime = ScriptedReconciliation::new([ProcessObservation::Absent], []);

    let error = JobService::new_with_reconciliation(&faulted, &RejectLauncher, Arc::new(runtime))
        .status(lease.job_id())
        .unwrap_err();

    assert!(error.to_string().contains("status replacement"), "{error}");
    assert_eq!(read_job_status(&faulted, &lease), status);
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(lease.clone())
    );
}

#[test]
fn lease_retirement_failure_happens_after_exact_mutable_cleanup_and_retains_the_lease() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("lease-retirement-failure");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(identity(89_001), 11)
        .unwrap()
        .with_child(identity(89_002), 12)
        .unwrap()
        .into_running(13)
        .unwrap()
        .into_succeeded(14, 0, 0)
        .unwrap();
    install_job_status(&store, &lease, &status, true);
    drop(store);
    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::BeforeJobLeaseRetirement)
            .unwrap();

    let error = JobService::new_with_reconciliation(
        &faulted,
        &RejectLauncher,
        Arc::new(ScriptedReconciliation::new([], [])),
    )
    .status(lease.job_id())
    .unwrap_err();

    assert!(error.to_string().contains("lease retirement"), "{error}");
    assert_mutable_job_scopes_absent(&faulted, &lease);
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(lease.clone())
    );
    assert_eq!(
        read_job_status(&faulted, &lease).cleanup_error_code(),
        Some("LEASE_RELEASE_FAILED")
    );
}

#[test]
fn payload_and_log_binding_failures_preserve_evidence_and_the_exact_lease() {
    for failure in ["payload", "stdout"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("binding-failure-{failure}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let supervisor = identity(90_001);
        let child = identity(90_002);
        let status = if failure == "payload" {
            JobStatus::accepted(10)
                .unwrap()
                .with_supervisor(supervisor, 11)
                .unwrap()
        } else {
            JobStatus::accepted(10)
                .unwrap()
                .with_supervisor(supervisor, 11)
                .unwrap()
                .with_child(child, 12)
                .unwrap()
                .into_running(13)
                .unwrap()
        };
        install_job_status(&store, &lease, &status, failure == "stdout");
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        let attacked = job.join(if failure == "payload" {
            "execution.json"
        } else {
            "stdout.log"
        });
        remove_and_sync(&attacked);
        let outside = temp.path().join(format!("outside-{failure}"));
        fs::write(&outside, b"preserve").unwrap();
        symlink(&outside, &attacked).unwrap();
        File::open(&job).unwrap().sync_all().unwrap();
        let runtime = if failure == "payload" {
            ScriptedReconciliation::new([ProcessObservation::Absent], [])
        } else {
            ScriptedReconciliation::new(
                [ProcessObservation::Absent, ProcessObservation::Absent],
                [ProcessGroupObservation::Absent],
            )
        };

        JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime))
            .status(lease.job_id())
            .unwrap_err();

        assert_eq!(fs::read(&outside).unwrap(), b"preserve", "{failure}");
        assert_eq!(read_job_status(&store, &lease), status, "{failure}");
        assert_eq!(
            LeaseService::new(&store).load().unwrap(),
            Some(lease.clone()),
            "{failure}"
        );
    }
}

fn endpoint_runtime_and_completed_job(
    temp: &tempfile::TempDir,
) -> (RuntimeContext, SubmitRequest, StatusResponse) {
    let home = temp.path().join("home");
    fs::create_dir(&home).unwrap();
    let environment = BTreeMap::from([("HOME".into(), home.as_os_str().to_os_string())]);
    let paths = PathLayout::discover(None, &environment, &home).unwrap();
    let (store, _lease, submit) = prepared_host_with_command(
        &paths.host_state_root(),
        CommandSpec::argv(vec!["/usr/bin/printf".into(), "endpoint-log".into()]).unwrap(),
    );
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let completed = JobService::new(&store, &launcher)
        .submit_at(submit.clone(), 10)
        .unwrap();
    let response = StatusResponse::new(
        match &completed {
            mac_worker::job::SubmitResponse::Accepted { meta, .. } => (**meta).clone(),
            mac_worker::job::SubmitResponse::Existing { .. } => unreachable!(),
        },
        completed.status().clone(),
    )
    .unwrap();
    (
        RuntimeContext::isolated(environment, home, temp.path().to_path_buf()),
        submit,
        response,
    )
}

fn run_query_endpoint(
    runtime: &RuntimeContext,
    command: &str,
    input: Vec<u8>,
) -> (u8, Vec<u8>, Vec<u8>) {
    let cli = Cli::try_parse_from([
        "worker",
        "--config",
        "/definitely/missing/inventory.toml",
        "host",
        command,
    ])
    .unwrap();
    let mut stdin = Cursor::new(input);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        &SystemProcessRunner,
        runtime,
        &mut stdin,
        &mut stdout,
        &mut stderr,
    );
    (exit, stdout, stderr)
}

#[test]
fn hidden_query_endpoints_are_argument_free_and_return_one_canonical_typed_line() {
    // Break caught: a query endpoint loads the missing inventory, accepts a
    // positional identity, emits stderr, or returns an unversioned envelope.
    let temp = tempfile::tempdir().unwrap();
    let (runtime, submit, expected_status) = endpoint_runtime_and_completed_job(&temp);
    let job_id = submit.material().job_id();
    let resolve = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
    let expected_log = LogChunkResponse::new(
        LogChunk::new(LogStream::Stdout, 0, b"endpoint-log".to_vec()).unwrap(),
    )
    .unwrap();
    let expected_resolve =
        mac_worker::job::ResolveOrAbandonResponse::accepted(expected_status.clone()).unwrap();
    let cases = [
        (
            "status",
            serde_json::to_vec(&StatusRequest::new(job_id)).unwrap(),
            serde_json::to_vec(&expected_status).unwrap(),
        ),
        (
            "log-chunk",
            serde_json::to_vec(&LogChunkRequest::new(job_id, LogStream::Stdout, 0, 64)).unwrap(),
            serde_json::to_vec(&expected_log).unwrap(),
        ),
        (
            "resolve-or-abandon",
            serde_json::to_vec(&resolve).unwrap(),
            serde_json::to_vec(&expected_resolve).unwrap(),
        ),
    ];

    for (command, input, expected) in cases {
        assert!(Cli::try_parse_from(["worker", "host", command]).is_ok());
        assert!(Cli::try_parse_from(["worker", "host", command, "planted-argument"]).is_err());
        let (exit, stdout, stderr) = run_query_endpoint(&runtime, command, input);
        assert_eq!(exit, 0, "{command}: {}", String::from_utf8_lossy(&stdout));
        assert!(stderr.is_empty(), "{command}");
        assert_eq!(stdout.last(), Some(&b'\n'), "{command}");
        assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
        let mut expected = expected;
        expected.push(b'\n');
        assert_eq!(stdout, expected, "{command}");
    }
}

#[test]
fn every_query_endpoint_rejects_noncanonical_or_unbounded_input_with_typed_stdout_only() {
    // Break caught: one endpoint accepts a duplicate/unknown/version/trailing
    // request shape or reflects malformed/oversized input through stderr.
    let temp = tempfile::tempdir().unwrap();
    let (runtime, submit, _) = endpoint_runtime_and_completed_job(&temp);
    let job_id = submit.material().job_id();
    let resolve = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
    let valid = [
        (
            "status",
            serde_json::to_vec(&StatusRequest::new(job_id)).unwrap(),
        ),
        (
            "log-chunk",
            serde_json::to_vec(&LogChunkRequest::new(job_id, LogStream::Stdout, 0, 64)).unwrap(),
        ),
        ("resolve-or-abandon", serde_json::to_vec(&resolve).unwrap()),
    ];
    for (command, valid) in valid {
        let mut wrong_version: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        wrong_version["protocol_version"] = serde_json::json!(999);
        let mut unknown: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        unknown["unknown"] = serde_json::json!(true);
        let mut duplicate = valid.clone();
        duplicate.splice(1..1, b"\"protocol_version\":2,".iter().copied());
        let invalid = vec![
            Vec::new(),
            b"{".to_vec(),
            b"[]".to_vec(),
            serde_json::to_vec(&wrong_version).unwrap(),
            serde_json::to_vec(&unknown).unwrap(),
            duplicate,
            [valid.as_slice(), b"{}"].concat(),
            [b" ".as_slice(), valid.as_slice()].concat(),
            vec![b'x'; 1024 * 1024 + 1],
        ];
        for input in invalid {
            let (exit, stdout, stderr) = run_query_endpoint(&runtime, command, input);
            assert_ne!(exit, 0, "{command}");
            assert!(stderr.is_empty(), "{command}: stderr was not empty");
            assert_eq!(stdout.last(), Some(&b'\n'), "{command}");
            assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
            let error: HostControlError = serde_json::from_slice(&stdout).unwrap();
            assert_eq!(error.error().code(), "INVALID_REQUEST", "{command}");
            let mut canonical = serde_json::to_vec(&error).unwrap();
            canonical.push(b'\n');
            assert_eq!(stdout, canonical, "{command}");
        }
    }
}

#[test]
fn query_endpoint_service_errors_are_versioned_and_broken_stdout_is_io_exit_74() {
    // Break caught: stable host authority is hidden behind INVALID_REQUEST or
    // a broken stdout incorrectly reports the underlying service result.
    let temp = tempfile::tempdir().unwrap();
    let (runtime, submit, _) = endpoint_runtime_and_completed_job(&temp);
    let missing = JobId::new(uuid::Uuid::from_u128(999_999));
    let (exit, stdout, stderr) = run_query_endpoint(
        &runtime,
        "status",
        serde_json::to_vec(&StatusRequest::new(missing)).unwrap(),
    );
    assert_ne!(exit, 0);
    assert!(stderr.is_empty());
    let error: HostControlError = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(error.error().code(), "JOB_NOT_FOUND");

    struct BrokenStdout;
    impl std::io::Write for BrokenStdout {
        fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "planted broken stdout",
            ))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "planted broken stdout",
            ))
        }
    }
    let cli = Cli::try_parse_from(["worker", "host", "status"]).unwrap();
    let mut stdin =
        Cursor::new(serde_json::to_vec(&StatusRequest::new(submit.material().job_id())).unwrap());
    let mut stdout = BrokenStdout;
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        &SystemProcessRunner,
        &runtime,
        &mut stdin,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
}

fn lease_request() -> LeaseAcquireRequest {
    LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            JOB_ID.parse().unwrap(),
            CLIENT_ID.parse().unwrap(),
            LEASE_TOKEN.parse().unwrap(),
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            MANIFEST_DIGEST.into(),
            String::new(),
            30_000,
            "heavy".into(),
            CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
        )
        .unwrap(),
    )
}

fn healthy() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 250 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

fn valid_manifest_bytes() -> Vec<u8> {
    format!(
        concat!(
            r#"{{"version":1,"project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""head":null,"branch":null,"dirty":false,"relative_working_dir":"","#,
            r#""entries":[{{"path":"payload.txt","kind":"file","mode":420,"size":7,"#,
            r#""sha256":"239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5","#,
            r#""symlink_target":null}}],"tracked_deletions":[]}}"#,
        ),
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
    )
    .into_bytes()
}

fn prepared_host(root: &Path) -> (HostStore, LeaseRecord, SubmitRequest) {
    prepared_host_with_command(
        root,
        CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
    )
}

fn prepared_host_with_command(
    root: &Path,
    command: CommandSpec,
) -> (HostStore, LeaseRecord, SubmitRequest) {
    let store = HostStore::open(root).unwrap();
    let manifest = valid_manifest_bytes();
    let digest = format!("{:x}", Sha256::digest(&manifest));
    let acquire = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            JOB_ID.parse().unwrap(),
            CLIENT_ID.parse().unwrap(),
            LEASE_TOKEN.parse().unwrap(),
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            digest.clone(),
            String::new(),
            30_000,
            "heavy".into(),
            command,
        )
        .unwrap(),
    );
    let lease = match LeaseService::new(&store)
        .acquire(&acquire, &healthy(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    fs::create_dir_all(incoming.join("tree")).unwrap();
    for private in [
        incoming.parent().unwrap().parent().unwrap(),
        incoming.parent().unwrap(),
        incoming.as_path(),
    ] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(incoming.join("manifest.json"), manifest).unwrap();
    fs::write(incoming.join("tree/payload.txt"), b"payload").unwrap();
    fs::set_permissions(
        incoming.join("manifest.json"),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    fs::set_permissions(
        incoming.join("tree/payload.txt"),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    fs::set_permissions(incoming.join("tree"), fs::Permissions::from_mode(0o555)).unwrap();
    RemoteSnapshotService::new(&store)
        .verify_and_promote_at(&lease, &digest, 2)
        .unwrap();
    (store, lease, SubmitRequest::new(acquire.material().clone()))
}

fn indexed_identityless_job(root: &Path) -> (HostStore, LeaseRecord, SubmitRequest) {
    let (store, lease, request) = prepared_host(root);
    drop(store);
    let faulted =
        HostStore::open_with_write_fault(root, HostStoreWritePoint::AfterJobIndexParentSync)
            .unwrap();
    let result = JobService::new(&faulted, &RejectLauncher).submit_at(request.clone(), 10);
    assert!(result.is_err());
    assert!(faulted.job_index(lease.job_id()).unwrap().is_file());
    drop(faulted);
    (HostStore::open(root).unwrap(), lease, request)
}

fn unindexed_identityless_job(root: &Path) -> (HostStore, LeaseRecord, SubmitRequest) {
    let (store, lease, request) = prepared_host(root);
    drop(store);
    let faulted =
        HostStore::open_with_write_fault(root, HostStoreWritePoint::AfterJobPublish).unwrap();
    let result = JobService::new(&faulted, &RejectLauncher).submit_at(request.clone(), 10);
    assert!(result.is_err());
    assert!(!faulted.job_index(lease.job_id()).unwrap().exists());
    assert!(
        faulted
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap()
            .is_dir()
    );
    drop(faulted);
    (HostStore::open(root).unwrap(), lease, request)
}

fn install_job_status(
    store: &HostStore,
    lease: &LeaseRecord,
    status: &JobStatus,
    erase_payload: bool,
) {
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    replace_json(&job.join("status.json"), status).unwrap();
    if erase_payload {
        remove_and_sync(&job.join("execution.json"));
    }
}

fn read_job_status(store: &HostStore, lease: &LeaseRecord) -> JobStatus {
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap()
}

fn assert_mutable_job_scopes_absent(store: &HostStore, lease: &LeaseRecord) {
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    for name in ["workspace", "home", "tmp", "execution.json"] {
        assert!(!job.join(name).exists(), "mutable scope {name} remains");
    }
}

fn mutable_job_scope_presence(store: &HostStore, lease: &LeaseRecord) -> Vec<(String, bool)> {
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    ["workspace", "home", "tmp", "execution.json"]
        .into_iter()
        .map(|name| (name.into(), job.join(name).exists()))
        .collect()
}

fn replace_json<T: serde::Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    let parent = path.parent().unwrap();
    let temporary = parent.join(format!(".query-test-{}", uuid::Uuid::new_v4().simple()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    let bytes = serde_json::to_vec(value).map_err(std::io::Error::other)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()
}

fn replace_bytes(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap();
    let temporary = parent.join(format!(".query-test-{}", uuid::Uuid::new_v4().simple()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()
}

fn replace_ascii_once(path: &Path, from: &str, to: &str) {
    assert_eq!(from.len(), to.len());
    let mut bytes = fs::read(path).unwrap();
    let offset = bytes
        .windows(from.len())
        .position(|window| window == from.as_bytes())
        .unwrap_or_else(|| panic!("{from:?} is absent from {}", path.display()));
    bytes[offset..offset + from.len()].copy_from_slice(to.as_bytes());
    replace_bytes(path, &bytes).unwrap();
}

fn remove_and_sync(path: &Path) {
    fs::remove_file(path).unwrap();
    File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}

fn assert_error_code(error: mac_worker::error::WorkerError, expected: &str, context: &str) {
    assert!(
        error.to_string().contains(expected),
        "{context}: expected {expected}, got {error}"
    );
}

#[allow(dead_code)]
fn identity(pid: u32) -> ProcessIdentity {
    ProcessIdentity::new(pid, u64::from(pid) * 1_000).unwrap()
}
