use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    ffi::{CString, OsStr, OsString},
    fs::{self, File, OpenOptions},
    io::{Cursor, Write as _},
    os::fd::AsRawFd,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt, symlink},
        process::ExitStatusExt,
    },
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
    config::WorkerEntry,
    error::WorkerError,
    failure_receipt::{RESIDUAL_LEASE, RESIDUAL_WORKSPACE, STAGE_CLEANUP, STAGE_DRAIN},
    host_store::{HostStore, HostStoreWritePoint, JobDisposition, SupervisorGuard},
    inputs::RelativePath,
    job::{
        CancelRequest, CancelResponse, ClientId, CommandSpec, FleetReconcileJobResult,
        FleetReconcileRequest, FleetReconcileResponse, HostControlError, JobId, JobMeta, JobState,
        JobStatus, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord, LeaseToken, LogChunk,
        LogChunkRequest, LogChunkResponse, LogCursor, LogStream, ProcessIdentity,
        RequestFingerprintMaterial, ResolveOrAbandonOutcome, ResolveOrAbandonRequest,
        ResolveOrAbandonResponse, StatusRequest, StatusResponse, SubmitRequest, SubmitResponse,
        TerminalLogDrain,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    lease::{AdmissionFacts, LeaseService},
    paths::PathLayout,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    protocol::MemoryPressure,
    remote_snapshot::RemoteSnapshotService,
    rooted_fs::RootedDir,
    run_with_stdio_in_context,
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
        ReconciliationRuntime, Supervisor, SystemProcessInspector,
    },
    transfer::{
        HostOperation, HostTransferService, RemoteJobClient, ResolutionRuntime,
        RsyncServerExecutor, RsyncServerInvocation, SshJsonTransport, TransferIdentity,
    },
};
use sha2::{Digest, Sha256};

const JOB_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const CLIENT_ID: &str = "102f0f4a6b5c7d8e9f00112233445566";
const LEASE_TOKEN: &str = "202f0f4a6b5c7d8e9f00112233445566";
const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MANIFEST_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
// These tests exercise real filesystem cleanup alongside a 100-row matrix.
// Keep the prompt post-marker contention assertion tight, but allow the
// pre-marker progress checks to tolerate normal parallel-test scheduling.
const CLEANUP_PROGRESS_DEADLINE: Duration = Duration::from_secs(10);

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
    fail_signal: Mutex<Option<i32>>,
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
                fail_signal: Mutex::new(None),
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

    fn with_signal_failure(self, signal: i32) -> Self {
        *self.inner.fail_signal.lock().unwrap() = Some(signal);
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
        let mut fail_signal = self.inner.fail_signal.lock().unwrap();
        if fail_signal
            .as_ref()
            .is_some_and(|expected| *expected == signal)
        {
            *fail_signal = None;
            return Err(mac_worker::error::WorkerError::Protocol(format!(
                "injected signal failure {signal}"
            )));
        }
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

fn wait_until_admission_is_busy(root: &Path, job_id: JobId) {
    let lock = root
        .join("locks/jobs")
        .join(job_id.to_string())
        .join("admission.lock");
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock)
            .expect("admission lock must exist");
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == -1 {
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::EWOULDBLOCK),
                "admission lock probe failed unexpectedly"
            );
            return;
        }
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) }, 0);
        if Instant::now() >= deadline {
            panic!("cancellation never entered its admission validation section");
        }
        thread::yield_now();
    }
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
fn cancel_fences_an_accepted_job_without_a_child_then_cleans_and_releases() {
    // Break caught: cancellation tries to signal an unrecorded launch child,
    // allows a later launcher to escape the fence, or releases capacity before
    // the durable Cancelled status and mutable payload cleanup.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("cancel-prelaunch");
    let (store, lease, _submit) = indexed_identityless_job(&root);
    let request = CancelRequest::new(
        lease.job_id(),
        lease.client_id(),
        lease.lease_token(),
        lease.request_fingerprint().clone(),
    )
    .unwrap();

    let response = JobService::new(&store, &RejectLauncher)
        .cancel(request)
        .unwrap();

    assert_eq!(response.status().status().state(), JobState::Cancelled);
    assert_eq!(response.status().status().child_identity(), None);
    assert_eq!(
        response.status().status().error_code(),
        Some("CANCELLED_PRELAUNCH")
    );
    assert_mutable_job_scopes_absent(&store, &lease);
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn cancel_waits_for_a_held_supervisor_without_retaining_admission() {
    // Break caught: a pre-Running supervisor needs admission to finish its
    // own terminal cleanup, while cancellation waits for its guard. Holding
    // admission during that wait creates a lock inversion and loses the
    // cancellation handoff.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("cancel-supervisor-handoff");
    let (store, lease, _submit) = indexed_identityless_job(&root);
    let job_id = lease.job_id();
    let request = CancelRequest::new(
        lease.job_id(),
        lease.client_id(),
        lease.lease_token(),
        lease.request_fingerprint().clone(),
    )
    .unwrap();

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let launching_store = store.clone();
    let job_path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = BlockingLauncher {
        launches: Arc::new(AtomicUsize::new(0)),
        job_path,
        identity: identity(90_101),
        entered: entered_tx,
        release: Mutex::new(release_rx),
    };
    let launch_job_id = job_id;
    let launching_status =
        thread::spawn(move || JobService::new(&launching_store, &launcher).status(launch_job_id));
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("pre-Running supervisor did not retain its guard");

    let cancel_store = store.clone();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let canceller = thread::spawn(move || {
        cancel_tx
            .send(JobService::new(&cancel_store, &RejectLauncher).cancel(request))
            .expect("cancellation result receiver must remain live");
    });

    wait_until_admission_is_busy(&root, job_id);
    let probe_root = root.clone();
    let (probe_tx, probe_rx) = mpsc::channel();
    let probe = thread::spawn(move || {
        let admission = OpenOptions::new()
            .read(true)
            .write(true)
            .open(
                probe_root
                    .join("locks/jobs")
                    .join(job_id.to_string())
                    .join("admission.lock"),
            )
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(admission.as_raw_fd(), libc::LOCK_EX) },
            0
        );
        probe_tx.send(()).unwrap();
        assert_eq!(
            unsafe { libc::flock(admission.as_raw_fd(), libc::LOCK_UN) },
            0
        );
    });
    let released_admission_before_guard = probe_rx.recv_timeout(Duration::from_secs(1)).is_ok();

    release_tx.send(()).unwrap();
    let response = cancel_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("cancellation must finish after the supervisor handoff")
        .unwrap();
    canceller.join().unwrap();
    probe.join().unwrap();
    launching_status.join().unwrap().unwrap();

    assert!(
        released_admission_before_guard,
        "cancellation retained admission while waiting for the supervisor guard"
    );
    assert_eq!(response.status().status().state(), JobState::Cancelled);
    assert_eq!(read_job_status(&store, &lease).state(), JobState::Cancelled);
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn cancel_terms_then_kills_only_the_exact_recorded_group_before_cleanup() {
    // Break caught: cancellation relies on supervisor absence, signals an
    // unrecorded/reused process, skips the full TERM grace, or frees capacity
    // before the exact group and mutable scopes are proven gone.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("cancel-term-kill");
    let (store, lease, request, _status, child) = indexed_running_job(&root, 90_201, 90_202);
    let runtime = ScriptedReconciliation::new(
        [
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
            .cancel(request.clone())
            .unwrap();

    assert_eq!(response.status().status().state(), JobState::Cancelled);
    assert_eq!(response.status().status().error_code(), Some("CANCELLED"));
    assert_eq!(response.status().status().cleanup_error_code(), None);
    assert_eq!(
        runtime.signals(),
        vec![(child.pid(), libc::SIGTERM), (child.pid(), libc::SIGKILL)]
    );
    assert_eq!(runtime.sleeps(), vec![Duration::from_secs(10)]);
    assert_mutable_job_scopes_absent(&store, &lease);
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);

    let retry = ScriptedReconciliation::new([], []);
    let repeated =
        JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(retry.clone()))
            .cancel(request)
            .unwrap();
    assert_eq!(repeated.status().status().state(), JobState::Cancelled);
    assert!(retry.signals().is_empty(), "terminal retry must not signal");
    assert_eq!(repeated.status().status().cleanup_error_code(), None);
    assert_eq!(
        read_job_status(&store, &lease).cleanup_error_code(),
        None,
        "a second cancel after a successful release must not stamp LEASE_RELEASE_FAILED"
    );
    let receipt = store.cleanup_job_owned(&lease).unwrap();
    LeaseService::new(&store)
        .release_after_cleanup(&lease, &receipt)
        .unwrap();
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    assert_eq!(read_job_status(&store, &lease).cleanup_error_code(), None);
}

#[test]
fn overlapping_cancels_do_not_record_lease_release_failed_after_the_slot_is_free() {
    // Break caught: two cancels in quick succession both capture the live
    // lease, the winner retires it, and the loser stamps LEASE_RELEASE_FAILED
    // even though workers() would already show idle.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("overlap-cancel-release");
    let (store, lease, request, status, _child) = indexed_running_job(&root, 90_401, 90_402);
    let cancelled = status
        .into_infrastructure_terminal(JobState::Cancelled, 14, 0, 0, "CANCELLED".into())
        .unwrap();
    install_job_status(&store, &lease, &cancelled, false);
    let start = Arc::new(Barrier::new(2));
    let (tx_a, rx_a) = mpsc::channel();
    let (tx_b, rx_b) = mpsc::channel();
    let root_a = root.clone();
    let root_b = root.clone();
    let request_a = request.clone();
    let request_b = request;
    let start_a = Arc::clone(&start);
    let handle_a = thread::spawn(move || {
        let store = HostStore::open(&root_a).unwrap();
        start_a.wait();
        let _ = tx_a.send(
            JobService::new_with_reconciliation(
                &store,
                &RejectLauncher,
                Arc::new(ScriptedReconciliation::new([], [])),
            )
            .cancel(request_a),
        );
    });
    let handle_b = thread::spawn(move || {
        let store = HostStore::open(&root_b).unwrap();
        start.wait();
        let _ = tx_b.send(
            JobService::new_with_reconciliation(
                &store,
                &RejectLauncher,
                Arc::new(ScriptedReconciliation::new([], [])),
            )
            .cancel(request_b),
        );
    });

    let first = rx_a
        .recv_timeout(Duration::from_secs(20))
        .expect("cancel A hung")
        .unwrap_or_else(|error| panic!("cancel A failed: {error}"));
    let second = rx_b
        .recv_timeout(Duration::from_secs(20))
        .expect("cancel B hung")
        .unwrap_or_else(|error| panic!("cancel B failed: {error}"));
    handle_a.join().expect("cancel A thread panicked");
    handle_b.join().expect("cancel B thread panicked");

    for (label, response) in [("A", &first), ("B", &second)] {
        assert_eq!(
            response.status().status().state(),
            JobState::Cancelled,
            "{label}"
        );
        assert_eq!(
            response.status().status().cleanup_error_code(),
            None,
            "{label} returned LEASE_RELEASE_FAILED after the slot was free"
        );
    }
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    assert_eq!(read_job_status(&store, &lease).cleanup_error_code(), None);
    assert_eq!(read_job_status(&store, &lease).state(), JobState::Cancelled);
}

#[test]
fn cancel_term_only_exact_absence_skips_kill_after_full_grace() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("cancel-term-only");
    let (store, lease, request, _status, child) = indexed_running_job(&root, 90_301, 90_302);
    let runtime = ScriptedReconciliation::new(
        [
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
            ProcessObservation::Absent,
        ],
        [ProcessGroupObservation::Absent],
    );

    JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
        .cancel(request)
        .unwrap();

    assert_eq!(runtime.signals(), vec![(child.pid(), libc::SIGTERM)]);
    assert_eq!(runtime.sleeps(), vec![Duration::from_secs(10)]);
    assert_mutable_job_scopes_absent(&store, &lease);
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn cancel_kill_failure_retains_status_payload_and_exact_lease_for_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("cancel-kill-failure");
    let (store, lease, request, status, child) = indexed_running_job(&root, 90_351, 90_352);
    let before_scopes = mutable_job_scope_presence(&store, &lease);
    let runtime = ScriptedReconciliation::new(
        [
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
        ],
        [],
    )
    .with_signal_failure(libc::SIGKILL);

    let error =
        JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
            .cancel(request)
            .unwrap_err();

    assert!(
        error.to_string().contains("injected signal failure"),
        "{error}"
    );
    assert_eq!(
        runtime.signals(),
        vec![(child.pid(), libc::SIGTERM), (child.pid(), libc::SIGKILL)]
    );
    assert_eq!(read_job_status(&store, &lease), status);
    assert_eq!(mutable_job_scope_presence(&store, &lease), before_scopes);
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
}

#[test]
fn cancel_never_signals_stale_reused_or_ambiguous_child_identity() {
    let cases = [
        (
            "reused",
            vec![ProcessObservation::Reused],
            Vec::<ProcessGroupObservation>::new(),
        ),
        (
            "ambiguous",
            vec![ProcessObservation::Ambiguous],
            Vec::<ProcessGroupObservation>::new(),
        ),
        (
            "wrong-group",
            vec![ProcessObservation::Matching { process_group: 1 }],
            Vec::<ProcessGroupObservation>::new(),
        ),
        (
            "leader-absent-group-live",
            vec![ProcessObservation::Absent],
            vec![ProcessGroupObservation::Present],
        ),
    ];

    for (index, (label, process, groups)) in cases.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("cancel-unsafe-child-{index}"));
        let (store, lease, request, status, _child) = indexed_running_job(
            &root,
            90_401 + index as u32 * 10,
            90_402 + index as u32 * 10,
        );
        let before_scopes = mutable_job_scope_presence(&store, &lease);
        let runtime = ScriptedReconciliation::new(process, groups);

        let error =
            JobService::new_with_reconciliation(&store, &RejectLauncher, Arc::new(runtime.clone()))
                .cancel(request)
                .unwrap_err();

        assert_error_code(error, "RECONCILIATION_AMBIGUOUS", label);
        assert!(runtime.signals().is_empty(), "{label}");
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
fn cancel_cleanup_failure_retains_the_exact_lease_for_terminal_retry() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("cancel-cleanup-failure");
    let (store, lease, request, _status, child) = indexed_running_job(&root, 90_501, 90_502);
    drop(store);
    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterJobCleanupProof).unwrap();
    let first_runtime = ScriptedReconciliation::new(
        [
            ProcessObservation::Matching {
                process_group: child.pid(),
            },
            ProcessObservation::Absent,
        ],
        [ProcessGroupObservation::Absent],
    );

    let error = JobService::new_with_reconciliation(
        &faulted,
        &RejectLauncher,
        Arc::new(first_runtime.clone()),
    )
    .cancel(request.clone())
    .unwrap_err();

    assert!(error.to_string().contains("cleanup-proof"), "{error}");
    assert_eq!(
        read_job_status(&faulted, &lease).state(),
        JobState::Cancelled
    );
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(lease.clone())
    );
    assert_eq!(first_runtime.signals(), vec![(child.pid(), libc::SIGTERM)]);
    drop(faulted);

    let reopened = HostStore::open(&root).unwrap();
    let retry_runtime = ScriptedReconciliation::new([], []);
    let response = JobService::new_with_reconciliation(
        &reopened,
        &RejectLauncher,
        Arc::new(retry_runtime.clone()),
    )
    .cancel(request)
    .unwrap();
    assert_eq!(response.status().status().state(), JobState::Cancelled);
    assert!(retry_runtime.signals().is_empty());
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), None);
}

#[test]
fn cancel_request_wire_rejects_malformed_identity_and_extra_fields() {
    let valid = serde_json::json!({
        "job_id": JOB_ID,
        "client_id": CLIENT_ID,
        "lease_token": LEASE_TOKEN,
        "request_fingerprint": "c".repeat(64),
    });
    assert!(serde_json::from_value::<CancelRequest>(valid.clone()).is_ok());

    let mut malformed_token = valid.clone();
    malformed_token["lease_token"] = serde_json::Value::String("not-a-token".into());
    assert!(serde_json::from_value::<CancelRequest>(malformed_token).is_err());

    let mut malformed_fingerprint = valid.clone();
    malformed_fingerprint["request_fingerprint"] =
        serde_json::Value::String("not-a-fingerprint".into());
    assert!(serde_json::from_value::<CancelRequest>(malformed_fingerprint).is_err());

    let mut extra_field = valid;
    extra_field["protocol_version"] = serde_json::Value::from(3);
    assert!(
        serde_json::from_value::<CancelRequest>(extra_field).is_err(),
        "fixed cancel requests must not grow a protocol envelope"
    );

    let acquire = lease_request();
    let response = CancelResponse::new(
        StatusResponse::new(
            JobMeta::new(acquire.material(), acquire.request_fingerprint().clone()).unwrap(),
            JobStatus::accepted(acquire.material().created_at_millis()).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let mut response_with_extra = serde_json::to_value(response).unwrap();
    response_with_extra["unexpected"] = serde_json::Value::Bool(true);
    assert!(serde_json::from_value::<CancelResponse>(response_with_extra).is_err());
}

#[test]
fn cancel_missing_job_is_a_typed_not_found_without_side_effects() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("cancel-missing")).unwrap();
    let acquire = lease_request();
    let request = CancelRequest::new(
        acquire.material().job_id(),
        acquire.material().client_id(),
        acquire.material().lease_token(),
        acquire.request_fingerprint().clone(),
    )
    .unwrap();

    let error = JobService::new(&store, &RejectLauncher)
        .cancel(request)
        .unwrap_err();
    assert_error_code(error, "JOB_NOT_FOUND", "not indexed");
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
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
    // Durable exact Abandoned authority is selected before lease absence.
    assert!(
        submit_error.to_string().contains("JOB_ABANDONED"),
        "{submit_error}"
    );
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
fn after_cleanup_intent_commit_incoming_token_retries_to_abandoned() {
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
    let request = ResolveOrAbandonRequest::from_submit_request(&SubmitRequest::new(
        acquire.material().clone(),
    ))
    .unwrap();
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    fs::create_dir_all(&incoming).unwrap();
    for private in [incoming.parent().unwrap(), incoming.as_path()] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    replace_bytes(&incoming.join("token-leaf"), b"incoming-token-bytes").unwrap();
    let incoming_job = incoming.parent().unwrap().to_path_buf();
    store.record_abandoned(&acquire, 2).unwrap();
    drop(store);

    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
            .unwrap();
    let pending = JobService::new(&faulted, &RejectLauncher)
        .resolve_or_abandon(request.clone())
        .unwrap();
    assert!(
        matches!(
            pending.outcome(),
            ResolveOrAbandonOutcome::CleanupPending { code }
                if code == "MUTABLE_CLEANUP_FAILED"
        ),
        "{pending:?}"
    );
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(lease.clone())
    );
    assert!(faulted.job_index(lease.job_id()).unwrap().is_file());
    assert!(!incoming.exists());
    assert_canonical_delete_journal(
        &incoming_job.join(".mac-worker-rooted-fs"),
        "cleanup-tree-v1-",
    );
    drop(faulted);

    let reopened = HostStore::open(&root).unwrap();
    let completed = JobService::new(&reopened, &RejectLauncher)
        .resolve_or_abandon(request)
        .unwrap();
    assert!(
        matches!(completed.outcome(), ResolveOrAbandonOutcome::Abandoned),
        "{completed:?}"
    );
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), None);
    assert!(!incoming.exists());
    assert_journal_empty_or_absent(&incoming_job.join(".mac-worker-rooted-fs"));
}

#[test]
fn after_cleanup_intent_commit_whole_incoming_job_terminal_retries() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, _submit) = indexed_identityless_job(&root);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(identity(96_001), 11)
        .unwrap()
        .with_child(identity(96_002), 12)
        .unwrap()
        .into_running(13)
        .unwrap()
        .into_succeeded(14, 0, 0)
        .unwrap();
    install_job_status(&store, &lease, &status, true);
    let incoming_job = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    fs::create_dir_all(&incoming_job).unwrap();
    fs::set_permissions(&incoming_job, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(!incoming_job.join(lease.lease_token().to_string()).exists());
    let incoming_namespace = incoming_job.parent().unwrap().join(".mac-worker-rooted-fs");
    drop(store);

    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
            .unwrap();
    let pending = JobService::new(&faulted, &RejectLauncher)
        .status(lease.job_id())
        .unwrap();
    assert_eq!(pending.status().state(), status.state());
    assert_eq!(
        pending.status().cleanup_error_code(),
        Some("MUTABLE_CLEANUP_FAILED")
    );
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(lease.clone())
    );
    assert!(!incoming_job.exists());
    assert_canonical_delete_journal(&incoming_namespace, "cleanup-tree-v1-");
    drop(faulted);

    let reopened = HostStore::open(&root).unwrap();
    let response = JobService::new(&reopened, &RejectLauncher)
        .status(lease.job_id())
        .unwrap();
    assert_eq!(response.status().state(), status.state());
    assert_eq!(response.status().cleanup_error_code(), None);
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), None);
    assert!(!incoming_job.exists());
    assert_journal_empty_or_absent(&incoming_namespace);
}

#[test]
fn retried_terminal_cleanup_clears_cleanup_error_after_an_injected_fault() {
    // Catches a succeeded job whose first deferred cleanup failed (FIFO residue
    // or an injected intent-commit fault): status.json kept
    // MUTABLE_CLEANUP_FAILED even after a later read finished cleanup and
    // released the lease.
    for later_caller in ["status", "log_chunk", "reconcile"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp
            .path()
            .join(format!("retried-cleanup-clear-{later_caller}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let status = JobStatus::accepted(10)
            .unwrap()
            .with_supervisor(identity(97_201), 11)
            .unwrap()
            .with_child(identity(97_202), 12)
            .unwrap()
            .into_running(13)
            .unwrap()
            .into_succeeded(14, 0, 0)
            .unwrap();
        install_job_status(&store, &lease, &status, true);
        drop(store);

        let faulted =
            HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
                .unwrap();
        let first = JobService::new(&faulted, &RejectLauncher)
            .status(lease.job_id())
            .unwrap();
        assert_eq!(first.status().state(), JobState::Succeeded);
        assert_eq!(
            first.status().cleanup_error_code(),
            Some("MUTABLE_CLEANUP_FAILED")
        );
        assert_eq!(
            LeaseService::new(&faulted).load().unwrap(),
            Some(lease.clone())
        );
        drop(faulted);

        let reopened = HostStore::open(&root).unwrap();
        let service = JobService::new(&reopened, &RejectLauncher);
        match later_caller {
            "status" => {
                let response = service.status(lease.job_id()).unwrap();
                assert_eq!(response.status().state(), JobState::Succeeded);
                assert_eq!(response.status().cleanup_error_code(), None);
            }
            "log_chunk" => {
                let chunk = service
                    .read_log(lease.job_id(), LogStream::Stdout, 0, 64)
                    .unwrap();
                assert_eq!(chunk.decoded_bytes().unwrap(), b"");
                assert_eq!(
                    read_job_status(&reopened, &lease).cleanup_error_code(),
                    None
                );
            }
            "reconcile" => {
                let response = service.reconcile_job(lease.job_id()).unwrap();
                assert_eq!(response.status().state(), JobState::Succeeded);
                assert_eq!(response.status().cleanup_error_code(), None);
            }
            _ => unreachable!(),
        }
        assert_eq!(
            read_job_status(&reopened, &lease).cleanup_error_code(),
            None,
            "{later_caller}"
        );
        assert_eq!(
            LeaseService::new(&reopened).load().unwrap(),
            None,
            "{later_caller}"
        );
        assert_mutable_job_scopes_absent(&reopened, &lease);
    }
}

#[test]
fn terminal_cleanup_removes_a_fifo_left_in_tmp() {
    // Catches the live Codex residue: a named pipe under tmp makes mutable
    // cleanup return EINVAL, so the lease stays held forever.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("fifo-tmp");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(identity(97_001), 11)
        .unwrap()
        .with_child(identity(97_002), 12)
        .unwrap()
        .into_running(13)
        .unwrap()
        .into_succeeded(14, 0, 0)
        .unwrap();
    install_job_status(&store, &lease, &status, true);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let nested = job.join("tmp/.tmpNbuUJ3");
    fs::create_dir_all(&nested).unwrap();
    fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
    create_fifo(&nested.join("command-ready.fifo"));

    let response = JobService::new(&store, &RejectLauncher)
        .status(lease.job_id())
        .unwrap();

    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(response.status().cleanup_error_code(), None);
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    assert_mutable_job_scopes_absent(&store, &lease);
}

#[test]
fn terminal_status_and_log_chunk_succeed_when_mutable_cleanup_still_fails() {
    // Catches log_chunk/status going through supervisor-ensure cleanup and
    // turning a leftover FIFO (or any injected cleanup fault) into HOST_IO, so
    // the laptop can never drain an already-terminal turn.
    for caller in ["status", "log_chunk", "reconcile"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("terminal-cleanup-{caller}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let status = JobStatus::accepted(10)
            .unwrap()
            .with_supervisor(identity(97_101), 11)
            .unwrap()
            .with_child(identity(97_102), 12)
            .unwrap()
            .into_running(13)
            .unwrap()
            .into_succeeded(14, 0, 0)
            .unwrap();
        install_job_status(&store, &lease, &status, true);
        drop(store);
        let faulted =
            HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
                .unwrap();
        let service = JobService::new(&faulted, &RejectLauncher);
        match caller {
            "status" => {
                let response = service.status(lease.job_id()).unwrap();
                assert_eq!(response.status().state(), JobState::Succeeded);
                assert_eq!(
                    response.status().cleanup_error_code(),
                    Some("MUTABLE_CLEANUP_FAILED")
                );
            }
            "log_chunk" => {
                let chunk = service
                    .read_log(lease.job_id(), LogStream::Stdout, 0, 64)
                    .unwrap();
                assert_eq!(chunk.decoded_bytes().unwrap(), b"");
                assert_eq!(
                    read_job_status(&faulted, &lease).cleanup_error_code(),
                    Some("MUTABLE_CLEANUP_FAILED")
                );
            }
            "reconcile" => {
                let error = service.reconcile_job(lease.job_id()).unwrap_err();
                assert!(error.to_string().contains("I/O error"), "{error}");
                let receipt = error
                    .failure_receipt()
                    .expect("cleanup HOST_IO carries a receipt");
                assert_eq!(receipt.stage(), STAGE_CLEANUP);
                assert!(receipt.residual().contains(&RESIDUAL_LEASE), "{receipt:?}");
                // AfterCleanupIntentCommit still has this job's public workspace;
                // the private tree sits in a shared ancestor namespace and must
                // not be claimed as this job's cleanup-tree.
                assert!(
                    receipt.residual().contains(&RESIDUAL_WORKSPACE),
                    "{receipt:?}"
                );
                let wire = HostControlError::new("HOST_IO", receipt.host_message()).unwrap();
                assert_eq!(wire.error().message(), receipt.host_message());
                assert_eq!(
                    read_job_status(&faulted, &lease).cleanup_error_code(),
                    Some("MUTABLE_CLEANUP_FAILED")
                );
            }
            _ => unreachable!(),
        }
        assert_eq!(
            LeaseService::new(&faulted).load().unwrap(),
            Some(lease),
            "{caller}"
        );
    }
}

#[test]
fn a_log_chunk_read_failure_reports_stage_drain() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("log-drain-receipt");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = HoldingRecordingLauncher {
        launches: Arc::new(AtomicUsize::new(0)),
        job_path: job.clone(),
        identity: identity(51_201),
        guard: Mutex::new(None),
    };
    JobService::new(&store, &launcher)
        .status(lease.job_id())
        .unwrap();
    fs::set_permissions(job.join("stdout.log"), fs::Permissions::from_mode(0o644)).unwrap();
    let error = JobService::new(&store, &launcher)
        .read_log(lease.job_id(), LogStream::Stdout, 0, 64)
        .unwrap_err();
    let receipt = error
        .failure_receipt()
        .unwrap_or_else(|| panic!("drain HOST_IO carries a receipt, got {error}"));
    assert_eq!(receipt.stage(), STAGE_DRAIN);
    assert!(error.to_string().contains("I/O error"), "{error}");
    drop(launcher.guard.lock().unwrap().take());
}

#[test]
fn after_cleanup_intent_commit_whole_incoming_job_resolution_resumes() {
    // Break caught: resolution starts a new outer incoming/{job} delete
    // instead of resuming the already-published canonical Tree journal.
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
    let request = ResolveOrAbandonRequest::from_submit_request(&SubmitRequest::new(
        acquire.material().clone(),
    ))
    .unwrap();
    let incoming_job = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    fs::create_dir_all(&incoming_job).unwrap();
    for private in [incoming_job.parent().unwrap(), incoming_job.as_path()] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    assert!(
        !incoming_job.join(lease.lease_token().to_string()).exists(),
        "exact token must be absent so the one-shot reaches outer incoming"
    );
    let incoming_namespace = incoming_job.parent().unwrap().join(".mac-worker-rooted-fs");
    store.record_abandoned(&acquire, 2).unwrap();
    drop(store);

    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
            .unwrap();
    let error = faulted.cleanup_job_owned(&lease).unwrap_err();
    assert!(
        matches!(error, WorkerError::Io(_)) || error.to_string().contains("I/O error"),
        "{error}"
    );
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(lease.clone())
    );
    assert!(
        !incoming_job.exists(),
        "public outer must already be absent"
    );
    assert_canonical_delete_journal(&incoming_namespace, "cleanup-tree-v1-");
    drop(faulted);

    let reopened = HostStore::open(&root).unwrap();
    let completed = JobService::new(&reopened, &RejectLauncher)
        .resolve_or_abandon(request)
        .unwrap();
    assert!(
        matches!(completed.outcome(), ResolveOrAbandonOutcome::Abandoned),
        "{completed:?}"
    );
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), None);
    assert!(!incoming_job.exists());
    assert_journal_empty_or_absent(&incoming_namespace);
}

#[test]
fn resolution_leaves_ordinary_empty_incoming_job_untouched() {
    // Break caught: resolution treats an ordinary public incoming/{job}
    // without a journal as owned cleanup and starts a new outer deletion.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, submit) = unindexed_identityless_job(&root);
    let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    remove_and_sync(&job.join("status.json"));
    let incoming_job = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    fs::create_dir_all(&incoming_job).unwrap();
    for private in [incoming_job.parent().unwrap(), incoming_job.as_path()] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    assert!(!incoming_job.join(lease.lease_token().to_string()).exists());
    File::open(&incoming_job).unwrap().sync_all().unwrap();
    File::open(incoming_job.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
    let incoming_meta = fs::symlink_metadata(&incoming_job).unwrap();
    let incoming_layout = directory_entry_names(&incoming_job);
    let incoming_namespace = incoming_job.parent().unwrap().join(".mac-worker-rooted-fs");
    drop(store);

    let reopened = HostStore::open(&root).unwrap();
    let completed = JobService::new(&reopened, &RejectLauncher)
        .resolve_or_abandon(request)
        .unwrap();
    assert!(
        matches!(completed.outcome(), ResolveOrAbandonOutcome::Abandoned),
        "{completed:?}"
    );
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), None);
    assert!(
        incoming_job.is_dir(),
        "ordinary public incoming/{{job}} was deleted"
    );
    let after = fs::symlink_metadata(&incoming_job).unwrap();
    assert_eq!(after.dev(), incoming_meta.dev());
    assert_eq!(after.ino(), incoming_meta.ino());
    let after_layout = directory_entry_names(&incoming_job);
    let added = after_layout
        .iter()
        .filter(|name| !incoming_layout.contains(name))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        added.is_empty() || (added.len() == 1 && added[0] == ".mac-worker-rooted-fs"),
        "incoming/{{job}} layout changed beyond an empty nested namespace: {added:?}"
    );
    for name in &incoming_layout {
        assert!(after_layout.contains(name), "incoming/{{job}} lost {name}");
    }
    assert!(!incoming_job.join(lease.lease_token().to_string()).exists());
    assert_journal_empty_or_absent(&incoming_job.join(".mac-worker-rooted-fs"));
    assert_journal_empty_or_absent(&incoming_namespace);
}

#[derive(Clone, Copy, Debug)]
enum CleanupSeamTarget {
    ExecutionRegular,
    JobMutableTree(&'static str),
    JobStageThisJob,
    ReleasedLeftover,
}

#[test]
fn after_cleanup_intent_commit_seam_matrix_retries() {
    for target in [
        CleanupSeamTarget::ExecutionRegular,
        CleanupSeamTarget::JobMutableTree("workspace"),
        CleanupSeamTarget::JobMutableTree("home"),
        CleanupSeamTarget::JobMutableTree("tmp"),
        CleanupSeamTarget::JobStageThisJob,
        CleanupSeamTarget::ReleasedLeftover,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("host");
        let (store, lease, submit) = unindexed_identityless_job(&root);
        let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        remove_and_sync(&job.join("status.json"));
        let foreign_job = JobId::new(uuid::Uuid::from_u128(
            0x1111_2222_3333_4444_5555_6666_7777_8888,
        ));
        let job_stage =
            root.join("leases")
                .join(format!(".job-{}-{}", lease.job_id(), "1".repeat(32)));
        let foreign_stage =
            root.join("leases")
                .join(format!(".job-{}-{}", foreign_job, "2".repeat(32)));
        let released = root
            .join("leases")
            .join(format!(".released-{}", lease.job_id()));
        match target {
            CleanupSeamTarget::ExecutionRegular => {}
            CleanupSeamTarget::JobMutableTree(name) => {
                isolate_resolution_scopes_before_staging(&job, &store, &lease);
                let path = job.join(name);
                fs::create_dir(&path).unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
                replace_bytes(&path.join("leaf"), b"mutable-tree").unwrap();
                File::open(&job).unwrap().sync_all().unwrap();
            }
            CleanupSeamTarget::JobStageThisJob => {
                isolate_resolution_scopes_before_staging(&job, &store, &lease);
                fs::create_dir(&job_stage).unwrap();
                fs::set_permissions(&job_stage, fs::Permissions::from_mode(0o700)).unwrap();
                replace_bytes(&job_stage.join("stage-leaf"), b"this-job-stage").unwrap();
                fs::create_dir(&foreign_stage).unwrap();
                fs::set_permissions(&foreign_stage, fs::Permissions::from_mode(0o700)).unwrap();
                replace_bytes(&foreign_stage.join("foreign-leaf"), b"foreign-job-stage").unwrap();
                File::open(root.join("leases")).unwrap().sync_all().unwrap();
            }
            CleanupSeamTarget::ReleasedLeftover => {
                isolate_resolution_scopes_before_staging(&job, &store, &lease);
                fs::create_dir(&released).unwrap();
                fs::set_permissions(&released, fs::Permissions::from_mode(0o700)).unwrap();
                replace_bytes(&released.join("released-leaf"), b"released-leftover").unwrap();
                File::open(root.join("leases")).unwrap().sync_all().unwrap();
            }
        }
        let foreign_meta = foreign_stage
            .exists()
            .then(|| fs::symlink_metadata(&foreign_stage).unwrap());
        drop(store);

        let faulted =
            HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
                .unwrap();
        let pending = JobService::new(&faulted, &RejectLauncher)
            .resolve_or_abandon(request.clone())
            .unwrap();
        assert!(
            matches!(
                pending.outcome(),
                ResolveOrAbandonOutcome::CleanupPending { code }
                    if code == "MUTABLE_CLEANUP_FAILED"
            ),
            "{target:?}: {pending:?}"
        );
        assert_eq!(
            LeaseService::new(&faulted).load().unwrap(),
            Some(lease.clone()),
            "{target:?}"
        );
        let (public, namespace, quarantine_prefix) = match target {
            CleanupSeamTarget::ExecutionRegular => (
                job.join("execution.json"),
                job.join(".mac-worker-rooted-fs"),
                "cleanup-regular-v1-",
            ),
            CleanupSeamTarget::JobMutableTree(name) => (
                job.join(name),
                job.join(".mac-worker-rooted-fs"),
                "cleanup-tree-v1-",
            ),
            CleanupSeamTarget::JobStageThisJob => (
                job_stage.clone(),
                root.join("leases/.mac-worker-rooted-fs"),
                "cleanup-tree-v1-",
            ),
            CleanupSeamTarget::ReleasedLeftover => (
                released.clone(),
                root.join("leases/.mac-worker-rooted-fs"),
                "cleanup-tree-v1-",
            ),
        };
        assert!(!public.exists(), "{target:?}: public target survived");
        assert_canonical_delete_journal(&namespace, quarantine_prefix);
        if let Some(meta) = &foreign_meta {
            let current = fs::symlink_metadata(&foreign_stage).unwrap();
            assert_eq!(current.dev(), meta.dev(), "{target:?}");
            assert_eq!(current.ino(), meta.ino(), "{target:?}");
            assert_eq!(
                fs::read(foreign_stage.join("foreign-leaf")).unwrap(),
                b"foreign-job-stage",
                "{target:?}"
            );
        }
        drop(faulted);

        let reopened = HostStore::open(&root).unwrap();
        let completed = JobService::new(&reopened, &RejectLauncher)
            .resolve_or_abandon(request)
            .unwrap();
        assert!(
            matches!(completed.outcome(), ResolveOrAbandonOutcome::Abandoned),
            "{target:?}: {completed:?}"
        );
        assert_eq!(
            LeaseService::new(&reopened).load().unwrap(),
            None,
            "{target:?}"
        );
        assert!(!public.exists(), "{target:?}: public target resurrected");
        assert_journal_empty_or_absent(&namespace);
        if let Some(meta) = &foreign_meta {
            let current = fs::symlink_metadata(&foreign_stage).unwrap();
            assert_eq!(
                current.dev(),
                meta.dev(),
                "{target:?}: foreign stage replaced"
            );
            assert_eq!(
                current.ino(),
                meta.ino(),
                "{target:?}: foreign stage replaced"
            );
            assert_eq!(
                fs::read(foreign_stage.join("foreign-leaf")).unwrap(),
                b"foreign-job-stage",
                "{target:?}"
            );
        }
    }
}

#[test]
fn durable_mid_incoming_cleanup_crash_fails_closed() {
    // Break caught: a resolver retry mistakes an absent caller-visible token
    // for completed cleanup even though RootedDir left a partially deleted,
    // privately acquired exact token behind after a crash.
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
    let request = ResolveOrAbandonRequest::from_submit_request(&SubmitRequest::new(
        acquire.material().clone(),
    ))
    .unwrap();
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    fs::create_dir_all(&incoming).unwrap();
    for private in [incoming.parent().unwrap(), incoming.as_path()] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    replace_bytes(&incoming.join("removed-before-crash"), b"removed").unwrap();
    replace_bytes(
        &incoming.join("retained-after-crash"),
        b"retain-exact-bytes",
    )
    .unwrap();

    // Resolution publishes this exact durable authority before it starts
    // mutable cleanup.
    store.record_abandoned(&acquire, 2).unwrap();

    // Recreate remove_owned_tree's durable post-rename, mid-recursion image.
    let incoming_job = incoming.parent().unwrap().to_path_buf();
    let private_namespace = incoming_job.join(".mac-worker-rooted-fs");
    fs::create_dir(&private_namespace).unwrap();
    fs::set_permissions(&private_namespace, fs::Permissions::from_mode(0o700)).unwrap();
    let residue = private_namespace.join("cleanup-303f0f4a-6b5c-4d8e-9f00-112233445566");
    fs::rename(&incoming, &residue).unwrap();
    File::open(&private_namespace).unwrap().sync_all().unwrap();
    File::open(&incoming_job).unwrap().sync_all().unwrap();
    remove_and_sync(&residue.join("removed-before-crash"));
    assert!(!incoming.exists());
    assert!(!residue.join("removed-before-crash").exists());
    assert_eq!(
        fs::read(residue.join("retained-after-crash")).unwrap(),
        b"retain-exact-bytes"
    );
    drop(store);

    let reopened = HostStore::open(&root).unwrap();
    let response = JobService::new(&reopened, &RejectLauncher)
        .resolve_or_abandon(request)
        .unwrap();

    assert!(
        matches!(
            response.outcome(),
            ResolveOrAbandonOutcome::CleanupPending { code }
                if code == "MUTABLE_CLEANUP_FAILED"
        ),
        "{response:?}"
    );
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), Some(lease));
    assert!(!residue.join("removed-before-crash").exists());
    assert_eq!(
        fs::read(residue.join("retained-after-crash")).unwrap(),
        b"retain-exact-bytes"
    );
}

#[test]
fn durable_private_cleanup_residue_matrix_fails_closed() {
    // Break caught: a resolver retry proves cleanup only from the vanished
    // caller-visible name and overlooks RootedDir's durable private crash
    // image, then releases the exact lease while owned bytes still remain.
    #[derive(Clone, Copy, Debug)]
    enum CrashImage {
        FinalJobTree,
        LeaseStageTree,
        VerifiedRegular,
        WholeIncomingJobTree,
        EmptyCleanupUuid,
        MalformedNamespace,
        WrongModeNamespace,
    }

    let mut failures = Vec::new();
    for (case_index, crash_image) in [
        CrashImage::FinalJobTree,
        CrashImage::LeaseStageTree,
        CrashImage::VerifiedRegular,
        CrashImage::WholeIncomingJobTree,
        CrashImage::EmptyCleanupUuid,
        CrashImage::MalformedNamespace,
        CrashImage::WrongModeNamespace,
    ]
    .into_iter()
    .enumerate()
    {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("host-{case_index}"));
        let (lease, request, residue, planted) = match crash_image {
            CrashImage::FinalJobTree => {
                let (store, lease, submit) = unindexed_identityless_job(&root);
                let acquire = LeaseAcquireRequest::new(submit.material().clone());
                let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
                let job = store
                    .job(lease.project_id(), lease.worktree_id(), lease.job_id())
                    .unwrap();
                remove_and_sync(&job.join("status.json"));
                store.record_abandoned(&acquire, 2).unwrap();

                // This is the durable point reached only after earlier exact
                // resolution scopes and the first two job trees are gone.
                remove_and_sync(&job.join("execution.json"));
                let receipt = store.verified_receipt(lease.job_id()).unwrap();
                remove_and_sync(&receipt);
                for name in ["workspace", "home"] {
                    fs::remove_dir_all(job.join(name)).unwrap();
                }
                let tmp = job.join("tmp");
                let planted = b"final-job-private-residue".to_vec();
                replace_bytes(&tmp.join("retained-after-crash"), &planted).unwrap();
                let private_namespace = job.join(".mac-worker-rooted-fs");
                fs::create_dir_all(&private_namespace).unwrap();
                fs::set_permissions(&private_namespace, fs::Permissions::from_mode(0o700)).unwrap();
                assert_eq!(fs::read_dir(&private_namespace).unwrap().count(), 0);
                let private_tree =
                    private_namespace.join("cleanup-303f0f4a-6b5c-4d8e-9f00-112233445566");
                fs::rename(&tmp, &private_tree).unwrap();
                File::open(&private_namespace).unwrap().sync_all().unwrap();
                File::open(&job).unwrap().sync_all().unwrap();
                for name in ["workspace", "home", "tmp"] {
                    assert!(!job.join(name).exists(), "{name} remains before retry");
                }
                let residue = private_tree.join("retained-after-crash");
                assert_eq!(fs::read(&residue).unwrap(), planted);
                drop(store);
                (lease, request, residue, planted)
            }
            CrashImage::LeaseStageTree => {
                let store = HostStore::open(&root).unwrap();
                let acquire = lease_request();
                let lease = match LeaseService::new(&store)
                    .acquire(&acquire, &healthy(), 1)
                    .unwrap()
                {
                    LeaseAcquireResponse::Acquired { lease } => lease,
                    LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
                };
                let submit = SubmitRequest::new(acquire.material().clone());
                let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
                let leases = root.join("leases");
                let stage = leases.join(format!(".job-{}-{}", lease.job_id(), "1".repeat(32)));
                fs::create_dir(&stage).unwrap();
                fs::set_permissions(&stage, fs::Permissions::from_mode(0o700)).unwrap();
                let planted = b"lease-stage-private-residue".to_vec();
                replace_bytes(&stage.join("retained-after-crash"), &planted).unwrap();
                store.record_abandoned(&acquire, 2).unwrap();

                let private_namespace = leases.join(".mac-worker-rooted-fs");
                fs::create_dir_all(&private_namespace).unwrap();
                fs::set_permissions(&private_namespace, fs::Permissions::from_mode(0o700)).unwrap();
                assert_eq!(fs::read_dir(&private_namespace).unwrap().count(), 0);
                let private_tree =
                    private_namespace.join("cleanup-303f0f4a-6b5c-4d8e-9f00-112233445566");
                fs::rename(&stage, &private_tree).unwrap();
                File::open(&private_namespace).unwrap().sync_all().unwrap();
                File::open(&leases).unwrap().sync_all().unwrap();
                assert!(!stage.exists());
                let residue = private_tree.join("retained-after-crash");
                assert_eq!(fs::read(&residue).unwrap(), planted);
                drop(store);
                (lease, request, residue, planted)
            }
            CrashImage::VerifiedRegular => {
                let (store, lease, submit) = prepared_host(&root);
                let acquire = LeaseAcquireRequest::new(submit.material().clone());
                let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
                store.record_abandoned(&acquire, 2).unwrap();
                let receipt = store.verified_receipt(lease.job_id()).unwrap();
                let planted = fs::read(&receipt).unwrap();
                let residue = receipt
                    .parent()
                    .unwrap()
                    .join("remove-303f0f4a-6b5c-4d8e-9f00-112233445566");
                fs::rename(&receipt, &residue).unwrap();
                File::open(receipt.parent().unwrap())
                    .unwrap()
                    .sync_all()
                    .unwrap();
                assert!(!receipt.exists());
                assert_eq!(fs::read(&residue).unwrap(), planted);
                drop(store);
                (lease, request, residue, planted)
            }
            CrashImage::WholeIncomingJobTree => {
                let store = HostStore::open(&root).unwrap();
                let acquire = lease_request();
                let lease = match LeaseService::new(&store)
                    .acquire(&acquire, &healthy(), 1)
                    .unwrap()
                {
                    LeaseAcquireResponse::Acquired { lease } => lease,
                    LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
                };
                let submit = SubmitRequest::new(acquire.material().clone());
                let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
                let token = store
                    .incoming_job(lease.job_id(), lease.lease_token())
                    .unwrap();
                fs::create_dir_all(&token).unwrap();
                for directory in [token.parent().unwrap(), token.as_path()] {
                    fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
                }
                let planted = b"whole-incoming-job-private-residue".to_vec();
                replace_bytes(&token.join("retained-after-crash"), &planted).unwrap();
                store.record_abandoned(&acquire, 2).unwrap();

                // Recreate cleanup_job_owned's outer remove_owned_child(job)
                // post-publish image: the public job name is already gone.
                let incoming_job = token.parent().unwrap().to_path_buf();
                let incoming_root = incoming_job.parent().unwrap().to_path_buf();
                let private_namespace = incoming_root.join(".mac-worker-rooted-fs");
                fs::create_dir_all(&private_namespace).unwrap();
                fs::set_permissions(&private_namespace, fs::Permissions::from_mode(0o700)).unwrap();
                assert_eq!(fs::read_dir(&private_namespace).unwrap().count(), 0);
                let private_tree =
                    private_namespace.join("cleanup-303f0f4a-6b5c-4d8e-9f00-112233445566");
                fs::rename(&incoming_job, &private_tree).unwrap();
                File::open(&private_namespace).unwrap().sync_all().unwrap();
                File::open(&incoming_root).unwrap().sync_all().unwrap();
                assert!(!incoming_job.exists());
                let residue = private_tree
                    .join(lease.lease_token().to_string())
                    .join("retained-after-crash");
                assert_eq!(fs::read(&residue).unwrap(), planted);
                drop(store);
                (lease, request, residue, planted)
            }
            CrashImage::EmptyCleanupUuid => {
                let (store, lease, submit) = unindexed_identityless_job(&root);
                let acquire = LeaseAcquireRequest::new(submit.material().clone());
                let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
                let job = store
                    .job(lease.project_id(), lease.worktree_id(), lease.job_id())
                    .unwrap();
                remove_and_sync(&job.join("status.json"));
                store.record_abandoned(&acquire, 2).unwrap();
                isolate_resolution_scopes_before_staging(&job, &store, &lease);
                let private_namespace = job.join(".mac-worker-rooted-fs");
                fs::create_dir_all(&private_namespace).unwrap();
                fs::set_permissions(&private_namespace, fs::Permissions::from_mode(0o700)).unwrap();
                let residue =
                    private_namespace.join("cleanup-303f0f4a-6b5c-4d8e-9f00-112233445566");
                fs::create_dir(&residue).unwrap();
                fs::set_permissions(&residue, fs::Permissions::from_mode(0o700)).unwrap();
                File::open(&residue).unwrap().sync_all().unwrap();
                File::open(&private_namespace).unwrap().sync_all().unwrap();
                File::open(&job).unwrap().sync_all().unwrap();
                drop(store);
                (lease, request, residue, Vec::new())
            }
            CrashImage::MalformedNamespace => {
                let (store, lease, submit) = unindexed_identityless_job(&root);
                let acquire = LeaseAcquireRequest::new(submit.material().clone());
                let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
                let job = store
                    .job(lease.project_id(), lease.worktree_id(), lease.job_id())
                    .unwrap();
                remove_and_sync(&job.join("status.json"));
                store.record_abandoned(&acquire, 2).unwrap();
                isolate_resolution_scopes_before_staging(&job, &store, &lease);
                let residue = job.join(".mac-worker-rooted-fs");
                let planted = b"malformed-namespace".to_vec();
                replace_bytes(&residue, &planted).unwrap();
                drop(store);
                (lease, request, residue, planted)
            }
            CrashImage::WrongModeNamespace => {
                let (store, lease, submit) = unindexed_identityless_job(&root);
                let acquire = LeaseAcquireRequest::new(submit.material().clone());
                let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
                let job = store
                    .job(lease.project_id(), lease.worktree_id(), lease.job_id())
                    .unwrap();
                remove_and_sync(&job.join("status.json"));
                store.record_abandoned(&acquire, 2).unwrap();
                isolate_resolution_scopes_before_staging(&job, &store, &lease);
                let residue = job.join(".mac-worker-rooted-fs");
                fs::create_dir_all(&residue).unwrap();
                fs::set_permissions(&residue, fs::Permissions::from_mode(0o755)).unwrap();
                File::open(&residue).unwrap().sync_all().unwrap();
                File::open(&job).unwrap().sync_all().unwrap();
                drop(store);
                (lease, request, residue, Vec::new())
            }
        };

        let residue_meta = fs::symlink_metadata(&residue).unwrap();
        let residue_dev = residue_meta.dev();
        let residue_ino = residue_meta.ino();
        let residue_mode = residue_meta.permissions().mode();
        let residue_bytes = residue_meta
            .file_type()
            .is_file()
            .then(|| fs::read(&residue).unwrap());
        let residue_entries = residue_meta
            .file_type()
            .is_dir()
            .then(|| directory_entry_names(&residue));
        if let Some(bytes) = &residue_bytes {
            assert_eq!(bytes, &planted, "{crash_image:?}: planted bytes drifted");
        }

        for attempt in 0..2 {
            let reopened = HostStore::open(&root).unwrap();
            let response = JobService::new(&reopened, &RejectLauncher)
                .resolve_or_abandon(request.clone())
                .unwrap();
            let cleanup_pending = matches!(
                response.outcome(),
                ResolveOrAbandonOutcome::CleanupPending { code }
                    if code == "MUTABLE_CLEANUP_FAILED"
            );
            let lease_retained =
                LeaseService::new(&reopened).load().unwrap() == Some(lease.clone());
            let current = fs::symlink_metadata(&residue).unwrap();
            let inode_stable = current.dev() == residue_dev && current.ino() == residue_ino;
            let mode_stable = current.permissions().mode() == residue_mode;
            let bytes_stable = match &residue_bytes {
                Some(bytes) => fs::read(&residue).unwrap() == *bytes,
                None => true,
            };
            let entries_stable = match &residue_entries {
                Some(entries) => directory_entry_names(&residue) == *entries,
                None => true,
            };
            if !(cleanup_pending
                && lease_retained
                && inode_stable
                && mode_stable
                && bytes_stable
                && entries_stable)
            {
                failures.push(format!(
                    "{crash_image:?} attempt {attempt}: outcome={:?}, lease_retained={lease_retained}, inode_stable={inode_stable}, mode_stable={mode_stable}, bytes_stable={bytes_stable}, entries_stable={entries_stable}",
                    response.outcome()
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "durable private cleanup was mistaken for absence:\n{}",
        failures.join("\n")
    );
}

#[test]
fn two_resolvers_race_one_committed_incoming_intent() {
    for iteration in 0..8 {
        two_resolvers_race_one_committed_incoming_intent_once(iteration);
    }
}

fn two_resolvers_race_one_committed_incoming_intent_once(iteration: usize) {
    let (planter, planted) = plant_committed_incoming_token_journal();
    drop(planter);
    let incoming_job_meta = fs::symlink_metadata(&planted.incoming_job).unwrap();
    let neighbors = planted.neighbors.clone();

    let outcomes = race_two_independent_resolvers(&planted.root, planted.request.clone());
    for (index, outcome) in outcomes.iter().enumerate() {
        assert_allowed_race_outcome(outcome, iteration, index);
    }
    finish_resolution_if_needed(&planted.root, planted.request.clone(), &outcomes);

    let reopened = HostStore::open(&planted.root).unwrap();
    assert_eq!(
        LeaseService::new(&reopened).load().unwrap(),
        None,
        "iteration {iteration}: exact lease was not retired once"
    );
    assert!(
        !planted.incoming.exists(),
        "iteration {iteration}: public token survived"
    );
    assert_journal_empty_or_absent(&planted.token_namespace);
    assert_journal_empty_or_absent(&planted.incoming_root_namespace);
    let after_job = fs::symlink_metadata(&planted.incoming_job).unwrap();
    assert_eq!(
        after_job.dev(),
        incoming_job_meta.dev(),
        "iteration {iteration}"
    );
    assert_eq!(
        after_job.ino(),
        incoming_job_meta.ino(),
        "iteration {iteration}: resolution started a new outer incoming delete"
    );
    assert_unrelated_neighbors_unchanged(&neighbors, iteration);
}

#[test]
fn two_job_intents_sharing_leases_do_not_cross_mutate() {
    // Single-worker execution admits one heavy lease. Two simultaneous live
    // leases are not representable durable state. The fixture keeps that
    // authoritative lease and plants a foreign `.job-<other>-<hex>` stage in
    // the shared leases namespace so both racers resolve one request while
    // the job-scoped predicate must resume only its own key.
    for iteration in 0..8 {
        two_job_intents_sharing_leases_do_not_cross_mutate_once(iteration);
    }
}

fn two_job_intents_sharing_leases_do_not_cross_mutate_once(iteration: usize) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, submit) = unindexed_identityless_job(&root);
    let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    remove_and_sync(&job.join("status.json"));
    isolate_resolution_scopes_before_staging(&job, &store, &lease);
    let foreign_job = JobId::new(uuid::Uuid::from_u128(
        0x1111_2222_3333_4444_5555_6666_7777_8888,
    ));
    let job_stage = root
        .join("leases")
        .join(format!(".job-{}-{}", lease.job_id(), "1".repeat(32)));
    let foreign_stage =
        root.join("leases")
            .join(format!(".job-{}-{}", foreign_job, "2".repeat(32)));
    fs::create_dir(&job_stage).unwrap();
    fs::set_permissions(&job_stage, fs::Permissions::from_mode(0o700)).unwrap();
    replace_bytes(&job_stage.join("stage-leaf"), b"this-job-stage").unwrap();
    fs::create_dir(&foreign_stage).unwrap();
    fs::set_permissions(&foreign_stage, fs::Permissions::from_mode(0o700)).unwrap();
    replace_bytes(&foreign_stage.join("foreign-leaf"), b"foreign-job-stage").unwrap();
    File::open(root.join("leases")).unwrap().sync_all().unwrap();
    let foreign_meta = fs::symlink_metadata(&foreign_stage).unwrap();
    let foreign_leaf = regular_file_identity(&foreign_stage.join("foreign-leaf"));
    drop(store);

    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
            .unwrap();
    let pending = JobService::new(&faulted, &RejectLauncher)
        .resolve_or_abandon(request.clone())
        .unwrap();
    assert!(
        matches!(
            pending.outcome(),
            ResolveOrAbandonOutcome::CleanupPending { code }
                if code == "MUTABLE_CLEANUP_FAILED"
        ),
        "iteration {iteration}: {pending:?}"
    );
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(lease.clone()),
        "iteration {iteration}: planted journal dropped the exact lease"
    );
    assert!(
        !job_stage.exists(),
        "iteration {iteration}: this-job stage still public"
    );
    assert_canonical_delete_journal(
        &root.join("leases/.mac-worker-rooted-fs"),
        "cleanup-tree-v1-",
    );
    let after_plant = fs::symlink_metadata(&foreign_stage).unwrap();
    assert_eq!(
        after_plant.dev(),
        foreign_meta.dev(),
        "iteration {iteration}"
    );
    assert_eq!(
        after_plant.ino(),
        foreign_meta.ino(),
        "iteration {iteration}"
    );
    assert_eq!(
        regular_file_identity(&foreign_stage.join("foreign-leaf")),
        foreign_leaf,
        "iteration {iteration}: foreign stage mutated while planting this-job journal"
    );
    drop(faulted);

    let outcomes = race_two_independent_resolvers(&root, request.clone());
    for (index, outcome) in outcomes.iter().enumerate() {
        assert_allowed_race_outcome(outcome, iteration, index);
        let after_race = fs::symlink_metadata(&foreign_stage).unwrap();
        assert_eq!(
            after_race.dev(),
            foreign_meta.dev(),
            "iteration {iteration} racer {index}: foreign stage replaced"
        );
        assert_eq!(
            after_race.ino(),
            foreign_meta.ino(),
            "iteration {iteration} racer {index}: foreign stage replaced"
        );
        assert_eq!(
            regular_file_identity(&foreign_stage.join("foreign-leaf")),
            foreign_leaf,
            "iteration {iteration} racer {index}: foreign bytes changed"
        );
    }
    finish_resolution_if_needed(&root, request, &outcomes);

    let reopened = HostStore::open(&root).unwrap();
    assert_eq!(
        LeaseService::new(&reopened).load().unwrap(),
        None,
        "iteration {iteration}: authoritative lease ownership drifted"
    );
    assert!(
        !job_stage.exists(),
        "iteration {iteration}: this-job stage resurrected"
    );
    assert_journal_empty_or_absent(&root.join("leases/.mac-worker-rooted-fs"));
    let final_foreign = fs::symlink_metadata(&foreign_stage).unwrap();
    assert_eq!(
        final_foreign.dev(),
        foreign_meta.dev(),
        "iteration {iteration}"
    );
    assert_eq!(
        final_foreign.ino(),
        foreign_meta.ino(),
        "iteration {iteration}"
    );
    assert_eq!(
        regular_file_identity(&foreign_stage.join("foreign-leaf")),
        foreign_leaf,
        "iteration {iteration}: predicate cross-resumed the foreign stage"
    );
}

#[test]
fn independent_hoststore_reopen_retries_committed_intent() {
    for iteration in 0..3 {
        independent_hoststore_reopen_retries_committed_intent_once(iteration);
    }
}

fn independent_hoststore_reopen_retries_committed_intent_once(iteration: usize) {
    let (faulted, planted) = plant_committed_incoming_token_journal();
    let incoming_job_meta = fs::symlink_metadata(&planted.incoming_job).unwrap();
    let neighbors = planted.neighbors.clone();
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(planted.lease.clone()),
        "iteration {iteration}: planter lease drifted before the independent resume"
    );
    assert_canonical_delete_journal(&planted.token_namespace, "cleanup-tree-v1-");

    let clean = HostStore::open(&planted.root).unwrap();
    let completed = JobService::new(&clean, &RejectLauncher)
        .resolve_or_abandon(planted.request.clone())
        .unwrap();
    assert!(
        matches!(completed.outcome(), ResolveOrAbandonOutcome::Abandoned),
        "iteration {iteration}: {completed:?}"
    );
    assert_eq!(LeaseService::new(&clean).load().unwrap(), None);
    assert!(!planted.incoming.exists(), "iteration {iteration}");
    assert_journal_empty_or_absent(&planted.token_namespace);
    assert_journal_empty_or_absent(&planted.incoming_root_namespace);
    let after_job = fs::symlink_metadata(&planted.incoming_job).unwrap();
    assert_eq!(
        after_job.dev(),
        incoming_job_meta.dev(),
        "iteration {iteration}"
    );
    assert_eq!(
        after_job.ino(),
        incoming_job_meta.ino(),
        "iteration {iteration}: independent resume started a new outer delete"
    );
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        None,
        "iteration {iteration}: kept-alive planter still observed a live lease"
    );
    assert_unrelated_neighbors_unchanged(&neighbors, iteration);
    drop(faulted);
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
            original.material().created_at_millis(),
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
fn status_rejects_terminal_nul_and_oversized_durable_state_without_side_effects() {
    // Break caught: a bounded canonical reader drops a terminal NUL, reads an
    // unbounded host record, or reaches supervisor launch after poisoned state.
    const MAX_HOST_JSON_BYTES: usize = 1024 * 1024;
    const PLANTED: &str = "PLANTED_DURABLE_STATE_SECRET";
    const EXACT_ARGV_ELEMENT: &str = "/usr/bin/true";

    for (row, (record, mutation)) in [
        ("job-index", "terminal-nul"),
        ("job-index", "oversized"),
        ("lease", "terminal-nul"),
        ("lease", "oversized"),
        ("meta", "terminal-nul"),
        ("meta", "oversized"),
        ("status", "terminal-nul"),
        ("status", "oversized"),
        ("execution", "terminal-nul"),
        ("execution", "oversized"),
        ("verified-receipt", "terminal-nul"),
        ("verified-receipt", "oversized"),
    ]
    .into_iter()
    .enumerate()
    {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("durable-state-{row}"));
        let (store, lease, _request) = indexed_identityless_job(&root);
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        let records = [
            (
                "job-index",
                store.job_index(lease.job_id()).unwrap(),
                "JOB_STATE_INVALID",
                "protocol error: JOB_STATE_INVALID: job disposition is invalid",
            ),
            (
                "lease",
                root.join("leases/heavy/lease.json"),
                "JOB_STATE_INVALID",
                "protocol error: JOB_STATE_INVALID: live lease is invalid",
            ),
            (
                "meta",
                job.join("meta.json"),
                "JOB_STATE_INVALID",
                "protocol error: JOB_STATE_INVALID: canonical job metadata is invalid",
            ),
            (
                "status",
                job.join("status.json"),
                "JOB_STATE_INVALID",
                "protocol error: JOB_STATE_INVALID: canonical mutable job status is invalid",
            ),
            (
                "execution",
                job.join("execution.json"),
                "JOB_STATE_INVALID",
                "protocol error: JOB_STATE_INVALID: identityless accepted payload is invalid",
            ),
            (
                "verified-receipt",
                store.verified_receipt(lease.job_id()).unwrap(),
                "UNSAFE_REMOTE_SNAPSHOT",
                "snapshot error [UNSAFE_REMOTE_SNAPSHOT]: remote snapshot filesystem state is unsafe",
            ),
        ];
        let (_, target, expected_code, expected_diagnostic) = records
            .iter()
            .find(|(name, _, _, _)| *name == record)
            .unwrap();
        let execution_payload = fs::read(job.join("execution.json")).unwrap();
        assert!(
            execution_payload
                .windows(EXACT_ARGV_ELEMENT.len())
                .any(|window| window == EXACT_ARGV_ELEMENT.as_bytes()),
            "row {row}: exact argv fixture is absent"
        );
        let canonical = fs::read(target).unwrap();
        let poisoned = match mutation {
            "terminal-nul" => {
                let mut bytes = canonical;
                bytes.extend_from_slice(PLANTED.as_bytes());
                bytes.push(0);
                bytes
            }
            "oversized" => {
                let mut bytes = vec![b'x'; MAX_HOST_JSON_BYTES + 1];
                bytes[..canonical.len()].copy_from_slice(&canonical);
                let planted_at = bytes.len() - PLANTED.len();
                bytes[planted_at..].copy_from_slice(PLANTED.as_bytes());
                bytes
            }
            _ => unreachable!(),
        };
        if mutation == "terminal-nul" {
            assert_eq!(poisoned.last(), Some(&0), "row {row}");
        } else {
            assert_eq!(poisoned.len(), MAX_HOST_JSON_BYTES + 1, "row {row}");
        }
        replace_bytes(target, &poisoned).unwrap();

        let durable_before = records
            .iter()
            .map(|(name, path, _, _)| (*name, matrix_snapshot_path(path)))
            .collect::<Vec<_>>();
        let workspace_before = matrix_snapshot_path(&job.join("workspace"));
        let cache = store
            .snapshot(
                lease.project_id(),
                lease.worktree_id(),
                lease.manifest_digest(),
            )
            .unwrap();
        let cache_before = matrix_snapshot_path(&cache);
        let sentinel = temp.path().join("outside-sentinel");
        fs::write(&sentinel, b"outside-state-must-survive").unwrap();
        let launches = Arc::new(AtomicUsize::new(0));
        let launcher = CountingRejectLauncher {
            launches: Arc::clone(&launches),
        };

        let error = JobService::new(&store, &launcher)
            .status(lease.job_id())
            .unwrap_err();

        let rendered = error.to_string();
        assert_eq!(
            error.public_code(),
            *expected_code,
            "row {row} {record} {mutation}: wrong public code for {rendered}"
        );
        assert_eq!(
            rendered, *expected_diagnostic,
            "row {row} {record} {mutation}: wrong durable read boundary"
        );
        assert!(
            !rendered.contains(PLANTED),
            "row {row} reflected planted durable content"
        );
        assert!(
            !rendered.contains(EXACT_ARGV_ELEMENT),
            "row {row} reflected exact argv fixture content"
        );
        assert_eq!(launches.load(Ordering::SeqCst), 0, "row {row}");
        assert_eq!(fs::read(target).unwrap(), poisoned, "row {row}");
        assert_eq!(
            fs::metadata(target).unwrap().len(),
            poisoned.len() as u64,
            "row {row}"
        );
        for ((expected_name, expected), (name, path, _, _)) in
            durable_before.iter().zip(records.iter())
        {
            assert_eq!(expected_name, name);
            assert_eq!(
                matrix_snapshot_path(path),
                *expected,
                "row {row} mutated {name}"
            );
        }
        assert_eq!(
            matrix_snapshot_path(&job.join("workspace")),
            workspace_before,
            "row {row} mutated workspace"
        );
        assert_eq!(
            matrix_snapshot_path(&cache),
            cache_before,
            "row {row} mutated cache"
        );
        assert_eq!(
            fs::read(&sentinel).unwrap(),
            b"outside-state-must-survive",
            "row {row}"
        );
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
fn submit_uses_request_creation_time_while_lease_uses_the_host_clock() {
    // Break caught: submit regenerates the job timestamp from host_now, or
    // lease TTL accidentally starts from the client-bound job timestamp.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("request-and-lease-clocks");
    let (store, lease, request) = prepared_host(&root);
    assert_eq!(request.material().created_at_millis(), 10);
    assert_eq!(lease.created_at_millis(), 1);
    assert_eq!(lease.expires_at_millis(), 30_001);
    drop(store);

    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterJobIndexParentSync)
            .unwrap();
    let error = JobService::new(&faulted, &RejectLauncher)
        .submit_at(request, 999)
        .unwrap_err();
    assert!(error.to_string().contains("injected"), "{error}");

    let job = faulted
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let meta: JobMeta = serde_json::from_slice(&fs::read(job.join("meta.json")).unwrap()).unwrap();
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(meta.created_at_millis(), 10);
    assert_eq!(status, JobStatus::accepted(10).unwrap());
}

#[test]
fn durable_execution_payload_reconstruction_binds_creation_time() {
    // Break caught: recovery recomputes the execution fingerprint without the
    // bound timestamp and accepts self-consistent metadata/status tampering.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload-created-at-binding");
    let (store, lease, _request) = indexed_identityless_job(&root);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    replace_ascii_once(
        &job.join("meta.json"),
        "\"created_at_millis\":10",
        "\"created_at_millis\":11",
    );
    replace_ascii_once(
        &job.join("status.json"),
        "\"updated_at_millis\":10",
        "\"updated_at_millis\":11",
    );
    replace_ascii_once(
        &store.job_index(lease.job_id()).unwrap(),
        "\"updated_at_millis\":10",
        "\"updated_at_millis\":11",
    );

    let error = JobService::new(&store, &RejectLauncher)
        .status(lease.job_id())
        .unwrap_err();
    assert_error_code(error, "JOB_ID_CONFLICT", "creation time fingerprint");
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
fn terminal_accepted_submit_retry_after_lease_retirement() {
    // Break caught: submit checks for a live lease before durable Accepted
    // authority, so an exact retry after legitimate terminal cleanup becomes
    // LEASE_MISSING and immutable conflicts are masked by the same error.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("terminal-submit-retry");
    let marker = temp.path().join("terminal-submit-marker");
    let (store, lease, request) = prepared_host_with_command(&root, matrix_command(&marker));
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let original = JobService::new(&store, &launcher)
        .submit_at(request.clone(), 10)
        .unwrap();
    assert!(original.status().state().is_terminal());
    assert_eq!(fs::read(&marker).unwrap(), b"x");
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);

    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let authority_paths = [
        store.job_index(lease.job_id()).unwrap(),
        job.join("meta.json"),
        job.join("status.json"),
        job.join("stdout.log"),
        job.join("stderr.log"),
    ];
    let authority_before = authority_paths
        .iter()
        .map(|path| fs::read(path).unwrap())
        .collect::<Vec<_>>();
    let names_before = fs::read_dir(&job)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();
    let launches = Arc::new(AtomicUsize::new(0));

    let retry = JobService::new(
        &store,
        &CountingRejectLauncher {
            launches: Arc::clone(&launches),
        },
    )
    .submit_at(request.clone(), 11)
    .unwrap();

    assert!(matches!(retry, SubmitResponse::Existing { .. }));
    assert_eq!(retry.status(), original.status());
    assert_eq!(launches.load(Ordering::SeqCst), 0);
    assert_eq!(fs::read(&marker).unwrap(), b"x");
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    assert_eq!(
        authority_paths
            .iter()
            .map(|path| fs::read(path).unwrap())
            .collect::<Vec<_>>(),
        authority_before
    );
    assert_eq!(
        fs::read_dir(&job)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>(),
        names_before
    );

    for mutation in [
        "client",
        "token",
        "created_at",
        "worker",
        "project",
        "worktree",
        "digest",
        "cwd",
        "timeout",
        "resource",
        "command",
    ] {
        let material = request.material();
        let changed = SubmitRequest::new(
            RequestFingerprintMaterial::new(
                material.job_id(),
                if mutation == "client" {
                    ClientId::new(uuid::Uuid::from_u128(91_001))
                } else {
                    material.client_id()
                },
                if mutation == "token" {
                    LeaseToken::new(uuid::Uuid::from_u128(91_002))
                } else {
                    material.lease_token()
                },
                if mutation == "created_at" {
                    material.created_at_millis() + 1
                } else {
                    material.created_at_millis()
                },
                if mutation == "worker" {
                    "mini-2".into()
                } else {
                    material.worker_name().into()
                },
                if mutation == "project" {
                    "d".repeat(64)
                } else {
                    material.project_id().into()
                },
                if mutation == "worktree" {
                    "e".repeat(64)
                } else {
                    material.worktree_id().into()
                },
                if mutation == "digest" {
                    "f".repeat(64)
                } else {
                    material.manifest_digest().into()
                },
                if mutation == "cwd" {
                    "nested".into()
                } else {
                    material.relative_working_dir().into()
                },
                if mutation == "timeout" {
                    material.timeout_millis() + 1
                } else {
                    material.timeout_millis()
                },
                if mutation == "resource" {
                    "light".into()
                } else {
                    material.resource_class().into()
                },
                if mutation == "command" {
                    CommandSpec::argv(vec!["/usr/bin/false".into()]).unwrap()
                } else {
                    material.command().clone()
                },
            )
            .unwrap(),
        );
        let error = JobService::new(
            &store,
            &CountingRejectLauncher {
                launches: Arc::clone(&launches),
            },
        )
        .submit_at(changed, 12)
        .unwrap_err();
        assert_error_code(error, "JOB_ID_CONFLICT", mutation);
    }
    assert_eq!(launches.load(Ordering::SeqCst), 0);
    assert_eq!(fs::read(&marker).unwrap(), b"x");
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);

    let absent = HostStore::open(&temp.path().join("absent")).unwrap();
    let error = JobService::new(&absent, &RejectLauncher)
        .submit_at(request.clone(), 13)
        .unwrap_err();
    assert_error_code(error, "LEASE_MISSING", "no durable disposition");

    for (index, mutation) in [
        "missing-final",
        "missing-meta",
        "corrupt-meta",
        "missing-status",
        "corrupt-status",
    ]
    .into_iter()
    .enumerate()
    {
        let corrupt_root = temp.path().join(format!("corrupt-terminal-{index}"));
        let corrupt_marker = temp.path().join(format!("corrupt-marker-{index}"));
        let (corrupt_store, corrupt_lease, corrupt_request) =
            prepared_host_with_command(&corrupt_root, matrix_command(&corrupt_marker));
        let corrupt_launcher = InlineSupervisorLauncher {
            store: corrupt_store.clone(),
        };
        JobService::new(&corrupt_store, &corrupt_launcher)
            .submit_at(corrupt_request.clone(), 20)
            .unwrap();
        assert_eq!(LeaseService::new(&corrupt_store).load().unwrap(), None);
        let corrupt_job = corrupt_store
            .job(
                corrupt_lease.project_id(),
                corrupt_lease.worktree_id(),
                corrupt_lease.job_id(),
            )
            .unwrap();
        match mutation {
            "missing-final" => {
                let parent = corrupt_job.parent().unwrap();
                fs::remove_dir_all(&corrupt_job).unwrap();
                File::open(parent).unwrap().sync_all().unwrap();
            }
            "missing-meta" => remove_and_sync(&corrupt_job.join("meta.json")),
            "corrupt-meta" => replace_bytes(&corrupt_job.join("meta.json"), b"{").unwrap(),
            "missing-status" => remove_and_sync(&corrupt_job.join("status.json")),
            "corrupt-status" => replace_bytes(&corrupt_job.join("status.json"), b"{").unwrap(),
            _ => unreachable!(),
        }
        let corrupt_launches = Arc::new(AtomicUsize::new(0));
        let error = JobService::new(
            &corrupt_store,
            &CountingRejectLauncher {
                launches: Arc::clone(&corrupt_launches),
            },
        )
        .submit_at(corrupt_request, 21)
        .unwrap_err();
        assert!(
            !error.to_string().contains("LEASE_MISSING"),
            "{mutation}: accepted corruption was masked by {error}"
        );
        assert_eq!(corrupt_launches.load(Ordering::SeqCst), 0, "{mutation}");
        assert_eq!(fs::read(&corrupt_marker).unwrap(), b"x", "{mutation}");
    }
}

#[test]
fn terminal_accepted_submit_retry_ignores_an_unrelated_live_lease() {
    // Break caught: the singleton live lease is treated as belonging to this
    // terminal job, so an exact retry conflicts instead of using its durable
    // Accepted authority.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("terminal-retry-unrelated-lease");
    let marker = temp.path().join("terminal-retry-unrelated-marker");
    let (store, _lease, request) = prepared_host_with_command(&root, matrix_command(&marker));
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let original = JobService::new(&store, &launcher)
        .submit_at(request.clone(), 10)
        .unwrap();
    assert!(original.status().state().is_terminal());
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);

    let unrelated = lease_request_with_identity(
        &request,
        JobId::new(uuid::Uuid::from_u128(92_001)),
        ClientId::new(uuid::Uuid::from_u128(92_002)),
        LeaseToken::new(uuid::Uuid::from_u128(92_003)),
    );
    let unrelated_lease = match LeaseService::new(&store)
        .acquire(&unrelated, &healthy(), 11)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    let unrelated_lease_bytes = fs::read(root.join("leases/heavy/lease.json")).unwrap();

    let retry = JobService::new(&store, &RejectLauncher)
        .submit_at(request, 12)
        .unwrap();

    assert!(matches!(retry, SubmitResponse::Existing { .. }));
    assert_eq!(retry.status(), original.status());
    assert_eq!(fs::read(&marker).unwrap(), b"x");
    assert_eq!(
        LeaseService::new(&store).load().unwrap(),
        Some(unrelated_lease)
    );
    assert_eq!(
        fs::read(root.join("leases/heavy/lease.json")).unwrap(),
        unrelated_lease_bytes
    );
}

#[test]
fn submit_requires_a_matching_live_lease_without_a_disposition() {
    // Break caught: singleton occupancy by another job is mistaken for a
    // matching lease and reported as an immutable identity conflict.
    let temp = tempfile::tempdir().unwrap();
    let request = SubmitRequest::new(lease_request().material().clone());
    let unrelated_root = temp.path().join("unrelated");
    let unrelated_store = HostStore::open(&unrelated_root).unwrap();
    let unrelated = lease_request_with_identity(
        &request,
        JobId::new(uuid::Uuid::from_u128(93_001)),
        ClientId::new(uuid::Uuid::from_u128(93_002)),
        LeaseToken::new(uuid::Uuid::from_u128(93_003)),
    );
    let unrelated_lease = match LeaseService::new(&unrelated_store)
        .acquire(&unrelated, &healthy(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    let unrelated_lease_bytes = fs::read(unrelated_root.join("leases/heavy/lease.json")).unwrap();

    let error = JobService::new(&unrelated_store, &RejectLauncher)
        .submit_at(request.clone(), 2)
        .unwrap_err();

    assert_error_code(error, "LEASE_MISSING", "unrelated live lease");
    assert_eq!(
        LeaseService::new(&unrelated_store).load().unwrap(),
        Some(unrelated_lease)
    );
    assert_eq!(
        fs::read(unrelated_root.join("leases/heavy/lease.json")).unwrap(),
        unrelated_lease_bytes
    );

    // A live lease for the submitted job is still authoritative for immutable
    // identity comparison and must not be filtered out.
    let same_job_root = temp.path().join("same-job");
    let same_job_store = HostStore::open(&same_job_root).unwrap();
    let same_job_lease = match LeaseService::new(&same_job_store)
        .acquire(&lease_request(), &healthy(), 3)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    let same_job_lease_bytes = fs::read(same_job_root.join("leases/heavy/lease.json")).unwrap();
    let changed = SubmitRequest::new(
        lease_request_with_identity(
            &request,
            request.material().job_id(),
            ClientId::new(uuid::Uuid::from_u128(93_004)),
            LeaseToken::new(uuid::Uuid::from_u128(93_005)),
        )
        .material()
        .clone(),
    );

    let error = JobService::new(&same_job_store, &RejectLauncher)
        .submit_at(changed, 4)
        .unwrap_err();

    assert_error_code(error, "JOB_ID_CONFLICT", "same-job lease identity");
    assert_eq!(
        LeaseService::new(&same_job_store).load().unwrap(),
        Some(same_job_lease)
    );
    assert_eq!(
        fs::read(same_job_root.join("leases/heavy/lease.json")).unwrap(),
        same_job_lease_bytes
    );
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
        let marker_deadline = Instant::now() + CLEANUP_PROGRESS_DEADLINE;
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
    .reconcile_job(lease.job_id())
    .unwrap_err();

    assert!(
        started.elapsed() < CLEANUP_PROGRESS_DEADLINE,
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
    .reconcile_job(lease.job_id())
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
    let (runtime, paths) = endpoint_runtime_and_paths(temp);
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
    (runtime, submit, response)
}

fn endpoint_runtime_and_paths(temp: &tempfile::TempDir) -> (RuntimeContext, PathLayout) {
    let home = temp.path().join("home");
    fs::create_dir(&home).unwrap();
    let environment = BTreeMap::from([("HOME".into(), home.as_os_str().to_os_string())]);
    let paths = PathLayout::discover(None, &environment, &home).unwrap();
    (
        RuntimeContext::isolated(environment, home, temp.path().to_path_buf()),
        paths,
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
    let cancel = CancelRequest::new(
        submit.material().job_id(),
        submit.material().client_id(),
        submit.material().lease_token(),
        submit.request_fingerprint().clone(),
    )
    .unwrap();
    let expected_log = LogChunkResponse::new(
        LogChunk::new(LogStream::Stdout, 0, b"endpoint-log".to_vec()).unwrap(),
    )
    .unwrap();
    let expected_resolve =
        mac_worker::job::ResolveOrAbandonResponse::accepted(expected_status.clone()).unwrap();
    let reconcile = FleetReconcileRequest::new(vec![job_id]).unwrap();
    let expected_reconcile = FleetReconcileResponse::new(vec![FleetReconcileJobResult::Status {
        status: Box::new(expected_status.clone()),
    }])
    .unwrap();
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
        (
            "cancel",
            serde_json::to_vec(&cancel).unwrap(),
            serde_json::to_vec(&CancelResponse::new(expected_status.clone()).unwrap()).unwrap(),
        ),
        (
            "reconcile",
            serde_json::to_vec(&reconcile).unwrap(),
            serde_json::to_vec(&expected_reconcile).unwrap(),
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
fn host_reconcile_replays_an_indexed_terminal_status_without_discovery_or_mutation() {
    // Break caught: the fleet endpoint scans beyond the supplied ID, changes a
    // terminal record, or gives a different answer on a repeated recovery.
    let temp = tempfile::tempdir().unwrap();
    let (runtime, submit, expected) = endpoint_runtime_and_completed_job(&temp);
    let request = FleetReconcileRequest::new(vec![submit.material().job_id()]).unwrap();
    let input = serde_json::to_vec(&request).unwrap();

    let (first_exit, first_stdout, first_stderr) =
        run_query_endpoint(&runtime, "reconcile", input.clone());
    let (second_exit, second_stdout, second_stderr) =
        run_query_endpoint(&runtime, "reconcile", input);

    assert_eq!(first_exit, 0);
    assert_eq!(second_exit, 0);
    assert!(first_stderr.is_empty());
    assert!(second_stderr.is_empty());
    assert_eq!(first_stdout, second_stdout);
    let response: FleetReconcileResponse =
        serde_json::from_slice(first_stdout.strip_suffix(b"\n").unwrap()).unwrap();
    assert_eq!(response.results().len(), 1);
    assert_eq!(response.results()[0].status(), Some(&expected));
}

#[test]
fn fleet_recovery_delegates_an_exact_nonindexed_crash_record_to_phase_three_reconciliation() {
    // Break caught: the fleet host operation bypasses Phase 3's exact
    // pre-index repair path instead of reconciling the supplied ID.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, _submit) = unindexed_identityless_job(&root);
    assert!(!store.job_index(lease.job_id()).unwrap().exists());
    let launches = Arc::new(AtomicUsize::new(0));
    let supervisor = identity(41_004);
    let launcher = HoldingRecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: supervisor,
        guard: Mutex::new(None),
    };

    let response = JobService::new(&store, &launcher)
        .reconcile_job(lease.job_id())
        .unwrap();

    assert_eq!(response.meta().job_id(), lease.job_id());
    assert_eq!(response.status().supervisor_identity(), Some(supervisor));
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    assert!(store.job_index(lease.job_id()).unwrap().is_file());
    drop(launcher.guard.lock().unwrap().take());
}

#[test]
fn host_reconcile_returns_per_job_errors_for_corrupt_status_or_lease() {
    // Break caught: fleet recovery masks corrupt durable authority as a
    // successful status, or mutates the corrupt evidence while reporting it.
    for (label, target) in [("status", "status.json"), ("lease", "lease.json")] {
        let temp = tempfile::tempdir().unwrap();
        let (runtime, paths) = endpoint_runtime_and_paths(&temp);
        let (store, lease, _submit) = indexed_identityless_job(&paths.host_state_root());
        let path = if label == "status" {
            store
                .job(lease.project_id(), lease.worktree_id(), lease.job_id())
                .unwrap()
                .join(target)
        } else {
            paths.host_state_root().join("leases/heavy").join(target)
        };
        replace_bytes(&path, b"{").unwrap();
        let request = FleetReconcileRequest::new(vec![lease.job_id()]).unwrap();

        let (exit, stdout, stderr) =
            run_query_endpoint(&runtime, "reconcile", serde_json::to_vec(&request).unwrap());

        assert_eq!(exit, 0, "{label}");
        assert!(stderr.is_empty(), "{label}");
        let response: FleetReconcileResponse =
            serde_json::from_slice(stdout.strip_suffix(b"\n").unwrap()).unwrap();
        assert_eq!(response.results().len(), 1, "{label}");
        assert_eq!(response.results()[0].job_id(), lease.job_id(), "{label}");
        assert_eq!(
            response.results()[0].error().unwrap().error().code(),
            "JOB_STATE_INVALID",
            "{label}"
        );
        assert_eq!(fs::read(path).unwrap(), b"{", "{label}");
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
    let cancel = CancelRequest::new(
        submit.material().job_id(),
        submit.material().client_id(),
        submit.material().lease_token(),
        submit.request_fingerprint().clone(),
    )
    .unwrap();
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
        ("cancel", serde_json::to_vec(&cancel).unwrap()),
        (
            "reconcile",
            serde_json::to_vec(&FleetReconcileRequest::new(vec![job_id]).unwrap()).unwrap(),
        ),
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

/// A loopback SSH runner: it feeds the exact bytes that the client would send
/// to the hidden endpoint, records them, and can drop a response only after the
/// real endpoint has produced it.  This keeps the disconnect seam outside the
/// host implementation.
struct MatrixEndpointRunner {
    runtime: RuntimeContext,
    requests: Mutex<Vec<(String, Vec<u8>)>>,
    lose_success_responses: AtomicUsize,
}

impl MatrixEndpointRunner {
    fn new(runtime: RuntimeContext) -> Self {
        Self {
            runtime,
            requests: Mutex::new(Vec::new()),
            lose_success_responses: AtomicUsize::new(0),
        }
    }

    fn requests(&self) -> Vec<(String, Vec<u8>)> {
        self.requests.lock().unwrap().clone()
    }
}

impl ProcessRunner for MatrixEndpointRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let command = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .and_then(|argument| argument.strip_prefix("~/.local/bin/worker host "))
            .expect("matrix runner received a fixed hidden host command")
            .to_owned();
        let input = request.stdin.clone().expect("matrix request has stdin");
        let (exit, stdout, stderr) = run_query_endpoint(&self.runtime, &command, input.clone());
        self.requests.lock().unwrap().push((command.clone(), input));
        if exit == 0
            && self
                .lose_success_responses
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return Err(WorkerError::Transport {
                code: "MATRIX_RESPONSE_LOST",
                message: "the test runner dropped a real host response".into(),
            });
        }
        Ok(ProcessResult {
            status: std::process::ExitStatus::from_raw((exit as i32) << 8),
            stdout,
            stderr,
        })
    }
}

struct MatrixSubmitLossRunner<'a> {
    endpoint: &'a MatrixEndpointRunner,
    store: HostStore,
    expected: SubmitRequest,
    launches: Arc<AtomicUsize>,
    marker: PathBuf,
    now: u64,
    serialize_success: bool,
    submit_calls: AtomicUsize,
    injected_losses: AtomicUsize,
    captured_success: Mutex<Option<Vec<u8>>>,
    events: Mutex<Vec<&'static str>>,
}

impl MatrixSubmitLossRunner<'_> {
    fn captured_success(&self) -> Option<Vec<u8>> {
        self.captured_success.lock().unwrap().clone()
    }

    fn events(&self) -> Vec<&'static str> {
        self.events.lock().unwrap().clone()
    }
}

impl ProcessRunner for MatrixSubmitLossRunner<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.args.last() != Some(&OsString::from(HostOperation::Submit.command())) {
            return self.endpoint.run(request);
        }
        assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
        assert_eq!(
            request.args,
            [
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "ForwardAgent=no",
                "-o",
                "ClearAllForwardings=yes",
                "--",
                "matrix-host",
                "~/.local/bin/worker host submit",
            ]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>()
        );
        assert!(request.environment.is_empty());
        assert!(request.environment_remove.is_empty());
        assert_eq!(request.policy, matrix_control_policy());
        let input = request.stdin.as_ref().expect("submit request has stdin");
        assert_eq!(input, &serde_json::to_vec(&self.expected).unwrap());
        let decoded: SubmitRequest = serde_json::from_slice(input).unwrap();
        assert_eq!(decoded, self.expected);
        assert_eq!(
            self.submit_calls.fetch_add(1, Ordering::SeqCst),
            0,
            "original submit ran more than once through the loss runner"
        );
        self.endpoint
            .requests
            .lock()
            .unwrap()
            .push(("submit".into(), input.clone()));

        let launcher = MatrixInlineLauncher {
            store: self.store.clone(),
            launches: Arc::clone(&self.launches),
        };
        let response =
            JobService::new(&self.store, &launcher).submit_at(self.expected.clone(), self.now)?;
        response.validate()?;
        assert!(
            matches!(&response, SubmitResponse::Accepted { .. }),
            "loss runner must execute the original submit, not an Existing replay"
        );
        assert_eq!(
            fs::read(&self.marker)?,
            b"x",
            "submit loss occurred before actual command execution"
        );
        self.events.lock().unwrap().push("real-submit-complete");

        if self.serialize_success {
            let canonical = serde_json::to_vec(&response).unwrap();
            let wire = [canonical.as_slice(), b"\n"].concat();
            let decoded: SubmitResponse = serde_json::from_slice(&wire).unwrap();
            decoded.validate().unwrap();
            assert_eq!(decoded, response);
            assert_eq!(
                wire,
                [serde_json::to_vec(&decoded).unwrap().as_slice(), b"\n"].concat()
            );
            *self.captured_success.lock().unwrap() = Some(wire);
            self.events
                .lock()
                .unwrap()
                .push("canonical-success-captured");
        } else {
            assert!(self.captured_success.lock().unwrap().is_none());
            self.events
                .lock()
                .unwrap()
                .push("pre-serialization-loss-point");
        }

        self.events.lock().unwrap().push("loss-injected");
        assert_eq!(
            self.injected_losses.fetch_add(1, Ordering::SeqCst),
            0,
            "original submit response loss was injected more than once"
        );
        Err(WorkerError::Transport {
            code: "MATRIX_SUBMIT_RESPONSE_LOST",
            message: "the test runner dropped the original submit response".into(),
        })
    }
}

fn matrix_control_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 1024 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(30),
    }
}

#[derive(Default)]
struct MatrixResolutionRuntime {
    now: Mutex<Duration>,
}

impl ResolutionRuntime for MatrixResolutionRuntime {
    fn monotonic_now(&self) -> Duration {
        *self.now.lock().unwrap()
    }

    fn sleep(&self, duration: Duration) {
        *self.now.lock().unwrap() += duration;
    }
}

struct MatrixInlineLauncher {
    store: HostStore,
    launches: Arc<AtomicUsize>,
}

struct MatrixRejectedReceiver(AtomicUsize);

struct MatrixReceiptExecutor {
    source: PathBuf,
    sink: PathBuf,
    calls: Arc<AtomicUsize>,
}

impl RsyncServerExecutor for MatrixReceiptExecutor {
    fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
        assert_eq!(
            invocation.server_args(),
            [
                OsStr::new("--server"),
                OsStr::new("--delete-before"),
                OsStr::new("-l"),
                OsStr::new("-p"),
                OsStr::new("-D"),
                OsStr::new("-r"),
                OsStr::new("-t"),
                OsStr::new("--dirs"),
                OsStr::new("."),
                OsStr::new("."),
            ]
        );
        assert_eq!(
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "matrix receiver executed more than once"
        );
        assert!(fs::metadata(self.source.join("manifest.json"))?.len() <= 1024 * 1024);
        assert_eq!(fs::metadata(self.source.join("tree/payload.txt"))?.len(), 7);
        let source = RootedDir::open(&self.source)?;
        let tree = RelativePath::parse(b"tree").unwrap();
        let manifest = RelativePath::parse(b"manifest.json").unwrap();
        let payload = RelativePath::parse(b"tree/payload.txt").unwrap();
        invocation.destination().create_empty_directory(&tree)?;
        source.copy_regular_to(&manifest, invocation.destination())?;
        source.copy_regular_to(&payload, invocation.destination())?;
        fs::set_permissions(self.sink.join("tree"), fs::Permissions::from_mode(0o555))?;
        Ok(())
    }
}

impl RsyncServerExecutor for MatrixRejectedReceiver {
    fn execute(&self, _invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("a delayed receiver must be fenced before opening its sink")
    }
}

impl SupervisorLauncher for MatrixInlineLauncher {
    fn launch(
        &self,
        job_id: JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, WorkerError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(std::process::id())?;
        Supervisor::new(&self.store, &inspector).run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

fn matrix_worker() -> WorkerEntry {
    WorkerEntry {
        name: "mini-1".into(),
        ssh: "matrix-host".into(),
        slots: 1,
        capabilities: vec!["darwin-arm64".into()],
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    }
}

fn matrix_command(marker: &Path) -> CommandSpec {
    let marker = marker.to_string_lossy().replace('\'', "'\\\"'\\\"'");
    CommandSpec::shell(format!("/usr/bin/printf x >> '{marker}'")).unwrap()
}

fn matrix_acquired_host(
    root: &Path,
    command: CommandSpec,
) -> (HostStore, LeaseRecord, SubmitRequest, Vec<u8>) {
    let store = HostStore::open(root).unwrap();
    let (acquire, submit, manifest) = matrix_request(command);
    let lease = match LeaseService::new(&store)
        .acquire(&acquire, &healthy(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    (store, lease, submit, manifest)
}

fn matrix_request(command: CommandSpec) -> (LeaseAcquireRequest, SubmitRequest, Vec<u8>) {
    let manifest = valid_manifest_bytes();
    let digest = format!("{:x}", Sha256::digest(&manifest));
    let acquire = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            JOB_ID.parse().unwrap(),
            CLIENT_ID.parse().unwrap(),
            LEASE_TOKEN.parse().unwrap(),
            10,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            digest,
            String::new(),
            30_000,
            "heavy".into(),
            command,
        )
        .unwrap(),
    );
    (
        acquire.clone(),
        SubmitRequest::new(acquire.material().clone()),
        manifest,
    )
}

fn matrix_receive_before_verification(
    store: &HostStore,
    lease: &LeaseRecord,
    manifest: &[u8],
    fixture: &Path,
    calls: Arc<AtomicUsize>,
) {
    assert!(
        manifest.len() <= 1024 * 1024,
        "matrix manifest is not bounded"
    );
    const PAYLOAD: &[u8] = b"payload";
    let source = fixture.join("receiver-source");
    fs::create_dir_all(source.join("tree")).unwrap();
    fs::write(source.join("manifest.json"), manifest).unwrap();
    fs::write(source.join("tree/payload.txt"), PAYLOAD).unwrap();
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    let identity = TransferIdentity::new(
        lease.job_id(),
        lease.client_id(),
        lease.lease_token(),
        lease.request_fingerprint().clone(),
    );
    let server_args = [
        "--server",
        "--delete-before",
        "-l",
        "-p",
        "-D",
        "-r",
        "-t",
        "--dirs",
        ".",
        "incoming",
    ]
    .into_iter()
    .map(OsString::from)
    .collect::<Vec<_>>();
    HostTransferService::new(store)
        .receive(
            &identity,
            &server_args,
            &MatrixReceiptExecutor {
                source,
                sink: incoming.clone(),
                calls,
            },
        )
        .unwrap();
    assert_eq!(fs::read(incoming.join("manifest.json")).unwrap(), manifest);
    assert_eq!(
        fs::read(incoming.join("tree/payload.txt")).unwrap(),
        PAYLOAD
    );
}

fn matrix_runtime(temp: &tempfile::TempDir) -> (RuntimeContext, PathBuf) {
    let home = temp.path().join("home");
    fs::create_dir(&home).unwrap();
    let environment = BTreeMap::from([("HOME".into(), home.as_os_str().to_os_string())]);
    let paths = PathLayout::discover(None, &environment, &home).unwrap();
    (
        RuntimeContext::isolated(environment, home, temp.path().to_path_buf()),
        paths.host_state_root(),
    )
}

fn matrix_remote_resolve(
    runner: &MatrixEndpointRunner,
    request: &ResolveOrAbandonRequest,
) -> mac_worker::job::ResolveOrAbandonResponse {
    SshJsonTransport::new(runner)
        .request(
            &matrix_worker(),
            HostOperation::ResolveOrAbandon,
            request,
            matrix_control_policy(),
        )
        .unwrap()
}

fn matrix_exact_submit(
    store: &HostStore,
    submit: &SubmitRequest,
    launches: &Arc<AtomicUsize>,
    now: u64,
) -> Result<SubmitResponse, WorkerError> {
    let launcher = MatrixInlineLauncher {
        store: store.clone(),
        launches: Arc::clone(launches),
    };
    JobService::new(store, &launcher).submit_at(submit.clone(), now)
}

struct MatrixOriginalMutation<'a> {
    boundary: usize,
    store: &'a HostStore,
    lease: Option<&'a LeaseRecord>,
    submit: &'a SubmitRequest,
    manifest: &'a [u8],
    fixture: &'a Path,
    receiver_calls: &'a Arc<AtomicUsize>,
    launches: &'a Arc<AtomicUsize>,
}

impl MatrixOriginalMutation<'_> {
    fn prepare(&self, now: u64) -> Result<LeaseRecord, WorkerError> {
        let lease = match self.lease {
            Some(lease) => lease.clone(),
            None => match LeaseService::new(self.store).acquire(
                &LeaseAcquireRequest::new(self.submit.material().clone()),
                &healthy(),
                now,
            )? {
                LeaseAcquireResponse::Acquired { lease } => lease,
                LeaseAcquireResponse::ExistingAccepted { .. } => {
                    return Err(WorkerError::Protocol(
                        "matrix original acquire unexpectedly observed Accepted".into(),
                    ));
                }
            },
        };
        if self.boundary == 0 {
            matrix_receive_before_verification(
                self.store,
                &lease,
                self.manifest,
                self.fixture,
                Arc::clone(self.receiver_calls),
            );
        }
        if self.boundary <= 1 {
            RemoteSnapshotService::new(self.store).verify_and_promote_at(
                &lease,
                lease.manifest_digest(),
                now + 1,
            )?;
        }
        Ok(lease)
    }

    fn complete(&self, now: u64) -> Result<(LeaseRecord, SubmitResponse), WorkerError> {
        let lease = self.prepare(now)?;
        matrix_exact_submit(self.store, self.submit, self.launches, now + 2)
            .map(|response| (lease, response))
    }
}

fn matrix_trace(trace: &Arc<Mutex<Vec<&'static str>>>, event: &'static str) {
    trace.lock().unwrap().push(event);
}

fn matrix_resolver_before_mutator<M, R>(
    mutator: M,
    resolver: R,
    trace: &Arc<Mutex<Vec<&'static str>>>,
    ready_event: &'static str,
    complete_event: &'static str,
) -> (
    mac_worker::job::ResolveOrAbandonResponse,
    Result<(LeaseRecord, SubmitResponse), WorkerError>,
)
where
    M: FnOnce() -> Result<(LeaseRecord, SubmitResponse), WorkerError> + Send,
    R: FnOnce() -> mac_worker::job::ResolveOrAbandonResponse,
{
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    thread::scope(|scope| {
        let delayed_trace = Arc::clone(trace);
        let delayed = scope.spawn(move || {
            matrix_trace(&delayed_trace, ready_event);
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            let result = mutator();
            matrix_trace(&delayed_trace, complete_event);
            result
        });
        ready_rx.recv().unwrap();
        let authority = resolver();
        matrix_trace(trace, "authority");
        matrix_trace(trace, "mutator-release-sent");
        release_tx.send(()).unwrap();
        (authority, delayed.join().unwrap())
    })
}

fn matrix_mutator_before_resolver<M, R>(
    mutator: M,
    resolver: R,
    trace: &Arc<Mutex<Vec<&'static str>>>,
    complete_event: &'static str,
) -> (
    Result<(LeaseRecord, SubmitResponse), WorkerError>,
    mac_worker::job::ResolveOrAbandonResponse,
)
where
    M: FnOnce() -> Result<(LeaseRecord, SubmitResponse), WorkerError>,
    R: FnOnce() -> mac_worker::job::ResolveOrAbandonResponse + Send,
{
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    thread::scope(|scope| {
        let delayed_trace = Arc::clone(trace);
        let delayed = scope.spawn(move || {
            matrix_trace(&delayed_trace, "resolver-ready");
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            let result = resolver();
            matrix_trace(&delayed_trace, "authority");
            result
        });
        ready_rx.recv().unwrap();
        let mutation = mutator();
        matrix_trace(trace, complete_event);
        matrix_trace(trace, "resolver-release-sent");
        release_tx.send(()).unwrap();
        (mutation, delayed.join().unwrap())
    })
}

fn matrix_assert_original_identity(submit: &SubmitRequest) {
    submit.validate().unwrap();
    assert_eq!(submit.material().job_id().to_string(), JOB_ID);
    assert_eq!(submit.material().client_id().to_string(), CLIENT_ID);
    assert_eq!(submit.material().lease_token().to_string(), LEASE_TOKEN);
    assert_eq!(submit.material().worker_name(), "mini-1");
    assert_eq!(submit.material().project_id(), PROJECT_ID);
    assert_eq!(submit.material().worktree_id(), WORKTREE_ID);
    assert_eq!(submit.material().relative_working_dir(), "");
    assert_eq!(submit.material().timeout_millis(), 30_000);
    assert_eq!(submit.material().resource_class(), "heavy");
    assert_eq!(
        submit.material().fingerprint(),
        submit.request_fingerprint().clone()
    );
    submit
        .material()
        .command()
        .summary()
        .unwrap()
        .validate()
        .unwrap();
}

fn matrix_assert_protocol_code(error: &WorkerError, expected: &str, context: &str) {
    let WorkerError::Protocol(message) = error else {
        panic!("{context}: expected protocol code {expected}, got {error}");
    };
    let (actual, _) = message
        .split_once(": ")
        .unwrap_or_else(|| panic!("{context}: protocol error omitted its code: {error}"));
    assert_eq!(actual, expected, "{context}: got {error}");
}

fn matrix_assert_abandoned_fences(
    store: &HostStore,
    lease: Option<&LeaseRecord>,
    submit: &SubmitRequest,
) {
    let acquire = LeaseAcquireRequest::new(submit.material().clone());
    let retry = LeaseService::new(store)
        .acquire(&acquire, &healthy(), 9)
        .unwrap_err();
    matrix_assert_protocol_code(&retry, "JOB_ABANDONED", "delayed acquire fence");
    let receiver = MatrixRejectedReceiver(AtomicUsize::new(0));
    let receiver_error = HostTransferService::new(store).receive(
        &TransferIdentity::from_acquire_request(&acquire).unwrap(),
        &[
            "--server".into(),
            "--delete-before".into(),
            "-l".into(),
            "-p".into(),
            "-D".into(),
            "-r".into(),
            "-t".into(),
            "--dirs".into(),
            ".".into(),
            "incoming".into(),
        ],
        &receiver,
    );
    let receiver_error = receiver_error.expect_err("delayed receiver resurrected abandoned work");
    matrix_assert_protocol_code(&receiver_error, "JOB_ABANDONED", "delayed receiver fence");
    assert_eq!(receiver.0.load(Ordering::SeqCst), 0);
    let stale_lease;
    let verification_lease = match lease {
        Some(lease) => lease,
        None => {
            stale_lease = LeaseRecord::new(
                submit.material(),
                submit.request_fingerprint().clone(),
                1,
                60_001,
            )
            .unwrap();
            &stale_lease
        }
    };
    let verify_attempts = AtomicUsize::new(0);
    verify_attempts.fetch_add(1, Ordering::SeqCst);
    let verify = RemoteSnapshotService::new(store)
        .verify_and_promote_at(verification_lease, verification_lease.manifest_digest(), 10)
        .expect_err("delayed verify resurrected abandoned work");
    matrix_assert_protocol_code(&verify, "LEASE_IDENTITY_MISMATCH", "delayed verify fence");
    assert_eq!(verify_attempts.load(Ordering::SeqCst), 1);
    let launches = Arc::new(AtomicUsize::new(0));
    let submit_error = JobService::new(
        store,
        &CountingRejectLauncher {
            launches: Arc::clone(&launches),
        },
    )
    .submit_at(submit.clone(), 11)
    .expect_err("delayed submit resurrected abandoned work");
    matrix_assert_protocol_code(&submit_error, "JOB_ABANDONED", "delayed submit fence");
    assert_eq!(launches.load(Ordering::SeqCst), 0);
}

fn matrix_assert_recorded_endpoint_identity(command: &str, bytes: &[u8], submit: &SubmitRequest) {
    match command {
        "status" => {
            let request: StatusRequest = serde_json::from_slice(bytes).unwrap();
            assert_eq!(request.job_id(), submit.material().job_id());
        }
        "submit" => {
            let request: SubmitRequest = serde_json::from_slice(bytes).unwrap();
            assert_eq!(request, *submit);
        }
        "resolve-or-abandon" => {
            let request: ResolveOrAbandonRequest = serde_json::from_slice(bytes).unwrap();
            assert_eq!(request.job_id(), submit.material().job_id());
            assert_eq!(request.client_id(), submit.material().client_id());
            assert_eq!(request.lease_token(), submit.material().lease_token());
            assert_eq!(request.request_fingerprint(), submit.request_fingerprint());
            assert_eq!(request.worker_name(), submit.material().worker_name());
            assert_eq!(request.project_id(), submit.material().project_id());
            assert_eq!(request.worktree_id(), submit.material().worktree_id());
            assert_eq!(
                request.manifest_digest(),
                submit.material().manifest_digest()
            );
            assert_eq!(
                request.relative_working_dir(),
                submit.material().relative_working_dir()
            );
            assert_eq!(request.timeout_millis(), submit.material().timeout_millis());
            assert_eq!(request.resource_class(), submit.material().resource_class());
            assert_eq!(
                request.command_summary(),
                &submit.material().command().summary().unwrap()
            );
        }
        unexpected => panic!("unexpected matrix host endpoint {unexpected}"),
    }
}

fn matrix_run_submit_loss(
    endpoint: &MatrixEndpointRunner,
    store: &HostStore,
    submit: &SubmitRequest,
    launches: &Arc<AtomicUsize>,
    marker: &Path,
    now: u64,
    serialize_success: bool,
) -> (Result<SubmitResponse, WorkerError>, Option<Vec<u8>>) {
    let runner = MatrixSubmitLossRunner {
        endpoint,
        store: store.clone(),
        expected: submit.clone(),
        launches: Arc::clone(launches),
        marker: marker.to_path_buf(),
        now,
        serialize_success,
        submit_calls: AtomicUsize::new(0),
        injected_losses: AtomicUsize::new(0),
        captured_success: Mutex::new(None),
        events: Mutex::new(Vec::new()),
    };
    let result = SshJsonTransport::new(&runner).request::<_, SubmitResponse>(
        &matrix_worker(),
        HostOperation::Submit,
        submit,
        matrix_control_policy(),
    );
    let error = result
        .as_ref()
        .expect_err("submit success escaped the loss runner");
    match error {
        WorkerError::Transport { code, message } => {
            assert_eq!(*code, "SSH_LAUNCH_FAILED");
            assert_eq!(message, "failed to launch SSH control request");
        }
        unexpected => panic!("submit loss runner returned the wrong actual error: {unexpected}"),
    }
    assert_eq!(runner.submit_calls.load(Ordering::SeqCst), 1);
    assert_eq!(runner.injected_losses.load(Ordering::SeqCst), 1);
    assert_eq!(
        runner.events(),
        if serialize_success {
            vec![
                "real-submit-complete",
                "canonical-success-captured",
                "loss-injected",
            ]
        } else {
            vec![
                "real-submit-complete",
                "pre-serialization-loss-point",
                "loss-injected",
            ]
        },
        "submit loss runner crossed its boundary out of order"
    );
    let captured = runner.captured_success();
    assert_eq!(captured.is_some(), serialize_success);
    (result, captured)
}

fn matrix_reconcile_actual_submit_loss(
    runner: &MatrixEndpointRunner,
    runtime: &MatrixResolutionRuntime,
    submit: &SubmitRequest,
    lost: Result<SubmitResponse, WorkerError>,
    expected_canonical: Option<&SubmitResponse>,
    forwarded: &AtomicUsize,
) -> SubmitResponse {
    assert!(lost.is_err(), "submit-loss reconciliation received success");
    let reconciled = RemoteJobClient::new_with_runtime(runner, runtime)
        .resolve_submission(&matrix_worker(), submit, lost)
        .unwrap();
    reconciled.validate().unwrap();
    let SubmitResponse::Accepted { meta, status } = &reconciled else {
        panic!("lost original submit reconciled as an idempotent replay")
    };
    matrix_assert_meta_identity(meta, submit);
    assert!(status.state().is_terminal());
    if let Some(expected) = expected_canonical {
        assert_eq!(
            serde_json::to_vec(&reconciled).unwrap(),
            serde_json::to_vec(expected).unwrap(),
            "reconciliation changed the exact canonical submit success"
        );
    }
    assert_eq!(
        forwarded.fetch_add(1, Ordering::SeqCst),
        0,
        "the same actual submit error was forwarded more than once"
    );
    reconciled
}

fn matrix_resolution_after_boundary(
    boundary: usize,
    store: &HostStore,
    submit: &SubmitRequest,
    launches: &Arc<AtomicUsize>,
    runner: &MatrixEndpointRunner,
) -> mac_worker::job::ResolveOrAbandonResponse {
    let request = ResolveOrAbandonRequest::from_submit_request(submit).unwrap();
    if boundary == 3 {
        let launcher = MatrixInlineLauncher {
            store: store.clone(),
            launches: Arc::clone(launches),
        };
        JobService::new(store, &launcher)
            .resolve_or_abandon(request)
            .unwrap()
    } else {
        matrix_remote_resolve(runner, &request)
    }
}

fn matrix_assert_meta_identity(meta: &JobMeta, submit: &SubmitRequest) {
    let material = submit.material();
    meta.validate().unwrap();
    assert_eq!(meta.job_id(), material.job_id());
    assert_eq!(meta.client_id(), material.client_id());
    assert_eq!(meta.worker_name(), material.worker_name());
    assert_eq!(meta.project_id(), material.project_id());
    assert_eq!(meta.worktree_id(), material.worktree_id());
    assert_eq!(meta.manifest_digest(), material.manifest_digest());
    assert_eq!(meta.request_fingerprint(), submit.request_fingerprint());
    assert_eq!(meta.relative_working_dir(), material.relative_working_dir());
    assert_eq!(meta.timeout_millis(), material.timeout_millis());
    assert_eq!(meta.resource_class(), material.resource_class());
    assert_eq!(
        meta.command_summary(),
        &material.command().summary().unwrap()
    );
}

fn matrix_assert_lease_identity(lease: &LeaseRecord, submit: &SubmitRequest) {
    let material = submit.material();
    lease.validate().unwrap();
    assert_eq!(lease.job_id(), material.job_id());
    assert_eq!(lease.client_id(), material.client_id());
    assert_eq!(lease.lease_token(), material.lease_token());
    assert_eq!(lease.request_fingerprint(), submit.request_fingerprint());
    assert_eq!(lease.worker_name(), material.worker_name());
    assert_eq!(lease.project_id(), material.project_id());
    assert_eq!(lease.worktree_id(), material.worktree_id());
    assert_eq!(lease.manifest_digest(), material.manifest_digest());
    assert_eq!(lease.timeout_millis(), material.timeout_millis());
    assert_eq!(lease.resource_class(), material.resource_class());
    assert_eq!(
        lease.command_summary(),
        &material.command().summary().unwrap()
    );
}

fn matrix_count_named_file(root: &Path, name: &str) -> usize {
    if !root.exists() {
        return 0;
    }
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| {
            if path.is_dir() {
                matrix_count_named_file(&path, name)
            } else {
                usize::from(path.file_name() == Some(OsStr::new(name)))
            }
        })
        .sum()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MatrixPathSnapshot {
    Absent,
    File(Vec<u8>),
    Directory(BTreeMap<OsString, MatrixPathSnapshot>),
    Symlink(OsString),
}

fn matrix_snapshot_path(path: &Path) -> MatrixPathSnapshot {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return MatrixPathSnapshot::Absent;
        }
        Err(error) => panic!("failed to snapshot {}: {error}", path.display()),
    };
    if metadata.file_type().is_symlink() {
        return MatrixPathSnapshot::Symlink(fs::read_link(path).unwrap().into_os_string());
    }
    if metadata.is_file() {
        return MatrixPathSnapshot::File(fs::read(path).unwrap());
    }
    assert!(
        metadata.is_dir(),
        "unsupported matrix state at {}",
        path.display()
    );
    MatrixPathSnapshot::Directory(
        fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), matrix_snapshot_path(&entry.path()))
            })
            .collect(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MatrixDurableSnapshot {
    disposition: MatrixPathSnapshot,
    jobs: MatrixPathSnapshot,
    leases: MatrixPathSnapshot,
    incoming: MatrixPathSnapshot,
    verified_receipt: MatrixPathSnapshot,
    verified_stage: MatrixPathSnapshot,
    snapshot_cache: MatrixPathSnapshot,
    job_locks: MatrixPathSnapshot,
    marker: MatrixPathSnapshot,
}

fn matrix_durable_snapshot(
    root: &Path,
    store: &HostStore,
    submit: &SubmitRequest,
    marker: &Path,
) -> MatrixDurableSnapshot {
    let material = submit.material();
    MatrixDurableSnapshot {
        disposition: matrix_snapshot_path(&store.job_index(material.job_id()).unwrap()),
        jobs: matrix_snapshot_path(&root.join("jobs")),
        leases: matrix_snapshot_path(&root.join("leases")),
        incoming: matrix_snapshot_path(&root.join("incoming").join(material.job_id().to_string())),
        verified_receipt: matrix_snapshot_path(&store.verified_receipt(material.job_id()).unwrap()),
        verified_stage: matrix_snapshot_path(
            &root
                .join("verified")
                .join(format!(".verify-{}.json.pending", material.job_id())),
        ),
        snapshot_cache: matrix_snapshot_path(
            &store
                .snapshot(
                    material.project_id(),
                    material.worktree_id(),
                    material.manifest_digest(),
                )
                .unwrap(),
        ),
        job_locks: matrix_snapshot_path(&root.join("locks/jobs")),
        marker: matrix_snapshot_path(marker),
    }
}

#[test]
fn acceptance_disconnect_matrix_100() {
    // One hundred deterministic rows exercise the six primary disconnect
    // boundaries and four channel-ordered recovery classes. Host receipt,
    // verification, durable authority, execution, endpoint parsing, and client
    // reconciliation all use production services.
    let mut boundary_counts = [0usize; 6];
    let mut accepted = 0usize;
    let mut abandoned = 0usize;
    let mut execution_markers = 0usize;
    let mut launcher_total = 0usize;
    let mut coverage = [[0usize; 4]; 6];
    let mut receiver_invocations = [0usize; 6];
    let mut forwarded_submit_losses = [0usize; 6];
    let mut pre_serialization_losses = 0usize;
    let mut canonical_success_losses = 0usize;
    let mut schedule_traces: [BTreeSet<Vec<&'static str>>; 6] =
        std::array::from_fn(|_| BTreeSet::new());
    let mut trace_table: [[Option<Vec<&'static str>>; 4]; 6] =
        std::array::from_fn(|_| std::array::from_fn(|_| None));
    let mut primary_boundaries: [BTreeSet<&'static str>; 6] =
        std::array::from_fn(|_| BTreeSet::new());

    for case_index in 0usize..100 {
        let boundary = case_index % 6;
        let schedule = (case_index / 6) % 4;
        boundary_counts[boundary] += 1;
        coverage[boundary][schedule] += 1;

        let temp = tempfile::tempdir().unwrap();
        let (runtime, root) = matrix_runtime(&temp);
        let marker = temp.path().join("actual-child-marker");
        let command = matrix_command(&marker);
        let (mut store, mut retained_lease, submit, manifest) = if boundary == 0 {
            let (acquire, submit, manifest) = matrix_request(command);
            assert_eq!(acquire.material(), submit.material());
            (HostStore::open(&root).unwrap(), None, submit, manifest)
        } else {
            let (store, lease, submit, manifest) = matrix_acquired_host(&root, command);
            (store, Some(lease), submit, manifest)
        };
        matrix_assert_original_identity(&submit);
        if let Some(lease) = retained_lease.as_ref() {
            matrix_assert_lease_identity(lease, &submit);
        }

        let launches = Arc::new(AtomicUsize::new(0));
        let receiver_calls = Arc::new(AtomicUsize::new(0));
        let resolve = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
        let runner = MatrixEndpointRunner::new(runtime);
        let resolution_runtime = MatrixResolutionRuntime::default();

        if boundary >= 1 {
            matrix_receive_before_verification(
                &store,
                retained_lease.as_ref().unwrap(),
                &manifest,
                temp.path(),
                Arc::clone(&receiver_calls),
            );
        }
        if boundary >= 2 {
            RemoteSnapshotService::new(&store)
                .verify_and_promote_at(
                    retained_lease.as_ref().unwrap(),
                    retained_lease.as_ref().unwrap().manifest_digest(),
                    2,
                )
                .unwrap();
        }

        let row_forwarded_submit_loss = AtomicUsize::new(0);
        let mut primary_submit_result: Option<Result<SubmitResponse, WorkerError>> = None;
        let mut primary_canonical_success: Option<SubmitResponse> = None;
        let primary_event = match boundary {
            0 => {
                assert_eq!(LeaseService::new(&store).load().unwrap(), None);
                assert_eq!(receiver_calls.load(Ordering::SeqCst), 0);
                assert!(
                    !store
                        .incoming_job(submit.material().job_id(), submit.material().lease_token(),)
                        .unwrap()
                        .exists()
                );
                "before-receipt-disconnect"
            }
            1 => {
                assert_eq!(receiver_calls.load(Ordering::SeqCst), 1);
                assert!(
                    store
                        .incoming_job(submit.material().job_id(), submit.material().lease_token(),)
                        .unwrap()
                        .join("manifest.json")
                        .is_file()
                );
                assert!(
                    !store
                        .verified_receipt(submit.material().job_id())
                        .unwrap()
                        .exists()
                );
                "post-receipt-disconnect"
            }
            2 => {
                drop(store);
                let faulted =
                    HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterJobMetaWrite)
                        .unwrap();
                let result =
                    JobService::new(&faulted, &RejectLauncher).submit_at(submit.clone(), 4);
                assert!(result.is_err(), "post-meta fault did not interrupt submit");
                drop(faulted);
                store = HostStore::open(&root).unwrap();
                assert!(
                    !store
                        .job_index(submit.material().job_id())
                        .unwrap()
                        .exists()
                );
                assert_eq!(
                    matrix_count_named_file(&root.join("leases"), "meta.json"),
                    1,
                    "post-meta boundary did not retain exactly one canonical meta"
                );
                assert!(!marker.exists());
                "post-meta-disconnect"
            }
            3 => {
                drop(store);
                let faulted = HostStore::open_with_write_fault(
                    &root,
                    HostStoreWritePoint::AfterJobIndexParentSync,
                )
                .unwrap();
                let result =
                    JobService::new(&faulted, &RejectLauncher).submit_at(submit.clone(), 5);
                assert!(result.is_err(), "post-index fault did not interrupt submit");
                primary_submit_result = Some(result);
                drop(faulted);
                store = HostStore::open(&root).unwrap();
                let disposition: JobDisposition = serde_json::from_slice(
                    &fs::read(store.job_index(submit.material().job_id()).unwrap()).unwrap(),
                )
                .unwrap();
                assert!(matches!(disposition, JobDisposition::Accepted { .. }));
                assert!(!marker.exists());
                assert_eq!(launches.load(Ordering::SeqCst), 0);
                "post-index-parent-sync-disconnect"
            }
            4 | 5 => {
                let (lost, captured) = matrix_run_submit_loss(
                    &runner,
                    &store,
                    &submit,
                    &launches,
                    &marker,
                    6 + boundary as u64,
                    boundary == 5,
                );
                primary_submit_result = Some(lost);
                assert_eq!(fs::read(&marker).unwrap(), b"x");
                if boundary == 4 {
                    pre_serialization_losses += 1;
                    assert!(
                        captured.is_none(),
                        "boundary 5 serialized bytes before its loss point"
                    );
                    "post-execution-pre-serialization-disconnect"
                } else {
                    canonical_success_losses += 1;
                    let wire = captured.expect("boundary 6 did not capture success bytes");
                    let body = wire
                        .strip_suffix(b"\n")
                        .expect("boundary 6 success omitted its sole allowed LF");
                    assert!(!body.contains(&b'\n'));
                    let decoded: SubmitResponse = serde_json::from_slice(&wire).unwrap();
                    decoded.validate().unwrap();
                    assert!(matches!(decoded, SubmitResponse::Accepted { .. }));
                    assert_eq!(body, serde_json::to_vec(&decoded).unwrap());
                    primary_canonical_success = Some(decoded);
                    "post-canonical-success-bytes-disconnect"
                }
            }
            _ => unreachable!(),
        };
        primary_boundaries[boundary].insert(primary_event);

        let schedule_trace = Arc::new(Mutex::new(Vec::new()));
        let first = match schedule {
            0 => {
                let primary_loss = (boundary >= 4).then(|| {
                    primary_submit_result
                        .take()
                        .expect("response-loss boundary omitted its actual submit error")
                });
                let expected_canonical = primary_canonical_success.clone();
                let mutation_store = store.clone();
                let mutation_lease = retained_lease.clone();
                let mutation_submit = submit.clone();
                let mutation_manifest = manifest.clone();
                let mutation_fixture = temp.path().to_path_buf();
                let mutation_receiver_calls = Arc::clone(&receiver_calls);
                let mutation_launches = Arc::clone(&launches);
                let resolution_trace = Arc::clone(&schedule_trace);
                let (authority, mutation) = matrix_resolver_before_mutator(
                    move || {
                        MatrixOriginalMutation {
                            boundary,
                            store: &mutation_store,
                            lease: mutation_lease.as_ref(),
                            submit: &mutation_submit,
                            manifest: &mutation_manifest,
                            fixture: &mutation_fixture,
                            receiver_calls: &mutation_receiver_calls,
                            launches: &mutation_launches,
                        }
                        .complete(20)
                    },
                    || {
                        if let Some(lost) = primary_loss {
                            matrix_reconcile_actual_submit_loss(
                                &runner,
                                &resolution_runtime,
                                &submit,
                                lost,
                                expected_canonical.as_ref(),
                                &row_forwarded_submit_loss,
                            );
                            matrix_trace(&resolution_trace, "submit-loss-reconciled");
                        }
                        matrix_resolution_after_boundary(
                            boundary, &store, &submit, &launches, &runner,
                        )
                    },
                    &schedule_trace,
                    if boundary <= 2 {
                        "original-mutator-ready"
                    } else {
                        "same-id-retry-ready"
                    },
                    if boundary <= 2 {
                        "original-mutator-complete"
                    } else {
                        "same-id-retry-complete"
                    },
                );
                match mutation {
                    Ok((lease, response)) => {
                        assert!(
                            boundary >= 3,
                            "pre-index mutator escaped durable abandonment"
                        );
                        assert!(response.status().state().is_terminal());
                        retained_lease = Some(lease);
                    }
                    Err(error) => {
                        assert!(
                            boundary < 3,
                            "accepted retry failed after authority: {error}"
                        );
                        let expected = if boundary == 1 {
                            "LEASE_IDENTITY_MISMATCH"
                        } else {
                            "JOB_ABANDONED"
                        };
                        matrix_assert_protocol_code(
                            &error,
                            expected,
                            &format!("case {case_index} delayed original mutator fence"),
                        );
                    }
                }
                authority
            }
            1 => {
                let primary_loss = (boundary >= 4).then(|| {
                    primary_submit_result
                        .take()
                        .expect("response-loss boundary omitted its actual submit error")
                });
                let expected_canonical = primary_canonical_success.clone();
                let resolver_store = store.clone();
                let resolver_submit = submit.clone();
                let resolver_launches = Arc::clone(&launches);
                let resolver_runner = &runner;
                let resolver_resolution_runtime = &resolution_runtime;
                let resolver_forwarded_submit_loss = &row_forwarded_submit_loss;
                let resolver_trace = Arc::clone(&schedule_trace);
                let (mutation, authority) = matrix_mutator_before_resolver(
                    || {
                        MatrixOriginalMutation {
                            boundary,
                            store: &store,
                            lease: retained_lease.as_ref(),
                            submit: &submit,
                            manifest: &manifest,
                            fixture: temp.path(),
                            receiver_calls: &receiver_calls,
                            launches: &launches,
                        }
                        .complete(30)
                    },
                    move || {
                        if let Some(lost) = primary_loss {
                            matrix_reconcile_actual_submit_loss(
                                resolver_runner,
                                resolver_resolution_runtime,
                                &resolver_submit,
                                lost,
                                expected_canonical.as_ref(),
                                resolver_forwarded_submit_loss,
                            );
                            matrix_trace(&resolver_trace, "submit-loss-reconciled");
                        }
                        matrix_resolution_after_boundary(
                            boundary,
                            &resolver_store,
                            &resolver_submit,
                            &resolver_launches,
                            resolver_runner,
                        )
                    },
                    &schedule_trace,
                    if boundary <= 2 {
                        "original-mutator-complete"
                    } else {
                        "same-id-retry-complete"
                    },
                );
                let (lease, response) =
                    mutation.expect("original mutator did not complete before resolution");
                assert!(response.status().state().is_terminal());
                retained_lease = Some(lease);
                authority
            }
            2 => {
                let lost = if boundary <= 2 {
                    let lease = MatrixOriginalMutation {
                        boundary,
                        store: &store,
                        lease: retained_lease.as_ref(),
                        submit: &submit,
                        manifest: &manifest,
                        fixture: temp.path(),
                        receiver_calls: &receiver_calls,
                        launches: &launches,
                    }
                    .prepare(40)
                    .unwrap();
                    retained_lease = Some(lease);
                    let (lost, captured) = matrix_run_submit_loss(
                        &runner, &store, &submit, &launches, &marker, 42, false,
                    );
                    assert!(
                        captured.is_none(),
                        "recovery submit loss invented canonical-byte boundary evidence"
                    );
                    lost
                } else {
                    if boundary == 3 {
                        let completed =
                            matrix_exact_submit(&store, &submit, &launches, 43).unwrap();
                        assert!(completed.status().state().is_terminal());
                    }
                    primary_submit_result
                        .take()
                        .expect("accepted boundary did not retain its actual submit loss")
                };
                assert!(lost.is_err(), "schedule 2 observed submit success");
                assert_eq!(fs::read(&marker).unwrap(), b"x");
                matrix_trace(&schedule_trace, "accepted");
                matrix_trace(&schedule_trace, "submit-success-lost");
                let reconciled = matrix_reconcile_actual_submit_loss(
                    &runner,
                    &resolution_runtime,
                    &submit,
                    lost,
                    primary_canonical_success.as_ref(),
                    &row_forwarded_submit_loss,
                );
                matrix_trace(&schedule_trace, "submit-loss-reconciled");
                let authority = matrix_remote_resolve(&runner, &resolve);
                let ResolveOrAbandonOutcome::Accepted { response } = authority.outcome() else {
                    panic!("accepted submit reconciliation lost durable authority")
                };
                assert_eq!(reconciled.status(), response.status());
                if let SubmitResponse::Accepted { meta, .. } = &reconciled {
                    assert_eq!(meta.as_ref(), response.meta());
                }
                matrix_trace(&schedule_trace, "authority");
                authority
            }
            3 => {
                if boundary >= 4 {
                    let lost = primary_submit_result
                        .take()
                        .expect("response-loss boundary omitted its actual submit error");
                    matrix_reconcile_actual_submit_loss(
                        &runner,
                        &resolution_runtime,
                        &submit,
                        lost,
                        primary_canonical_success.as_ref(),
                        &row_forwarded_submit_loss,
                    );
                    matrix_trace(&schedule_trace, "submit-loss-reconciled");
                }
                if boundary == 3 {
                    let completed = matrix_exact_submit(&store, &submit, &launches, 50).unwrap();
                    assert!(completed.status().state().is_terminal());
                }
                runner.lose_success_responses.store(1, Ordering::SeqCst);
                let lost = SshJsonTransport::new(&runner)
                    .request::<_, mac_worker::job::ResolveOrAbandonResponse>(
                        &matrix_worker(),
                        HostOperation::ResolveOrAbandon,
                        &resolve,
                        matrix_control_policy(),
                    );
                assert!(
                    lost.is_err(),
                    "case {case_index} kept its first resolution response"
                );
                match lost.as_ref().unwrap_err() {
                    WorkerError::Transport { code, message } => {
                        assert_eq!(*code, "SSH_LAUNCH_FAILED");
                        assert_eq!(message, "failed to launch SSH control request");
                    }
                    unexpected => panic!(
                        "case {case_index} lost resolution returned wrong error: {unexpected}"
                    ),
                }
                assert_eq!(runner.lose_success_responses.load(Ordering::SeqCst), 0);
                matrix_trace(&schedule_trace, "resolution-success-lost");
                let authority = matrix_remote_resolve(&runner, &resolve);
                matrix_trace(&schedule_trace, "authority-repeat");
                authority
            }
            _ => unreachable!(),
        };

        let should_accept = boundary >= 3 || matches!(schedule, 1 | 2);
        let first_bytes = serde_json::to_vec(&first).unwrap();
        let disposition_path = store.job_index(submit.material().job_id()).unwrap();
        let disposition_bytes = fs::read(&disposition_path).unwrap();
        let disposition: JobDisposition = serde_json::from_slice(&disposition_bytes).unwrap();
        assert_eq!(disposition_bytes, serde_json::to_vec(&disposition).unwrap());

        let final_job = store
            .job(
                submit.material().project_id(),
                submit.material().worktree_id(),
                submit.material().job_id(),
            )
            .unwrap();
        let mut observed_job_ids = BTreeSet::from([submit.material().job_id().to_string()]);
        if let Some(lease) = retained_lease.as_ref() {
            matrix_assert_lease_identity(lease, &submit);
            observed_job_ids.insert(lease.job_id().to_string());
        }

        match (&disposition, first.outcome()) {
            (
                JobDisposition::Accepted {
                    job_id,
                    client_id,
                    project_id,
                    worktree_id,
                    request_fingerprint,
                    status,
                    ..
                },
                ResolveOrAbandonOutcome::Accepted { response },
            ) if should_accept => {
                accepted += 1;
                assert_eq!(*job_id, submit.material().job_id());
                assert_eq!(*client_id, submit.material().client_id());
                assert_eq!(project_id, submit.material().project_id());
                assert_eq!(worktree_id, submit.material().worktree_id());
                assert_eq!(request_fingerprint, submit.request_fingerprint());
                status.validate().unwrap();
                assert_eq!(status.state(), JobState::Accepted);
                assert_eq!(status.supervisor_identity(), None);
                assert_eq!(status.child_identity(), None);
                response.validate().unwrap();
                matrix_assert_meta_identity(response.meta(), &submit);
                assert!(response.status().state().is_terminal());
                observed_job_ids.insert(job_id.to_string());
                observed_job_ids.insert(response.meta().job_id().to_string());

                let meta_bytes = fs::read(final_job.join("meta.json")).unwrap();
                let meta: JobMeta = serde_json::from_slice(&meta_bytes).unwrap();
                assert_eq!(meta_bytes, serde_json::to_vec(&meta).unwrap());
                let status_bytes = fs::read(final_job.join("status.json")).unwrap();
                let final_status: JobStatus = serde_json::from_slice(&status_bytes).unwrap();
                assert_eq!(status_bytes, serde_json::to_vec(&final_status).unwrap());
                matrix_assert_meta_identity(&meta, &submit);
                assert_eq!(&final_status, response.status());
                observed_job_ids.insert(meta.job_id().to_string());
                assert_eq!(
                    usize::from(final_job.join("meta.json").is_file())
                        * usize::from(final_job.join("status.json").is_file()),
                    1,
                    "accepted authority lacks its exact final"
                );
                assert_eq!(fs::read(final_job.join("stdout.log")).unwrap(), b"");
                assert_eq!(fs::read(final_job.join("stderr.log")).unwrap(), b"");
                assert!(!final_job.join("execution.json").exists());

                let retry = matrix_exact_submit(&store, &submit, &launches, 60).unwrap();
                assert!(matches!(retry, SubmitResponse::Existing { .. }));
                assert_eq!(retry.status(), response.status());
                let remote_status = RemoteJobClient::new_with_runtime(&runner, &resolution_runtime)
                    .status(&matrix_worker(), submit.material().job_id())
                    .unwrap();
                matrix_assert_meta_identity(remote_status.meta(), &submit);
                assert_eq!(remote_status.status(), response.status());

                assert_eq!(fs::read(&marker).unwrap(), b"x");
                execution_markers += 1;
            }
            (
                JobDisposition::Abandoned {
                    job_id,
                    client_id,
                    project_id,
                    worktree_id,
                    request_fingerprint,
                    lease_token_sha256,
                    ..
                },
                ResolveOrAbandonOutcome::Abandoned,
            ) if !should_accept => {
                abandoned += 1;
                assert_eq!(*job_id, submit.material().job_id());
                assert_eq!(*client_id, submit.material().client_id());
                assert_eq!(project_id, submit.material().project_id());
                assert_eq!(worktree_id, submit.material().worktree_id());
                assert_eq!(request_fingerprint, submit.request_fingerprint());
                assert_eq!(
                    lease_token_sha256,
                    &format!(
                        "{:x}",
                        Sha256::digest(submit.material().lease_token().to_string().as_bytes())
                    )
                );
                observed_job_ids.insert(job_id.to_string());
                assert!(
                    !final_job.join("meta.json").exists()
                        && !final_job.join("status.json").exists()
                        && !final_job.join("stdout.log").exists()
                        && !final_job.join("stderr.log").exists(),
                    "abandoned authority retained an accepted final"
                );
                matrix_assert_abandoned_fences(&store, retained_lease.as_ref(), &submit);
                assert!(!marker.exists(), "abandoned row {case_index} executed");
            }
            _ => panic!(
                "case {case_index} selected authority inconsistent with its event order: {first:?}"
            ),
        }

        let accepted_index = matches!(disposition, JobDisposition::Accepted { .. });
        let abandoned_tombstone = matches!(disposition, JobDisposition::Abandoned { .. });
        let accepted_final =
            final_job.join("meta.json").is_file() && final_job.join("status.json").is_file();
        assert_eq!(
            accepted_index, accepted_final,
            "case {case_index} index/final Accepted authority disagreed"
        );
        assert_eq!(
            usize::from(accepted_index && accepted_final)
                + usize::from(abandoned_tombstone && !accepted_final),
            1,
            "case {case_index} durable authority cardinality"
        );
        assert!(
            root.join("locks/jobs")
                .join(submit.material().job_id().to_string())
                .join("cleanup-complete.json")
                .is_file(),
            "case {case_index} retired its lease without exact cleanup proof"
        );
        assert_eq!(LeaseService::new(&store).load().unwrap(), None);
        assert!(
            !store
                .incoming_job(submit.material().job_id(), submit.material().lease_token(),)
                .unwrap()
                .exists(),
            "case {case_index} retained exact incoming state"
        );
        if !should_accept {
            assert!(
                !store
                    .verified_receipt(submit.material().job_id())
                    .unwrap()
                    .exists(),
                "case {case_index} abandoned authority retained a verified receipt"
            );
        }
        for name in ["workspace", "home", "tmp", "execution.json"] {
            assert!(
                !final_job.join(name).exists(),
                "case {case_index} retained exact mutable scope {name}"
            );
        }

        let expected_launches = usize::from(should_accept);
        assert_eq!(
            launches.load(Ordering::SeqCst),
            expected_launches,
            "case {case_index} launch cardinality"
        );
        launcher_total += launches.load(Ordering::SeqCst);
        let receiver_call_count = receiver_calls.load(Ordering::SeqCst);
        assert_eq!(
            receiver_call_count,
            if boundary == 0 {
                usize::from(should_accept)
            } else {
                1
            },
            "case {case_index} receiver invocation cardinality"
        );
        receiver_invocations[boundary] += receiver_call_count;
        let forwarded_count = row_forwarded_submit_loss.load(Ordering::SeqCst);
        assert_eq!(
            forwarded_count,
            usize::from(schedule == 2 || boundary >= 4),
            "case {case_index} did not forward its exact actual submit error"
        );
        forwarded_submit_losses[boundary] += forwarded_count;

        let before_repeat = matrix_durable_snapshot(&root, &store, &submit, &marker);
        let resolution_requests_before = runner
            .requests()
            .iter()
            .filter(|(command, _)| command == "resolve-or-abandon")
            .count();
        let repeated = matrix_remote_resolve(&runner, &resolve);
        assert_eq!(
            serde_json::to_vec(&repeated).unwrap(),
            first_bytes,
            "case {case_index} changed exact repeated resolution"
        );
        matrix_trace(&schedule_trace, "repeat");
        let resolution_requests_after = runner
            .requests()
            .iter()
            .filter(|(command, _)| command == "resolve-or-abandon")
            .count();
        assert_eq!(
            resolution_requests_after,
            resolution_requests_before + 1,
            "case {case_index} omitted or falsified its exact repeat"
        );
        assert_eq!(
            matrix_durable_snapshot(&root, &store, &submit, &marker),
            before_repeat,
            "case {case_index} repeated resolution mutated durable state"
        );
        assert_eq!(
            launches.load(Ordering::SeqCst),
            expected_launches,
            "case {case_index} duplicated launch after repeat"
        );
        assert_eq!(
            matrix_snapshot_path(&marker),
            if should_accept {
                MatrixPathSnapshot::File(b"x".to_vec())
            } else {
                MatrixPathSnapshot::Absent
            },
            "case {case_index} duplicate or missing execution marker"
        );

        let requests = runner.requests();
        assert!(
            !requests.is_empty(),
            "case {case_index} made no host request"
        );
        for (command, bytes) in &requests {
            matrix_assert_recorded_endpoint_identity(command, bytes, &submit);
            match command.as_str() {
                "status" => {
                    let request: StatusRequest = serde_json::from_slice(bytes).unwrap();
                    observed_job_ids.insert(request.job_id().to_string());
                }
                "submit" => {
                    let request: SubmitRequest = serde_json::from_slice(bytes).unwrap();
                    observed_job_ids.insert(request.material().job_id().to_string());
                }
                "resolve-or-abandon" => {
                    let request: ResolveOrAbandonRequest = serde_json::from_slice(bytes).unwrap();
                    observed_job_ids.insert(request.job_id().to_string());
                }
                unexpected => panic!("unexpected endpoint {unexpected}"),
            }
        }
        assert_eq!(
            observed_job_ids,
            BTreeSet::from([submit.material().job_id().to_string()]),
            "case {case_index} observed a replacement job ID"
        );

        let mut expected_trace = match schedule {
            0 => vec![if boundary <= 2 {
                "original-mutator-ready"
            } else {
                "same-id-retry-ready"
            }],
            1 => vec![
                "resolver-ready",
                if boundary <= 2 {
                    "original-mutator-complete"
                } else {
                    "same-id-retry-complete"
                },
                "resolver-release-sent",
            ],
            2 => vec!["accepted", "submit-success-lost"],
            3 => Vec::new(),
            _ => unreachable!(),
        };
        if boundary >= 4 && matches!(schedule, 0 | 1 | 3) {
            expected_trace.push("submit-loss-reconciled");
        }
        match schedule {
            0 => expected_trace.extend([
                "authority",
                "mutator-release-sent",
                if boundary <= 2 {
                    "original-mutator-complete"
                } else {
                    "same-id-retry-complete"
                },
                "repeat",
            ]),
            1 => expected_trace.extend(["authority", "repeat"]),
            2 => expected_trace.extend(["submit-loss-reconciled", "authority", "repeat"]),
            3 => expected_trace.extend(["resolution-success-lost", "authority-repeat", "repeat"]),
            _ => unreachable!(),
        }
        let schedule_trace = schedule_trace.lock().unwrap().clone();
        assert_eq!(schedule_trace, expected_trace, "case {case_index}");
        if let Some(previous) = &trace_table[boundary][schedule] {
            assert_eq!(
                &schedule_trace, previous,
                "boundary {boundary} schedule {schedule} changed order classes"
            );
        } else {
            trace_table[boundary][schedule] = Some(schedule_trace.clone());
        }
        schedule_traces[boundary].insert(schedule_trace);
    }

    assert_eq!(boundary_counts, [17, 17, 17, 17, 16, 16]);
    assert_eq!(
        coverage,
        [
            [5, 4, 4, 4],
            [5, 4, 4, 4],
            [5, 4, 4, 4],
            [5, 4, 4, 4],
            [4, 4, 4, 4],
            [4, 4, 4, 4],
        ],
        "boundary/schedule table did not cover all 100 deterministic rows"
    );
    assert_eq!(receiver_invocations, [8, 17, 17, 17, 16, 16]);
    assert_eq!(forwarded_submit_losses, [4, 4, 4, 4, 16, 16]);
    assert_eq!(pre_serialization_losses, 16);
    assert_eq!(canonical_success_losses, 16);
    let expected_primary = [
        "before-receipt-disconnect",
        "post-receipt-disconnect",
        "post-meta-disconnect",
        "post-index-parent-sync-disconnect",
        "post-execution-pre-serialization-disconnect",
        "post-canonical-success-bytes-disconnect",
    ];
    for (boundary, events) in primary_boundaries.into_iter().enumerate() {
        assert_eq!(
            events,
            BTreeSet::from([expected_primary[boundary]]),
            "boundary {boundary} did not preserve its primary disconnect"
        );
    }
    for (boundary, traces) in schedule_traces.into_iter().enumerate() {
        assert_eq!(traces.len(), 4, "boundary {boundary} collapsed schedules");
        assert!(
            trace_table[boundary].iter().all(Option::is_some),
            "boundary {boundary} omitted a schedule class"
        );
    }
    // Derived from the complete boundary/schedule table above: pre-index S0/S3
    // abandon, while every post-index or mutator-first/Accepted-first row wins.
    assert_eq!((accepted, abandoned), (73, 27));
    assert_eq!((execution_markers, launcher_total), (73, 73));
    assert_eq!(accepted + abandoned, 100);
    eprintln!(
        "matrix distribution={boundary_counts:?} schedules={coverage:?} accepted={accepted} abandoned={abandoned} markers={execution_markers} launches={launcher_total} receivers={receiver_invocations:?} forwarded-losses={forwarded_submit_losses:?} pre-serialization-losses={pre_serialization_losses} canonical-success-losses={canonical_success_losses} traces={trace_table:?}"
    );
}
fn lease_request() -> LeaseAcquireRequest {
    LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            JOB_ID.parse().unwrap(),
            CLIENT_ID.parse().unwrap(),
            LEASE_TOKEN.parse().unwrap(),
            10,
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

fn lease_request_with_identity(
    request: &SubmitRequest,
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
) -> LeaseAcquireRequest {
    let material = request.material();
    LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            job_id,
            client_id,
            lease_token,
            material.created_at_millis(),
            material.worker_name().into(),
            material.project_id().into(),
            material.worktree_id().into(),
            material.manifest_digest().into(),
            material.relative_working_dir().into(),
            material.timeout_millis(),
            material.resource_class().into(),
            material.command().clone(),
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
            10,
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

fn indexed_running_job(
    root: &Path,
    supervisor_pid: u32,
    child_pid: u32,
) -> (
    HostStore,
    LeaseRecord,
    CancelRequest,
    JobStatus,
    ProcessIdentity,
) {
    let (store, lease, _submit) = indexed_identityless_job(root);
    let child = identity(child_pid);
    let status = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(identity(supervisor_pid), 11)
        .unwrap()
        .with_child(child, 12)
        .unwrap()
        .into_running(13)
        .unwrap();
    install_job_status(&store, &lease, &status, true);
    let request = CancelRequest::new(
        lease.job_id(),
        lease.client_id(),
        lease.lease_token(),
        lease.request_fingerprint().clone(),
    )
    .unwrap();
    (store, lease, request, status, child)
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

fn create_fifo(path: &Path) {
    let name = CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: `name` is a live NUL-terminated pathname for this call.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct RegularFileIdentity {
    dev: u64,
    ino: u64,
    bytes: Vec<u8>,
}

#[derive(Clone)]
struct UnrelatedNeighbors {
    foreign_incoming: PathBuf,
    foreign_incoming_dev: u64,
    foreign_incoming_ino: u64,
    foreign_leaf: RegularFileIdentity,
    cache: PathBuf,
    cache_identity: RegularFileIdentity,
    sentinel: PathBuf,
    sentinel_identity: RegularFileIdentity,
}

struct PlantedIncomingTokenJournal {
    _temp: tempfile::TempDir,
    root: PathBuf,
    lease: LeaseRecord,
    request: ResolveOrAbandonRequest,
    incoming: PathBuf,
    incoming_job: PathBuf,
    token_namespace: PathBuf,
    incoming_root_namespace: PathBuf,
    neighbors: UnrelatedNeighbors,
}

fn regular_file_identity(path: &Path) -> RegularFileIdentity {
    let meta = fs::symlink_metadata(path).unwrap();
    RegularFileIdentity {
        dev: meta.dev(),
        ino: meta.ino(),
        bytes: fs::read(path).unwrap(),
    }
}

fn plant_unrelated_resolution_neighbors(root: &Path, incoming_root: &Path) -> UnrelatedNeighbors {
    let foreign_job = JobId::new(uuid::Uuid::from_u128(
        0x1111_2222_3333_4444_5555_6666_7777_8888,
    ));
    let foreign_incoming = incoming_root.join(foreign_job.to_string());
    fs::create_dir_all(&foreign_incoming).unwrap();
    fs::set_permissions(&foreign_incoming, fs::Permissions::from_mode(0o700)).unwrap();
    replace_bytes(&foreign_incoming.join("foreign-leaf"), b"foreign-incoming").unwrap();
    File::open(&foreign_incoming).unwrap().sync_all().unwrap();
    File::open(incoming_root).unwrap().sync_all().unwrap();
    let foreign_meta = fs::symlink_metadata(&foreign_incoming).unwrap();
    let cache = root.join("snapshots").join("unrelated-cache");
    replace_bytes(&cache, b"snapshot-cache-sentinel").unwrap();
    let sentinel = root.join("leases").join("unrelated-sentinel");
    replace_bytes(&sentinel, b"lease-sentinel").unwrap();
    UnrelatedNeighbors {
        foreign_incoming: foreign_incoming.clone(),
        foreign_incoming_dev: foreign_meta.dev(),
        foreign_incoming_ino: foreign_meta.ino(),
        foreign_leaf: regular_file_identity(&foreign_incoming.join("foreign-leaf")),
        cache: cache.clone(),
        cache_identity: regular_file_identity(&cache),
        sentinel: sentinel.clone(),
        sentinel_identity: regular_file_identity(&sentinel),
    }
}

fn assert_unrelated_neighbors_unchanged(neighbors: &UnrelatedNeighbors, iteration: usize) {
    let foreign_meta = fs::symlink_metadata(&neighbors.foreign_incoming).unwrap();
    assert_eq!(
        foreign_meta.dev(),
        neighbors.foreign_incoming_dev,
        "iteration {iteration}: foreign incoming replaced"
    );
    assert_eq!(
        foreign_meta.ino(),
        neighbors.foreign_incoming_ino,
        "iteration {iteration}: foreign incoming replaced"
    );
    assert_eq!(
        regular_file_identity(&neighbors.foreign_incoming.join("foreign-leaf")),
        neighbors.foreign_leaf,
        "iteration {iteration}: foreign incoming bytes changed"
    );
    assert_eq!(
        regular_file_identity(&neighbors.cache),
        neighbors.cache_identity,
        "iteration {iteration}: snapshot cache sentinel changed"
    );
    assert_eq!(
        regular_file_identity(&neighbors.sentinel),
        neighbors.sentinel_identity,
        "iteration {iteration}: lease sentinel changed"
    );
}

fn plant_committed_incoming_token_journal() -> (HostStore, PlantedIncomingTokenJournal) {
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
    let request = ResolveOrAbandonRequest::from_submit_request(&SubmitRequest::new(
        acquire.material().clone(),
    ))
    .unwrap();
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    fs::create_dir_all(&incoming).unwrap();
    for private in [incoming.parent().unwrap(), incoming.as_path()] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    replace_bytes(&incoming.join("token-leaf"), b"incoming-token-bytes").unwrap();
    let incoming_job = incoming.parent().unwrap().to_path_buf();
    let incoming_root = incoming_job.parent().unwrap().to_path_buf();
    let neighbors = plant_unrelated_resolution_neighbors(&root, &incoming_root);
    store.record_abandoned(&acquire, 2).unwrap();
    drop(store);

    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
            .unwrap();
    let pending = JobService::new(&faulted, &RejectLauncher)
        .resolve_or_abandon(request.clone())
        .unwrap();
    assert!(
        matches!(
            pending.outcome(),
            ResolveOrAbandonOutcome::CleanupPending { code }
                if code == "MUTABLE_CLEANUP_FAILED"
        ),
        "{pending:?}"
    );
    assert_eq!(
        LeaseService::new(&faulted).load().unwrap(),
        Some(lease.clone())
    );
    assert!(!incoming.exists(), "public token must already be absent");
    let token_namespace = incoming_job.join(".mac-worker-rooted-fs");
    assert_canonical_delete_journal(&token_namespace, "cleanup-tree-v1-");
    assert!(
        incoming_job.is_dir(),
        "planting the token journal must not delete outer incoming/{{job}}"
    );
    (
        faulted,
        PlantedIncomingTokenJournal {
            _temp: temp,
            root,
            lease,
            request,
            incoming,
            incoming_job,
            token_namespace,
            incoming_root_namespace: incoming_root.join(".mac-worker-rooted-fs"),
            neighbors,
        },
    )
}

fn race_two_independent_resolvers(
    root: &Path,
    request: ResolveOrAbandonRequest,
) -> [ResolveOrAbandonResponse; 2] {
    let start = Arc::new(Barrier::new(2));
    let (tx_a, rx_a) = mpsc::channel();
    let (tx_b, rx_b) = mpsc::channel();
    let root_a = root.to_path_buf();
    let root_b = root.to_path_buf();
    let request_a = request.clone();
    let start_a = Arc::clone(&start);
    let handle_a = thread::spawn(move || {
        let store = HostStore::open(&root_a).expect("independent HostStore A");
        start_a.wait();
        let _ = tx_a.send(JobService::new(&store, &RejectLauncher).resolve_or_abandon(request_a));
    });
    let handle_b = thread::spawn(move || {
        let store = HostStore::open(&root_b).expect("independent HostStore B");
        start.wait();
        let _ = tx_b.send(JobService::new(&store, &RejectLauncher).resolve_or_abandon(request));
    });
    let timeout = Duration::from_secs(20);
    let first = rx_a
        .recv_timeout(timeout)
        .unwrap_or_else(|_| panic!("resolver A hung after {timeout:?}"))
        .unwrap_or_else(|error| panic!("resolver A failed: {error}"));
    let second = rx_b
        .recv_timeout(timeout)
        .unwrap_or_else(|_| panic!("resolver B hung after {timeout:?}"))
        .unwrap_or_else(|error| panic!("resolver B failed: {error}"));
    handle_a.join().expect("resolver A thread panicked");
    handle_b.join().expect("resolver B thread panicked");
    [first, second]
}

fn assert_allowed_race_outcome(
    response: &ResolveOrAbandonResponse,
    iteration: usize,
    racer: usize,
) {
    let allowed = match response.outcome() {
        ResolveOrAbandonOutcome::Abandoned => true,
        ResolveOrAbandonOutcome::CleanupPending { code } => matches!(
            code.as_str(),
            "MUTABLE_CLEANUP_FAILED" | "LEASE_RELEASE_FAILED"
        ),
        _ => false,
    };
    assert!(allowed, "iteration {iteration} racer {racer}: {response:?}");
}

fn finish_resolution_if_needed(
    root: &Path,
    request: ResolveOrAbandonRequest,
    outcomes: &[ResolveOrAbandonResponse],
) {
    assert!(
        outcomes
            .iter()
            .any(|response| { matches!(response.outcome(), ResolveOrAbandonOutcome::Abandoned) }),
        "at least one concurrent resolver must commit abandonment: {outcomes:?}"
    );
    let store = HostStore::open(root).unwrap();
    assert_eq!(
        LeaseService::new(&store).load().unwrap(),
        None,
        "the concurrent Abandoned outcome must already retire the exact lease"
    );
    let pending = outcomes.iter().any(|response| match response.outcome() {
        ResolveOrAbandonOutcome::CleanupPending { code } => matches!(
            code.as_str(),
            "MUTABLE_CLEANUP_FAILED" | "LEASE_RELEASE_FAILED"
        ),
        _ => false,
    });
    if !pending {
        return;
    }
    let completed = JobService::new(&store, &RejectLauncher)
        .resolve_or_abandon(request)
        .unwrap();
    assert!(
        matches!(completed.outcome(), ResolveOrAbandonOutcome::Abandoned),
        "final independent retry {completed:?}"
    );
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

fn directory_entry_names(path: &Path) -> Vec<String> {
    let mut names = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn isolate_resolution_scopes_before_staging(job: &Path, store: &HostStore, lease: &LeaseRecord) {
    if job.join("execution.json").exists() {
        remove_and_sync(&job.join("execution.json"));
    }
    for name in ["workspace", "home", "tmp"] {
        let path = job.join(name);
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
    }
    File::open(job).unwrap().sync_all().unwrap();
    let receipt = store.verified_receipt(lease.job_id()).unwrap();
    if receipt.exists() {
        remove_and_sync(&receipt);
    }
    let pending = receipt
        .parent()
        .unwrap()
        .join(format!(".verify-{}.json.pending", lease.job_id()));
    if pending.exists() {
        remove_and_sync(&pending);
    }
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    if let Some(incoming_job) = incoming.parent()
        && incoming_job.exists()
    {
        fs::remove_dir_all(incoming_job).unwrap();
        if let Some(incoming_root) = incoming_job.parent() {
            File::open(incoming_root).unwrap().sync_all().unwrap();
        }
    }
}

fn assert_canonical_delete_journal(namespace: &Path, quarantine_prefix: &str) {
    assert!(
        namespace.is_dir(),
        "journal namespace {}",
        namespace.display()
    );
    let mut intent = 0usize;
    let mut decision = 0usize;
    let mut operation = 0usize;
    let mut quarantine = 0usize;
    let mut decision_path = None;
    for entry in fs::read_dir(namespace).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let name = entry.file_name();
        let name = name
            .to_str()
            .unwrap_or_else(|| panic!("non-UTF-8 journal entry under {}", namespace.display()));
        let file_type = entry.file_type().unwrap();
        if name.starts_with("cleanup-intent-v1-") {
            intent += 1;
            assert!(
                file_type.is_file(),
                "cleanup intent must be a regular file: {}",
                path.display()
            );
        } else if name.starts_with("cleanup-decision-v1-") {
            decision += 1;
            assert!(
                file_type.is_file(),
                "cleanup decision must be a regular file: {}",
                path.display()
            );
            decision_path = Some(path);
        } else if name.starts_with("cleanup-op-v1-") {
            operation += 1;
            assert!(
                file_type.is_dir(),
                "cleanup operation must be a directory: {}",
                path.display()
            );
        } else if name.starts_with(quarantine_prefix) {
            quarantine += 1;
            match quarantine_prefix {
                "cleanup-tree-v1-" => assert!(
                    file_type.is_dir(),
                    "tree cleanup quarantine must be a directory: {}",
                    path.display()
                ),
                "cleanup-regular-v1-" => assert!(
                    file_type.is_file(),
                    "regular cleanup quarantine must be a regular file: {}",
                    path.display()
                ),
                _ => panic!("unsupported cleanup quarantine prefix {quarantine_prefix}"),
            }
        } else {
            panic!(
                "unexpected journal entry {} under {}",
                name,
                namespace.display()
            );
        }
    }
    assert_eq!(
        (intent, decision, operation, quarantine),
        (1, 1, 1, 1),
        "unexpected journal roles under {}",
        namespace.display()
    );
    let decision_path = decision_path.expect("decision role path");
    let decision: serde_json::Value = serde_json::from_slice(&fs::read(decision_path).unwrap())
        .expect("cleanup decision must be valid JSON");
    assert!(
        decision.is_object(),
        "cleanup decision must be a JSON object: {decision:?}"
    );
    assert_eq!(
        decision.get("decision"),
        Some(&serde_json::json!("delete")),
        "cleanup decision must be Delete: {decision}"
    );
}

fn assert_journal_empty_or_absent(namespace: &Path) {
    if !namespace.exists() {
        return;
    }
    assert_eq!(
        fs::read_dir(namespace).unwrap().count(),
        0,
        "journal residue remains in {}",
        namespace.display()
    );
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
