use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::Write as _,
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

use mac_worker::{
    host_store::{HostStore, HostStoreWritePoint, SupervisorGuard},
    job::{
        CommandSpec, JobId, JobState, JobStatus, LeaseAcquireRequest, LeaseAcquireResponse,
        LeaseRecord, ProcessIdentity, RequestFingerprintMaterial, SubmitRequest,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    lease::{AdmissionFacts, LeaseService},
    protocol::MemoryPressure,
    remote_snapshot::RemoteSnapshotService,
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
        ReconciliationRuntime, Supervisor, SystemProcessInspector,
    },
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
