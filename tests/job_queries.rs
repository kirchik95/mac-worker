use std::{
    fs::{self, File, OpenOptions},
    io::Write as _,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
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
    supervisor::{Supervisor, SystemProcessInspector},
};
use sha2::{Digest, Sha256};

const JOB_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const CLIENT_ID: &str = "102f0f4a6b5c7d8e9f00112233445566";
const LEASE_TOKEN: &str = "202f0f4a6b5c7d8e9f00112233445566";
const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MANIFEST_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

struct RejectLauncher;

struct RecordingLauncher {
    launches: Arc<AtomicUsize>,
    job_path: PathBuf,
    identity: ProcessIdentity,
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

impl SupervisorLauncher for RejectLauncher {
    fn launch(
        &self,
        _job_id: JobId,
        _guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        panic!("a missing job must not launch a supervisor")
    }
}

impl SupervisorLauncher for RecordingLauncher {
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
        drop(guard);
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
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: supervisor,
    };
    let service = JobService::new(&store, &launcher);

    let first = service.status(lease.job_id()).unwrap();
    let second = service.status(lease.job_id()).unwrap();

    assert_eq!(first.meta().job_id(), lease.job_id());
    assert_eq!(first.status().state(), JobState::Accepted);
    assert_eq!(first.status().supervisor_identity(), Some(supervisor));
    assert_eq!(second.status(), first.status());
    assert_eq!(launches.load(Ordering::SeqCst), 1);
}

#[test]
fn status_repairs_an_exact_preindex_final_job_and_launches_it_once() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, _request) = unindexed_identityless_job(&root);
    let launches = Arc::new(AtomicUsize::new(0));
    let supervisor = identity(41_002);
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: supervisor,
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
        replace_json(&job.join("status.json"), &expected).unwrap();

        let response = JobService::new(&store, &RejectLauncher)
            .status(lease.job_id())
            .unwrap();

        assert_eq!(response.status(), &expected, "state row {index}");
        assert_eq!(response.meta().job_id(), lease.job_id());
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
    let launcher = Arc::new(RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: supervisor,
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
