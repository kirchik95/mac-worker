use std::{
    collections::BTreeSet,
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    process::ExitStatus,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use mac_worker::{
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    error::ProcessError,
    job::{
        AdmissionObservation, CommandSpec, FleetReconcileJobResult, FleetReconcileRequest,
        FleetReconcileResponse, JobId, JobMeta, JobStatus, LeaseToken, LocalJobRecord,
        RemoteUncertainty, RequestFingerprintMaterial, StatusResponse,
    },
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    run::{FleetOutcome, FleetReconcileObserver, FleetReconcileStage, FleetReconciler},
    scheduler::CandidateSlot,
    transfer::{HostOperation, RemoteJobClient},
};
use proptest::prelude::*;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn job_id(value: u128) -> JobId {
    format!("{value:032x}").parse().unwrap()
}

fn worker(name: &str) -> WorkerEntry {
    WorkerEntry {
        name: name.into(),
        ssh: name.into(),
        slots: 1,
        capabilities: Vec::new(),
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    }
}

fn worker_with_capabilities(name: &str, capabilities: &[&str]) -> WorkerEntry {
    WorkerEntry {
        capabilities: capabilities
            .iter()
            .map(|capability| (*capability).into())
            .collect(),
        ..worker(name)
    }
}

fn local_record(store: &ClientStateStore, job_id: JobId, worker: &str) -> LocalJobRecord {
    let material = RequestFingerprintMaterial::new(
        job_id,
        store.client_id(),
        LeaseToken::new("dddddddddddddddddddddddddddddddd".parse().unwrap()),
        1_000,
        worker.into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        DIGEST.into(),
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::argv(vec!["tool".into()]).unwrap(),
    )
    .unwrap();
    LocalJobRecord::new(
        JobMeta::new(&material, material.fingerprint()).unwrap(),
        material.lease_token(),
        None,
        RemoteUncertainty::None,
    )
    .unwrap()
}

struct FleetRunner {
    statuses: Vec<(JobId, StatusResponse)>,
    reconciled_hosts: Mutex<BTreeSet<String>>,
    unavailable_hosts: BTreeSet<String>,
    reconcile_timeout_hosts: BTreeSet<String>,
}

impl FleetRunner {
    fn host(request: &ProcessRequest) -> String {
        request
            .args
            .iter()
            .find_map(|argument| argument.to_str().filter(|value| value.starts_with("mini-")))
            .unwrap()
            .to_owned()
    }

    fn success(stdout: Vec<u8>) -> Result<ProcessResult, mac_worker::error::WorkerError> {
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        })
    }
}

impl ProcessRunner for FleetRunner {
    fn run(
        &self,
        request: &ProcessRequest,
    ) -> Result<ProcessResult, mac_worker::error::WorkerError> {
        let host = Self::host(request);
        let command = request.args.last().unwrap().to_str().unwrap();
        if command.ends_with("host probe") {
            if self.unavailable_hosts.contains(&host) {
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(1 << 8),
                    stdout: Vec::new(),
                    stderr: b"offline".to_vec(),
                });
            }
            return Self::success(
                serde_json::to_vec(&ProbeResponse {
                    protocol_version: PROTOCOL_VERSION,
                    supervision_version: SUPERVISION_VERSION,
                    hostname: host,
                    arch: "arm64".into(),
                    os_version: "14".into(),
                    free_disk_bytes: 64 * 1024 * 1024 * 1024,
                    total_disk_bytes: 128 * 1024 * 1024 * 1024,
                    memory_pressure: MemoryPressure::Normal,
                    swap_used_bytes: None,
                    available_memory_bytes: Some(8 * 1024 * 1024 * 1024),
                    cpu_counters: None,
                    slot_state: mac_worker::lease::SlotState::Idle,
                    active_lease: None,
                    capabilities: Vec::new(),
                    agent_facts: None,
                    facts_age_millis: None,
                })
                .unwrap(),
            );
        }
        assert_eq!(command, "~/.local/bin/worker host reconcile");
        self.reconciled_hosts.lock().unwrap().insert(host.clone());
        if self.reconcile_timeout_hosts.contains(&host) {
            return Err(ProcessError::DeadlineExceeded {
                deadline: Duration::from_secs(5),
            }
            .into());
        }
        let reconcile: FleetReconcileRequest =
            serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
        let results = reconcile
            .known_job_ids()
            .iter()
            .map(|job_id| FleetReconcileJobResult::Status {
                status: Box::new(
                    self.statuses
                        .iter()
                        .find(|(known, _)| known == job_id)
                        .unwrap()
                        .1
                        .clone(),
                ),
            })
            .collect();
        Self::success(serde_json::to_vec(&FleetReconcileResponse::new(results).unwrap()).unwrap())
    }
}

fn fleet_fixture() -> (
    tempfile::TempDir,
    ClientStateStore,
    Config,
    FleetRunner,
    [JobId; 3],
) {
    let directory = tempfile::tempdir().unwrap();
    let state_root = directory.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open(&state_root).unwrap();
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker("mini-1"), worker("mini-2"), worker("mini-3")],
    };
    let ids = [job_id(1), job_id(2), job_id(3)];
    let statuses = ids
        .iter()
        .zip(["mini-1", "mini-2", "mini-3"])
        .map(|(job_id, worker)| {
            let record = local_record(&store, *job_id, worker);
            store.create_job(record.clone()).unwrap();
            (
                *job_id,
                StatusResponse::new(record.meta().clone(), JobStatus::accepted(1_000).unwrap())
                    .unwrap(),
            )
        })
        .collect();
    (
        directory,
        store,
        config,
        FleetRunner {
            statuses,
            reconciled_hosts: Mutex::new(BTreeSet::new()),
            unavailable_hosts: BTreeSet::from(["mini-1".into()]),
            reconcile_timeout_hosts: BTreeSet::new(),
        },
        ids,
    )
}

#[test]
fn fleet_request_is_bounded_and_rejects_duplicate_canonical_job_ids() {
    assert!(FleetReconcileRequest::new(vec![job_id(1), job_id(1)]).is_err());
    assert!(FleetReconcileRequest::new((1..=101).map(job_id).collect()).is_err());
    assert_eq!(
        HostOperation::Reconcile.command(),
        "~/.local/bin/worker host reconcile"
    );
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    #[test]
    fn fleet_requests_accept_every_varying_unique_input_within_the_bound(
        values in prop::collection::btree_set(any::<u128>(), 0..=100)
    ) {
        let ids = values.into_iter().map(job_id).collect::<Vec<_>>();
        prop_assert!(FleetReconcileRequest::new(ids).is_ok());
    }
}

#[test]
fn one_unavailable_worker_does_not_block_other_reconciliation_or_queue_progress() {
    let (_directory, store, config, runner, _ids) = fleet_fixture();
    let report = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
        reconcile_observer: None,
    }
    .reconcile()
    .unwrap();

    assert_eq!(report.workers()[0].outcome(), FleetOutcome::Unavailable);
    assert_eq!(report.workers()[1].outcome(), FleetOutcome::Reconciled);
    assert_eq!(report.workers()[2].outcome(), FleetOutcome::Reconciled);
    assert_eq!(
        runner.reconciled_hosts.lock().unwrap().clone(),
        BTreeSet::from(["mini-2".into(), "mini-3".into()])
    );
}

#[derive(Default)]
struct CountingFleetObserver(Mutex<usize>);

struct BlockingFleetObserver {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    blocked: AtomicBool,
    calls: AtomicUsize,
}

impl FleetReconcileObserver for CountingFleetObserver {
    fn observe(&self, stage: FleetReconcileStage) -> Result<(), mac_worker::error::WorkerError> {
        assert_eq!(stage, FleetReconcileStage::ResponseHandling);
        *self.0.lock().unwrap() += 1;
        Ok(())
    }
}

impl FleetReconcileObserver for BlockingFleetObserver {
    fn observe(&self, stage: FleetReconcileStage) -> Result<(), mac_worker::error::WorkerError> {
        assert_eq!(stage, FleetReconcileStage::ResponseHandling);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.blocked.swap(true, Ordering::SeqCst) {
            self.entered.send(()).map_err(|_| {
                mac_worker::error::WorkerError::Protocol(
                    "reconciliation handoff gate receiver disappeared".into(),
                )
            })?;
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| {
                    mac_worker::error::WorkerError::Protocol(
                        "reconciliation handoff gate timed out".into(),
                    )
                })?;
        }
        Ok(())
    }
}

#[test]
fn reconciliation_response_handoff_is_observable_without_changing_authority() {
    let (_directory, store, config, runner, _ids) = fleet_fixture();
    let observer = CountingFleetObserver::default();
    let report = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
        reconcile_observer: None,
    }
    .with_reconcile_observer(&observer)
    .reconcile()
    .unwrap();

    assert_eq!(report.workers().len(), 3);
    assert_eq!(*observer.0.lock().unwrap(), 2);
}

#[test]
fn reconciliation_response_handoff_matrix_preserves_one_authoritative_update_per_case() {
    // Break caught: reconciliation response handling bypasses the durable
    // status CAS or processes a response more than once for a known row.
    for case in 0..100 {
        let (_directory, store, config, runner, ids) = fleet_fixture();
        let observer = CountingFleetObserver::default();
        let report = FleetReconciler {
            config: &config,
            client_state: &store,
            remote: RemoteJobClient::new(&runner),
            reconcile_observer: None,
        }
        .with_reconcile_observer(&observer)
        .reconcile()
        .unwrap();

        assert_eq!(*observer.0.lock().unwrap(), 2, "case {case}");
        assert_eq!(report.workers().len(), 3, "case {case}");
        assert!(
            store.load_job(ids[1]).unwrap().last_status().is_some(),
            "case {case}"
        );
    }
}

#[test]
fn reboot_reconcile_handoff_matrix_retains_ambiguity_and_applies_only_authoritative_status() {
    // Break caught: a reboot races response handling and loses an ambiguous
    // owner record, applies a response for the wrong job, or duplicates a
    // durable status update.
    let mut schedule_counts = [0usize; 4];
    for case in 0..100 {
        let schedule = case % 4;
        schedule_counts[schedule] += 1;
        let (directory, store, config, runner, ids) = fleet_fixture();
        let first_job_id = ids[0];
        let unavailable = match case % 4 {
            0 => Some("mini-1"),
            1 => Some("mini-2"),
            2 => Some("mini-3"),
            _ => None,
        };
        let mut runner = runner;
        runner.unavailable_hosts = unavailable
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let state_root = directory.path().canonicalize().unwrap().join("state");
        let store = Arc::new(store);
        let config = Arc::new(config);
        let runner = Arc::new(runner);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let observer = Arc::new(BlockingFleetObserver {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            blocked: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        });
        let reconcile_store = Arc::clone(&store);
        let reconcile_config = Arc::clone(&config);
        let reconcile_runner = Arc::clone(&runner);
        let reconcile_observer = Arc::clone(&observer);
        let reconcile = thread::spawn(move || {
            FleetReconciler {
                config: &reconcile_config,
                client_state: &reconcile_store,
                remote: RemoteJobClient::new(&*reconcile_runner),
                reconcile_observer: None,
            }
            .with_reconcile_observer(&*reconcile_observer)
            .reconcile()
        });
        let mut reconcile = Some(reconcile);
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("case {case} did not reach response handling"));

        let (start_reboot, wait_for_reboot) = mpsc::channel();
        let (reboot_ready, reboot_ready_rx) = mpsc::channel();
        let (allow_reboot_read, wait_for_release) = mpsc::channel();
        let reboot = thread::spawn(move || {
            wait_for_reboot.recv().unwrap();
            let reopened = ClientStateStore::open(&state_root).unwrap();
            let before = reopened.load_job(first_job_id).unwrap();
            reboot_ready.send(()).unwrap();
            wait_for_release.recv().unwrap();
            let after = reopened.load_job(first_job_id).unwrap();
            (before, after)
        });
        let mut reboot = Some(reboot);
        let (report, (before, after)) = match schedule {
            0 => {
                start_reboot.send(()).unwrap();
                reboot_ready_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap_or_else(|_| panic!("case {case} reboot did not reopen state"));
                allow_reboot_read.send(()).unwrap();
                let reboot_result = reboot.take().unwrap().join().unwrap();
                release_tx.send(()).unwrap();
                let report = reconcile.take().unwrap().join().unwrap().unwrap();
                (report, reboot_result)
            }
            1 => {
                start_reboot.send(()).unwrap();
                reboot_ready_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap_or_else(|_| panic!("case {case} reboot did not reopen state"));
                release_tx.send(()).unwrap();
                let report = reconcile.take().unwrap().join().unwrap().unwrap();
                allow_reboot_read.send(()).unwrap();
                let reboot_result = reboot.take().unwrap().join().unwrap();
                (report, reboot_result)
            }
            2 => {
                release_tx.send(()).unwrap();
                let report = reconcile.take().unwrap().join().unwrap().unwrap();
                start_reboot.send(()).unwrap();
                reboot_ready_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap_or_else(|_| panic!("case {case} reboot did not reopen state"));
                allow_reboot_read.send(()).unwrap();
                let reboot_result = reboot.take().unwrap().join().unwrap();
                (report, reboot_result)
            }
            3 => {
                start_reboot.send(()).unwrap();
                reboot_ready_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap_or_else(|_| panic!("case {case} reboot did not reopen state"));
                release_tx.send(()).unwrap();
                allow_reboot_read.send(()).unwrap();
                let reboot_result = reboot.take().unwrap().join().unwrap();
                let report = reconcile.take().unwrap().join().unwrap().unwrap();
                (report, reboot_result)
            }
            _ => unreachable!(),
        };
        let final_status = store.load_job(first_job_id).unwrap().last_status().cloned();
        if schedule == 2 {
            assert_eq!(before.last_status(), final_status.as_ref(), "case {case}");
        } else {
            assert!(before.last_status().is_none(), "case {case}");
        }
        assert_eq!(
            observer.calls.load(Ordering::SeqCst),
            3 - usize::from(unavailable.is_some()),
            "case {case} response handoff count"
        );
        assert_eq!(
            report.workers().len(),
            3,
            "case {case} fleet report retained all configured workers"
        );
        for (index, (job_id, worker)) in ids
            .into_iter()
            .zip(["mini-1", "mini-2", "mini-3"])
            .enumerate()
        {
            let record = store.load_job(job_id).unwrap();
            if unavailable == Some(worker) {
                assert!(
                    record.last_status().is_none(),
                    "case {case} worker {worker}"
                );
                assert!(
                    matches!(
                        record.remote_uncertainty(),
                        RemoteUncertainty::UnknownRemote { .. }
                    ),
                    "case {case} worker {worker} retained uncertainty"
                );
            } else {
                assert_eq!(
                    record.last_status().unwrap().state(),
                    mac_worker::job::JobState::Accepted
                );
                assert_eq!(record.remote_uncertainty(), &RemoteUncertainty::None);
                assert_eq!(
                    report.workers()[index].jobs.len(),
                    1,
                    "case {case} worker {worker}"
                );
            }
        }
        if schedule == 3 {
            assert!(
                after.last_status().is_none() || after.last_status() == final_status.as_ref(),
                "case {case} reboot observed a non-authoritative status"
            );
        } else {
            assert_eq!(
                after.last_status(),
                final_status.as_ref(),
                "case {case} reboot observed the same durable status"
            );
        }
    }
    assert_eq!(schedule_counts, [25, 25, 25, 25]);
}

#[test]
fn failed_probe_is_not_evidence_that_a_retained_job_is_lost() {
    let (_directory, store, config, runner, ids) = fleet_fixture();
    store
        .record_affinity(PROJECT_ID, WORKTREE_ID, "mini-1", 1_001)
        .unwrap();
    FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
        reconcile_observer: None,
    }
    .reconcile()
    .unwrap();

    assert!(matches!(
        store.load_job(ids[0]).unwrap().remote_uncertainty(),
        RemoteUncertainty::UnknownRemote { .. }
    ));
    assert!(!runner.reconciled_hosts.lock().unwrap().contains("mini-1"));
    let affinity = store.affinity_hints(PROJECT_ID, WORKTREE_ID).unwrap();
    assert_eq!(affinity.worktree_worker.as_deref(), Some("mini-1"));
    assert_eq!(affinity.project_worker.as_deref(), Some("mini-1"));
}

#[test]
fn fresh_protocol_compatible_capability_mismatch_still_repairs_known_jobs_and_clears_affinity() {
    // Break caught: admission eligibility is mistaken for transport reachability,
    // leaving a reachable worker's retained lifecycle and stale affinity untouched.
    let (_directory, store, _config, mut runner, ids) = fleet_fixture();
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker_with_capabilities("mini-2", &["gpu"])],
    };
    runner.unavailable_hosts.clear();
    store
        .record_affinity(PROJECT_ID, WORKTREE_ID, "mini-2", 1_001)
        .unwrap();

    let report = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
        reconcile_observer: None,
    }
    .reconcile()
    .unwrap();

    assert_eq!(report.workers().len(), 1);
    assert_eq!(report.workers()[0].outcome(), FleetOutcome::Reconciled);
    assert_eq!(report.workers()[0].jobs.len(), 1);
    assert!(runner.reconciled_hosts.lock().unwrap().contains("mini-2"));
    let affinity = store.affinity_hints(PROJECT_ID, WORKTREE_ID).unwrap();
    assert_eq!(affinity.worktree_worker, None);
    assert_eq!(affinity.project_worker, None);
    assert!(matches!(
        store.load_job(ids[1]).unwrap().remote_uncertainty(),
        RemoteUncertainty::None
    ));
}

#[test]
fn conflicting_remote_status_is_uncertain_and_not_reported_as_reconciled() {
    // Break caught: a conflict with durable local history is silently presented
    // as a successful reconciliation.
    let directory = tempfile::tempdir().unwrap();
    let state_root = directory.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open(&state_root).unwrap();
    let id = job_id(41);
    let local = LocalJobRecord::new(
        local_record(&store, id, "mini-1").meta().clone(),
        LeaseToken::new("dddddddddddddddddddddddddddddddd".parse().unwrap()),
        Some(JobStatus::succeeded(2_000, 0, 0).unwrap()),
        RemoteUncertainty::unknown_remote("STALE_REMOTE").unwrap(),
    )
    .unwrap();
    store.create_job(local.clone()).unwrap();
    let remote = StatusResponse::new(
        local.meta().clone(),
        JobStatus::failed(2_001, 1, 0, 0).unwrap(),
    )
    .unwrap();
    let runner = FleetRunner {
        statuses: vec![(id, remote)],
        reconciled_hosts: Mutex::new(BTreeSet::new()),
        unavailable_hosts: BTreeSet::new(),
        reconcile_timeout_hosts: BTreeSet::new(),
    };
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker("mini-1")],
    };

    let report = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
        reconcile_observer: None,
    }
    .reconcile()
    .unwrap();

    assert_eq!(report.workers()[0].outcome(), FleetOutcome::InvalidResponse);
    assert!(report.workers()[0].jobs.is_empty());
    assert!(matches!(
        store.load_job(id).unwrap().remote_uncertainty(),
        RemoteUncertainty::UnknownRemote { .. }
    ));
}

#[test]
fn immutable_metadata_mismatch_is_uncertain_and_not_reported_as_reconciled() {
    // Break caught: a response that passes the transport's job/worker boundary
    // but conflicts with local immutable identity is reported as repaired.
    let (directory, store, _config, mut runner, ids) = fleet_fixture();
    runner.unavailable_hosts.clear();
    let local = store.load_job(ids[0]).unwrap();
    let mut value = serde_json::to_value(
        StatusResponse::new(local.meta().clone(), JobStatus::accepted(1_001).unwrap()).unwrap(),
    )
    .unwrap();
    value["meta"]["client_id"] =
        serde_json::Value::String("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".into());
    let mismatched: StatusResponse = serde_json::from_value(value).unwrap();
    runner.statuses.retain(|(id, _)| *id != ids[0]);
    runner.statuses.push((ids[0], mismatched));
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker("mini-1")],
    };

    let report = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
        reconcile_observer: None,
    }
    .reconcile()
    .unwrap();

    assert_eq!(report.workers()[0].outcome(), FleetOutcome::InvalidResponse);
    assert!(report.workers()[0].jobs.is_empty());
    assert!(matches!(
        store.load_job(ids[0]).unwrap().remote_uncertainty(),
        RemoteUncertainty::UnknownRemote { .. }
    ));
    drop(directory);
}

#[test]
fn terminal_recovery_matrix_is_authoritative_and_repeatable() {
    // Break caught: fleet repair drops a terminal remote outcome, treats it as
    // a second lifecycle authority, or keeps re-contacting a settled record.
    let terminals = [
        JobStatus::succeeded(2_000, 0, 0).unwrap(),
        JobStatus::failed(2_000, 7, 0, 0).unwrap(),
        JobStatus::running(1_000, 11, 12, 13, 14)
            .unwrap()
            .into_infrastructure_terminal(
                mac_worker::job::JobState::Cancelled,
                2_000,
                0,
                0,
                "CANCELLED".into(),
            )
            .unwrap(),
        JobStatus::running(1_000, 11, 12, 13, 14)
            .unwrap()
            .into_infrastructure_terminal(
                mac_worker::job::JobState::TimedOut,
                2_000,
                0,
                0,
                "REMOTE_TIMEOUT".into(),
            )
            .unwrap(),
        JobStatus::running(1_000, 11, 12, 13, 14)
            .unwrap()
            .into_infrastructure_terminal(
                mac_worker::job::JobState::Lost,
                2_000,
                0,
                0,
                "REMOTE_LOST".into(),
            )
            .unwrap(),
    ];

    for (index, terminal) in terminals.into_iter().enumerate() {
        let directory = tempfile::tempdir().unwrap();
        let state_root = directory.path().canonicalize().unwrap().join("state");
        let store = ClientStateStore::open(&state_root).unwrap();
        let id = job_id(100 + index as u128);
        let record = local_record(&store, id, "mini-1");
        store.create_job(record.clone()).unwrap();
        let runner = FleetRunner {
            statuses: vec![(
                id,
                StatusResponse::new(record.meta().clone(), terminal.clone()).unwrap(),
            )],
            reconciled_hosts: Mutex::new(BTreeSet::new()),
            unavailable_hosts: BTreeSet::new(),
            reconcile_timeout_hosts: BTreeSet::new(),
        };
        let config = Config {
            version: 1,
            notifications: mac_worker::config::NotificationsConfig::default(),
            workers: vec![worker("mini-1")],
        };

        let first = FleetReconciler {
            config: &config,
            client_state: &store,
            remote: RemoteJobClient::new(&runner),
            reconcile_observer: None,
        }
        .reconcile()
        .unwrap();
        assert_eq!(
            first.workers()[0].outcome(),
            FleetOutcome::Reconciled,
            "row {index}"
        );
        assert_eq!(
            store.load_job(id).unwrap().last_status(),
            Some(&terminal),
            "row {index}"
        );

        let second = FleetReconciler {
            config: &config,
            client_state: &store,
            remote: RemoteJobClient::new(&runner),
            reconcile_observer: None,
        }
        .reconcile()
        .unwrap();
        assert!(second.workers().is_empty(), "row {index}");
    }
}

#[test]
fn stale_admission_cache_is_refreshed_before_repairing_a_known_job() {
    // Break caught: a stale advisory observation suppresses the fresh probe
    // that establishes whether it is safe to contact the recorded worker.
    let (_directory, store, _config, mut runner, ids) = fleet_fixture();
    runner.unavailable_hosts.clear();
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker("mini-1")],
    };
    let stale = AdmissionObservation::new(
        "mini-1".into(),
        true,
        CandidateSlot::Idle,
        Vec::new(),
        Some(1),
        1,
        1,
    )
    .unwrap();
    store
        .admission_observation("mini-1", 1, || Ok(stale))
        .unwrap();

    let report = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
        reconcile_observer: None,
    }
    .reconcile()
    .unwrap();

    assert_eq!(report.workers()[0].outcome(), FleetOutcome::Reconciled);
    assert!(report.workers()[0].probe.probe.is_some());
    assert_eq!(report.workers()[0].jobs[0].meta().job_id(), ids[0]);
}

#[test]
fn reconcile_timeout_retains_remote_uncertainty_and_affinity() {
    // Break caught: a timed-out remote control call is treated as proof that a
    // known job or its advisory affinity may be discarded.
    let (_directory, store, _config, mut runner, ids) = fleet_fixture();
    runner.unavailable_hosts.clear();
    runner.reconcile_timeout_hosts.insert("mini-2".into());
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker("mini-2")],
    };
    store
        .record_affinity(PROJECT_ID, WORKTREE_ID, "mini-2", 1_001)
        .unwrap();

    let report = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
        reconcile_observer: None,
    }
    .reconcile()
    .unwrap();

    assert_eq!(report.workers()[0].outcome(), FleetOutcome::Unavailable);
    assert!(matches!(
        store.load_job(ids[1]).unwrap().remote_uncertainty(),
        RemoteUncertainty::UnknownRemote { .. }
    ));
    let affinity = store.affinity_hints(PROJECT_ID, WORKTREE_ID).unwrap();
    assert_eq!(affinity.worktree_worker.as_deref(), Some("mini-2"));
    assert_eq!(affinity.project_worker.as_deref(), Some("mini-2"));
}

#[test]
fn cross_client_local_record_stops_fleet_recovery_before_remote_contact() {
    // Break caught: a copied job record from another client is used as a
    // recovery selector, crossing the local lifecycle authority boundary.
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let local_root = root.join("local-state");
    let foreign_root = root.join("foreign-state");
    let store = ClientStateStore::open(&local_root).unwrap();
    let foreign = ClientStateStore::open(&foreign_root).unwrap();
    let id = job_id(501);
    foreign
        .create_job(local_record(&foreign, id, "mini-1"))
        .unwrap();
    let local_file = local_root.join("jobs").join(format!("{id}.json"));
    fs::copy(
        foreign_root.join("jobs").join(format!("{id}.json")),
        &local_file,
    )
    .unwrap();
    fs::set_permissions(&local_file, fs::Permissions::from_mode(0o600)).unwrap();
    let runner = FleetRunner {
        statuses: Vec::new(),
        reconciled_hosts: Mutex::new(BTreeSet::new()),
        unavailable_hosts: BTreeSet::new(),
        reconcile_timeout_hosts: BTreeSet::new(),
    };
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker("mini-1")],
    };

    let error = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
        reconcile_observer: None,
    }
    .reconcile()
    .unwrap_err();

    assert!(error.to_string().contains("CLIENT_ID_MISMATCH"));
    assert!(runner.reconciled_hosts.lock().unwrap().is_empty());
}
