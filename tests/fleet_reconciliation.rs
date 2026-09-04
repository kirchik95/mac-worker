use std::{
    collections::BTreeSet, os::unix::process::ExitStatusExt, process::ExitStatus, sync::Mutex,
};

use mac_worker::{
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    job::{
        CommandSpec, FleetReconcileJobResult, FleetReconcileRequest, FleetReconcileResponse, JobId,
        JobMeta, JobStatus, LeaseToken, LocalJobRecord, RemoteUncertainty,
        RequestFingerprintMaterial, StatusResponse,
    },
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    run::{FleetOutcome, FleetReconciler},
    transfer::{HostOperation, RemoteJobClient},
};

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
                })
                .unwrap(),
            );
        }
        assert_eq!(command, "~/.local/bin/worker host reconcile");
        self.reconciled_hosts.lock().unwrap().insert(host);
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

#[test]
fn one_unavailable_worker_does_not_block_other_reconciliation_or_queue_progress() {
    let (_directory, store, config, runner, _ids) = fleet_fixture();
    let report = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
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
    };
    let config = Config {
        version: 1,
        workers: vec![worker("mini-1")],
    };

    let report = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
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
        workers: vec![worker("mini-1")],
    };

    let report = FleetReconciler {
        config: &config,
        client_state: &store,
        remote: RemoteJobClient::new(&runner),
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
        };
        let config = Config {
            version: 1,
            workers: vec![worker("mini-1")],
        };

        let first = FleetReconciler {
            config: &config,
            client_state: &store,
            remote: RemoteJobClient::new(&runner),
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
        }
        .reconcile()
        .unwrap();
        assert!(second.workers().is_empty(), "row {index}");
    }
}
