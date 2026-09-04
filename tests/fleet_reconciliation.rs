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
            if host == "mini-1" {
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
}
