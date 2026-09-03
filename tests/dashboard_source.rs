use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use mac_worker::{
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    dashboard::{
        model::{DashboardJobState, DashboardMemoryPressure, DashboardSlotState, WorkerHealth},
        service::{DashboardDataSource, WorkerObservationResult},
        source::{
            DashboardRemoteReader, DashboardWorkerReader, MacWorkerDashboardSource,
            MacWorkerLogSource,
        },
        web::DashboardLogSource,
    },
    error::WorkerError,
    job::{
        CommandSpec, JobId, JobMeta, JobState, JobStatus, LeaseToken, LocalJobRecord, LogChunk,
        LogStream, RemoteUncertainty, RequestFingerprintMaterial, StatusResponse,
    },
    lease::{LeaseSummary, SlotState},
    protocol::{
        HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        WorkerHealth as ProbeWorkerHealth, WorkersReport,
    },
};

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MANIFEST_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const REMOTE_SSH: &str = "operator@mini-1.internal";

#[test]
fn source_uses_typed_probe_data_without_exposing_ssh() {
    let active_job_id = job_id(91);
    let fixture = Fixture::new(ready_report(active_job_id));
    let source = fixture.source();

    assert_eq!(source.configured_workers().unwrap(), vec!["mini-1"]);
    let rows = source.collect_workers(Duration::from_secs(7));
    assert_eq!(fixture.workers.deadlines(), vec![Duration::from_secs(7)]);

    let WorkerObservationResult::Current(observation) = rows.into_iter().next().unwrap() else {
        panic!("ready probe must project to a current observation");
    };
    assert_eq!(observation.worker.name, "mini-1");
    assert_eq!(observation.worker.health, WorkerHealth::Ready);
    assert_eq!(observation.worker.hostname.as_deref(), Some("mini-1.local"));
    assert_eq!(observation.worker.slot.state, DashboardSlotState::Busy);
    assert_eq!(observation.worker.slot.capacity, 1);
    assert_eq!(observation.worker.slot.active_job_id, Some(active_job_id));
    assert_eq!(
        observation.worker.system.memory_pressure,
        Some(DashboardMemoryPressure::Warn)
    );
    assert_eq!(observation.worker.system.cpu_busy_percent, None);
    let wire = serde_json::to_string(&observation.worker).unwrap();
    assert!(!wire.contains(REMOTE_SSH));
    assert!(!wire.contains("operator@"));
    assert!(fixture.remote.status_calls().is_empty());
    assert!(fixture.remote.log_calls().is_empty());
    assert!(fixture.remote.mutating_calls().is_empty());
}

#[test]
fn adapter_uses_authoritative_status_only_for_active_or_uncertain_local_records() {
    let fixture = Fixture::new(ready_report(job_id(99)));
    let accepted = fixture.record(1, Some(JobState::Accepted), RemoteUncertainty::None);
    let running = fixture.record(2, Some(JobState::Running), RemoteUncertainty::None);
    let uploading = fixture.record(3, None, RemoteUncertainty::None);
    let terminal = fixture.record(4, Some(JobState::Succeeded), RemoteUncertainty::None);
    let uncertain = fixture.record(
        5,
        None,
        RemoteUncertainty::unknown_remote("STATUS_NOT_CONFIRMED").unwrap(),
    );
    for record in [&accepted, &running, &uploading, &terminal, &uncertain] {
        fixture.state.create_job(record.clone()).unwrap();
    }
    fixture.remote.set_status(
        accepted.meta().job_id(),
        RemoteReply::Status(Box::new(status_response(&accepted, JobState::Running))),
    );
    fixture.remote.set_status(
        running.meta().job_id(),
        RemoteReply::Status(Box::new(status_response(&running, JobState::Running))),
    );
    fixture.remote.set_status(
        uncertain.meta().job_id(),
        RemoteReply::Status(Box::new(status_response(&uncertain, JobState::Accepted))),
    );

    let source = fixture.source();
    let jobs = source.authoritative_active_jobs(Duration::from_secs(20));
    assert_eq!(jobs.len(), 3);
    assert_eq!(
        jobs.iter()
            .map(|job| job.as_ref().unwrap().state.clone())
            .collect::<Vec<_>>(),
        vec![
            DashboardJobState::Running,
            DashboardJobState::Running,
            DashboardJobState::Accepted,
        ]
    );
    assert_eq!(
        fixture.remote.status_calls(),
        vec![
            accepted.meta().job_id(),
            running.meta().job_id(),
            uncertain.meta().job_id(),
        ]
    );
    assert!(fixture.remote.log_calls().is_empty());
    assert!(fixture.remote.mutating_calls().is_empty());
}

#[test]
fn typed_remote_job_not_found_stays_bounded_and_does_not_expose_transport_text() {
    let fixture = Fixture::new(ready_report(job_id(99)));
    let record = fixture.record(1, Some(JobState::Accepted), RemoteUncertainty::None);
    fixture.state.create_job(record.clone()).unwrap();
    fixture.remote.set_status(
        record.meta().job_id(),
        RemoteReply::Protocol(format!("JOB_NOT_FOUND: {}", "/private/secret/".repeat(100))),
    );

    let source = fixture.source();
    let errors = source.authoritative_active_jobs(Duration::from_secs(20));
    let error = errors.into_iter().next().unwrap().unwrap_err();
    assert_eq!(error.code, "JOB_NOT_FOUND");
    assert!(error.message.chars().count() <= 512);
    assert!(!error.message.contains("/private/secret"));
    assert_eq!(fixture.remote.status_calls(), vec![record.meta().job_id()]);
    assert!(fixture.remote.mutating_calls().is_empty());
}

#[test]
fn local_job_projection_contains_only_safe_summary_metadata() {
    let fixture = Fixture::new(ready_report(job_id(99)));
    let record = fixture.record(1, Some(JobState::Accepted), RemoteUncertainty::None);
    fixture.state.create_job(record).unwrap();

    let jobs = fixture.source().local_jobs().unwrap();
    let wire = serde_json::to_string(&jobs).unwrap();
    assert_eq!(
        jobs[0].command_summary.mode,
        mac_worker::dashboard::model::DashboardCommandMode::Argv
    );
    assert_eq!(jobs[0].command_summary.arg_count, Some(2));
    assert_eq!(jobs[0].project_label, None);
    assert_eq!(jobs[0].artifact_status, None);
    assert!(!wire.contains("secret-command-value"));
    assert!(!wire.contains("packages/dashboard"));
    assert!(!wire.contains(REMOTE_SSH));
    assert!(fixture.remote.status_calls().is_empty());
    assert!(fixture.remote.mutating_calls().is_empty());
}

#[test]
fn log_source_projects_the_typed_log_chunk_without_reencoding_bytes() {
    let fixture = Fixture::new(ready_report(job_id(99)));
    let record = fixture.record(1, Some(JobState::Accepted), RemoteUncertainty::None);
    fixture.state.create_job(record.clone()).unwrap();
    fixture.remote.set_status(
        record.meta().job_id(),
        RemoteReply::Status(Box::new(status_response(&record, JobState::Running))),
    );
    let chunk = LogChunk::new(LogStream::Stderr, 7, vec![0, 255, b'\n', b'x']).unwrap();
    fixture.remote.set_log(chunk.clone());

    let logs = fixture.logs();
    let detail = logs.job_detail(record.meta().job_id()).unwrap();
    assert_eq!(detail.state, DashboardJobState::Running);
    let result = logs
        .read_log(record.meta().job_id(), LogStream::Stderr, 7, 12)
        .unwrap();
    assert_eq!(result.stream(), LogStream::Stderr);
    assert_eq!(result.offset(), chunk.offset());
    assert_eq!(result.next_offset(), chunk.next_offset());
    assert_eq!(result.data(), chunk.data());
    assert_eq!(
        fixture.remote.log_calls(),
        vec![(record.meta().job_id(), LogStream::Stderr, 7, 12)]
    );
    assert!(fixture.remote.mutating_calls().is_empty());
}

#[test]
fn source_rejects_a_remote_status_for_different_immutable_job_metadata() {
    let fixture = Fixture::new(ready_report(job_id(99)));
    let local = fixture.record(1, Some(JobState::Accepted), RemoteUncertainty::None);
    let mismatched = fixture.record(2, Some(JobState::Accepted), RemoteUncertainty::None);
    fixture.state.create_job(local.clone()).unwrap();
    fixture.remote.set_status(
        local.meta().job_id(),
        RemoteReply::Status(Box::new(status_response(&mismatched, JobState::Running))),
    );

    let source = fixture.source();
    let error = source
        .authoritative_active_jobs(Duration::from_secs(20))
        .into_iter()
        .next()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, "REMOTE_JOB_IDENTITY_MISMATCH");
    assert!(fixture.remote.mutating_calls().is_empty());
}

#[test]
fn log_source_resolves_only_locally_owned_jobs_before_contacting_a_worker() {
    let fixture = Fixture::new(ready_report(job_id(99)));
    let logs = fixture.logs();

    let error = logs.job_detail(job_id(777)).unwrap_err();
    assert_eq!(error.code, "JOB_NOT_FOUND");
    assert!(fixture.remote.status_calls().is_empty());
    assert!(fixture.remote.log_calls().is_empty());
    assert!(fixture.remote.mutating_calls().is_empty());
}

struct Fixture {
    _temp: tempfile::TempDir,
    state: Arc<ClientStateStore>,
    config: Arc<Config>,
    workers: Arc<RecordingWorkers>,
    remote: Arc<RecordingRemote>,
}

impl Fixture {
    fn new(report: WorkersReport) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let state_root = temp.path().canonicalize().unwrap().join("state");
        let state = Arc::new(ClientStateStore::open(&state_root).unwrap());
        let config = Arc::new(Config {
            version: 1,
            workers: vec![WorkerEntry {
                name: "mini-1".into(),
                ssh: REMOTE_SSH.into(),
                slots: 1,
                capabilities: vec!["swift".into()],
                remote_binary: "~/.local/bin/worker".into(),
            }],
        });
        config.validate().unwrap();
        Self {
            _temp: temp,
            state,
            config,
            workers: Arc::new(RecordingWorkers::new(report)),
            remote: Arc::new(RecordingRemote::default()),
        }
    }

    fn source(&self) -> MacWorkerDashboardSource {
        MacWorkerDashboardSource::new(
            Arc::clone(&self.config),
            Arc::clone(&self.workers) as Arc<dyn DashboardWorkerReader>,
            Arc::clone(&self.state),
            Arc::clone(&self.remote) as Arc<dyn DashboardRemoteReader>,
        )
    }

    fn logs(&self) -> MacWorkerLogSource {
        MacWorkerLogSource::new(
            Arc::clone(&self.config),
            Arc::clone(&self.state),
            Arc::clone(&self.remote) as Arc<dyn DashboardRemoteReader>,
        )
    }

    fn record(
        &self,
        id: u128,
        last_state: Option<JobState>,
        uncertainty: RemoteUncertainty,
    ) -> LocalJobRecord {
        let job_id = job_id(id);
        let lease_token = LeaseToken::new(uuid::Uuid::from_u128(10_000 + id));
        let material = RequestFingerprintMaterial::new(
            job_id,
            self.state.client_id(),
            lease_token,
            1_000 + u64::try_from(id).unwrap(),
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            MANIFEST_DIGEST.into(),
            "packages/dashboard".into(),
            60_000,
            "heavy".into(),
            CommandSpec::argv(vec!["tool".into(), "secret-command-value".into()]).unwrap(),
        )
        .unwrap();
        let meta = JobMeta::new(&material, material.fingerprint()).unwrap();
        LocalJobRecord::new(
            meta,
            lease_token,
            last_state.map(|state| status_for(state, 2_000 + u64::try_from(id).unwrap())),
            uncertainty,
        )
        .unwrap()
    }
}

struct RecordingWorkers {
    report: WorkersReport,
    deadlines: Mutex<Vec<Duration>>,
}

impl RecordingWorkers {
    fn new(report: WorkersReport) -> Self {
        Self {
            report,
            deadlines: Mutex::new(Vec::new()),
        }
    }

    fn deadlines(&self) -> Vec<Duration> {
        self.deadlines.lock().unwrap().clone()
    }
}

impl DashboardWorkerReader for RecordingWorkers {
    fn inspect(&self, _config: &Config, deadline: Duration) -> WorkersReport {
        self.deadlines.lock().unwrap().push(deadline);
        self.report.clone()
    }
}

#[derive(Clone)]
enum RemoteReply {
    Status(Box<StatusResponse>),
    Protocol(String),
}

#[derive(Default)]
struct RecordingRemote {
    statuses: Mutex<HashMap<JobId, RemoteReply>>,
    log: Mutex<Option<LogChunk>>,
    status_calls: Mutex<Vec<JobId>>,
    log_calls: Mutex<Vec<(JobId, LogStream, u64, u32)>>,
    mutating_calls: Mutex<Vec<&'static str>>,
}

impl RecordingRemote {
    fn set_status(&self, job_id: JobId, response: RemoteReply) {
        self.statuses.lock().unwrap().insert(job_id, response);
    }

    fn set_log(&self, chunk: LogChunk) {
        *self.log.lock().unwrap() = Some(chunk);
    }

    fn status_calls(&self) -> Vec<JobId> {
        self.status_calls.lock().unwrap().clone()
    }

    fn log_calls(&self) -> Vec<(JobId, LogStream, u64, u32)> {
        self.log_calls.lock().unwrap().clone()
    }

    fn mutating_calls(&self) -> Vec<&'static str> {
        self.mutating_calls.lock().unwrap().clone()
    }
}

impl DashboardRemoteReader for RecordingRemote {
    fn status(&self, _worker: &WorkerEntry, job_id: JobId) -> Result<StatusResponse, WorkerError> {
        self.status_calls.lock().unwrap().push(job_id);
        match self.statuses.lock().unwrap().get(&job_id).cloned() {
            Some(RemoteReply::Status(response)) => Ok(*response),
            Some(RemoteReply::Protocol(message)) => Err(WorkerError::Protocol(message)),
            None => Err(WorkerError::Protocol(
                "JOB_NOT_FOUND: missing fake response".into(),
            )),
        }
    }

    fn log_chunk(
        &self,
        _worker: &WorkerEntry,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<LogChunk, WorkerError> {
        self.log_calls
            .lock()
            .unwrap()
            .push((job_id, stream, offset, limit));
        self.log
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| WorkerError::Protocol("JOB_NOT_FOUND: missing fake log".into()))
    }
}

fn ready_report(active_job_id: JobId) -> WorkersReport {
    WorkersReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![ProbeWorkerHealth {
            name: "mini-1".into(),
            ssh: REMOTE_SSH.into(),
            status: HealthStatus::Ready,
            probe: Some(ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: SUPERVISION_VERSION,
                hostname: "mini-1.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 400,
                total_disk_bytes: 1_000,
                memory_pressure: MemoryPressure::Warn,
                swap_used_bytes: Some(42),
                slot_state: SlotState::Busy,
                active_lease: Some(LeaseSummary {
                    job_id: active_job_id,
                    project_id: PROJECT_ID.into(),
                    worktree_id: WORKTREE_ID.into(),
                    created_at_millis: 1_500,
                }),
                capabilities: vec!["swift".into(), "xcode".into()],
            }),
            missing_capabilities: vec!["docker".into()],
            error_code: None,
            error_message: None,
        }],
    }
}

fn status_response(record: &LocalJobRecord, state: JobState) -> StatusResponse {
    StatusResponse::new(
        record.meta().clone(),
        status_for(state, record.meta().created_at_millis() + 100),
    )
    .unwrap()
}

fn status_for(state: JobState, updated_at_millis: u64) -> JobStatus {
    match state {
        JobState::Accepted => JobStatus::accepted(updated_at_millis).unwrap(),
        JobState::Running => JobStatus::running(updated_at_millis, 10, 11, 12, 13).unwrap(),
        JobState::Succeeded => JobStatus::succeeded(updated_at_millis, 20, 30).unwrap(),
        other => JobStatus::new(
            other,
            updated_at_millis,
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
    }
}

fn job_id(value: u128) -> JobId {
    JobId::new(uuid::Uuid::from_u128(value))
}
