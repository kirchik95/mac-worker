use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use mac_worker::{
    agent_facts::{AgentAuth, AgentFacts, AgentProbe, FACTS_TTL, ProfileProbe},
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
        AdmissionObservation, CommandSpec, CommandSummary, JobId, JobMeta, JobState, JobStatus,
        LeaseToken, LocalJobRecord, LogChunk, LogStream, ProcessIdentity, QueueEntry,
        QueueEntryKind, RemoteUncertainty, RequestFingerprintMaterial, StatusResponse,
    },
    lease::{LeaseSummary, SlotState},
    protocol::{
        HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        WorkerHealth as ProbeWorkerHealth, WorkersReport,
    },
    scheduler::{CandidateSlot, WorkerPreference},
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
fn source_projects_only_allowlisted_agent_facts_and_profile_auth() {
    let mut report = ready_report(job_id(91));
    let probe = report.workers[0].probe.as_mut().unwrap();
    probe.agent_facts = Some(AgentFacts {
        agents: vec![
            AgentProbe::new(
                "codex",
                Some("0.42.0".into()),
                AgentAuth::UnknownWithReason("keychain_locked"),
                vec![
                    ("team-ci".into(), AgentAuth::Authenticated),
                    ("review".into(), AgentAuth::Unauthenticated),
                ],
            )
            .unwrap(),
        ],
        env_profiles: vec![ProfileProbe::new("team-ci", true).unwrap()],
        git_identity: true,
        collected_at_millis: u64::MAX - 1,
        herdr: None,
    });
    probe.facts_age_millis = Some(FACTS_TTL + 99);

    let fixture = Fixture::new(report);
    let rows = fixture.source().collect_workers(Duration::from_secs(7));
    let WorkerObservationResult::Current(observation) = rows.into_iter().next().unwrap() else {
        panic!("ready probe must project to a current observation");
    };
    let facts = observation.worker.agent_facts.as_ref().unwrap();
    assert_eq!(facts.collected_at_millis, u64::MAX - 1);
    assert_eq!(facts.agents[0].auth, "unknown");
    assert_eq!(facts.agents[0].auth_by_profile[0].profile, "team-ci");
    assert_eq!(facts.agents[0].auth_by_profile[0].auth, "authenticated");

    let value = serde_json::to_value(facts).unwrap();
    assert_eq!(value["freshness"], "stale");
    assert!(value.get("git_identity").is_none());
    assert!(value.get("env_profiles").is_none());
    assert!(!value.to_string().contains("keychain_locked"));
}

#[test]
fn source_keeps_fresh_facts_current_despite_an_old_remote_timestamp() {
    let mut report = ready_report(job_id(91));
    let probe = report.workers[0].probe.as_mut().unwrap();
    probe.agent_facts = Some(AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: false,
        collected_at_millis: 1,
        herdr: None,
    });
    probe.facts_age_millis = Some(0);

    let fixture = Fixture::new(report);
    let rows = fixture.source().collect_workers(Duration::from_secs(7));
    let WorkerObservationResult::Current(observation) = rows.into_iter().next().unwrap() else {
        panic!("ready probe must project to a current observation");
    };

    let value = serde_json::to_value(observation.worker.agent_facts.unwrap()).unwrap();
    assert_eq!(value["collected_at_millis"], 1);
    assert_eq!(value["freshness"], "current");
}

#[test]
fn source_keeps_agent_facts_unavailable_when_facts_or_worker_age_is_missing() {
    let fixture = Fixture::new(ready_report(job_id(91)));
    let rows = fixture.source().collect_workers(Duration::from_secs(7));
    let WorkerObservationResult::Current(observation) = rows.into_iter().next().unwrap() else {
        panic!("ready probe must project to a current observation");
    };

    assert_eq!(observation.worker.agent_facts, None);

    let mut report = ready_report(job_id(92));
    report.workers[0].probe.as_mut().unwrap().agent_facts = Some(AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: false,
        collected_at_millis: 1,
        herdr: None,
    });
    let fixture = Fixture::new(report);
    let rows = fixture.source().collect_workers(Duration::from_secs(7));
    let WorkerObservationResult::Current(observation) = rows.into_iter().next().unwrap() else {
        panic!("ready probe must project to a current observation");
    };
    assert_eq!(observation.worker.agent_facts, None);
}

#[test]
fn source_stamps_current_worker_observations_after_collection_finishes() {
    let fixture = Fixture::new(ready_report(job_id(99)));
    fixture.workers.set_inspect_delay(Duration::from_millis(25));

    let rows = fixture.source().collect_workers(Duration::from_secs(1));

    let WorkerObservationResult::Current(observation) = rows.into_iter().next().unwrap() else {
        panic!("ready probe must project to a current observation");
    };
    assert!(
        observation.observed_at_millis >= fixture.workers.finished_at_millis(),
        "a current observation must not predate the completed collection"
    );
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
fn active_status_collection_uses_one_shrinking_deadline() {
    let fixture = Fixture::new(ready_report(job_id(99)));
    let first = fixture.record(1, Some(JobState::Accepted), RemoteUncertainty::None);
    let second = fixture.record(2, Some(JobState::Running), RemoteUncertainty::None);
    for record in [&first, &second] {
        fixture.state.create_job(record.clone()).unwrap();
        fixture.remote.set_status(
            record.meta().job_id(),
            RemoteReply::Status(Box::new(status_response(record, JobState::Running))),
        );
    }
    fixture.remote.set_status_delay(Duration::from_millis(20));

    let jobs = fixture
        .source()
        .authoritative_active_jobs(Duration::from_secs(1));

    assert_eq!(jobs.len(), 2);
    let deadlines = fixture.remote.status_deadlines();
    assert_eq!(deadlines.len(), 2);
    assert!(deadlines[0] <= Duration::from_secs(1));
    assert!(deadlines[1] < deadlines[0]);
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
fn source_projects_client_state_fifo_rows_and_advisory_blocking_codes_without_refreshing() {
    // Break caught: the production constructor keeps the empty queue reader,
    // reorders waiting rows, or causes a dashboard read to refresh/mutate
    // scheduler state rather than project the persisted advisory view.
    let fixture = Fixture::new(ready_report(job_id(99)));
    let source = fixture.source();
    assert!(source.queue_entries().unwrap().is_empty());

    let first = fixture
        .state
        .enqueue(queue_entry(
            &fixture,
            11,
            1_000,
            WorkerPreference::Automatic,
        ))
        .unwrap();
    let second = fixture
        .state
        .enqueue(queue_entry(
            &fixture,
            12,
            1_001,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
        ))
        .unwrap();
    cache_busy_observation(&fixture, 1_002);
    let before = fixture.state.queue_snapshot().unwrap();

    let queue = source.queue_entries().unwrap();

    assert_eq!(
        queue.iter().map(|entry| entry.position).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        queue.iter().map(|entry| entry.job_id).collect::<Vec<_>>(),
        vec![first.job_id(), second.job_id()]
    );
    assert_eq!(queue[0].blocking_code, "NO_COMPATIBLE_IDLE_WORKER");
    assert_eq!(queue[1].blocking_code, "PINNED_WORKER_BUSY");
    assert_eq!(queue[0].project_id, PROJECT_ID);
    assert_eq!(queue[0].worktree_id, WORKTREE_ID);
    assert_eq!(queue[0].project_label, None);
    assert_eq!(queue[0].command_summary.arg_count, Some(2));
    assert_eq!(fixture.state.queue_snapshot().unwrap(), before);
    assert!(fixture.workers.deadlines().is_empty());
    assert!(fixture.remote.status_calls().is_empty());
    assert!(fixture.remote.log_calls().is_empty());
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
            notifications: mac_worker::config::NotificationsConfig::default(),
            workers: vec![WorkerEntry {
                name: "mini-1".into(),
                ssh: REMOTE_SSH.into(),
                slots: 1,
                capabilities: vec!["swift".into()],
                remote_binary: "~/.local/bin/worker".into(),
                herdr: false,
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
    inspect_delay: Mutex<Duration>,
    finished_at_millis: Mutex<Option<u64>>,
}

impl RecordingWorkers {
    fn new(report: WorkersReport) -> Self {
        Self {
            report,
            deadlines: Mutex::new(Vec::new()),
            inspect_delay: Mutex::new(Duration::ZERO),
            finished_at_millis: Mutex::new(None),
        }
    }

    fn deadlines(&self) -> Vec<Duration> {
        self.deadlines.lock().unwrap().clone()
    }

    fn set_inspect_delay(&self, delay: Duration) {
        *self.inspect_delay.lock().unwrap() = delay;
    }

    fn finished_at_millis(&self) -> u64 {
        self.finished_at_millis
            .lock()
            .unwrap()
            .expect("inspection must have completed")
    }
}

impl DashboardWorkerReader for RecordingWorkers {
    fn inspect(&self, _config: &Config, deadline: Duration) -> WorkersReport {
        self.deadlines.lock().unwrap().push(deadline);
        std::thread::sleep(*self.inspect_delay.lock().unwrap());
        *self.finished_at_millis.lock().unwrap() = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .try_into()
                .unwrap(),
        );
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
    status_deadlines: Mutex<Vec<Duration>>,
    status_delay: Mutex<Duration>,
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

    fn status_deadlines(&self) -> Vec<Duration> {
        self.status_deadlines.lock().unwrap().clone()
    }

    fn set_status_delay(&self, delay: Duration) {
        *self.status_delay.lock().unwrap() = delay;
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
        std::thread::sleep(*self.status_delay.lock().unwrap());
        match self.statuses.lock().unwrap().get(&job_id).cloned() {
            Some(RemoteReply::Status(response)) => Ok(*response),
            Some(RemoteReply::Protocol(message)) => Err(WorkerError::Protocol(message)),
            None => Err(WorkerError::Protocol(
                "JOB_NOT_FOUND: missing fake response".into(),
            )),
        }
    }

    fn status_with_deadline(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        deadline: Duration,
    ) -> Result<StatusResponse, WorkerError> {
        self.status_deadlines.lock().unwrap().push(deadline);
        self.status(worker, job_id)
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
                available_memory_bytes: None,
                cpu_counters: None,
                slot_state: SlotState::Busy,
                active_lease: Some(LeaseSummary {
                    job_id: active_job_id,
                    project_id: PROJECT_ID.into(),
                    worktree_id: WORKTREE_ID.into(),
                    created_at_millis: 1_500,
                }),
                capabilities: vec!["swift".into(), "xcode".into()],
                agent_facts: None,
                facts_age_millis: None,
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

fn queue_entry(
    fixture: &Fixture,
    id: u128,
    enqueued_at_millis: u64,
    preference: WorkerPreference,
) -> QueueEntry {
    QueueEntry::new(
        job_id(id),
        fixture.state.client_id(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        CommandSummary::argv(2).unwrap(),
        Vec::new(),
        preference,
        QueueEntryKind::Batch,
        None,
        ProcessIdentity::new(10_000 + id as u32, 100_000 + id as u64).unwrap(),
        enqueued_at_millis,
    )
    .unwrap()
}

fn cache_busy_observation(fixture: &Fixture, observed_at_millis: u64) {
    let observation = AdmissionObservation::new(
        "mini-1".into(),
        true,
        CandidateSlot::Busy,
        vec!["swift".into()],
        Some(8 * 1024 * 1024 * 1024),
        64 * 1024 * 1024 * 1024,
        observed_at_millis,
    )
    .unwrap();
    fixture
        .state
        .admission_observation("mini-1", observed_at_millis, || Ok(observation))
        .unwrap();
}

fn job_id(value: u128) -> JobId {
    JobId::new(uuid::Uuid::from_u128(value))
}

#[test]
fn source_passes_the_herdr_fact_through_agent_facts_and_projects_null_without_it() {
    // The dashboard sees the same fact doctor and workers see, unchanged; a
    // record that predates the fact projects `null` rather than inventing one.
    use mac_worker::agent_facts::{HerdrFactState, HerdrFacts};

    let mut report = ready_report(job_id(91));
    let probe = report.workers[0].probe.as_mut().unwrap();
    probe.agent_facts = Some(AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: true,
        collected_at_millis: 1,
        herdr: Some(HerdrFacts {
            state: HerdrFactState::NoSocket,
            version: Some("0.9.0".into()),
            interactive_agents: None,
        }),
    });
    probe.facts_age_millis = Some(0);
    let fixture = Fixture::new(report);
    let rows = fixture.source().collect_workers(Duration::from_secs(7));
    let WorkerObservationResult::Current(observation) = rows.into_iter().next().unwrap() else {
        panic!("ready probe must project to a current observation");
    };
    let value = serde_json::to_value(observation.worker.agent_facts.unwrap()).unwrap();
    assert_eq!(
        value["herdr"],
        serde_json::json!({ "state": "no_socket", "version": "0.9.0" })
    );

    let mut report = ready_report(job_id(92));
    let probe = report.workers[0].probe.as_mut().unwrap();
    probe.agent_facts = Some(AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: true,
        collected_at_millis: 1,
        herdr: None,
    });
    probe.facts_age_millis = Some(0);
    let fixture = Fixture::new(report);
    let rows = fixture.source().collect_workers(Duration::from_secs(7));
    let WorkerObservationResult::Current(observation) = rows.into_iter().next().unwrap() else {
        panic!("ready probe must project to a current observation");
    };
    let value = serde_json::to_value(observation.worker.agent_facts.unwrap()).unwrap();
    assert!(value.get("herdr").is_some(), "the key is present: {value}");
    assert!(value["herdr"].is_null(), "{value}");
}

#[test]
fn source_projects_a_fresh_herdr_chip_and_omits_it_when_facts_are_stale() {
    use mac_worker::agent_facts::{HerdrFactState, HerdrFacts};

    let mut report = ready_report(job_id(93));
    let probe = report.workers[0].probe.as_mut().unwrap();
    probe.agent_facts = Some(AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: true,
        collected_at_millis: 1,
        herdr: Some(HerdrFacts {
            state: HerdrFactState::Available,
            version: Some("0.9.0".into()),
            interactive_agents: Some(2),
        }),
    });
    probe.facts_age_millis = Some(0);
    let fixture = Fixture::new(report);
    let rows = fixture.source().collect_workers(Duration::from_secs(7));
    let WorkerObservationResult::Current(observation) = rows.into_iter().next().unwrap() else {
        panic!("ready probe must project to a current observation");
    };
    let value = serde_json::to_value(observation.worker.herdr.as_ref()).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "state": "available",
            "version": "0.9.0",
            "interactive_agents": 2
        })
    );
    let json = value.to_string();
    for forbidden in ["pane_id", "cwd", "title", "w1:"] {
        assert!(!json.contains(forbidden), "{forbidden:?} leaked: {json}");
    }

    let mut report = ready_report(job_id(94));
    let probe = report.workers[0].probe.as_mut().unwrap();
    probe.agent_facts = Some(AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: true,
        collected_at_millis: 1,
        herdr: Some(HerdrFacts {
            state: HerdrFactState::Available,
            version: Some("0.9.0".into()),
            interactive_agents: Some(2),
        }),
    });
    probe.facts_age_millis = Some(FACTS_TTL + 1);
    let fixture = Fixture::new(report);
    let rows = fixture.source().collect_workers(Duration::from_secs(7));
    let WorkerObservationResult::Current(observation) = rows.into_iter().next().unwrap() else {
        panic!("stale facts still project a worker");
    };
    assert_eq!(observation.worker.herdr, None);
}
