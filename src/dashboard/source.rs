use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    agent_facts::{AgentFacts, HerdrFacts},
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    dashboard::{
        cache::{CpuCounters, Observation},
        model::{
            ApiError, DashboardAgent, DashboardAgentFacts, DashboardCommandMode,
            DashboardCommandSummary, DashboardError, DashboardHerdr, DashboardJob,
            DashboardJobState, DashboardLogChunk, DashboardMemoryPressure, DashboardProfileAuth,
            DashboardProjectDefaults, DashboardSlotState, DashboardWorker, Freshness, SlotSummary,
            SystemSummary, WorkerHealth,
        },
        queue::ClientStateDashboardQueueReader,
        service::{
            DashboardDataSource, DashboardQueueReader, DashboardTaskCollection,
            WorkerObservationResult,
        },
        task::collect_task_projection,
        web::DashboardLogSource,
    },
    error::WorkerError,
    job::{JobId, JobState, JobStatus, LocalJobRecord, LogChunk, LogStream, StatusResponse},
    lease::SlotState,
    process::{ProcessRunner, SystemProcessRunner},
    project_config::ProjectSettings,
    protocol::{
        CpuCounters as ProbeCpuCounters, HealthStatus, MemoryPressure, ProbeResponse,
        WorkerHealth as ProbeWorkerHealth, WorkersReport,
    },
    redaction::RedactionBoundary,
    task_store::{TaskStatusRequest, TaskStatusResponse},
    transfer::RemoteJobClient,
    transport::{SshTransport, WorkersService},
};

const MAX_LOG_LIMIT: u32 = 65_536;
const ACTIVE_JOB_COLLECTION_DEADLINE_EXCEEDED: &str = "ACTIVE_JOB_COLLECTION_DEADLINE_EXCEEDED";

pub trait DashboardWorkerReader: Send + Sync + 'static {
    fn inspect(&self, config: &Config, deadline: Duration) -> WorkersReport;
}

pub trait DashboardRemoteReader: Send + Sync + 'static {
    fn status(&self, worker: &WorkerEntry, job_id: JobId) -> Result<StatusResponse, WorkerError>;
    fn status_with_deadline(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        _deadline: Duration,
    ) -> Result<StatusResponse, WorkerError> {
        self.status(worker, job_id)
    }
    fn task_status(
        &self,
        _worker: &WorkerEntry,
        _request: &TaskStatusRequest,
    ) -> Result<TaskStatusResponse, WorkerError> {
        Err(WorkerError::Unavailable(
            "TASK_STATUS_UNSUPPORTED: task status reader is unavailable".into(),
        ))
    }
    fn task_status_with_deadline(
        &self,
        worker: &WorkerEntry,
        request: &TaskStatusRequest,
        _deadline: Duration,
    ) -> Result<TaskStatusResponse, WorkerError> {
        self.task_status(worker, request)
    }
    fn log_chunk(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<LogChunk, WorkerError>;
}

pub struct SystemDashboardWorkerReader {
    runner: Arc<dyn ProcessRunner>,
}

impl SystemDashboardWorkerReader {
    pub fn new(runner: Arc<dyn ProcessRunner>) -> Self {
        Self { runner }
    }
}

impl Default for SystemDashboardWorkerReader {
    fn default() -> Self {
        Self::new(Arc::new(SystemProcessRunner))
    }
}

impl DashboardWorkerReader for SystemDashboardWorkerReader {
    fn inspect(&self, config: &Config, deadline: Duration) -> WorkersReport {
        WorkersService::new(SshTransport::new(self.runner.as_ref()))
            .inspect_with_budget(config, deadline)
    }
}

pub struct SystemDashboardRemoteReader {
    runner: Arc<dyn ProcessRunner>,
}

impl SystemDashboardRemoteReader {
    pub fn new(runner: Arc<dyn ProcessRunner>) -> Self {
        Self { runner }
    }
}

impl Default for SystemDashboardRemoteReader {
    fn default() -> Self {
        Self::new(Arc::new(SystemProcessRunner))
    }
}

impl DashboardRemoteReader for SystemDashboardRemoteReader {
    fn status(&self, worker: &WorkerEntry, job_id: JobId) -> Result<StatusResponse, WorkerError> {
        RemoteJobClient::new(self.runner.as_ref()).status(worker, job_id)
    }

    fn status_with_deadline(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        deadline: Duration,
    ) -> Result<StatusResponse, WorkerError> {
        RemoteJobClient::new(self.runner.as_ref()).status_with_deadline(worker, job_id, deadline)
    }

    fn task_status(
        &self,
        worker: &WorkerEntry,
        request: &TaskStatusRequest,
    ) -> Result<TaskStatusResponse, WorkerError> {
        RemoteJobClient::new(self.runner.as_ref()).task_status(worker, request)
    }

    fn task_status_with_deadline(
        &self,
        worker: &WorkerEntry,
        request: &TaskStatusRequest,
        deadline: Duration,
    ) -> Result<TaskStatusResponse, WorkerError> {
        RemoteJobClient::new(self.runner.as_ref())
            .task_status_with_deadline(worker, request, deadline)
    }

    fn log_chunk(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<LogChunk, WorkerError> {
        RemoteJobClient::new(self.runner.as_ref()).log_chunk(worker, job_id, stream, offset, limit)
    }
}

pub struct MacWorkerDashboardSource {
    pub config: Arc<Config>,
    pub workers: Arc<dyn DashboardWorkerReader>,
    pub local_jobs: Arc<ClientStateStore>,
    pub remote: Arc<dyn DashboardRemoteReader>,
    pub queue: Arc<dyn DashboardQueueReader>,
    pub project_defaults: Option<DashboardProjectDefaults>,
}

impl MacWorkerDashboardSource {
    pub fn new(
        config: Arc<Config>,
        workers: Arc<dyn DashboardWorkerReader>,
        local_jobs: Arc<ClientStateStore>,
        remote: Arc<dyn DashboardRemoteReader>,
    ) -> Self {
        let queue = Arc::new(ClientStateDashboardQueueReader::new(
            Arc::clone(&config),
            Arc::clone(&local_jobs),
        ));
        Self::with_queue(config, workers, local_jobs, remote, queue)
    }

    pub fn with_queue(
        config: Arc<Config>,
        workers: Arc<dyn DashboardWorkerReader>,
        local_jobs: Arc<ClientStateStore>,
        remote: Arc<dyn DashboardRemoteReader>,
        queue: Arc<dyn DashboardQueueReader>,
    ) -> Self {
        Self {
            config,
            workers,
            local_jobs,
            remote,
            queue,
            project_defaults: None,
        }
    }

    pub fn with_project_settings(mut self, settings: ProjectSettings) -> Self {
        self.project_defaults = Some(project_defaults(&settings));
        self
    }

    fn authoritative_job(
        &self,
        record: &LocalJobRecord,
        deadline: Duration,
    ) -> Result<DashboardJob, DashboardError> {
        let worker = self.worker_for(record)?;
        let response = self
            .remote
            .status_with_deadline(worker, record.meta().job_id(), deadline)
            .map_err(map_remote_error)?;
        project_authoritative_job(record, &response)
    }

    fn worker_for(&self, record: &LocalJobRecord) -> Result<&WorkerEntry, DashboardError> {
        self.config
            .worker(record.meta().worker_name())
            .ok_or_else(|| {
                DashboardError::new(
                    "DASHBOARD_WORKER_NOT_FOUND",
                    "local job references an unknown configured worker",
                )
            })
    }
}

impl DashboardDataSource for MacWorkerDashboardSource {
    fn project_defaults(&self) -> Option<DashboardProjectDefaults> {
        self.project_defaults.clone()
    }

    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        Ok(self
            .config
            .workers
            .iter()
            .map(|worker| worker.name.clone())
            .collect())
    }

    fn collect_workers(&self, deadline: Duration) -> Vec<WorkerObservationResult> {
        let report = self.workers.inspect(&self.config, deadline);
        let observed_at_millis = current_time_millis();
        report
            .workers
            .into_iter()
            .map(|worker| {
                let worker_name = worker.name.clone();
                project_worker(&worker, observed_at_millis)
                    .map(WorkerObservationResult::Current)
                    .unwrap_or_else(|error| WorkerObservationResult::Failed { error, worker_name })
            })
            .collect()
    }

    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
        self.local_jobs
            .list_jobs()
            .map_err(map_local_error)
            .map(|records| {
                records
                    .iter()
                    .map(|record| project_job(record, record.last_status()))
                    .collect()
            })
    }

    fn authoritative_active_jobs(
        &self,
        deadline: Duration,
    ) -> Vec<Result<DashboardJob, DashboardError>> {
        let records = match self.local_jobs.list_jobs() {
            Ok(records) => records,
            Err(error) => return vec![Err(map_local_error(error))],
        };
        let started = Instant::now();
        records
            .iter()
            .filter(|record| should_query_authoritative_status(record))
            .map(|record| {
                let remaining = deadline.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Err(DashboardError::new(
                        ACTIVE_JOB_COLLECTION_DEADLINE_EXCEEDED,
                        "active-job collection budget expired before remote status collection",
                    ));
                }
                self.authoritative_job(record, remaining)
            })
            .collect()
    }

    fn queue_entries(
        &self,
    ) -> Result<Vec<crate::dashboard::model::DashboardQueueEntry>, DashboardError> {
        self.queue.ordered_pending()
    }

    fn task_projection(
        &self,
        deadline: Duration,
    ) -> Result<DashboardTaskCollection, DashboardError> {
        collect_task_projection(
            &self.config,
            &self.local_jobs,
            self.remote.as_ref(),
            deadline,
        )
    }
}

pub struct MacWorkerLogSource {
    pub config: Arc<Config>,
    pub local_jobs: Arc<ClientStateStore>,
    pub remote: Arc<dyn DashboardRemoteReader>,
}

impl MacWorkerLogSource {
    pub fn new(
        config: Arc<Config>,
        local_jobs: Arc<ClientStateStore>,
        remote: Arc<dyn DashboardRemoteReader>,
    ) -> Self {
        Self {
            config,
            local_jobs,
            remote,
        }
    }

    fn owned_record(&self, job_id: JobId) -> Result<LocalJobRecord, ApiError> {
        self.local_jobs
            .list_jobs()
            .map_err(map_local_api_error)?
            .into_iter()
            .find(|record| record.meta().job_id() == job_id)
            .ok_or_else(|| ApiError::new("JOB_NOT_FOUND", "job is not present in local state"))
    }

    fn worker_for(&self, record: &LocalJobRecord) -> Result<&WorkerEntry, ApiError> {
        self.config
            .worker(record.meta().worker_name())
            .ok_or_else(|| {
                ApiError::new(
                    "DASHBOARD_WORKER_NOT_FOUND",
                    "local job references an unknown configured worker",
                )
            })
    }
}

impl DashboardLogSource for MacWorkerLogSource {
    fn job_detail(&self, job_id: JobId) -> Result<DashboardJob, ApiError> {
        let record = self.owned_record(job_id)?;
        let worker = self.worker_for(&record)?;
        let response = self
            .remote
            .status(worker, job_id)
            .map_err(map_remote_api_error)?;
        project_authoritative_job(&record, &response).map_err(dashboard_api_error)
    }

    fn read_log(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        if !(1..=MAX_LOG_LIMIT).contains(&limit) {
            return Err(ApiError::new(
                "INVALID_LOG_LIMIT",
                "log limit must be between 1 and 65536 bytes",
            ));
        }
        let record = self.owned_record(job_id)?;
        let worker = self.worker_for(&record)?;
        let chunk = self
            .remote
            .log_chunk(worker, job_id, stream, offset, limit)
            .map_err(map_remote_api_error)?;
        DashboardLogChunk::from_log_chunk(&chunk).map_err(dashboard_api_error)
    }
}

pub fn project_worker(
    worker: &ProbeWorkerHealth,
    observed_at_millis: u64,
) -> Result<Observation, DashboardError> {
    if worker.status != HealthStatus::Ready {
        return Err(DashboardError::new(
            safe_code(worker.error_code.as_deref(), "WORKER_UNAVAILABLE"),
            "worker observation is unavailable",
        ));
    }
    let probe = worker.probe.as_ref().ok_or_else(|| {
        DashboardError::new(
            "PROBE_RESPONSE_MISSING",
            "ready worker did not provide a probe response",
        )
    })?;
    Ok(Observation {
        worker: DashboardWorker {
            name: worker.name.clone(),
            health: WorkerHealth::Ready,
            freshness: Freshness::Current,
            observed_at_millis: Some(observed_at_millis),
            hostname: Some(probe.hostname.clone()),
            agent_facts: match (&probe.agent_facts, probe.facts_age_millis) {
                (Some(facts), Some(facts_age_millis)) => Some(project_agent_facts(
                    facts,
                    facts_age_millis,
                    observed_at_millis,
                )),
                _ => None,
            },
            herdr: project_dashboard_herdr(probe),
            slot: SlotSummary {
                state: match probe.slot_state {
                    SlotState::Idle => DashboardSlotState::Idle,
                    SlotState::Busy => DashboardSlotState::Busy,
                },
                capacity: 1,
                active_job_id: probe.active_lease.as_ref().map(|lease| lease.job_id),
            },
            capabilities: probe.capabilities.clone(),
            missing_capabilities: worker.missing_capabilities.clone(),
            system: SystemSummary {
                free_disk_bytes: Some(probe.free_disk_bytes),
                total_disk_bytes: Some(probe.total_disk_bytes),
                memory_pressure: Some(match probe.memory_pressure {
                    MemoryPressure::Normal => DashboardMemoryPressure::Normal,
                    MemoryPressure::Warn => DashboardMemoryPressure::Warn,
                    MemoryPressure::Critical => DashboardMemoryPressure::Critical,
                    MemoryPressure::Unknown => DashboardMemoryPressure::Unknown,
                }),
                swap_used_bytes: probe.swap_used_bytes,
                cpu_busy_percent: None,
            },
            error: None,
            active_task: None,
        },
        observed_at_millis,
        cpu_counters: probe.cpu_counters.clone().and_then(cache_counters),
    })
}

fn project_agent_facts(
    facts: &AgentFacts,
    facts_age_millis: u64,
    observed_at_millis: u64,
) -> DashboardAgentFacts {
    let boundary = RedactionBoundary::from_env();
    let agents = facts
        .agents
        .iter()
        .map(|agent| DashboardAgent {
            name: boundary.text(&agent.name, 128),
            version: agent
                .version
                .as_deref()
                .map(|version| boundary.text(version, 128)),
            auth: agent.auth.as_str().to_owned(),
            auth_by_profile: agent
                .auth_by_profile
                .iter()
                .map(|(profile, auth)| DashboardProfileAuth {
                    profile: boundary.text(profile, 128),
                    auth: auth.as_str().to_owned(),
                })
                .collect(),
        })
        .collect();
    let herdr = facts.herdr.as_ref().map(|herdr| HerdrFacts {
        state: herdr.state,
        version: herdr
            .version
            .as_deref()
            .map(|version| boundary.text(version, 64)),
        interactive_agents: herdr.interactive_agents,
    });
    DashboardAgentFacts::from_observation(
        facts.collected_at_millis(),
        facts_age_millis,
        observed_at_millis,
        agents,
        herdr,
    )
}

fn project_dashboard_herdr(probe: &ProbeResponse) -> Option<DashboardHerdr> {
    let herdr = probe.herdr_fact()?;
    let boundary = RedactionBoundary::from_env();
    Some(DashboardHerdr {
        state: herdr.state.as_str().to_owned(),
        version: herdr
            .version
            .as_deref()
            .map(|version| boundary.text(version, 64)),
        interactive_agents: herdr.interactive_agents,
    })
}

fn project_defaults(settings: &ProjectSettings) -> DashboardProjectDefaults {
    let boundary = RedactionBoundary::from_env();
    DashboardProjectDefaults {
        default_agent: boundary.text(&settings.task.default_agent, 128),
        timeout_seconds: settings.task.timeout.as_secs(),
        max_followups: settings.task.max_followups,
        source: settings.task.source.clone(),
        publish: settings.task.publish.clone(),
        env_profile: settings
            .task
            .env_profile
            .as_deref()
            .map(|profile| boundary.text(profile, 128)),
        permissions: settings
            .task
            .permissions
            .iter()
            .map(|(agent, policy)| (boundary.text(agent, 128), policy.clone()))
            .collect(),
    }
}

pub fn cache_counters(counters: ProbeCpuCounters) -> Option<CpuCounters> {
    let total_ticks = counters
        .user_ticks
        .checked_add(counters.system_ticks)?
        .checked_add(counters.idle_ticks)?
        .checked_add(counters.nice_ticks)?;
    CpuCounters::new(total_ticks, counters.idle_ticks).ok()
}

pub fn project_job(record: &LocalJobRecord, status: Option<&JobStatus>) -> DashboardJob {
    let meta = record.meta();
    let (
        state,
        updated_at_millis,
        exit_code,
        terminating_signal,
        final_stdout_bytes,
        final_stderr_bytes,
    ) = match status {
        Some(status) => (
            project_job_state(status.state()),
            status.updated_at_millis(),
            status.exit_code(),
            status.terminating_signal(),
            status.final_stdout_bytes(),
            status.final_stderr_bytes(),
        ),
        None => (
            DashboardJobState::Uploading,
            meta.created_at_millis(),
            None,
            None,
            None,
            None,
        ),
    };
    DashboardJob {
        job_id: meta.job_id(),
        worker_name: meta.worker_name().to_owned(),
        project_id: meta.project_id().to_owned(),
        worktree_id: meta.worktree_id().to_owned(),
        project_label: None,
        manifest_digest: meta.manifest_digest().to_owned(),
        command_summary: project_command_summary(meta),
        resource_class: meta.resource_class().to_owned(),
        created_at_millis: meta.created_at_millis(),
        updated_at_millis,
        state,
        exit_code,
        terminating_signal,
        final_stdout_bytes,
        final_stderr_bytes,
        artifact_status: None,
        remote_uncertainty: record.remote_uncertainty().code().map(str::to_owned),
    }
}

pub fn map_remote_error(error: WorkerError) -> DashboardError {
    let code = error.public_code();
    let message = match code.as_str() {
        "JOB_NOT_FOUND" => "remote job was not found",
        "SSH_UNAVAILABLE" | "UNAVAILABLE" => "remote worker is unavailable",
        "INVALID_RESPONSE" | "PROTOCOL" => "remote worker response is invalid",
        _ => "remote job operation is unavailable",
    };
    DashboardError::new(code, message)
}

fn project_authoritative_job(
    record: &LocalJobRecord,
    response: &StatusResponse,
) -> Result<DashboardJob, DashboardError> {
    if response.meta() != record.meta() {
        return Err(DashboardError::new(
            "REMOTE_JOB_IDENTITY_MISMATCH",
            "remote job metadata did not match local state",
        ));
    }
    Ok(project_job(record, Some(response.status())))
}

fn should_query_authoritative_status(record: &LocalJobRecord) -> bool {
    matches!(
        record.last_status().map(JobStatus::state),
        Some(JobState::Accepted | JobState::Running)
    ) || record.remote_uncertainty().code().is_some()
}

fn project_command_summary(meta: &crate::job::JobMeta) -> DashboardCommandSummary {
    match meta.command_summary().arg_count() {
        Some(arg_count) => DashboardCommandSummary {
            mode: DashboardCommandMode::Argv,
            arg_count: Some(u16::try_from(arg_count).unwrap_or(u16::MAX)),
        },
        None => DashboardCommandSummary {
            mode: DashboardCommandMode::Shell,
            arg_count: None,
        },
    }
}

fn project_job_state(state: JobState) -> DashboardJobState {
    match state {
        JobState::Uploading => DashboardJobState::Uploading,
        JobState::Verified => DashboardJobState::Verified,
        JobState::Accepted => DashboardJobState::Accepted,
        JobState::Running => DashboardJobState::Running,
        JobState::Succeeded => DashboardJobState::Succeeded,
        JobState::Failed => DashboardJobState::Failed,
        JobState::Cancelled => DashboardJobState::Cancelled,
        JobState::TimedOut => DashboardJobState::TimedOut,
        JobState::Lost => DashboardJobState::Lost,
    }
}

fn map_local_error(error: WorkerError) -> DashboardError {
    DashboardError::new(error.public_code(), "local dashboard state is unavailable")
}

fn map_remote_api_error(error: WorkerError) -> ApiError {
    dashboard_api_error(map_remote_error(error))
}

fn map_local_api_error(error: WorkerError) -> ApiError {
    dashboard_api_error(map_local_error(error))
}

fn dashboard_api_error(error: DashboardError) -> ApiError {
    ApiError::new(error.code, error.message)
}

fn safe_code(candidate: Option<&str>, fallback: &str) -> String {
    candidate
        .filter(|code| is_stable_code(code))
        .unwrap_or(fallback)
        .to_owned()
}

fn is_stable_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 128
        && code.starts_with(|character: char| character.is_ascii_uppercase())
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| duration.as_millis().try_into().ok())
        .unwrap_or(0)
}
