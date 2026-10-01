use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    agent_facts::{AgentFacts, HerdrFacts},
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    dashboard::{
        cache::{CpuCounters, Observation},
        model::{
            DashboardAgent, DashboardAgentFacts, DashboardError, DashboardHerdr,
            DashboardMemoryPressure, DashboardProfileAuth, DashboardProjectDefaults,
            DashboardSlotState, DashboardWorker, Freshness, SlotSummary, SystemSummary,
            WorkerHealth,
        },
        queue::ClientStateDashboardQueueReader,
        service::{
            DashboardDataSource, DashboardQueueReader, DashboardTaskCollection,
            WorkerObservationResult,
        },
        task::{collect_task_projection, project_local_tasks},
    },
    error::WorkerError,
    job::{JobId, LogChunk, LogStream},
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

pub trait DashboardWorkerReader: Send + Sync + 'static {
    fn inspect(&self, config: &Config, deadline: Duration) -> WorkersReport;

    /// Refresh cached agent facts on `worker`. The ordinary probe never
    /// starts agent processes; this is the path that does. A default no-op
    /// lets tests that only inspect skip the SSH round-trip.
    fn refresh_facts(&self, worker: &WorkerEntry) -> Result<(), WorkerError> {
        let _ = worker;
        Ok(())
    }
}

pub trait DashboardRemoteReader: Send + Sync + 'static {
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

pub(crate) struct SystemDashboardWorkerReader {
    runner: Arc<dyn ProcessRunner>,
}

impl SystemDashboardWorkerReader {
    pub(crate) fn new(runner: Arc<dyn ProcessRunner>) -> Self {
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

    fn refresh_facts(&self, worker: &WorkerEntry) -> Result<(), WorkerError> {
        SshTransport::new(self.runner.as_ref()).refresh_facts(worker)
    }
}

pub(crate) struct SystemDashboardRemoteReader {
    runner: Arc<dyn ProcessRunner>,
}

impl SystemDashboardRemoteReader {
    pub(crate) fn new(runner: Arc<dyn ProcessRunner>) -> Self {
        Self { runner }
    }
}

impl Default for SystemDashboardRemoteReader {
    fn default() -> Self {
        Self::new(Arc::new(SystemProcessRunner))
    }
}

impl DashboardRemoteReader for SystemDashboardRemoteReader {
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

    fn inspect_workers(&self, config: &Config, deadline: Duration) -> Vec<WorkerObservationResult> {
        let report = self.workers.inspect(config, deadline);
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

    fn configured_worker_slots(&self, worker_name: &str) -> u8 {
        self.config
            .worker(worker_name)
            .map(|worker| worker.slots)
            .unwrap_or(1)
    }

    fn collect_workers(&self, deadline: Duration) -> Vec<WorkerObservationResult> {
        self.inspect_workers(&self.config, deadline)
    }

    fn probe_workers(&self, names: &[String], deadline: Duration) -> Vec<WorkerObservationResult> {
        let wanted: HashSet<&str> = names.iter().map(String::as_str).collect();
        let config = self.config.with_workers(
            self.config
                .workers
                .iter()
                .filter(|worker| wanted.contains(worker.name.as_str()))
                .cloned()
                .collect(),
        );
        self.inspect_workers(&config, deadline)
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

    fn local_task_projection(&self) -> Result<DashboardTaskCollection, DashboardError> {
        project_local_tasks(&self.config, &self.local_jobs)
    }

    fn refresh_worker_facts(&self, worker_name: &str) -> Result<(), WorkerError> {
        let worker = self.config.worker(worker_name).ok_or_else(|| {
            WorkerError::Unavailable(format!(
                "DASHBOARD_WORKER_NOT_FOUND: configured worker {worker_name} is not in inventory"
            ))
        })?;
        self.workers.refresh_facts(worker)
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
            herdr: project_dashboard_herdr(probe, observed_at_millis),
            slot: slot_summary(probe),
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

/// Occupancy the dashboard shows for one worker.
///
/// Capacity and busy come from the probe helpers. An older helper that omits
/// `configured_slots` keeps `slot_state` as the Idle/Busy fallback. Protocol 7
/// still carries a single `active_lease`, not a list of live leases, so
/// `active_job_ids` is that job when present.
fn slot_summary(probe: &ProbeResponse) -> SlotSummary {
    let capacity = probe.configured_slot_count();
    let busy = probe.busy_slot_count();
    let state = if probe.configured_slots == 0 {
        match probe.slot_state {
            SlotState::Idle => DashboardSlotState::Idle,
            SlotState::Busy => DashboardSlotState::Busy,
        }
    } else if busy < capacity {
        DashboardSlotState::Idle
    } else {
        DashboardSlotState::Busy
    };
    let active_job_ids = probe
        .active_lease
        .as_ref()
        .map(|lease| vec![lease.job_id])
        .unwrap_or_default();
    SlotSummary {
        state,
        capacity,
        busy,
        active_job_id: active_job_ids.first().copied(),
        active_job_ids,
    }
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

fn project_dashboard_herdr(
    probe: &ProbeResponse,
    observed_at_millis: u64,
) -> Option<DashboardHerdr> {
    // Pass a known fact through even after the TTL so the chip can show
    // `herdr 0.9.0 · 69m ago` instead of `unknown`. Capability derivation still
    // uses `ProbeResponse::herdr_fact()`, which returns nothing for stale facts.
    let facts = probe.agent_facts.as_ref()?;
    let herdr = facts.herdr.as_ref()?;
    let facts_age_millis = probe.facts_age_millis?;
    let boundary = RedactionBoundary::from_env();
    Some(DashboardHerdr::from_observation(
        herdr.state.as_str().to_owned(),
        herdr
            .version
            .as_deref()
            .map(|version| boundary.text(version, 64)),
        herdr.interactive_agents,
        facts_age_millis,
        observed_at_millis,
    ))
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
