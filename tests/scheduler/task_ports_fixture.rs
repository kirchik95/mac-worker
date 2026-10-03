//! Real task client/runner and lease storage, with only the remote process boundary faked.
use mac_worker::test_support::{
    agents::{
        agent::AgentKind,
        agent_facts::{AgentAuth, AgentFacts, AgentProbe},
    },
    client_state::{ClientStateStore, scheduler::WorkerPreference},
    core::{
        config::{Config, WorkerEntry},
        error::WorkerError,
        paths::PathLayout,
        protocol::{MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    },
    host::{
        job::{
            ExecutionScope, HostControlError, JobId, JobMeta, JobStatus, LeaseAcquireRequest,
            LeaseAcquireResponse, LogChunk, LogChunkRequest, LogChunkResponse, StatusRequest,
            StatusResponse, SubmitResponse,
        },
        lease::{AdmissionFacts, LeaseService},
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
        store::HostStore,
    },
    task::{
        client::{TaskClient, TaskSubmitRequest},
        model::{
            ClosePolicy, TaskId, TaskLimits, TaskMeta, TaskOutcome, TaskState, TaskStatus, TurnId,
            TurnSummary, TurnTerminal,
        },
        store::{TaskPrepareRequest, TaskPrepareResponse, TaskStatusRequest, TaskStatusResponse},
        turn::{TaskTurnRequest, TaskTurnResponse},
        turn_runner::{InlineRunnerExecutor, TurnOutcomeReport, TurnRunner},
    },
    transfer::HostOperation,
};
use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

pub(super) fn canonical(value: &impl serde::Serialize) -> Result<ProcessResult, WorkerError> {
    let mut stdout = serde_json::to_vec(value).unwrap();
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })
}
fn decode<T: serde::de::DeserializeOwned>(request: &ProcessRequest) -> T {
    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap()
}
fn success() -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout: Vec::new(),
        stderr: Vec::new(),
    })
}
fn healthy() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 << 30,
        total_disk_bytes: 250 << 30,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

pub(super) struct TaskRemote {
    pub host: HostStore,
    pub requests: Mutex<Vec<ProcessRequest>>,
    pub before_acquire: Option<Arc<dyn Fn() + Send + Sync>>,
    pub on_busy_probe: Option<Arc<dyn Fn() + Send + Sync>>,
    pub probe_count: AtomicUsize,
    acquires: Mutex<HashMap<JobId, LeaseAcquireRequest>>,
    tasks: Mutex<HashMap<TaskId, (TaskMeta, TurnId, bool)>>,
    jobs: Mutex<HashMap<JobId, JobMeta>>,
}
impl TaskRemote {
    pub fn new(host: HostStore) -> Self {
        Self {
            host,
            requests: Mutex::new(Vec::new()),
            before_acquire: None,
            on_busy_probe: None,
            probe_count: AtomicUsize::new(0),
            acquires: Mutex::new(HashMap::new()),
            tasks: Mutex::new(HashMap::new()),
            jobs: Mutex::new(HashMap::new()),
        }
    }
    pub fn accepted_jobs(&self) -> HashSet<JobId> {
        self.acquires.lock().unwrap().keys().copied().collect()
    }
    pub fn release(&self, job: JobId) {
        let request = self.acquires.lock().unwrap()[&job].clone();
        self.host
            .record_abandoned(&request, request.material().created_at_millis() + 2)
            .unwrap();
        let leases = LeaseService::new(&self.host);
        let lease = leases.load_for_job(job).unwrap().unwrap();
        let receipt = self.host.cleanup_job_owned(&lease).unwrap();
        leases.release_after_cleanup(&lease, &receipt).unwrap();
    }
    fn status(&self, task: TaskId) -> TaskStatus {
        let tasks = self.tasks.lock().unwrap();
        let (meta, turn, done) = &tasks[&task];
        TaskStatus::new(
            if *done {
                TaskState::Open
            } else {
                TaskState::Active
            },
            done.then_some(TaskOutcome::Done),
            Some("mini-a".into()),
            true,
            Some(meta.base_oid().clone()),
            done.then_some("finished".into()),
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1,
                *turn,
                done.then_some(TurnTerminal::Succeeded),
                done.then_some(TaskOutcome::Done),
                done.then_some(true),
                false,
                Some(meta.created_at_millis()),
                done.then_some(meta.created_at_millis() + 2),
            )],
            meta.created_at_millis() + u64::from(*done) * 2,
        )
        .unwrap()
    }
}
impl ProcessRunner for TaskRemote {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        if request.program == OsStr::new("/usr/bin/git") {
            if request.args.iter().any(|a| {
                a.to_string_lossy().starts_with("--receive-pack=")
                    || a.to_string_lossy().starts_with("--upload-pack=")
            }) || (request.args.iter().any(|a| a == "fetch")
                && request
                    .args
                    .iter()
                    .any(|a| a.to_string_lossy().contains("refs/mac-worker/results/")))
            {
                return success();
            }
            if request.args.iter().any(|a| a == "rev-parse")
                && request.args.iter().any(|a| {
                    a.to_string_lossy().contains("refs/remotes/mac-worker/")
                        || a.to_string_lossy().contains("refs/mac-worker/results/")
                })
            {
                let tasks = self.tasks.lock().unwrap();
                let (meta, _, _) = tasks.values().next().unwrap();
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: format!("{}\n", meta.base_oid()).into_bytes(),
                    stderr: Vec::new(),
                });
            }
            return SystemProcessRunner.run(request);
        }
        assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
        let operation = request.args.last().unwrap().to_str().unwrap();
        match operation {
            "~/.local/bin/worker host probe" => {
                self.probe_count.fetch_add(1, Ordering::SeqCst);
                let occupancy = LeaseService::new(&self.host).occupancy()?;
                if occupancy.busy_slots == occupancy.configured_slots
                    && let Some(callback) = &self.on_busy_probe { callback(); }
                canonical(&ProbeResponse { features: None, protocol_version: PROTOCOL_VERSION,
                    supervision_version: SUPERVISION_VERSION, hostname: "task-fixture".into(),
                    arch: "arm64".into(), os_version: "26.2".into(), free_disk_bytes: 100 << 30,
                    total_disk_bytes: 250 << 30, memory_pressure: MemoryPressure::Normal,
                    swap_used_bytes: Some(0), available_memory_bytes: Some(12 << 30), cpu_counters: None,
                    slot_state: occupancy.slot_state, active_lease: occupancy.active_lease,
                    capabilities: Vec::new(), agent_facts: Some(AgentFacts { agents: vec![AgentProbe {
                        autoupdate: None, name: "codex".into(), version: Some("0.1.0".into()),
                        auth: AgentAuth::Authenticated, auth_by_profile: Vec::new() }],
                        env_profiles: Vec::new(), git_identity: true, collected_at_millis: u64::MAX / 2,
                        herdr: None, origin_https_helpers: Default::default() }), facts_age_millis: Some(0),
                    configured_slots: occupancy.configured_slots, busy_slots: occupancy.busy_slots,
                    build_id: None, binary_sha256: None })
            }
            op if op == HostOperation::LeaseAcquire.command() => {
                if let Some(callback) = &self.before_acquire { callback(); }
                let acquire: LeaseAcquireRequest = decode(request);
                assert!(matches!(acquire.execution_scope(), ExecutionScope::Task { .. }));
                match LeaseService::new(&self.host).acquire(&acquire, &healthy(), acquire.material().created_at_millis()) {
                    Ok(response) => {
                        if matches!(response, LeaseAcquireResponse::Acquired { .. }) {
                            self.acquires.lock().unwrap().insert(acquire.material().job_id(), acquire);
                        }
                        canonical(&response)
                    }
                    Err(WorkerError::Capacity { code, message, .. }) => {
                        let mut result = canonical(&HostControlError::new(code, message.into_owned())?)?;
                        result.status = ExitStatus::from_raw(23 << 8); Ok(result)
                    }
                    Err(error) => Err(error),
                }
            }
            op if op == HostOperation::TaskPrepare.command() => {
                let prepare: TaskPrepareRequest = decode(request);
                self.tasks.lock().unwrap().insert(prepare.meta().task_id(), (prepare.meta().clone(), prepare.job_id(), false));
                canonical(&TaskPrepareResponse::new(prepare.meta().base_oid().clone(), false))
            }
            op if op == HostOperation::TaskStatus.command() => {
                let query: TaskStatusRequest = decode(request); canonical(&TaskStatusResponse::new(self.status(query.task_id())))
            }
            op if op == HostOperation::TaskTurn.command() => {
                let turn: TaskTurnRequest = decode(request); let material = turn.submit().material();
                let meta = JobMeta::new(material, material.fingerprint())?;
                self.jobs.lock().unwrap().insert(material.job_id(), meta.clone());
                let active = self.status(turn.turn().task_id());
                self.tasks.lock().unwrap().get_mut(&turn.turn().task_id()).unwrap().2 = true;
                canonical(&TaskTurnResponse::new(SubmitResponse::Accepted { meta: Box::new(meta),
                    status: JobStatus::accepted(material.created_at_millis() + 1)? }, active))
            }
            op if op == HostOperation::Status.command() => {
                let query: StatusRequest = decode(request); let meta = self.jobs.lock().unwrap()[&query.job_id()].clone();
                canonical(&StatusResponse::new(meta.clone(), JobStatus::succeeded(meta.created_at_millis() + 2, 0, 0)?)?)
            }
            op if op == HostOperation::LogChunk.command() => {
                let query: LogChunkRequest = decode(request);
                canonical(&LogChunkResponse::new(LogChunk::new(query.stream(), query.offset(), Vec::new())?)?)
            }
            op if op == HostOperation::StatusLogs.command() => Ok(ProcessResult {
                status: ExitStatus::from_raw(2 << 8), stdout: Vec::new(),
                stderr: b"error: unrecognized subcommand 'status-logs'\n\nUsage: worker host <COMMAND>\n".to_vec() }),
            other => panic!("unexpected task operation {other}"),
        }
    }
}
pub(super) fn config(slots: u8) -> Config {
    Config {
        version: 1,
        notifications: Default::default(),
        controller: Default::default(),
        ssh: Default::default(),
        workers: vec![WorkerEntry {
            name: "mini-a".into(),
            ssh: "mini-a".into(),
            slots,
            capabilities: Vec::new(),
            remote_binary: "~/.local/bin/worker".into(),
            herdr: false,
        }],
    }
}
pub(super) fn request(repo: &crate::support::GitRepo, wait: bool) -> TaskSubmitRequest {
    request_from_path(repo.root(), wait)
}
pub(super) fn request_from_path(project: &std::path::Path, wait: bool) -> TaskSubmitRequest {
    TaskSubmitRequest {
        integrate: Default::default(),
        verify_merge: None,
        session_import: None,
        questions: None,
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        prompt: "scheduler task fixture".into(),
        project: project.into(),
        base: "main".into(),
        wip: false,
        source: None,
        publish: Some(vec!["fetch".into()]),
        publish_branch: None,
        cli_includes: Vec::new(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        preference: WorkerPreference::Pinned {
            worker: "mini-a".into(),
        },
        wait_for_capacity: wait,
        attached: false,
        run_id: None,
    }
}
pub(super) fn enqueue(
    remote: &dyn ProcessRunner,
    config: &Config,
    paths: &PathLayout,
    store: &ClientStateStore,
    repo: &crate::support::GitRepo,
    wait: bool,
) -> Result<(TaskId, TurnId), WorkerError> {
    let report = TaskClient::new(remote, config, paths, store, &InlineRunnerExecutor).submit(
        request(repo, wait),
        &mut Vec::new(),
        &mut Vec::new(),
    )?;
    let row = store.queue_entry_for_task_turn(report.task_id())?.unwrap();
    Ok((report.task_id(), row.job_id()))
}
pub(super) fn run(
    remote: &dyn ProcessRunner,
    config: &Config,
    paths: &PathLayout,
    store: &ClientStateStore,
    task: TaskId,
    turn: TurnId,
) -> Result<TurnOutcomeReport, WorkerError> {
    TurnRunner::new(remote, config, paths, store, &InlineRunnerExecutor).run(task, turn, None)
}
