use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use serde::Deserialize;

use crate::{
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    dashboard::{
        model::{ApiError, DashboardError, DashboardLogChunk},
        service::DashboardTaskCollection,
        source::DashboardRemoteReader,
    },
    error::WorkerError,
    job::LogStream,
    paths::PathLayout,
    process::{ProcessRunner, SystemProcessRunner},
    task::{BaseOid, LocalTaskRecord, TaskId, TaskState, TurnId},
    task_client::TaskClient,
    task_store::TaskStatusRequest,
    task_view::{
        TaskDetailProjection, TaskFreshness, TaskViewError, project_task_detail,
        project_task_list_with_blocking_codes,
    },
    turn_runner::{DetachedRunnerExecutor, RunnerExecutor},
};

pub const MAX_TASK_LOG_LIMIT: u32 = 65_536;

pub trait DashboardTaskSource: Send + Sync + 'static {
    fn task_detail(&self, task_id: TaskId) -> Result<TaskDetailProjection, ApiError>;
    fn read_task_log(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<DashboardLogChunk, ApiError>;
}

pub struct MacWorkerTaskSource {
    pub config: Arc<Config>,
    pub local_tasks: Arc<ClientStateStore>,
    pub remote: Arc<dyn DashboardRemoteReader>,
}

impl MacWorkerTaskSource {
    pub fn new(
        config: Arc<Config>,
        local_tasks: Arc<ClientStateStore>,
        remote: Arc<dyn DashboardRemoteReader>,
    ) -> Self {
        Self {
            config,
            local_tasks,
            remote,
        }
    }

    fn owned_record(&self, task_id: TaskId) -> Result<LocalTaskRecord, ApiError> {
        self.local_tasks
            .load_task_optional(task_id)
            .map_err(map_local_api_error)?
            .ok_or_else(|| ApiError::new("TASK_NOT_FOUND", "task is not present in local state"))
    }

    fn worker_for(&self, record: &LocalTaskRecord) -> Result<&WorkerEntry, ApiError> {
        let worker_name = record.status().worker().ok_or_else(|| {
            ApiError::new(
                "TASK_SOURCE_FAILED",
                "task has no recorded worker for the requested operation",
            )
        })?;
        self.config.worker(worker_name).ok_or_else(|| {
            ApiError::new(
                "TASK_SOURCE_FAILED",
                "task references an unknown configured worker",
            )
        })
    }

    fn effective_task_view(
        &self,
        record: &LocalTaskRecord,
    ) -> Result<(LocalTaskRecord, TaskFreshness), ApiError> {
        if record.close_intent().is_some() || record.abandon_code() == Some("LOG_DRAIN_UNAVAILABLE")
        {
            return Ok((record.clone(), TaskFreshness::Current));
        }
        if !record.needs_remote_observation() {
            return Ok((record.clone(), TaskFreshness::Current));
        }
        let Some(worker_name) = record.status().worker() else {
            return Ok((record.clone(), TaskFreshness::Current));
        };
        let Some(worker) = self.config.worker(worker_name) else {
            return Ok((record.clone(), TaskFreshness::Stale));
        };
        let request = TaskStatusRequest::new(record.meta().project_id(), record.meta().task_id());
        match self
            .remote
            .task_status_with_deadline(worker, &request, Duration::from_secs(30))
        {
            Ok(response) => {
                let observed = record
                    .with_remote_observation(response.status(), response.deliveries())
                    .map_err(map_local_api_error)?;
                Ok((observed, TaskFreshness::Current))
            }
            Err(_) if record.needs_remote_status_refresh() => {
                Ok((record.clone(), TaskFreshness::Stale))
            }
            Err(_) => Ok((record.clone(), TaskFreshness::Current)),
        }
    }
}

impl DashboardTaskSource for MacWorkerTaskSource {
    fn task_detail(&self, task_id: TaskId) -> Result<TaskDetailProjection, ApiError> {
        let record = self.owned_record(task_id)?;
        let runner = self
            .local_tasks
            .runner_liveness(task_id)
            .map_err(map_local_api_error)?;
        let (view, freshness) = self.effective_task_view(&record)?;
        project_task_detail(&view, view.status(), runner, freshness)
            .map_err(map_task_view_api_error)
    }

    fn read_task_log(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        if !(1..=MAX_TASK_LOG_LIMIT).contains(&limit) {
            return Err(ApiError::new(
                "INVALID_LOG_LIMIT",
                "log limit must be between 1 and 65536 bytes",
            ));
        }
        let record = self.owned_record(task_id)?;
        if record.retains_log_drain_unavailable() {
            return Err(ApiError::new(
                "LOG_DRAIN_UNAVAILABLE",
                "worker job or logs are gone; remaining stdout/stderr cannot be drained",
            ));
        }
        let turn_ids = self
            .local_tasks
            .turn_ids_for_task(task_id)
            .map_err(map_local_api_error)?;
        if !turn_ids.contains(&turn_id) {
            return Err(ApiError::new(
                "TURN_NOT_FOUND",
                "turn is not present in the requested task",
            ));
        }
        let worker = self.worker_for(&record)?;
        let chunk = self
            .remote
            .log_chunk(worker, turn_id, stream, offset, limit)
            .map_err(map_remote_api_error)?;
        DashboardLogChunk::from_log_chunk(&chunk).map_err(dashboard_api_error)
    }
}

pub(crate) fn collect_task_projection(
    config: &Config,
    state: &ClientStateStore,
    remote: &dyn DashboardRemoteReader,
    deadline: Duration,
) -> Result<DashboardTaskCollection, DashboardError> {
    let records = state.list_tasks().map_err(map_local_error)?;
    let runs = state.list_runs().map_err(map_local_error)?;
    let blocking_codes = state.task_blocking_codes(config).map_err(map_local_error)?;
    let started = Instant::now();
    let mut effective_records = Vec::with_capacity(records.len());
    let mut runner_states = HashMap::with_capacity(records.len());
    let mut freshness = HashMap::with_capacity(records.len());
    let mut errors = Vec::new();

    for record in records {
        let task_id = record.meta().task_id();
        let runner = state.runner_liveness(task_id).map_err(map_local_error)?;
        runner_states.insert(task_id, runner);

        let mut status = record.status().clone();
        let mut task_freshness = TaskFreshness::Current;
        let mut view = record.clone();
        if record.close_intent().is_none()
            && record.abandon_code() != Some("LOG_DRAIN_UNAVAILABLE")
            && record.needs_remote_observation()
            && status.worker().is_some()
        {
            let remaining = deadline.saturating_sub(started.elapsed());
            let remote_status = config
                .worker(status.worker().expect("worker presence checked"))
                .and_then(|worker| {
                    if remaining.is_zero() {
                        None
                    } else {
                        let request = TaskStatusRequest::new(record.meta().project_id(), task_id);
                        remote
                            .task_status_with_deadline(worker, &request, remaining)
                            .ok()
                    }
                });
            if let Some(response) = remote_status {
                view = record
                    .with_remote_observation(response.status(), response.deliveries())
                    .map_err(map_local_error)?;
                status = view.status().clone();
            } else if record.needs_remote_status_refresh() {
                task_freshness = TaskFreshness::Stale;
                errors.push(DashboardError::new(
                    "TASK_STATUS_STALE",
                    "remote task status is unavailable; local task state is shown",
                ));
            }
        }

        let effective = view.with_status(status).map_err(map_local_error)?;
        effective_records.push(effective);
        freshness.insert(task_id, task_freshness);
    }

    let projection = project_task_list_with_blocking_codes(
        &effective_records,
        &runs,
        &runner_states,
        &freshness,
        &blocking_codes,
    )
    .map_err(map_task_view_error)?;
    Ok(DashboardTaskCollection { projection, errors })
}

fn map_task_view_error(error: TaskViewError) -> DashboardError {
    DashboardError::new(error.code(), error.message())
}

fn map_local_error(error: WorkerError) -> DashboardError {
    DashboardError::new(
        error.public_code(),
        "local dashboard task state is unavailable",
    )
}

fn map_local_api_error(error: WorkerError) -> ApiError {
    dashboard_api_error(map_local_error(error))
}

fn map_remote_api_error(error: WorkerError) -> ApiError {
    let code = error.public_code();
    let message = match code.as_str() {
        "TASK_NOT_FOUND" => "remote task was not found",
        "SSH_UNAVAILABLE" | "UNAVAILABLE" => "remote worker is unavailable",
        "INVALID_RESPONSE" | "PROTOCOL" => "remote worker response is invalid",
        _ => "remote task operation is unavailable",
    };
    ApiError::new(code, message)
}

fn dashboard_api_error(error: DashboardError) -> ApiError {
    ApiError::new(error.code, error.message)
}

fn map_task_view_api_error(error: TaskViewError) -> ApiError {
    dashboard_api_error(map_task_view_error(error))
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskMutationRequest {
    #[serde(default)]
    pub message: Option<String>,
    pub expected_task_id: TaskId,
    pub expected_turn_id: Option<TurnId>,
    pub expected_turn_count: u32,
    pub expected_head_oid: Option<BaseOid>,
    pub expected_updated_at_millis: u64,
    pub expected_state: TaskState,
}

pub trait DashboardTaskMutationSource: Send + Sync + 'static {
    fn reply(
        &self,
        task_id: TaskId,
        request: &TaskMutationRequest,
    ) -> Result<TaskDetailProjection, ApiError>;
    fn accept(
        &self,
        task_id: TaskId,
        request: &TaskMutationRequest,
    ) -> Result<TaskDetailProjection, ApiError>;
}

pub struct MacWorkerTaskMutationSource {
    config: Arc<Config>,
    local_tasks: Arc<ClientStateStore>,
    paths: PathLayout,
    runner: Arc<dyn ProcessRunner>,
    executor: Arc<dyn RunnerExecutor>,
    after_expected_check: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl MacWorkerTaskMutationSource {
    pub fn new(config: Arc<Config>, local_tasks: Arc<ClientStateStore>, paths: PathLayout) -> Self {
        Self {
            config,
            local_tasks,
            paths,
            runner: Arc::new(SystemProcessRunner),
            executor: Arc::new(DetachedRunnerExecutor),
            after_expected_check: None,
        }
    }

    pub fn with_process_runner(mut self, runner: Arc<dyn ProcessRunner>) -> Self {
        self.runner = runner;
        self
    }

    pub fn with_executor(mut self, executor: Arc<dyn RunnerExecutor>) -> Self {
        self.executor = executor;
        self
    }

    pub fn with_after_expected_check(mut self, hook: Arc<dyn Fn() + Send + Sync>) -> Self {
        self.after_expected_check = Some(hook);
        self
    }

    fn client(&self) -> TaskClient<'_> {
        TaskClient::new(
            self.runner.as_ref(),
            self.config.as_ref(),
            &self.paths,
            self.local_tasks.as_ref(),
            self.executor.as_ref(),
        )
    }

    fn load_matching(
        &self,
        task_id: TaskId,
        request: &TaskMutationRequest,
    ) -> Result<LocalTaskRecord, ApiError> {
        if request.expected_task_id != task_id {
            return Err(ApiError::new(
                "TASK_REVISION_CONFLICT",
                "task changed before this action",
            ));
        }
        let record = self
            .local_tasks
            .load_task_optional(task_id)
            .map_err(map_local_api_error)?
            .ok_or_else(|| ApiError::new("TASK_NOT_FOUND", "task is not present in local state"))?;
        if !expected_status_matches(&record, request) {
            return Err(ApiError::new(
                "TASK_REVISION_CONFLICT",
                "task changed before this action",
            ));
        }
        Ok(record)
    }

    fn after_expected_check(&self) {
        if let Some(hook) = &self.after_expected_check {
            hook();
        }
    }

    fn project(&self, task_id: TaskId) -> Result<TaskDetailProjection, ApiError> {
        let record = self
            .local_tasks
            .load_task_optional(task_id)
            .map_err(map_local_api_error)?
            .ok_or_else(|| ApiError::new("TASK_NOT_FOUND", "task is not present in local state"))?;
        let runner = self
            .local_tasks
            .runner_liveness(task_id)
            .map_err(map_local_api_error)?;
        project_task_detail(&record, record.status(), runner, TaskFreshness::Current)
            .map_err(map_task_view_api_error)
    }
}

fn expected_status_matches(record: &LocalTaskRecord, expected: &TaskMutationRequest) -> bool {
    let status = record.status();
    let last_turn = status.turns().last().map(|turn| turn.turn_id());
    let turn_count = u32::try_from(status.turns().len()).ok();
    last_turn == expected.expected_turn_id
        && turn_count == Some(expected.expected_turn_count)
        && status.head_oid() == expected.expected_head_oid.as_ref()
        && status.updated_at_millis() == expected.expected_updated_at_millis
        && status.state() == expected.expected_state
}

impl DashboardTaskMutationSource for MacWorkerTaskMutationSource {
    fn reply(
        &self,
        task_id: TaskId,
        request: &TaskMutationRequest,
    ) -> Result<TaskDetailProjection, ApiError> {
        let expected = self.load_matching(task_id, request)?;
        let message = request
            .message
            .as_deref()
            .map(str::trim)
            .filter(|message| !message.is_empty())
            .ok_or_else(|| ApiError::new("TASK_REQUEST_INVALID", "reply requires a message"))?
            .to_owned();
        self.after_expected_check();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        self.client()
            .say_from_expected(&expected, message, false, &mut stdout, &mut stderr)
            .map_err(map_mutation_error)?;
        self.project(task_id)
    }

    fn accept(
        &self,
        task_id: TaskId,
        request: &TaskMutationRequest,
    ) -> Result<TaskDetailProjection, ApiError> {
        let expected = self.load_matching(task_id, request)?;
        self.after_expected_check();
        self.client()
            .close_from_expected(&expected, false)
            .map_err(map_mutation_error)?;
        self.project(task_id)
    }
}

fn map_mutation_error(error: WorkerError) -> ApiError {
    let code = error.public_code();
    let message = error.public_message();
    ApiError::new(code, message)
}
