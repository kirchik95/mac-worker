use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

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
    task::{LocalTaskRecord, TaskId, TaskState, TaskStatus, TurnId},
    task_store::TaskStatusRequest,
    task_view::{
        TaskDetailProjection, TaskFreshness, TaskViewError, project_task_detail,
        project_task_list_with_blocking_codes,
    },
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

    fn effective_detail_status(
        &self,
        record: &LocalTaskRecord,
    ) -> Result<(TaskStatus, TaskFreshness), ApiError> {
        if !matches!(record.status().state(), TaskState::Active | TaskState::Open) {
            return Ok((record.status().clone(), TaskFreshness::Current));
        }
        if record.retains_log_drain_unavailable() {
            return Ok((record.status().clone(), TaskFreshness::Current));
        }
        let Some(worker_name) = record.status().worker() else {
            return Ok((record.status().clone(), TaskFreshness::Current));
        };
        let Some(worker) = self.config.worker(worker_name) else {
            return Ok((record.status().clone(), TaskFreshness::Stale));
        };
        let request = TaskStatusRequest::new(record.meta().project_id(), record.meta().task_id());
        match self
            .remote
            .task_status_with_deadline(worker, &request, Duration::from_secs(30))
        {
            Ok(response) => Ok((response.status().clone(), TaskFreshness::Current)),
            Err(_) => Ok((record.status().clone(), TaskFreshness::Stale)),
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
        let (status, freshness) = self.effective_detail_status(&record)?;
        project_task_detail(&record, &status, runner, freshness).map_err(map_task_view_api_error)
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
        if record.retains_log_drain_unavailable() {
            let effective = record.with_status(status).map_err(map_local_error)?;
            effective_records.push(effective);
            freshness.insert(task_id, task_freshness);
            continue;
        }
        if matches!(status.state(), TaskState::Active | TaskState::Open)
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
                status = response.status().clone();
            } else {
                task_freshness = TaskFreshness::Stale;
                errors.push(DashboardError::new(
                    "TASK_STATUS_STALE",
                    "remote task status is unavailable; local task state is shown",
                ));
            }
        }

        let effective = record.with_status(status).map_err(map_local_error)?;
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
