use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

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
        project_task_list_with_blocking_codes, remote_observation_allowed,
    },
    turn_runner::{DetachedRunnerExecutor, RunnerExecutor},
};

pub const MAX_TASK_LOG_LIMIT: u32 = 65_536;

/// Owner/controller adapter. Reads return durable snapshots; redrive publishes intent.
pub trait DashboardIntegrationSource: Send + Sync + 'static {
    fn requested_close(
        &self,
        _task: TaskId,
    ) -> Result<Option<crate::task::ClosePolicy>, WorkerError> {
        Ok(None)
    }
    /// Read durable preparation identity; never infer auxiliary work from time.
    fn is_auxiliary_turn(&self, _task: TaskId, _turn: TurnId) -> Result<bool, WorkerError> {
        Ok(false)
    }
    fn revoke(
        &self,
        _task_id: TaskId,
        _expected: crate::integration::contracts::IntegrationRevision,
    ) -> Result<crate::integration::contracts::IntegrationSnapshot, WorkerError> {
        Err(crate::integration::contracts::integration_unavailable())
    }
    fn snapshot(
        &self,
        task_id: TaskId,
    ) -> Result<Option<crate::integration::contracts::IntegrationSnapshot>, WorkerError>;
    /// Project a read page; concrete rooted sources reuse one companion handle.
    fn views(
        &self,
        records: &[(&LocalTaskRecord, bool)],
    ) -> Result<HashMap<TaskId, crate::integration::contracts::IntegrationView>, WorkerError> {
        let mut views = HashMap::new();
        for (record, runner) in records {
            let task = record.meta().task_id();
            let snapshot = self.snapshot(task)?;
            let current = snapshot
                .as_ref()
                .map(|snapshot| {
                    crate::integration::view::snapshot_covers_latest_work(
                        snapshot,
                        record,
                        |turn| self.is_auxiliary_turn(task, turn),
                    )
                })
                .transpose()?
                .unwrap_or(false);
            let facts =
                crate::integration::contracts::IntegrationTaskFacts::from_record(record, *runner);
            let mut view = crate::integration::view::project_integration_for_current_work(
                snapshot.as_ref(),
                &facts,
                current,
            )?;
            view.integration = snapshot;
            if let Some(close) = self.requested_close(task)? {
                view.requested_close = close;
            }
            views.insert(task, view);
        }
        Ok(views)
    }
    fn dashboard_redrive(
        &self,
        _request: &crate::integration::contracts::IntegrationRedriveRequest,
        _body: &serde_json::Value,
        validate: &mut dyn FnMut() -> Result<bool, WorkerError>,
        execute: &mut dyn FnMut(
            &crate::integration::runner::ReplayRedrive<'_>,
        ) -> Result<serde_json::Value, WorkerError>,
    ) -> Result<serde_json::Value, WorkerError> {
        validate()?;
        execute(&|request| self.redrive(request))
    }
    fn redrive(
        &self,
        request: &crate::integration::contracts::IntegrationRedriveRequest,
    ) -> Result<crate::integration::contracts::IntegrationSnapshot, WorkerError>;
}

/// Adapter over T1's owner/controller boundary. Redrive publishes the durable epoch;
/// phase execution belongs to the owner runner, never the HTTP handler.
#[cfg(any(test, feature = "test-support"))]
pub struct OwnerDashboardIntegrations {
    pub state: Arc<dyn crate::integration::contracts::IntegrationState>,
    pub host: Arc<dyn crate::integration::contracts::IntegrationHost>,
    pub turns: Arc<dyn crate::integration::contracts::IntegrationTurns>,
    pub runtime: Arc<dyn crate::integration::contracts::IntegrationRuntime>,
    pub observer: Arc<dyn crate::integration::contracts::IntegrationObserver>,
}
#[cfg(any(test, feature = "test-support"))]
impl DashboardIntegrationSource for OwnerDashboardIntegrations {
    fn requested_close(
        &self,
        task: TaskId,
    ) -> Result<Option<crate::task::ClosePolicy>, WorkerError> {
        self.state
            .load_policy(task)
            .map(|policy| policy.map(|policy| policy.requested_close))
    }
    fn is_auxiliary_turn(&self, task: TaskId, turn: TurnId) -> Result<bool, WorkerError> {
        self.state
            .load_prepared(task, turn)
            .map(|prepared| prepared.is_some())
    }
    fn revoke(
        &self,
        task_id: TaskId,
        expected: crate::integration::contracts::IntegrationRevision,
    ) -> Result<crate::integration::contracts::IntegrationSnapshot, WorkerError> {
        let coordinator = crate::integration::coordinator::IntegrationCoordinator::new(
            self.state.as_ref(),
            self.host.as_ref(),
            self.turns.as_ref(),
            self.runtime.as_ref(),
            self.observer.as_ref(),
        );
        coordinator.revoke(task_id, expected)
    }
    fn snapshot(
        &self,
        task_id: TaskId,
    ) -> Result<Option<crate::integration::contracts::IntegrationSnapshot>, WorkerError> {
        let read = crate::controller::integration::serve_integration_read(
            self.state.as_ref(),
            &[task_id],
        )?;
        read.integrations
            .get(&task_id)
            .cloned()
            .ok_or_else(crate::integration::contracts::integration_unavailable)
    }
    fn redrive(
        &self,
        request: &crate::integration::contracts::IntegrationRedriveRequest,
    ) -> Result<crate::integration::contracts::IntegrationSnapshot, WorkerError> {
        let request = crate::controller::integration::prepare_integration_redrive(request)?;
        let coordinator = crate::integration::coordinator::IntegrationCoordinator::new(
            self.state.as_ref(),
            self.host.as_ref(),
            self.turns.as_ref(),
            self.runtime.as_ref(),
            self.observer.as_ref(),
        );
        crate::controller::integration::execute_integration_redrive(&coordinator, &request)
    }
}

/// The normal HTTP composition reads sidecars without opening a writer and
/// publishes mutations through the same native owner as CLI/RPC recovery.
pub(crate) struct NativeDashboardIntegrations {
    pub config: Arc<Config>,
    pub paths: PathLayout,
    pub client: Arc<ClientStateStore>,
    pub runner: Arc<dyn ProcessRunner>,
}
impl NativeDashboardIntegrations {
    fn owner(&self) -> Result<crate::integration::runner::OwnerIntegration<'_>, WorkerError> {
        crate::integration::runner::OwnerIntegration::new(
            self.runner.as_ref(),
            &self.config,
            &self.paths,
            &self.client,
            &DetachedRunnerExecutor,
        )
    }
}
impl DashboardIntegrationSource for NativeDashboardIntegrations {
    fn requested_close(
        &self,
        task: TaskId,
    ) -> Result<Option<crate::task::ClosePolicy>, WorkerError> {
        crate::integration::store::RootedIntegrationState::read_task(&self.paths, task)
            .map(|(policy, _)| policy.map(|policy| policy.requested_close))
    }
    fn is_auxiliary_turn(&self, task: TaskId, turn: TurnId) -> Result<bool, WorkerError> {
        crate::integration::store::RootedIntegrationState::read_auxiliary(&self.paths, task, turn)
            .map(|prepared| prepared.is_some())
    }
    fn snapshot(
        &self,
        task: TaskId,
    ) -> Result<Option<crate::integration::contracts::IntegrationSnapshot>, WorkerError> {
        crate::integration::store::RootedIntegrationState::read_task(&self.paths, task)
            .map(|(_, record)| record.map(|record| record.snapshot))
    }
    fn views(
        &self,
        records: &[(&LocalTaskRecord, bool)],
    ) -> Result<HashMap<TaskId, crate::integration::contracts::IntegrationView>, WorkerError> {
        let reader =
            crate::integration::store::ExistingIntegrationReader::open_at(&self.paths.state)?;
        let mut views = HashMap::new();
        if !reader.present() {
            return Ok(views);
        }
        for (ordinary, runner) in records {
            let task = ordinary.meta().task_id();
            let (policy, record) = reader.read_task(task)?;
            if policy.is_some() || record.is_some() {
                views.insert(
                    task,
                    crate::integration::view::project_owner_view(
                        &reader,
                        ordinary,
                        *runner,
                        policy.as_ref(),
                        record.as_ref(),
                    )?,
                );
            }
        }
        Ok(views)
    }
    fn dashboard_redrive(
        &self,
        request: &crate::integration::contracts::IntegrationRedriveRequest,
        body: &serde_json::Value,
        validate: &mut dyn FnMut() -> Result<bool, WorkerError>,
        execute: &mut dyn FnMut(
            &crate::integration::runner::ReplayRedrive<'_>,
        ) -> Result<serde_json::Value, WorkerError>,
    ) -> Result<serde_json::Value, WorkerError> {
        if crate::integration::store::RootedIntegrationState::read_task(
            &self.paths,
            request.task_id,
        )?
        .0
        .is_none()
        {
            validate()?;
            return Err(crate::integration::contracts::integration_unavailable());
        }
        self.owner()?
            .dashboard_redrive(request, body, validate, execute)
    }
    fn redrive(
        &self,
        request: &crate::integration::contracts::IntegrationRedriveRequest,
    ) -> Result<crate::integration::contracts::IntegrationSnapshot, WorkerError> {
        self.owner()?.redrive(request)
    }
    fn revoke(
        &self,
        task: TaskId,
        expected: crate::integration::contracts::IntegrationRevision,
    ) -> Result<crate::integration::contracts::IntegrationSnapshot, WorkerError> {
        self.owner()?.coordinator().revoke(task, expected)
    }
}

fn attach_detail_integration(
    detail: TaskDetailProjection,
    record: &LocalTaskRecord,
    runner: bool,
    source: Option<&dyn DashboardIntegrationSource>,
) -> Result<TaskDetailProjection, ApiError> {
    let Some(source) = source else {
        return Ok(detail);
    };
    let views = source
        .views(&[(record, runner)])
        .map_err(map_mutation_error)?;
    let Some(view) = views.get(&record.meta().task_id()) else {
        return Ok(detail);
    };
    let facts = crate::integration::contracts::IntegrationTaskFacts::from_record(record, runner);
    let mut detail = detail
        .with_current_integration(
            view.integration.as_ref(),
            &facts,
            view.workflow_state.is_some(),
        )
        .map_err(map_mutation_error)?;
    detail.close_policy = view.requested_close;
    detail.task.close_policy = view.requested_close;
    Ok(detail)
}

fn attach_list_integrations(
    projection: &mut crate::task_view::TaskListProjection,
    records: &[LocalTaskRecord],
    source: Option<&dyn DashboardIntegrationSource>,
) -> Result<(), DashboardError> {
    let Some(source) = source else {
        return Ok(());
    };
    let records = records
        .iter()
        .map(|record| (record.meta().task_id(), record))
        .collect::<HashMap<_, _>>();
    let page = projection
        .tasks
        .iter()
        .filter_map(|row| {
            records
                .get(&row.task_id)
                .map(|record| (*record, row.runner.is_some()))
        })
        .collect::<Vec<_>>();
    let views = source.views(&page).map_err(map_local_error)?;
    for row in &mut projection.tasks {
        let (Some(record), Some(view)) = (records.get(&row.task_id), views.get(&row.task_id))
        else {
            continue;
        };
        let facts = crate::integration::contracts::IntegrationTaskFacts::from_record(
            record,
            row.runner.is_some(),
        );
        *row = row
            .clone()
            .with_current_integration(
                view.integration.as_ref(),
                &facts,
                view.workflow_state.is_some(),
            )
            .map_err(map_local_error)?;
        row.close_policy = view.requested_close;
    }
    Ok(())
}

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
    integrations: Option<Arc<dyn DashboardIntegrationSource>>,
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
            integrations: None,
        }
    }

    pub fn with_integrations(mut self, source: Arc<dyn DashboardIntegrationSource>) -> Self {
        self.integrations = Some(source);
        self
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
        if !remote_observation_allowed(record) {
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
            Err(_) => Ok((record.clone(), TaskFreshness::Stale)),
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
        let detail = project_task_detail(&view, view.status(), runner, freshness)
            .map_err(map_task_view_api_error)?;
        attach_detail_integration(
            detail,
            &view,
            runner.is_some(),
            self.integrations.as_deref(),
        )
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

/// Project saved records only. The event fast path deliberately has no remote
/// reader argument, so it cannot inherit the full collector's SSH overlay.
pub(crate) fn project_local_tasks(
    config: &Config,
    state: &ClientStateStore,
    integrations: Option<&dyn DashboardIntegrationSource>,
) -> Result<DashboardTaskCollection, DashboardError> {
    let records = state.list_tasks().map_err(map_local_error)?;
    let runs = state.list_runs().map_err(map_local_error)?;
    let blocking_codes = state.task_blocking_codes(config).map_err(map_local_error)?;
    let runner_states = records
        .iter()
        .map(|record| {
            let task_id = record.meta().task_id();
            state
                .runner_liveness(task_id)
                .map(|runner| (task_id, runner))
        })
        .collect::<Result<HashMap<_, _>, _>>()
        .map_err(map_local_error)?;
    let mut projection = project_task_list_with_blocking_codes(
        &records,
        &runs,
        &runner_states,
        &HashMap::new(),
        &blocking_codes,
    )
    .map_err(map_task_view_error)?;
    attach_list_integrations(&mut projection, &records, integrations)?;
    Ok(DashboardTaskCollection {
        projection,
        errors: Vec::new(),
    })
}

pub(crate) fn collect_task_projection(
    config: &Config,
    state: &ClientStateStore,
    remote: &dyn DashboardRemoteReader,
    deadline: Duration,
    integrations: Option<&dyn DashboardIntegrationSource>,
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
        if remote_observation_allowed(&record) && status.worker().is_some() {
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
            } else {
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

    let mut projection = project_task_list_with_blocking_codes(
        &effective_records,
        &runs,
        &runner_states,
        &freshness,
        &blocking_codes,
    )
    .map_err(map_task_view_error)?;
    attach_list_integrations(&mut projection, &effective_records, integrations)?;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskMutationRequest {
    #[serde(default)]
    pub expected_integration_id: Option<crate::integration::contracts::IntegrationId>,
    #[serde(default)]
    pub expected_integration_revision: Option<crate::integration::contracts::IntegrationRevision>,
    #[serde(default)]
    pub message: Option<String>,
    pub expected_task_id: TaskId,
    pub expected_turn_id: Option<TurnId>,
    pub expected_turn_count: u32,
    pub expected_head_oid: Option<BaseOid>,
    pub expected_updated_at_millis: u64,
    pub expected_state: TaskState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskIntegrationRequest {
    pub expected: TaskMutationRequest,
    pub expected_integration_id: crate::integration::contracts::IntegrationId,
    pub integration: crate::integration::contracts::IntegrationRedriveRequest,
}

pub trait DashboardTaskMutationSource: Send + Sync + 'static {
    fn integrate_response(
        &self,
        task_id: TaskId,
        request: &TaskIntegrationRequest,
    ) -> Result<serde_json::Value, ApiError> {
        serde_json::to_value(self.integrate(task_id, request)?)
            .map_err(|_| ApiError::new("INTEGRATION_STATE_INVALID", "invalid integration response"))
    }
    fn integrate(
        &self,
        _task_id: TaskId,
        _request: &TaskIntegrationRequest,
    ) -> Result<TaskDetailProjection, ApiError> {
        Err(ApiError::new(
            "INTEGRATION_UNAVAILABLE",
            "integration support is unavailable",
        ))
    }
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
    integrations: Option<Arc<dyn DashboardIntegrationSource>>,
    #[cfg(any(test, feature = "test-support"))]
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
            integrations: None,
            #[cfg(any(test, feature = "test-support"))]
            after_expected_check: None,
        }
    }

    pub fn with_integrations(mut self, source: Arc<dyn DashboardIntegrationSource>) -> Self {
        self.integrations = Some(source);
        self
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_process_runner(mut self, runner: Arc<dyn ProcessRunner>) -> Self {
        self.runner = runner;
        self
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_executor(mut self, executor: Arc<dyn RunnerExecutor>) -> Self {
        self.executor = executor;
        self
    }

    #[cfg(any(test, feature = "test-support"))]
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
        if request.expected_integration_id.is_some()
            != request.expected_integration_revision.is_some()
        {
            return Err(ApiError::new(
                "TASK_REQUEST_INVALID",
                "integration identity and revision must be paired",
            ));
        }
        match (
            &self.integrations,
            request.expected_integration_id,
            request.expected_integration_revision,
        ) {
            (Some(source), id, revision) => {
                let snapshot = source.snapshot(task_id).map_err(map_mutation_error)?;
                if let Some(snapshot) = &snapshot {
                    crate::integration::contracts::ValidateIntegration::validate(snapshot)
                        .map_err(map_mutation_error)?;
                }
                let matches = match (snapshot, id, revision) {
                    (Some(snapshot), Some(id), Some(revision)) => {
                        snapshot.integration_id == id && snapshot.revision == revision
                    }
                    (None, None, None) => true,
                    _ => false,
                };
                if !matches {
                    return Err(ApiError::new(
                        "TASK_REVISION_CONFLICT",
                        "integration changed before this action",
                    ));
                }
            }
            (None, Some(_), Some(_)) => {
                return Err(ApiError::new(
                    "INTEGRATION_UNAVAILABLE",
                    "compatible integration owner unavailable",
                ));
            }
            _ => {}
        }
        Ok(record)
    }

    #[cfg(any(test, feature = "test-support"))]
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
        let detail = project_task_detail(&record, record.status(), runner, TaskFreshness::Current)
            .map_err(map_task_view_api_error)?;
        attach_detail_integration(
            detail,
            &record,
            runner.is_some(),
            self.integrations.as_deref(),
        )
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

impl MacWorkerTaskMutationSource {
    fn validate_integration_request(
        &self,
        task_id: TaskId,
        request: &TaskIntegrationRequest,
    ) -> Result<crate::integration::contracts::IntegrationSnapshot, ApiError> {
        use crate::integration::contracts::{IntegrationStatus, ValidateIntegration};
        let mut ordinary_expected = request.expected.clone();
        if ordinary_expected
            .expected_integration_id
            .is_some_and(|id| id != request.expected_integration_id)
            || ordinary_expected
                .expected_integration_revision
                .is_some_and(|revision| revision != request.integration.expected)
        {
            return Err(ApiError::new(
                "TASK_REVISION_CONFLICT",
                "integration changed before this action",
            ));
        }
        ordinary_expected.expected_integration_id = Some(request.expected_integration_id);
        ordinary_expected.expected_integration_revision = Some(request.integration.expected);
        let expected = self.load_matching(task_id, &ordinary_expected)?;
        request.integration.validate().map_err(|_| {
            ApiError::new(
                "TASK_REQUEST_INVALID",
                "invalid integration re-drive identity",
            )
        })?;
        if request.integration.task_id != task_id {
            return Err(ApiError::new(
                "TASK_REVISION_CONFLICT",
                "task changed before this action",
            ));
        }
        let source = self.integrations.as_ref().ok_or_else(|| {
            ApiError::new(
                "INTEGRATION_UNAVAILABLE",
                "compatible integration owner unavailable",
            )
        })?;
        let snapshot = source
            .snapshot(task_id)
            .map_err(map_mutation_error)?
            .ok_or_else(|| {
                ApiError::new(
                    "TASK_REVISION_CONFLICT",
                    "integration changed before this action",
                )
            })?;
        snapshot.validate().map_err(map_mutation_error)?;
        if snapshot.integration_id != request.expected_integration_id
            || snapshot.revision != request.integration.expected
        {
            return Err(ApiError::new(
                "TASK_REVISION_CONFLICT",
                "integration changed before this action",
            ));
        }
        if expected.status().state() != TaskState::Open {
            return Err(ApiError::new(
                "TASK_CLOSED",
                "terminal task cannot be re-driven",
            ));
        }
        if !matches!(
            snapshot.state,
            IntegrationStatus::Blocked | IntegrationStatus::Integrated
        ) {
            return Err(ApiError::new(
                "TASK_BUSY",
                "integration is already in progress",
            ));
        }
        Ok(snapshot)
    }

    fn drive_integration_request(
        &self,
        task_id: TaskId,
        request: &TaskIntegrationRequest,
        previous: Option<&crate::integration::contracts::IntegrationSnapshot>,
        redrive: Option<&crate::integration::runner::ReplayRedrive<'_>>,
    ) -> Result<TaskDetailProjection, ApiError> {
        use crate::integration::contracts::{IntegrationStatus, ValidateIntegration};
        let source = self.integrations.as_ref().ok_or_else(|| {
            ApiError::new(
                "INTEGRATION_UNAVAILABLE",
                "compatible integration owner unavailable",
            )
        })?;
        if previous.is_none_or(|snapshot| snapshot.state != IntegrationStatus::Integrated) {
            let next = match redrive {
                Some(redrive) => redrive(&request.integration),
                None => source.redrive(&request.integration),
            }
            .map_err(map_mutation_error)?;
            next.validate().map_err(map_mutation_error)?;
            // A pending native replay can recover an already-Integrated no-op
            // whose validated receipt/result was saved at the same revision.
            let recovered_noop = previous.is_none()
                && next.state == IntegrationStatus::Integrated
                && next.revision == request.integration.expected;
            if next.integration_id != request.expected_integration_id
                || (!recovered_noop && next.revision <= request.integration.expected)
                || previous.is_some_and(|snapshot| next.epoch <= snapshot.epoch)
            {
                return Err(ApiError::new(
                    "INTEGRATION_STATE_INVALID",
                    "re-drive returned a different integration binding",
                ));
            }
        }
        self.project(task_id)
    }
}

impl DashboardTaskMutationSource for MacWorkerTaskMutationSource {
    fn integrate(
        &self,
        task_id: TaskId,
        request: &TaskIntegrationRequest,
    ) -> Result<TaskDetailProjection, ApiError> {
        let snapshot = self.validate_integration_request(task_id, request)?;
        self.drive_integration_request(task_id, request, Some(&snapshot), None)
    }

    fn integrate_response(
        &self,
        task_id: TaskId,
        request: &TaskIntegrationRequest,
    ) -> Result<serde_json::Value, ApiError> {
        use crate::integration::contracts::ValidateIntegration;
        request.integration.validate().map_err(map_mutation_error)?;
        if request.integration.task_id != task_id || request.expected.expected_task_id != task_id {
            return Err(ApiError::new(
                "TASK_REVISION_CONFLICT",
                "task changed before this action",
            ));
        }
        let source = self.integrations.as_ref().ok_or_else(|| {
            ApiError::new(
                "INTEGRATION_UNAVAILABLE",
                "compatible integration owner unavailable",
            )
        })?;
        let body = serde_json::to_value(request)
            .map_err(|_| ApiError::new("TASK_REQUEST_INVALID", "invalid integration request"))?;
        let checked = std::cell::RefCell::new(None);
        let api_failure = std::cell::RefCell::new(None);
        let mut validate = || {
            *checked.borrow_mut() = Some(
                self.validate_integration_request(task_id, request)
                    .map_err(|error| {
                        *api_failure.borrow_mut() = Some(error);
                        crate::integration::contracts::IntegrationCode::IntegrationStateInvalid
                            .error()
                    })?,
            );
            Ok(checked.borrow().as_ref().is_some_and(|snapshot| {
                snapshot.state != crate::integration::contracts::IntegrationStatus::Integrated
            }))
        };
        let mut execute = |redrive: &crate::integration::runner::ReplayRedrive<'_>| {
            let detail = self
                .drive_integration_request(
                    task_id,
                    request,
                    checked.borrow().as_ref(),
                    Some(redrive),
                )
                .map_err(|error| {
                    *api_failure.borrow_mut() = Some(error);
                    crate::integration::contracts::IntegrationCode::IntegrationStateInvalid.error()
                })?;
            serde_json::to_value(detail).map_err(|_| {
                crate::integration::contracts::IntegrationCode::IntegrationStateInvalid.error()
            })
        };
        match source.dashboard_redrive(&request.integration, &body, &mut validate, &mut execute) {
            Ok(result) => Ok(result),
            Err(error) => Err(api_failure
                .borrow_mut()
                .take()
                .unwrap_or_else(|| map_mutation_error(error))),
        }
    }

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
        #[cfg(any(test, feature = "test-support"))]
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
        #[cfg(any(test, feature = "test-support"))]
        self.after_expected_check();
        if let Some(revision) = request.expected_integration_revision {
            let source = self.integrations.as_ref().ok_or_else(|| {
                ApiError::new(
                    "INTEGRATION_UNAVAILABLE",
                    "compatible integration owner unavailable",
                )
            })?;
            let stopped = source
                .revoke(task_id, revision)
                .map_err(map_mutation_error)?;
            crate::integration::contracts::ValidateIntegration::validate(&stopped)
                .map_err(map_mutation_error)?;
            if Some(stopped.integration_id) != request.expected_integration_id
                || !matches!(
                    stopped.state,
                    crate::integration::contracts::IntegrationStatus::Revoked
                        | crate::integration::contracts::IntegrationStatus::Integrated
                )
            {
                return Err(ApiError::new(
                    "INTEGRATION_STOP_UNCONFIRMED",
                    "integration stop is not confirmed",
                ));
            }
        }
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

#[cfg(test)]
mod read_cost_tests {
    use super::*;
    use crate::integration::{contracts::*, store::RootedIntegrationState, testing::*};

    struct NoProcesses;
    impl ProcessRunner for NoProcesses {
        fn run(
            &self,
            _: &crate::process::ProcessRequest,
        ) -> Result<crate::process::ProcessResult, WorkerError> {
            panic!("projection must not execute a process");
        }
    }
    fn counts_since(before: [u64; 3]) -> [u64; 3] {
        let after = crate::rooted_fs::read_open_counts();
        std::array::from_fn(|i| after[i] - before[i])
    }
    #[test]
    fn disabled_dashboard_poll_keeps_baseline_locks_opens_and_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let paths = PathLayout {
            state: root.join("state"),
            config: root.join("config"),
            cache: root.join("cache"),
            data: root.join("data"),
        };
        let client = Arc::new(ClientStateStore::open(&paths.state).unwrap());
        for i in 1..=8 {
            client
                .create_task(sample_ordinary(
                    TaskId::new(uuid::Uuid::from_u128(i)),
                    fixture_source(),
                ))
                .unwrap();
        }
        let config = Arc::new(Config::parse("version = 1\n").unwrap());
        let source = NativeDashboardIntegrations {
            paths: paths.clone(),
            client: client.clone(),
            config: config.clone(),
            runner: Arc::new(NoProcesses),
        };
        let before = crate::rooted_fs::read_open_counts();
        let locks = client.state_lock_count();
        let baseline = project_local_tasks(&config, &client, None).unwrap();
        let baseline_opens = counts_since(before);
        let baseline_locks = client.state_lock_count() - locks;
        let before = crate::rooted_fs::read_open_counts();
        let locks = client.state_lock_count();
        let current = project_local_tasks(&config, &client, Some(&source)).unwrap();
        assert_eq!(
            serde_json::to_vec(&current.projection).unwrap(),
            serde_json::to_vec(&baseline.projection).unwrap()
        );
        assert_eq!(client.state_lock_count() - locks, baseline_locks);
        assert_eq!(
            counts_since(before),
            baseline_opens,
            "absent integration root must stop every per-row companion open"
        );
        assert!(!paths.state.join("integrations").exists());
    }
    #[test]
    fn enabled_dashboard_poll_reads_companions_as_one_batch() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let paths = PathLayout {
            state: root.join("state"),
            config: root.join("config"),
            cache: root.join("cache"),
            data: root.join("data"),
        };
        let client = Arc::new(ClientStateStore::open(&paths.state).unwrap());
        let state =
            RootedIntegrationState::open(&paths, Arc::new(ManualIntegrationRuntime::default()))
                .unwrap();
        for i in 1..=8 {
            let task = TaskId::new(uuid::Uuid::from_u128(i));
            client
                .create_task(sample_ordinary(task, fixture_source()))
                .unwrap();
            let record = sample_record(task, fixture_source(), "main");
            state.publish_policy(task, &record.policy).unwrap();
            state
                .replace(task, IntegrationRevision(0), &record)
                .unwrap();
        }
        let config = Arc::new(Config::parse("version = 1\n").unwrap());
        let source = NativeDashboardIntegrations {
            paths: paths.clone(),
            client: client.clone(),
            config: config.clone(),
            runner: Arc::new(NoProcesses),
        };
        let before = crate::rooted_fs::read_open_counts();
        let baseline = project_local_tasks(&config, &client, None).unwrap();
        assert_eq!(baseline.projection.tasks.len(), 8);
        let baseline_opens = counts_since(before);
        let before = crate::rooted_fs::read_open_counts();
        let projection = project_local_tasks(&config, &client, Some(&source))
            .unwrap()
            .projection;
        let opens = counts_since(before);
        assert_eq!(
            opens[0] - baseline_opens[0],
            1,
            "one anchored companion root per batch"
        );
        assert_eq!(
            opens[2] - baseline_opens[2],
            16,
            "one policy and record read per task"
        );
        assert!(projection.tasks.iter().all(|row| row.integration.is_some()));
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;
    use crate::integration::{contracts::*, store::RootedIntegrationState, testing::*};
    use crate::turn_runner::InlineRunnerExecutor;

    struct NoProbeProcesses;
    impl ProcessRunner for NoProbeProcesses {
        fn run(
            &self,
            _: &crate::process::ProcessRequest,
        ) -> Result<crate::process::ProcessResult, WorkerError> {
            panic!("this probe must not run an external process")
        }
    }

    struct Fixture {
        _temp: tempfile::TempDir,
        paths: PathLayout,
        client: Arc<ClientStateStore>,
        config: Arc<Config>,
        state: RootedIntegrationState,
        request: TaskIntegrationRequest,
        now: Arc<std::sync::atomic::AtomicU64>,
    }
    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().canonicalize().unwrap();
            let paths = PathLayout {
                config: root.join("config"),
                state: root.join("state"),
                cache: root.join("cache"),
                data: root.join("data"),
            };
            let now = Arc::new(std::sync::atomic::AtomicU64::new(1_000));
            let clock = now.clone();
            let client = Arc::new(
                ClientStateStore::open(&paths.state)
                    .unwrap()
                    .with_admission_clock(Arc::new(move || {
                        Ok(clock.load(std::sync::atomic::Ordering::SeqCst))
                    })),
            );
            let ordinary = sample_ordinary(fixture_task(), fixture_source());
            client.create_task(ordinary.clone()).unwrap();
            let state =
                RootedIntegrationState::open(&paths, Arc::new(ManualIntegrationRuntime::default()))
                    .unwrap();
            let mut record = sample_record(fixture_task(), fixture_source(), "main");
            record.snapshot.state = IntegrationStatus::Integrated;
            record.snapshot.observed_target_oid = Some(record.snapshot.source_head.clone());
            record.snapshot.disposition = Some(IntegrationDisposition::AlreadyIntegrated);
            record.receipt = Some(IntegrationReceipt {
                integration_id: record.snapshot.integration_id,
                epoch: record.snapshot.epoch,
                source_turn_id: record.snapshot.source_turn_id,
                source_head: record.snapshot.source_head.clone(),
                target_head: record.snapshot.source_head.clone(),
                merge_oid: None,
                disposition: IntegrationDisposition::AlreadyIntegrated,
                imported: true,
                recorded_at_millis: 1001,
            });
            record.validate().unwrap();
            state
                .publish_policy(record.task_id, &record.policy)
                .unwrap();
            state
                .replace(record.task_id, IntegrationRevision(0), &record)
                .unwrap();
            let request = serde_json::from_value(serde_json::json!({
                "expected": {
                    "expected_task_id":record.task_id,
                    "expected_turn_id":ordinary.status().turns().last().unwrap().turn_id(),
                    "expected_turn_count":ordinary.status().turns().len(),
                    "expected_head_oid":ordinary.status().head_oid(),
                    "expected_updated_at_millis":ordinary.status().updated_at_millis(),
                    "expected_state":"open"
                },
                "expected_integration_id":record.snapshot.integration_id,
                "integration":{"task_id":record.task_id,"expected":record.snapshot.revision,
                    "request_id":uuid::Uuid::new_v4().simple().to_string()}
            }))
            .unwrap();
            Self {
                _temp: temp,
                paths,
                client,
                config: Arc::new(Config::parse("version = 1\n").unwrap()),
                state,
                request,
                now,
            }
        }
        fn native(&self) -> Arc<NativeDashboardIntegrations> {
            Arc::new(NativeDashboardIntegrations {
                config: self.config.clone(),
                paths: self.paths.clone(),
                client: self.client.clone(),
                runner: Arc::new(NoProbeProcesses),
            })
        }
        fn mutation(&self) -> MacWorkerTaskMutationSource {
            MacWorkerTaskMutationSource::new(
                self.config.clone(),
                self.client.clone(),
                self.paths.clone(),
            )
            .with_process_runner(Arc::new(NoProbeProcesses))
            .with_executor(Arc::new(InlineRunnerExecutor))
            .with_integrations(self.native())
        }
        fn binding(&self) -> std::path::PathBuf {
            self.paths.state.join(format!(
                "integrations/tasks/{}/dashboard-redrive-{}.json",
                fixture_task(),
                self.request.integration.request_id
            ))
        }
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    struct ProbeBinding {
        request: IntegrationRedriveRequest,
        body: serde_json::Value,
        result: Option<serde_json::Value>,
    }

    #[test]
    fn review_pending_integrated_noop_replay_keeps_idempotent_success() {
        let f = Fixture::new();
        let mutation = f.mutation();
        let original = f.state.load(fixture_task()).unwrap().unwrap();
        let first = mutation
            .integrate_response(fixture_task(), &f.request)
            .unwrap();
        let mut binding: ProbeBinding =
            serde_json::from_slice(&std::fs::read(f.binding()).unwrap()).unwrap();
        binding.result = None; // Crash after publishing the request, before saving its no-op response.
        std::fs::write(f.binding(), serde_json::to_vec(&binding).unwrap()).unwrap();
        let replay = mutation.integrate_response(fixture_task(), &f.request);

        assert_eq!(
            replay.unwrap(),
            first,
            "a pending already-integrated replay must not demand a new revision"
        );
        assert_eq!(f.state.load(fixture_task()).unwrap().unwrap(), original);
        let mut changed = f.request.clone();
        changed.expected.expected_turn_count += 1;
        assert_eq!(
            mutation
                .integrate_response(fixture_task(), &changed)
                .unwrap_err()
                .code,
            "INTEGRATION_STATE_INVALID"
        );
        let mut stale = changed;
        stale.integration.request_id = uuid::Uuid::new_v4().simple().to_string();
        assert_eq!(
            mutation
                .integrate_response(fixture_task(), &stale)
                .unwrap_err()
                .code,
            "TASK_REVISION_CONFLICT"
        );
        assert_eq!(f.state.load(fixture_task()).unwrap().unwrap(), original);
    }
    fn storage_request(f: &Fixture, ordinal: u128) -> IntegrationRedriveRequest {
        let mut request = f.request.integration.clone();
        request.request_id = uuid::Uuid::from_u128(ordinal).simple().to_string();
        request
    }
    fn storage_name(ordinal: u128) -> String {
        format!(
            "dashboard-redrive-{}.json",
            uuid::Uuid::from_u128(ordinal).simple()
        )
    }
    fn storage_rows(f: &Fixture) -> std::collections::BTreeMap<String, Vec<u8>> {
        let root = f
            .paths
            .state
            .join(format!("integrations/tasks/{}", fixture_task()));
        std::fs::read_dir(root)
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("dashboard-redrive-")
                    && entry.path().extension().is_some_and(|ext| ext == "json")
            })
            .map(|entry| {
                (
                    entry.file_name().into_string().unwrap(),
                    std::fs::read(entry.path()).unwrap(),
                )
            })
            .collect()
    }
    fn storage_write(
        f: &Fixture,
        ordinal: u128,
        pending: bool,
        padding: usize,
    ) -> Result<serde_json::Value, WorkerError> {
        let source = f.native();
        let owner = source.owner().unwrap();
        let mut validate = || Ok(false);
        let mut execute = |_: &crate::integration::runner::ReplayRedrive<'_>| {
            if pending {
                Err(IntegrationCode::IntegrationNetwork.error())
            } else {
                Ok(serde_json::json!({"accepted": ordinal}))
            }
        };
        owner.dashboard_redrive(
            &storage_request(f, ordinal),
            &serde_json::json!({"padding": "x".repeat(padding)}),
            &mut validate,
            &mut execute,
        )
    }

    #[test]
    fn dashboard_replay_evicts_oldest_completed_without_evicting_pending() {
        let f = Fixture::new();
        assert!(storage_write(&f, 1, true, 0).is_err());
        let pending = storage_rows(&f)[&storage_name(1)].clone();
        for ordinal in 2..=32 {
            f.now
                .store(1_000 + ordinal as u64, std::sync::atomic::Ordering::SeqCst);
            storage_write(&f, ordinal, false, 0).unwrap();
        }
        storage_write(&f, 33, false, 0).unwrap();
        let rows = storage_rows(&f);
        assert_eq!(rows.len(), 32);
        assert_eq!(rows[&storage_name(1)], pending);
        assert!(!rows.contains_key(&storage_name(2)));
        assert!(rows.contains_key(&storage_name(3)));
        assert!(rows.contains_key(&storage_name(33)));
    }

    #[test]
    fn dashboard_replay_refuses_at_a_pending_count_budget_without_publication() {
        let f = Fixture::new();
        for ordinal in 1..=32 {
            assert!(storage_write(&f, ordinal, true, 0).is_err());
        }
        let before = storage_rows(&f);
        let original = f.state.load(fixture_task()).unwrap();
        assert_eq!(
            storage_write(&f, 33, false, 0).unwrap_err().public_code(),
            "TASK_BUSY"
        );
        assert_eq!(storage_rows(&f), before);
        assert_eq!(f.state.load(fixture_task()).unwrap(), original);
    }

    #[test]
    fn dashboard_replay_evicts_completed_to_keep_total_bytes_bounded() {
        let f = Fixture::new();
        for ordinal in 1..=4 {
            f.now
                .store(1_000 + ordinal as u64, std::sync::atomic::Ordering::SeqCst);
            storage_write(&f, ordinal, false, 80_000).unwrap();
        }
        let rows = storage_rows(&f);
        assert!(rows.values().map(Vec::len).sum::<usize>() <= 262_144);
        assert_eq!(rows.len(), 3);
        assert!(!rows.contains_key(&storage_name(1)));
    }

    #[test]
    fn dashboard_replay_refuses_when_pending_bytes_fill_the_budget() {
        let f = Fixture::new();
        for ordinal in 1..=2 {
            assert!(storage_write(&f, ordinal, true, 130_000).is_err());
        }
        let before = storage_rows(&f);
        assert_eq!(
            storage_write(&f, 3, false, 4_096)
                .unwrap_err()
                .public_code(),
            "TASK_BUSY"
        );
        assert_eq!(storage_rows(&f), before);
    }

    #[test]
    fn dashboard_replay_prunes_after_24_hours_using_injected_time() {
        let f = Fixture::new();
        storage_write(&f, 1, false, 0).unwrap();
        assert!(storage_write(&f, 2, true, 0).is_err());
        let pending = storage_rows(&f)[&storage_name(2)].clone();
        f.now.store(86_401_000, std::sync::atomic::Ordering::SeqCst);
        storage_write(&f, 3, false, 0).unwrap();
        assert!(
            storage_rows(&f).contains_key(&storage_name(1)),
            "the 24-hour boundary is still supported"
        );
        f.now.store(86_401_001, std::sync::atomic::Ordering::SeqCst);
        storage_write(&f, 4, false, 0).unwrap();
        let rows = storage_rows(&f);
        assert!(!rows.contains_key(&storage_name(1)));
        assert_eq!(rows[&storage_name(2)], pending);
        assert!(rows.contains_key(&storage_name(3)));
    }

    #[test]
    fn dashboard_replay_bindings_are_removed_with_the_owner_record() {
        let f = Fixture::new();
        storage_write(&f, 1, false, 0).unwrap();
        assert!(storage_write(&f, 2, true, 0).is_err());
        let before = f.client.load_task(fixture_task()).unwrap();
        let mut wire = serde_json::to_value(before.status()).unwrap();
        wire["state"] = "closed".into();
        f.client
            .update_task_if_current(
                &before,
                before
                    .clone()
                    .with_status(serde_json::from_value(wire).unwrap())
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(storage_rows(&f).len(), 2, "Closed alone preserves replay");
        f.client
            .remove_task_submission_record(fixture_task())
            .unwrap();
        assert!(storage_rows(&f).is_empty());
        assert!(
            f.client
                .load_task_optional(fixture_task())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn dashboard_replay_cleanup_precedes_owner_removal_and_retries_after_crash() {
        let f = Fixture::new();
        storage_write(&f, 1, false, 0).unwrap();
        assert!(storage_write(&f, 2, true, 0).is_err());
        f.client.inject_write_failure_once(
            crate::client_state::ClientStateWritePoint::BeforeTaskSubmissionRecordRemoval,
        );
        assert!(
            f.client
                .remove_task_submission_record(fixture_task())
                .is_err()
        );
        assert!(
            storage_rows(&f).is_empty(),
            "bindings must be removed before the record Delete decision"
        );
        assert!(
            f.client
                .load_task_optional(fixture_task())
                .unwrap()
                .is_some()
        );
        f.client
            .remove_task_submission_record(fixture_task())
            .unwrap();
        assert!(
            f.client
                .load_task_optional(fixture_task())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn review_dashboard_replay_storage_is_bounded_and_tied_to_owner_lifetime() {
        let f = Fixture::new();
        let mutation = f.mutation();
        let original = f.state.load(fixture_task()).unwrap();
        let mut last = f.request.clone();
        let mut result = serde_json::Value::Null;
        for ordinal in 1..=64 {
            f.now
                .store(1_000 + ordinal, std::sync::atomic::Ordering::SeqCst);
            last.integration.request_id = uuid::Uuid::new_v4().simple().to_string();
            result = mutation.integrate_response(fixture_task(), &last).unwrap();
            let rows = storage_rows(&f);
            assert!(rows.len() <= 32);
            assert!(rows.values().map(Vec::len).sum::<usize>() <= 262_144);
        }
        assert_eq!(f.state.load(fixture_task()).unwrap(), original);
        let task_root = f
            .paths
            .state
            .join(format!("integrations/tasks/{}", fixture_task()));
        f.native().redrive(&last.integration).unwrap();
        let native = task_root.join(format!("redrive-{}.json", last.integration.request_id));
        assert!(native.exists());
        let before = f.client.load_task(fixture_task()).unwrap();
        let mut wire = serde_json::to_value(before.status()).unwrap();
        wire["state"] = "closed".into();
        f.client
            .update_task_if_current(
                &before,
                before
                    .clone()
                    .with_status(serde_json::from_value(wire).unwrap())
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            mutation.integrate_response(fixture_task(), &last).unwrap(),
            result
        );
        f.client
            .remove_task_submission_record(fixture_task())
            .unwrap();
        assert!(storage_rows(&f).is_empty());
        assert!(!native.exists());
        assert!(
            f.client
                .load_task_optional(fixture_task())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn dashboard_replay_removal_refuses_a_live_request_before_deleting_owner() {
        use std::os::fd::AsRawFd;
        let f = Fixture::new();
        storage_write(&f, 1, false, 0).unwrap();
        let path = f
            .paths
            .state
            .join(format!("integrations/tasks/{}", fixture_task()));
        let root = crate::rooted_fs::RootedDir::open_anchored_absolute(&path).unwrap();
        let lock = root.open_private_lock("dashboard-redrive.lock").unwrap();
        assert_eq!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        let before = storage_rows(&f);
        assert_eq!(
            f.client
                .remove_task_submission_record(fixture_task())
                .unwrap_err()
                .public_code(),
            "TASK_BUSY"
        );
        assert_eq!(storage_rows(&f), before);
        assert!(
            f.client
                .load_task_optional(fixture_task())
                .unwrap()
                .is_some()
        );
        drop(lock);
        f.client
            .remove_task_submission_record(fixture_task())
            .unwrap();
        assert!(storage_rows(&f).is_empty());
    }

    fn native_storage_name(ordinal: u128) -> String {
        format!("redrive-{}.json", uuid::Uuid::from_u128(ordinal).simple())
    }

    fn combined_storage_rows(f: &Fixture) -> std::collections::BTreeMap<String, Vec<u8>> {
        let root = f
            .paths
            .state
            .join(format!("integrations/tasks/{}", fixture_task()));
        std::fs::read_dir(root)
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                name.ends_with(".json")
                    && (name.starts_with("dashboard-redrive-") || name.starts_with("redrive-"))
            })
            .map(|entry| {
                (
                    entry.file_name().into_string().unwrap(),
                    std::fs::read(entry.path()).unwrap(),
                )
            })
            .collect()
    }

    fn seed_native_pending(f: &Fixture, ordinal: u128) {
        #[derive(serde::Serialize)]
        struct Pending {
            request: IntegrationRedriveRequest,
            intent: IntegrationId,
            epoch: u32,
            result: Option<IntegrationSnapshot>,
        }
        let snapshot = f.state.load(fixture_task()).unwrap().unwrap().snapshot;
        let bytes = serde_json::to_vec(&Pending {
            request: storage_request(f, ordinal),
            intent: snapshot.integration_id,
            epoch: snapshot.epoch,
            result: None,
        })
        .unwrap();
        let path = f
            .paths
            .state
            .join(format!("integrations/tasks/{}", fixture_task()));
        crate::rooted_fs::RootedDir::open_anchored_absolute(&path)
            .unwrap()
            .write_private_atomic_no_replace(&native_storage_name(ordinal), &bytes)
            .unwrap();
    }

    #[test]
    fn review_r2_measure_both_replay_binding_families() {
        let f = Fixture::new();
        let mutation = f.mutation();
        let original = f.state.load(fixture_task()).unwrap();
        let root = f
            .paths
            .state
            .join(format!("integrations/tasks/{}", fixture_task()));
        for ordinal in 1..=32 {
            let mut request = f.request.clone();
            request.integration = storage_request(&f, ordinal);
            let first = mutation
                .integrate_response(fixture_task(), &request)
                .unwrap();
            let path = root.join(storage_name(ordinal));
            let mut binding: ProbeBinding =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            binding.result = None;
            std::fs::write(&path, serde_json::to_vec(&binding).unwrap()).unwrap();
            assert_eq!(
                mutation
                    .integrate_response(fixture_task(), &request)
                    .unwrap(),
                first
            );
            let rows = combined_storage_rows(&f);
            assert!(
                rows.len() <= 32,
                "the replay budget covers both families together"
            );
            assert!(rows.values().map(Vec::len).sum::<usize>() <= 262_144);
            assert!(rows.contains_key(&storage_name(ordinal)));
            assert!(rows.contains_key(&native_storage_name(ordinal)));
        }
        assert_eq!(f.state.load(fixture_task()).unwrap(), original);
    }

    #[test]
    fn review_r2_native_only_replays_obey_the_shared_file_budget() {
        let f = Fixture::new();
        let original = f.state.load(fixture_task()).unwrap();
        for ordinal in 1..=64 {
            let result = f.native().redrive(&storage_request(&f, ordinal)).unwrap();
            assert_eq!(
                f.native().redrive(&storage_request(&f, ordinal)).unwrap(),
                result
            );
            let rows = combined_storage_rows(&f);
            assert!(
                rows.len() <= 32,
                "native requests share the per-task replay budget"
            );
            assert!(rows.values().map(Vec::len).sum::<usize>() <= 262_144);
        }
        assert_eq!(f.state.load(fixture_task()).unwrap(), original);
    }

    #[test]
    fn shared_replay_native_only_expiry_uses_injected_completion_time() {
        let f = Fixture::new();
        f.native().redrive(&storage_request(&f, 1)).unwrap();
        seed_native_pending(&f, 2);
        let before = combined_storage_rows(&f);
        let saved: serde_json::Value =
            serde_json::from_slice(&before[&native_storage_name(1)]).unwrap();
        assert_eq!(saved["completed_at_millis"], 1_000);
        f.now.store(86_401_000, std::sync::atomic::Ordering::SeqCst);
        f.native().redrive(&storage_request(&f, 3)).unwrap();
        assert!(combined_storage_rows(&f).contains_key(&native_storage_name(1)));
        f.now.store(86_401_001, std::sync::atomic::Ordering::SeqCst);
        storage_write(&f, 4, false, 0).unwrap();
        let rows = combined_storage_rows(&f);
        assert!(!rows.contains_key(&native_storage_name(1)));
        assert_eq!(
            rows[&native_storage_name(2)],
            before[&native_storage_name(2)]
        );
        assert!(rows.contains_key(&native_storage_name(3)));
    }

    #[test]
    fn shared_replay_pending_count_refuses_both_writers_without_publication() {
        let f = Fixture::new();
        for ordinal in 1..=16 {
            seed_native_pending(&f, ordinal);
        }
        for ordinal in 17..=32 {
            assert!(storage_write(&f, ordinal, true, 0).is_err());
        }
        let before = combined_storage_rows(&f);
        assert_eq!(before.len(), 32);
        let original = f.state.load(fixture_task()).unwrap();
        assert_eq!(
            f.native()
                .redrive(&storage_request(&f, 33))
                .unwrap_err()
                .public_code(),
            "TASK_BUSY"
        );
        assert_eq!(
            storage_write(&f, 34, false, 0).unwrap_err().public_code(),
            "TASK_BUSY"
        );
        assert_eq!(combined_storage_rows(&f), before);
        assert_eq!(f.state.load(fixture_task()).unwrap(), original);
    }

    #[test]
    fn shared_replay_pending_bytes_refuse_native_publication() {
        let f = Fixture::new();
        seed_native_pending(&f, 1);
        let native = combined_storage_rows(&f)[&native_storage_name(1)].len();
        let calibration = Fixture::new();
        assert!(storage_write(&calibration, 2, true, 0).is_err());
        let overhead = storage_rows(&calibration)[&storage_name(2)].len();
        assert!(storage_write(&f, 2, true, 262_144 - native - overhead).is_err());
        let before = combined_storage_rows(&f);
        assert_eq!(before.values().map(Vec::len).sum::<usize>(), 262_144);
        let original = f.state.load(fixture_task()).unwrap();
        assert_eq!(
            f.native()
                .redrive(&storage_request(&f, 3))
                .unwrap_err()
                .public_code(),
            "TASK_BUSY"
        );
        assert_eq!(combined_storage_rows(&f), before);
        assert_eq!(f.state.load(fixture_task()).unwrap(), original);
    }

    #[test]
    fn shared_replay_byte_eviction_can_retire_an_old_completed_native_row() {
        let f = Fixture::new();
        f.native().redrive(&storage_request(&f, 1)).unwrap();
        let native = combined_storage_rows(&f)[&native_storage_name(1)].len();
        let calibration = Fixture::new();
        assert!(storage_write(&calibration, 2, true, 0).is_err());
        let overhead = storage_rows(&calibration)[&storage_name(2)].len();
        assert!(storage_write(&f, 2, true, 262_144 - native - overhead).is_err());
        let before = combined_storage_rows(&f);
        assert_eq!(before.values().map(Vec::len).sum::<usize>(), 262_144);
        storage_write(&f, 3, false, 0).unwrap();
        let rows = combined_storage_rows(&f);
        assert!(!rows.contains_key(&native_storage_name(1)));
        assert_eq!(rows[&storage_name(2)], before[&storage_name(2)]);
        assert!(rows.values().map(Vec::len).sum::<usize>() <= 262_144);
    }

    #[test]
    fn shared_replay_paired_admission_refuses_before_either_publication() {
        let f = Fixture::new();
        for ordinal in 1..=16 {
            seed_native_pending(&f, ordinal);
        }
        for ordinal in 17..=31 {
            assert!(storage_write(&f, ordinal, true, 0).is_err());
        }
        let before = combined_storage_rows(&f);
        assert_eq!(before.len(), 31);
        let original = f.state.load(fixture_task()).unwrap();
        let request = storage_request(&f, 32);
        let owner = f.native();
        let owner = owner.owner().unwrap();
        let mut executions = 0;
        let mut validate = || Ok(true);
        let mut execute = |redrive: &crate::integration::runner::ReplayRedrive<'_>| {
            executions += 1;
            redrive(&request)?;
            Ok(serde_json::json!({"accepted": true}))
        };
        let error = owner
            .dashboard_redrive(
                &request,
                &serde_json::json!({}),
                &mut validate,
                &mut execute,
            )
            .unwrap_err();
        assert_eq!(error.public_code(), "TASK_BUSY");
        assert_eq!(executions, 0);
        assert_eq!(combined_storage_rows(&f), before);
        assert_eq!(f.state.load(fixture_task()).unwrap(), original);
    }

    #[test]
    fn shared_replay_concurrent_native_and_dashboard_publication_refuses_without_waiting() {
        let f = Fixture::new();
        for ordinal in 1..=15 {
            seed_native_pending(&f, ordinal);
        }
        for ordinal in 16..=30 {
            assert!(storage_write(&f, ordinal, true, 0).is_err());
        }
        let before = combined_storage_rows(&f);
        let original = f.state.load(fixture_task()).unwrap();
        std::thread::scope(|scope| {
            let f = &f;
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let dashboard = scope.spawn(move || {
                let source = f.native();
                let owner = source.owner().unwrap();
                let mut validate = || Ok(false);
                let mut execute = |_: &crate::integration::runner::ReplayRedrive<'_>| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(serde_json::json!({"accepted": true}))
                };
                owner.dashboard_redrive(
                    &storage_request(f, 31),
                    &serde_json::json!({}),
                    &mut validate,
                    &mut execute,
                )
            });
            entered_rx.recv().unwrap();
            let native = f.native().redrive(&storage_request(f, 32));
            release_tx.send(()).unwrap();
            dashboard.join().unwrap().unwrap();
            assert_eq!(native.unwrap_err().public_code(), "TASK_BUSY");
        });
        let rows = combined_storage_rows(&f);
        assert!(rows.len() <= 32);
        for (name, bytes) in before {
            assert_eq!(rows[&name], bytes);
        }
        assert_eq!(f.state.load(fixture_task()).unwrap(), original);
    }

    #[test]
    fn shared_replay_both_writers_refuse_either_live_family_lock() {
        use std::os::fd::AsRawFd;
        for name in ["dashboard-redrive.lock", "redrive.lock"] {
            let f = Fixture::new();
            let path = f
                .paths
                .state
                .join(format!("integrations/tasks/{}", fixture_task()));
            let root = crate::rooted_fs::RootedDir::open_anchored_absolute(&path).unwrap();
            let lock = root.open_private_lock(name).unwrap();
            assert_eq!(
                unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
                0
            );
            let before = combined_storage_rows(&f);
            let original = f.state.load(fixture_task()).unwrap();
            let native = f.native().redrive(&storage_request(&f, 1));
            let dashboard = storage_write(&f, 2, false, 0);
            assert_eq!(native.unwrap_err().public_code(), "TASK_BUSY", "{name}");
            assert_eq!(dashboard.unwrap_err().public_code(), "TASK_BUSY", "{name}");
            assert_eq!(combined_storage_rows(&f), before);
            assert_eq!(f.state.load(fixture_task()).unwrap(), original);
        }
    }

    #[test]
    fn shared_replay_pending_partners_protect_completed_rows_in_both_families() {
        let f = Fixture::new();
        assert!(storage_write(&f, 1, true, 0).is_err());
        f.native().redrive(&storage_request(&f, 1)).unwrap();
        seed_native_pending(&f, 2);
        storage_write(&f, 2, false, 0).unwrap();
        let before = combined_storage_rows(&f);
        assert_eq!(before.len(), 4);
        f.now.store(86_401_001, std::sync::atomic::Ordering::SeqCst);
        f.native().redrive(&storage_request(&f, 3)).unwrap();
        storage_write(&f, 4, false, 0).unwrap();
        let rows = combined_storage_rows(&f);
        for (name, bytes) in before {
            assert_eq!(rows[&name], bytes);
        }
        assert!(rows.len() <= 32);
        assert!(rows.values().map(Vec::len).sum::<usize>() <= 262_144);
    }
}
