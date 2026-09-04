use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Deserialize;

use crate::{
    agent::{AgentKind, PermissionPolicy, TurnLimits, adapter_for},
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    error::WorkerError,
    git_transport::GitTransport,
    job::{
        AdmissionObservation, CommandSpec, ProcessIdentity, QueueEntry, QueueEntryKind,
        QueueRunReference, QueueState,
    },
    paths::PathLayout,
    process::ProcessRunner,
    project_config::TaskSettings,
    project_state::ProjectState,
    scheduler::{CandidateObservation, SchedulerPolicy, Selection, WorkerPreference},
    scheduler_adapter::SchedulerProbeAdapter,
    supervisor::{ProcessObservation, SystemProcessInspector},
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, RunId, RunProgress, RunRecord,
        RunnerIdentity, RunnerState, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome,
        TaskState, TaskStatus, TaskSummary, TurnId, TurnSummary,
    },
    transfer::RemoteJobClient,
    transfer_repo::TransferRepo,
    transport::{SshTransport, WorkersService},
    turn_runner::{DetachedRunnerExecutor, RunnerExecutor, TurnRunner},
};

const TASK_COMMAND: &str = "task-turn";
const TASK_CAPABILITY_PREFIX: &str = "agent:";
const DEFAULT_GIT_NAME: &str = "mac-worker";
const DEFAULT_GIT_EMAIL: &str = "mac-worker@localhost";
const WAIT_POLL: Duration = Duration::from_millis(100);
const WAIT_MAX_POLL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct TaskSubmitRequest {
    pub agent: AgentKind,
    pub model: Option<String>,
    pub prompt: String,
    pub project: PathBuf,
    pub base: String,
    pub wip: bool,
    pub cli_includes: Vec<String>,
    pub limits: TaskLimits,
    pub close_policy: ClosePolicy,
    pub env_profile: Option<String>,
    pub preference: WorkerPreference,
    pub wait_for_capacity: bool,
    pub attached: bool,
    pub run_id: Option<RunId>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskListFilter {
    pub run_id: Option<RunId>,
    pub state: Option<TaskState>,
    pub full: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskReport {
    task_id: TaskId,
    run_id: Option<RunId>,
    status: TaskStatus,
    events: Vec<serde_json::Value>,
    runner: Option<RunnerState>,
}

impl TaskReport {
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn run_id(&self) -> Option<RunId> {
        self.run_id
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }

    pub fn events(&self) -> &[serde_json::Value] {
        &self.events
    }

    pub fn runner(&self) -> Option<RunnerState> {
        self.runner
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskListReport {
    tasks: Vec<TaskSummary>,
}

impl TaskListReport {
    pub fn tasks(&self) -> &[TaskSummary] {
        &self.tasks
    }

    pub fn progress(&self) -> RunProgress {
        RunProgress::from_states(self.tasks.iter().map(TaskSummary::state))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskResultReport {
    task_id: TaskId,
    status: TaskStatus,
    branch: String,
    fetch_instruction: String,
}

impl TaskResultReport {
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }

    pub fn branch(&self) -> &str {
        &self.branch
    }

    pub fn fetch_instruction(&self) -> &str {
        &self.fetch_instruction
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchReport {
    task_id: TaskId,
    head: BaseOid,
    local_ref: String,
}

impl FetchReport {
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn head(&self) -> &BaseOid {
        &self.head
    }

    pub fn local_ref(&self) -> &str {
        &self.local_ref
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReconcileReport {
    replaced_runners: usize,
    started_runners: usize,
}

impl ReconcileReport {
    pub fn replaced_runners(&self) -> usize {
        self.replaced_runners
    }

    pub fn started_runners(&self) -> usize {
        self.started_runners
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitSelector {
    Task(TaskId),
    Run(RunId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitReport {
    task_ids: Vec<TaskId>,
    exit_code: u8,
}

impl WaitReport {
    pub fn task_ids(&self) -> &[TaskId] {
        &self.task_ids
    }

    pub fn exit_code(&self) -> u8 {
        self.exit_code
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReport {
    run_id: RunId,
    task_ids: Vec<TaskId>,
}

impl RunReport {
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn task_ids(&self) -> &[TaskId] {
        &self.task_ids
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchFile {
    #[serde(default)]
    pub defaults: BatchDefaults,
    pub tasks: Vec<BatchTask>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchDefaults {
    #[serde(default = "default_agent_name")]
    pub agent: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default = "default_base_name")]
    pub base: String,
    #[serde(default)]
    pub wip: bool,
    #[serde(default)]
    pub timeout: Option<String>,
    #[serde(default)]
    pub max_turns: Option<u32>,
    #[serde(default)]
    pub max_budget_usd_cents: Option<u64>,
    #[serde(default)]
    pub max_followups: Option<u32>,
    #[serde(default)]
    pub close_on: Option<String>,
    #[serde(default)]
    pub env_profile: Option<String>,
    #[serde(default)]
    pub worker: Option<String>,
}

impl Default for BatchDefaults {
    fn default() -> Self {
        Self {
            agent: default_agent_name(),
            model: None,
            base: default_base_name(),
            wip: false,
            timeout: None,
            max_turns: None,
            max_budget_usd_cents: None,
            max_followups: None,
            close_on: None,
            env_profile: None,
            worker: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchTask {
    pub prompt: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub base: Option<String>,
    #[serde(default)]
    pub wip: Option<bool>,
    #[serde(default)]
    pub timeout: Option<String>,
    #[serde(default)]
    pub max_turns: Option<u32>,
    #[serde(default)]
    pub max_budget_usd_cents: Option<u64>,
    #[serde(default)]
    pub max_followups: Option<u32>,
    #[serde(default)]
    pub close_on: Option<String>,
    #[serde(default)]
    pub env_profile: Option<String>,
    #[serde(default)]
    pub worker: Option<String>,
}

pub struct TaskClient<'a> {
    pub(crate) runner: &'a dyn ProcessRunner,
    pub(crate) config: &'a Config,
    pub(crate) paths: &'a PathLayout,
    pub(crate) client_state: &'a ClientStateStore,
    pub(crate) executor: &'a dyn RunnerExecutor,
}

impl<'a> TaskClient<'a> {
    pub fn new(
        runner: &'a dyn ProcessRunner,
        config: &'a Config,
        paths: &'a PathLayout,
        client_state: &'a ClientStateStore,
        executor: &'a dyn RunnerExecutor,
    ) -> Self {
        Self {
            runner,
            config,
            paths,
            client_state,
            executor,
        }
    }

    pub fn with_detached_executor(
        runner: &'a dyn ProcessRunner,
        config: &'a Config,
        paths: &'a PathLayout,
        client_state: &'a ClientStateStore,
    ) -> Self {
        Self::new(runner, config, paths, client_state, &DetachedRunnerExecutor)
    }

    pub fn submit(
        &self,
        request: TaskSubmitRequest,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<TaskReport, WorkerError> {
        self.submit_with_ids(request, None, None, None, stdout, stderr)
    }

    /// Submits a task with an explicit, redacted title.  The public request
    /// intentionally stays identical to the plan's wire-facing shape; batch
    /// metadata uses this internal extension so the run record and task
    /// record are created with the same identifiers.
    pub fn submit_titled(
        &self,
        request: TaskSubmitRequest,
        title: Option<String>,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<TaskReport, WorkerError> {
        self.submit_with_ids(request, None, None, title, stdout, stderr)
    }

    fn submit_with_ids(
        &self,
        request: TaskSubmitRequest,
        task_id_override: Option<TaskId>,
        _turn_id_override: Option<TurnId>,
        title: Option<String>,
        stdout: &mut dyn Write,
        _stderr: &mut dyn Write,
    ) -> Result<TaskReport, WorkerError> {
        self.reconcile_runners()?;
        validate_prompt(&request.prompt)?;
        if request.agent != AgentKind::Codex {
            return Err(task_error(
                "AGENT_UNSUPPORTED",
                "this client accepts Codex tasks only",
            ));
        }
        validate_preference(self.config, &request.preference)?;

        let initial = ProjectState::load(self.runner, &request.project, &request.cli_includes)?;
        let settings = &initial.settings.task;
        let limits = effective_task_limits(&request.limits, settings)?;
        let env_profile = request
            .env_profile
            .clone()
            .or_else(|| settings.env_profile.clone());
        let requirements =
            task_requirements(&initial.requirements, request.agent, env_profile.as_deref());
        let observations = self.observe_admission(&request.preference)?;
        let affinity = self
            .client_state
            .affinity_hints(&initial.context.project_id, &initial.context.worktree_id)?;
        if let Selection::NoEligible { rejections } =
            SchedulerPolicy::select(&observations, &requirements, &request.preference, &affinity)
        {
            if let WorkerPreference::Pinned { worker } = &request.preference
                && let Some(missing) = rejections.iter().find_map(|rejection| match rejection {
                    crate::scheduler::CandidateRejection::MissingCapabilities { name, missing }
                        if name == worker =>
                    {
                        Some(missing.as_slice())
                    }
                    _ => None,
                })
            {
                return Err(capability_missing(worker, missing));
            }
            if !request.wait_for_capacity {
                return Err(capacity_busy());
            }
        }

        let task_id = task_id_override.unwrap_or_else(TaskId::generate);
        let turn_id = _turn_id_override.unwrap_or_else(TurnId::generate);
        let now = current_time_millis()?;
        let identity = GitIdentity::new(DEFAULT_GIT_NAME, DEFAULT_GIT_EMAIL)?;
        let transfer =
            TransferRepo::open_or_create(&self.paths.cache, &initial.context.common_dir)?;
        let base = if request.wip {
            transfer.build_wip_base(
                self.runner,
                &initial.context,
                task_id,
                &initial.settings,
                &identity,
            )?
        } else {
            transfer.resolve_base(self.runner, &initial.context, &request.base)?
        };
        if let Err(error) =
            transfer.check_sensitive_tree(self.runner, base.oid(), &initial.settings)
        {
            let _ = transfer.release_base(self.runner, task_id);
            return Err(error);
        }
        let composed_prompt = compose_turn_prompt(
            task_id,
            1,
            base.oid(),
            initial.context.branch.as_deref(),
            &request.prompt,
            false,
        );
        if let Err(error) = validate_prompt(&composed_prompt) {
            let _ = transfer.release_base(self.runner, task_id);
            return Err(error);
        }

        let policy = permission_policy(settings, request.agent);
        let source = crate::task::TaskSource::Local { wip: request.wip };
        let meta = TaskMeta::new(TaskMetaInput {
            task_id,
            run_id: request.run_id,
            project_id: initial.context.project_id.clone(),
            worktree_id: initial.context.worktree_id.clone(),
            agent: request.agent,
            model: request.model.clone(),
            policy,
            source,
            publish: vec![crate::task::PublishMode::Fetch],
            publish_branch: None,
            base_oid: base.oid().clone(),
            limits,
            close_policy: request.close_policy,
            env_profile,
            git_identity: identity,
            title,
            prompt: request.prompt.clone(),
            created_at_millis: now,
        })?;
        let status = TaskStatus::new(
            TaskState::Queued,
            None,
            None,
            false,
            Some(base.oid().clone()),
            None,
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            now,
        )?;
        let pinned_worker = match &request.preference {
            WorkerPreference::Automatic => None,
            WorkerPreference::Pinned { worker } => Some(worker.clone()),
        };
        let record = LocalTaskRecord::new(
            meta.clone(),
            status,
            None,
            None,
            None,
            transfer.repo_id().to_owned(),
            pinned_worker,
            request.wait_for_capacity,
            None,
        )?;
        self.client_state.create_task(record)?;
        if let Err(error) = self
            .client_state
            .write_turn_prompt(task_id, turn_id, &composed_prompt)
        {
            let _ = transfer.release_base(self.runner, task_id);
            return Err(error);
        }

        let run_reference = match request.run_id {
            Some(run_id) => {
                let run = self.client_state.load_run(run_id)?;
                Some(QueueRunReference::new(
                    crate::job::RunId::new(run_id.to_string())?,
                    run.max_parallel(),
                )?)
            }
            None => None,
        };
        let command = CommandSpec::argv(vec![TASK_COMMAND.to_owned()])?;
        let owner = current_process_identity()?;
        let entry = QueueEntry::new(
            turn_id,
            self.client_state.client_id(),
            initial.context.project_id.clone(),
            initial.context.worktree_id.clone(),
            command.summary()?,
            requirements,
            request.preference.clone(),
            QueueEntryKind::TaskTurn,
            run_reference,
            owner,
            now,
        )?;
        let entry = match self.client_state.enqueue(entry) {
            Ok(entry) => entry,
            Err(error) => {
                let _ = transfer.release_base(self.runner, task_id);
                return Err(error);
            }
        };

        let should_start =
            request.attached || self.live_runner_count()? < self.config.workers.len();
        if should_start {
            if let Err(error) = self.start_runner(task_id, turn_id) {
                return Err(self.fail_handoff(task_id, turn_id, transfer, error));
            }
        } else {
            self.client_state.park_row(entry.job_id())?;
        }

        let mut report = self.report_for(task_id)?;
        report.events.push(event_task_created(&report));
        if request.attached {
            let mut follow = stdout;
            let outcome = TurnRunner::new(
                self.runner,
                self.config,
                self.paths,
                self.client_state,
                self.executor,
            )
            .run(task_id, turn_id, Some(&mut follow))?;
            report.status = outcome.status().clone();
            report.events.extend(outcome.events().iter().cloned());
        }
        Ok(report)
    }

    pub fn status(&self, task_id: TaskId) -> Result<TaskReport, WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        self.report_for_readonly(&record)
    }

    pub fn list(&self, filter: TaskListFilter) -> Result<TaskListReport, WorkerError> {
        let mut tasks = self
            .client_state
            .list_tasks()?
            .into_iter()
            .filter(|task| {
                filter
                    .run_id
                    .is_none_or(|run| task.meta().run_id() == Some(run))
            })
            .filter(|task| {
                filter
                    .state
                    .is_none_or(|state| task.status().state() == state)
            })
            .map(|task| {
                let runner = self.client_state.runner_liveness(task.meta().task_id())?;
                Ok(task.summary().with_runner_state(runner))
            })
            .collect::<Result<Vec<_>, WorkerError>>()?;
        tasks.sort_by_key(TaskSummary::updated_at_millis);
        Ok(TaskListReport { tasks })
    }

    pub fn logs(
        &self,
        task_id: TaskId,
        turn: Option<u32>,
        follow: bool,
        raw: bool,
        stdout: &mut dyn Write,
        _stderr: &mut dyn Write,
    ) -> Result<(), WorkerError> {
        let mut record = self.client_state.load_task(task_id)?;
        let selected = select_turn(record.status(), turn)?;
        let turn_id = selected.turn_id();
        let mut offset = 0_u64;
        loop {
            let bytes = self.read_runner_log(task_id, turn_id)?;
            if raw {
                stdout.write_all(&bytes[offset as usize..])?;
            } else {
                render_agent_log(&bytes[offset as usize..], record.meta().agent(), stdout)?;
            }
            offset = bytes.len() as u64;
            stdout.flush()?;
            record = self.client_state.load_task(task_id)?;
            if !follow || record.status().state().is_terminal() {
                break;
            }
            std::thread::sleep(WAIT_POLL);
        }
        Ok(())
    }

    pub fn diff(
        &self,
        task_id: TaskId,
        stat: bool,
        stdout: &mut dyn Write,
    ) -> Result<(), WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        let worker = task_worker(self.config, record.status())?;
        let remote = RemoteJobClient::new(self.runner);
        let response = remote.task_diff(
            worker,
            &crate::task_store::TaskDiffRequest::new(record.meta().project_id(), task_id, stat),
        )?;
        stdout.write_all(response.text().as_bytes())?;
        if !response.text().ends_with('\n') {
            stdout.write_all(b"\n")?;
        }
        stdout.flush()?;
        Ok(())
    }

    pub fn result(&self, task_id: TaskId) -> Result<TaskResultReport, WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        Ok(TaskResultReport {
            task_id,
            status: record.status().clone(),
            branch: format!("task/{task_id}"),
            fetch_instruction: format!("worker task fetch {task_id}"),
        })
    }

    pub fn fetch(&self, task_id: TaskId) -> Result<FetchReport, WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        if record.status().state() == TaskState::Abandoned {
            return Err(task_error(
                "TASK_CLOSED",
                "discarded tasks cannot be fetched",
            ));
        }
        let worker = task_worker(self.config, record.status())?;
        let project = ProjectState::load(self.runner, &self.current_project()?, &[])?;
        require_project_match(&project, record.meta())?;
        let transfer =
            TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)?;
        let _remote_receipt = GitTransport::new(self.runner).fetch_result(
            worker,
            self.client_state.client_id(),
            record.meta().project_id(),
            task_id,
            transfer.path(),
        )?;
        let imported = transfer.import_result(
            self.runner,
            &project.context.common_dir,
            worker.name.as_str(),
            task_id,
        )?;
        let replacement = record.with_fetched_head(Some(imported.head().clone()))?;
        self.client_state.update_task(replacement)?;
        Ok(FetchReport {
            task_id,
            head: imported.head().clone(),
            local_ref: imported.local_ref().to_owned(),
        })
    }

    pub fn close(&self, task_id: TaskId, discard: bool) -> Result<TaskReport, WorkerError> {
        self.reconcile_runners()?;
        let record = self.client_state.load_task(task_id)?;
        if record.status().state() == TaskState::Active {
            return Err(task_error("TASK_BUSY", "task has an active turn"));
        }
        if let Some(entry) = self.client_state.queue_entry_for_task_turn(task_id)? {
            if matches!(entry.state(), QueueState::Dispatching { .. }) {
                return Err(task_error("TASK_BUSY", "task turn is being dispatched"));
            }
            match self
                .client_state
                .request_queue_cancel(entry.job_id(), current_time_millis()?)?
            {
                Some(crate::job::QueueCancel::RemovedWaiting { .. }) => {}
                Some(crate::job::QueueCancel::RequestedDispatch { .. }) => {
                    return Err(task_error("TASK_BUSY", "task turn is being dispatched"));
                }
                None => {
                    return Err(task_error(
                        "TASK_INCONSISTENT",
                        "task queue turn disappeared",
                    ));
                }
            }
            let _ = self
                .client_state
                .remove_turn_prompt(task_id, entry.job_id());
            let status = TaskStatus::new(
                if discard {
                    TaskState::Abandoned
                } else {
                    TaskState::Closed
                },
                record.status().last_outcome().cloned(),
                record.status().worker().map(str::to_owned),
                record.status().session_present(),
                record.status().head_oid().cloned(),
                record.status().summary().map(str::to_owned),
                record.status().questions().to_vec(),
                record.status().files_changed().to_vec(),
                record.status().diff_stat().map(str::to_owned),
                record.status().turns().to_vec(),
                current_time_millis()?,
            )?;
            self.client_state.update_task(
                record
                    .with_status(status)?
                    .with_abandon_code(discard.then_some("TASK_CLOSED".to_owned()))?,
            )?;
            self.release_task_base(&record)?;
            return self.report_for(task_id);
        }
        if record.status().state() == TaskState::Queued {
            return Err(task_error(
                "TASK_INCONSISTENT",
                "queued task has no queue turn",
            ));
        }
        let worker = task_worker(self.config, record.status())?;
        let response = RemoteJobClient::new(self.runner).task_close(
            worker,
            &crate::task_store::TaskCloseRequest::new(record.meta().project_id(), task_id, discard),
        )?;
        self.client_state
            .update_task(record.with_status(response.status().clone())?)?;
        self.release_task_base(&record)?;
        self.report_for(task_id)
    }

    pub fn reconcile_runners(&self) -> Result<ReconcileReport, WorkerError> {
        let mut report = ReconcileReport::default();
        let owner = current_process_identity()?;

        // A task-turn row is owned by its runner in both waiting and
        // dispatching states.  Unlike legacy batch rows it must never be
        // reaped just because the owner disappeared: the next mutating
        // command adopts it and either resumes the accepted turn or retries
        // the pre-acceptance handoff.
        for entry in self.client_state.queue_snapshot()?.entries().iter() {
            if entry.kind() != QueueEntryKind::TaskTurn
                || matches!(entry.state(), QueueState::Parked)
            {
                continue;
            }
            let Some(row_owner) = entry.owner_opt().copied() else {
                continue;
            };
            if row_owner != owner
                && matches!(
                    self.client_state.process_observation(row_owner),
                    ProcessObservation::Absent | ProcessObservation::Reused
                )
            {
                self.client_state.adopt_row(entry.job_id(), owner)?;
            }
        }

        // Refresh only the task records referenced by the local queue.  This
        // is intentionally a mutating-command path; status/list remain
        // read-only projections.  Probe failures are not evidence that a
        // worker lost a task, so a failed refresh leaves the local record
        // untouched for a later reconciliation.
        for record in self.client_state.list_tasks()? {
            if matches!(
                record.status().state(),
                TaskState::Closed | TaskState::Abandoned | TaskState::Lost
            ) {
                continue;
            }
            let _ = self.refresh_task_status(&record);
        }

        // A crash can occur after the task record is durable but before the
        // queue publication. Recreate only the task-turn row, retaining the
        // original task and turn identifiers.
        for record in self.client_state.list_tasks()? {
            if matches!(
                record.status().state(),
                TaskState::Queued | TaskState::Active
            ) && self
                .client_state
                .queue_entry_for_task_turn(record.meta().task_id())?
                .is_none()
            {
                self.enqueue_missing_turn(&record, owner)?;
            }
        }

        for record in self.client_state.list_tasks()? {
            if matches!(
                record.status().state(),
                TaskState::Closed | TaskState::Abandoned | TaskState::Lost
            ) {
                continue;
            }
            let Some(turn_id) = pending_turn_id(record.status()).or_else(|| {
                self.client_state
                    .turn_ids_for_task(record.meta().task_id())
                    .ok()?
                    .last()
                    .copied()
            }) else {
                continue;
            };
            let mut entry = self.client_state.queue_entry(turn_id)?;
            if entry.is_none()
                && matches!(
                    record.status().state(),
                    TaskState::Queued | TaskState::Active
                )
            {
                self.enqueue_missing_turn(&record, owner)?;
                entry = self.client_state.queue_entry(turn_id)?;
            }
            let Some(entry) = entry else { continue };
            if matches!(entry.state(), QueueState::Parked) {
                if record.status().state() == TaskState::Open {
                    let _ = self
                        .client_state
                        .remove_task_turn_after_terminal(turn_id, owner);
                }
                continue;
            }
            let liveness = self.client_state.runner_liveness(record.meta().task_id())?;
            if record.status().state() == TaskState::Open {
                if liveness != Some(RunnerState::Live)
                    && !matches!(
                        entry
                            .owner_opt()
                            .copied()
                            .map(|identity| { self.client_state.process_observation(identity) }),
                        Some(ProcessObservation::Matching { .. } | ProcessObservation::Ambiguous)
                    )
                {
                    let row_owner = entry.owner_opt().copied().unwrap_or(owner);
                    let _ = self
                        .client_state
                        .remove_task_turn_after_terminal(turn_id, row_owner);
                    let _ = self
                        .client_state
                        .remove_turn_prompt(record.meta().task_id(), turn_id);
                }
                continue;
            }
            if liveness == Some(RunnerState::Dead) {
                self.client_state
                    .record_runner(record.meta().task_id(), None)?;
                report.replaced_runners += 1;
            }
            if self
                .client_state
                .runner_liveness(record.meta().task_id())?
                .is_none()
                && self.live_runner_count()? < self.config.workers.len()
            {
                self.start_runner(record.meta().task_id(), turn_id)?;
                report.started_runners += 1;
            }
        }

        while self.live_runner_count()? < self.config.workers.len() {
            if !self.start_oldest_parked_runner(owner)? {
                break;
            }
            report.started_runners += 1;
        }
        Ok(report)
    }

    pub fn say(
        &self,
        task_id: TaskId,
        message: String,
        attached: bool,
        stdout: &mut dyn Write,
        _stderr: &mut dyn Write,
    ) -> Result<TaskReport, WorkerError> {
        self.reconcile_runners()?;
        let record = self.client_state.load_task(task_id)?;
        match record.status().state() {
            TaskState::Active => return Err(task_error("TASK_BUSY", "task has an active turn")),
            TaskState::Closed | TaskState::Abandoned | TaskState::Lost => {
                return Err(task_error("TASK_CLOSED", "task is terminal"));
            }
            TaskState::Queued => {
                return Err(task_error("TASK_BUSY", "task has not reached an open turn"));
            }
            TaskState::Open => {}
        }
        if record.status().turns().len() as u32 >= record.meta().limits().max_followups {
            return Err(task_error(
                "FOLLOWUP_LIMIT",
                "task follow-up limit has been reached",
            ));
        }
        let worker = task_worker(self.config, record.status())?.name.clone();
        let turn_id = TurnId::generate();
        self.client_state
            .write_turn_prompt(task_id, turn_id, &message)?;
        let replacement = record.with_runner(None)?;
        self.client_state.update_task(replacement)?;
        let entry = self.enqueue_followup(&record, turn_id, worker)?;
        self.start_runner(task_id, turn_id)?;
        let mut report = self.report_for(task_id)?;
        report.events.push(event_task_created(&report));
        if attached {
            let outcome = TurnRunner::new(
                self.runner,
                self.config,
                self.paths,
                self.client_state,
                self.executor,
            )
            .run(task_id, entry.job_id(), Some(stdout))?;
            report.status = outcome.status().clone();
            report.events.extend(outcome.events().iter().cloned());
        }
        Ok(report)
    }

    pub fn cancel(&self, task_id: TaskId) -> Result<TaskReport, WorkerError> {
        self.reconcile_runners()?;
        let record = self.client_state.load_task(task_id)?;
        if let Some(turn_id) = pending_turn_id(record.status())
            && let Some(job) = self.client_state.load_job_optional(turn_id)?
        {
            let worker = task_worker(self.config, record.status())?;
            let response = RemoteJobClient::new(self.runner)
                .cancel(worker, &crate::job::CancelRequest::from_local_record(&job)?)?;
            self.client_state
                .update_observation(turn_id, response.status().status().clone())?;
            self.client_state
                .update_task(record.with_status(TaskStatus::new(
                    TaskState::Open,
                    Some(TaskOutcome::Cancelled),
                    response.status().meta().worker_name().to_owned().into(),
                    record.status().session_present(),
                    record.status().head_oid().cloned(),
                    record.status().summary().map(str::to_owned),
                    record.status().questions().to_vec(),
                    record.status().files_changed().to_vec(),
                    record.status().diff_stat().map(str::to_owned),
                    record.status().turns().to_vec(),
                    current_time_millis()?,
                )?)?)?;
            return self.report_for(task_id);
        }
        if let Some(turn_id) = pending_turn_id(record.status()) {
            if self.client_state.queue_entry(turn_id)?.is_some() {
                let _ = self.client_state.remove_queued(turn_id)?;
                let status = abandoned_status(record.status(), "CANCELLED")?;
                self.client_state.update_task(
                    record
                        .with_status(status)?
                        .with_abandon_code(Some("CANCELLED".to_owned()))?,
                )?;
            }
        }
        self.report_for(task_id)
    }

    pub fn wait(
        &self,
        selector: WaitSelector,
        timeout: Option<Duration>,
    ) -> Result<WaitReport, WorkerError> {
        let started = SystemTime::now();
        let task_ids = match selector {
            WaitSelector::Task(task_id) => vec![task_id],
            WaitSelector::Run(run_id) => self.client_state.load_run(run_id)?.task_ids().to_vec(),
        };
        loop {
            self.reconcile_runners()?;
            let records = task_ids
                .iter()
                .map(|task_id| self.client_state.load_task(*task_id))
                .collect::<Result<Vec<_>, _>>()?;
            if records
                .iter()
                .all(|record| is_wait_terminal(record.status().state()))
            {
                let exit_code = if records.iter().any(|record| {
                    matches!(
                        record.status().last_outcome(),
                        Some(
                            TaskOutcome::Blocked
                                | TaskOutcome::Failed { .. }
                                | TaskOutcome::Cancelled
                                | TaskOutcome::TimedOut
                                | TaskOutcome::Lost
                        )
                    )
                }) {
                    1
                } else {
                    0
                };
                return Ok(WaitReport {
                    task_ids,
                    exit_code,
                });
            }
            if timeout.is_some_and(|limit| started.elapsed().is_ok_and(|elapsed| elapsed >= limit))
            {
                return Err(WorkerError::Task {
                    code: "WAIT_TIMEOUT",
                    message: "task wait timed out without cancelling the task".into(),
                });
            }
            std::thread::sleep(WAIT_POLL.min(WAIT_MAX_POLL));
        }
    }

    pub fn batch(
        &self,
        file: &Path,
        run_name: Option<String>,
        max_parallel: Option<u32>,
        _stdout: &mut dyn Write,
    ) -> Result<RunReport, WorkerError> {
        self.reconcile_runners()?;
        let contents = std::fs::read_to_string(file).map_err(WorkerError::Io)?;
        let batch: BatchFile = toml::from_str(&contents).map_err(|error| {
            task_error(
                "TASK_CONFIG_INVALID",
                format!("invalid batch file: {error}"),
            )
        })?;
        if batch.tasks.is_empty() {
            return Err(task_error("TASK_CONFIG_INVALID", "batch has no tasks"));
        }
        let max_parallel = max_parallel.unwrap_or(1);
        if max_parallel == 0 {
            return Err(task_error(
                "TASK_CONFIG_INVALID",
                "batch max_parallel must be positive",
            ));
        }
        let run_id = RunId::generate();
        let created = current_time_millis()?;
        let mut requests = Vec::with_capacity(batch.tasks.len());
        for task in &batch.tasks {
            requests.push((
                self.batch_request(&batch.defaults, task, run_id)?,
                task.title.clone(),
            ));
        }
        let task_ids = requests
            .iter()
            .map(|_| TaskId::generate())
            .collect::<Vec<_>>();
        let run = RunRecord::new(run_id, run_name, task_ids.clone(), max_parallel, created)?;
        self.client_state.create_run(run)?;
        for ((request, title), task_id) in requests.into_iter().zip(task_ids.iter().copied()) {
            let _ = self.submit_with_ids(
                request,
                Some(task_id),
                None,
                title,
                &mut io::sink(),
                &mut io::sink(),
            )?;
        }
        Ok(RunReport { run_id, task_ids })
    }

    fn batch_request(
        &self,
        defaults: &BatchDefaults,
        task: &BatchTask,
        run_id: RunId,
    ) -> Result<TaskSubmitRequest, WorkerError> {
        let agent = parse_agent(task.agent.as_deref().unwrap_or(&defaults.agent))?;
        if agent != AgentKind::Codex {
            return Err(task_error(
                "AGENT_UNSUPPORTED",
                "this client accepts Codex tasks only",
            ));
        }
        let timeout = task
            .timeout
            .as_deref()
            .or(defaults.timeout.as_deref())
            .map(|value| humantime::parse_duration(value))
            .transpose()
            .map_err(|_| task_error("TASK_CONFIG_INVALID", "batch timeout is invalid"))?;
        let mut limits = TaskLimits::default();
        if let Some(timeout) = timeout {
            limits = TaskLimits::new(
                TurnLimits::new(
                    timeout.as_millis().try_into().map_err(|_| {
                        task_error("TASK_CONFIG_INVALID", "batch timeout is too large")
                    })?,
                    task.max_turns.or(defaults.max_turns),
                    task.max_budget_usd_cents.or(defaults.max_budget_usd_cents),
                )
                .map_err(WorkerError::from)?,
                task.max_followups
                    .or(defaults.max_followups)
                    .unwrap_or(limits.max_followups),
            )?;
        }
        Ok(TaskSubmitRequest {
            agent,
            model: task.model.clone().or_else(|| defaults.model.clone()),
            prompt: task.prompt.clone(),
            project: self.current_project()?,
            base: task.base.clone().unwrap_or_else(|| defaults.base.clone()),
            wip: task.wip.unwrap_or(defaults.wip),
            cli_includes: Vec::new(),
            limits,
            close_policy: parse_close_policy(
                task.close_on.as_deref().or(defaults.close_on.as_deref()),
            )?,
            env_profile: task
                .env_profile
                .clone()
                .or_else(|| defaults.env_profile.clone()),
            preference: match task.worker.clone().or_else(|| defaults.worker.clone()) {
                Some(worker) => WorkerPreference::Pinned { worker },
                None => WorkerPreference::Automatic,
            },
            wait_for_capacity: true,
            attached: false,
            run_id: Some(run_id),
        })
    }

    fn enqueue_followup(
        &self,
        record: &LocalTaskRecord,
        turn_id: TurnId,
        worker: String,
    ) -> Result<QueueEntry, WorkerError> {
        let now = current_time_millis()?;
        let command = CommandSpec::argv(vec![TASK_COMMAND.to_owned()])?;
        let preference = WorkerPreference::Pinned { worker };
        let run = record
            .meta()
            .run_id()
            .map(|run_id| self.client_state.load_run(run_id))
            .transpose()?
            .map(|run| {
                crate::job::RunId::new(run.run_id().to_string())
                    .and_then(|job_run_id| QueueRunReference::new(job_run_id, run.max_parallel()))
            })
            .transpose()?;
        self.client_state.enqueue(QueueEntry::new(
            turn_id,
            self.client_state.client_id(),
            record.meta().project_id().to_owned(),
            record.meta().worktree_id().to_owned(),
            command.summary()?,
            task_requirements(&[], record.meta().agent(), record.meta().env_profile()),
            preference,
            QueueEntryKind::TaskTurn,
            run,
            current_process_identity()?,
            now,
        )?)
    }

    fn refresh_task_status(&self, record: &LocalTaskRecord) -> Result<(), WorkerError> {
        let Some(worker_name) = record.status().worker() else {
            return Ok(());
        };
        let Some(worker) = self.config.worker(worker_name) else {
            return Ok(());
        };
        let response = RemoteJobClient::new(self.runner).task_status(
            worker,
            &crate::task_store::TaskStatusRequest::new(
                record.meta().project_id(),
                record.meta().task_id(),
            ),
        )?;
        let observed_at = response.status().updated_at_millis();
        self.client_state.update_task(
            record
                .with_status(response.status().clone())?
                .with_status_observed_at(Some(observed_at))?,
        )
    }

    fn enqueue_missing_turn(
        &self,
        record: &LocalTaskRecord,
        owner: ProcessIdentity,
    ) -> Result<QueueEntry, WorkerError> {
        let turn_id = pending_turn_id(record.status())
            .or_else(|| {
                self.client_state
                    .turn_ids_for_task(record.meta().task_id())
                    .ok()?
                    .last()
                    .copied()
            })
            .ok_or_else(|| task_error("TASK_INCONSISTENT", "queued task has no turn prompt"))?;
        let project = ProjectState::load(self.runner, &self.current_project()?, &[])?;
        require_project_match(&project, record.meta())?;
        let command = CommandSpec::argv(vec![TASK_COMMAND.to_owned()])?;
        let run = record
            .meta()
            .run_id()
            .map(|run_id| self.client_state.load_run(run_id))
            .transpose()?
            .map(|run| {
                crate::job::RunId::new(run.run_id().to_string())
                    .and_then(|job_run_id| QueueRunReference::new(job_run_id, run.max_parallel()))
            })
            .transpose()?;
        let preference = record.status().worker().map_or_else(
            || record.preference(),
            |worker| WorkerPreference::Pinned {
                worker: worker.to_owned(),
            },
        );
        self.client_state.enqueue(QueueEntry::new(
            turn_id,
            self.client_state.client_id(),
            record.meta().project_id().to_owned(),
            record.meta().worktree_id().to_owned(),
            command.summary()?,
            task_requirements(
                &project.requirements,
                record.meta().agent(),
                record.meta().env_profile(),
            ),
            preference,
            QueueEntryKind::TaskTurn,
            run,
            owner,
            current_time_millis()?,
        )?)
    }

    fn start_runner(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<RunnerIdentity, WorkerError> {
        let identity = self.executor.start(self.paths, task_id, turn_id)?;
        self.client_state
            .record_runner(task_id, Some(identity.clone()))?;
        if let Err(error) = self
            .client_state
            .adopt_row(turn_id, identity.process_identity())
        {
            // The runner record and queue owner are one hand-off.  If the
            // queue row disappeared or was concurrently claimed, do not
            // leave reconciliation believing a live runner owns it.
            let _ = self.client_state.record_runner(task_id, None);
            return Err(error);
        }
        Ok(identity)
    }

    fn start_oldest_parked_runner(&self, owner: ProcessIdentity) -> Result<bool, WorkerError> {
        let Some(entry) = self.client_state.unpark_oldest(owner)? else {
            return Ok(false);
        };
        let task_id = self
            .client_state
            .task_id_for_turn(entry.job_id())?
            .ok_or_else(|| {
                task_error("TASK_INCONSISTENT", "parked task turn has no task record")
            })?;
        if let Err(error) = self.start_runner(task_id, entry.job_id()) {
            if let Ok(record) = self.client_state.load_task(task_id)
                && let Ok(status) = abandoned_status(record.status(), "RUNNER_HANDOFF_FAILED")
                && let Ok(replacement) = record.with_status(status).and_then(|record| {
                    record.with_abandon_code(Some("RUNNER_HANDOFF_FAILED".to_owned()))
                })
            {
                let _ = self.client_state.update_task(replacement);
            }
            let _ = self.client_state.remove_queued(entry.job_id());
            let _ = self
                .client_state
                .remove_turn_prompt(task_id, entry.job_id());
            if let Ok(record) = self.client_state.load_task(task_id) {
                let _ = self.release_task_base(&record);
                let _ = self.client_state.record_runner(task_id, None);
            }
            return Err(task_error(
                "RUNNER_HANDOFF_FAILED",
                format!("runner handoff failed: {error}"),
            ));
        }
        Ok(true)
    }

    fn report_for(&self, task_id: TaskId) -> Result<TaskReport, WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        Ok(TaskReport {
            task_id,
            run_id: record.meta().run_id(),
            status: record.status().clone(),
            events: Vec::new(),
            runner: self.client_state.runner_liveness(task_id)?,
        })
    }

    fn report_for_readonly(&self, record: &LocalTaskRecord) -> Result<TaskReport, WorkerError> {
        let mut status = record.status().clone();
        if matches!(status.state(), TaskState::Active | TaskState::Open)
            && let Some(worker_name) = status.worker()
            && let Some(worker) = self.config.worker(worker_name)
            && let Ok(remote) = RemoteJobClient::new(self.runner).task_status(
                worker,
                &crate::task_store::TaskStatusRequest::new(
                    record.meta().project_id(),
                    record.meta().task_id(),
                ),
            )
        {
            status = remote.status().clone();
        }
        Ok(TaskReport {
            task_id: record.meta().task_id(),
            run_id: record.meta().run_id(),
            status,
            events: Vec::new(),
            runner: self.client_state.runner_liveness(record.meta().task_id())?,
        })
    }

    fn fail_handoff(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        transfer: TransferRepo,
        error: WorkerError,
    ) -> WorkerError {
        let _ = self.client_state.remove_queued(turn_id);
        let _ = self.client_state.remove_turn_prompt(task_id, turn_id);
        let _ = self.client_state.record_runner(task_id, None);
        if let Ok(record) = self.client_state.load_task(task_id)
            && let Ok(status) = abandoned_status(record.status(), "RUNNER_HANDOFF_FAILED")
            && let Ok(record) = record.with_status(status)
            && let Ok(record) = record.with_abandon_code(Some("RUNNER_HANDOFF_FAILED".to_owned()))
        {
            let _ = self.client_state.update_task(record);
        }
        let _ = transfer.release_base(self.runner, task_id);
        task_error(
            "RUNNER_HANDOFF_FAILED",
            format!("runner handoff failed: {error}"),
        )
    }

    fn release_task_base(&self, record: &LocalTaskRecord) -> Result<(), WorkerError> {
        let project = ProjectState::load(self.runner, &self.current_project()?, &[])?;
        require_project_match(&project, record.meta())?;
        let transfer =
            TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)?;
        transfer.release_base(self.runner, record.meta().task_id())
    }

    fn live_runner_count(&self) -> Result<usize, WorkerError> {
        Ok(self
            .client_state
            .list_tasks()?
            .into_iter()
            .filter(|task| {
                !matches!(
                    task.status().state(),
                    TaskState::Closed | TaskState::Abandoned | TaskState::Lost
                )
            })
            .filter_map(|task| {
                self.client_state
                    .runner_liveness(task.meta().task_id())
                    .ok()
            })
            .flatten()
            .filter(|state| *state == RunnerState::Live)
            .count())
    }

    fn observe_admission(
        &self,
        preference: &WorkerPreference,
    ) -> Result<Vec<CandidateObservation>, WorkerError> {
        self.config
            .workers
            .iter()
            .filter(|worker| match preference {
                WorkerPreference::Automatic => true,
                WorkerPreference::Pinned { worker: pinned } => worker.name == *pinned,
            })
            .map(|worker| {
                let one = Config {
                    version: self.config.version,
                    workers: vec![worker.clone()],
                };
                let cached = self.client_state.admission_observation(
                    &worker.name,
                    current_time_millis()?,
                    || {
                        let health = WorkersService::new(SshTransport::new(self.runner))
                            .inspect(&one)
                            .workers
                            .into_iter()
                            .next()
                            .ok_or_else(|| {
                                WorkerError::Protocol("worker probe was empty".into())
                            })?;
                        let candidate = SchedulerProbeAdapter::observations(
                            &one,
                            std::slice::from_ref(&health),
                        )?
                        .into_iter()
                        .next()
                        .ok_or_else(|| WorkerError::Protocol("worker probe was empty".into()))?;
                        AdmissionObservation::new(
                            candidate.worker_name().to_owned(),
                            candidate.ready(),
                            candidate.slot(),
                            candidate.capabilities().to_vec(),
                            candidate.available_memory_bytes(),
                            candidate.free_disk_bytes(),
                            current_time_millis()?,
                        )
                    },
                )?;
                CandidateObservation::new(
                    cached.observation().worker_name().to_owned(),
                    cached.observation().ready(),
                    cached.observation().slot(),
                    cached.observation().capabilities().to_vec(),
                    cached.observation().available_memory_bytes(),
                    cached.observation().free_disk_bytes(),
                )
                .map_err(|_| {
                    WorkerError::Protocol("cached scheduler observation is invalid".into())
                })
            })
            .collect()
    }

    fn read_runner_log(&self, task_id: TaskId, turn_id: TurnId) -> Result<Vec<u8>, WorkerError> {
        let root = crate::rooted_fs::RootedDir::open(&self.paths.state).map_err(WorkerError::Io)?;
        let runners = root
            .open_child_directory(&relative_path("runners")?, false)
            .map_err(WorkerError::Io)?;
        let task = runners
            .open_child_directory(&relative_path(&task_id.to_string())?, false)
            .map_err(WorkerError::Io)?;
        task.read_private_regular(&format!("{turn_id}.log"), 8 * 1024 * 1024)
            .map_err(WorkerError::Io)
    }

    fn current_project(&self) -> Result<PathBuf, WorkerError> {
        std::env::current_dir().map_err(WorkerError::Io)
    }
}

fn event_task_created(report: &TaskReport) -> serde_json::Value {
    serde_json::json!({
        "type": "task_created",
        "protocol_version": crate::protocol::PROTOCOL_VERSION,
        "task_id": report.task_id().to_string(),
        "run_id": report.run_id().map(|id| id.to_string()),
    })
}

fn select_turn(status: &TaskStatus, turn: Option<u32>) -> Result<&TurnSummary, WorkerError> {
    turn.map_or_else(
        || {
            status
                .turns()
                .last()
                .ok_or_else(|| task_error("TASK_LOG_NOT_FOUND", "task has no turn logs"))
        },
        |number| {
            status
                .turns()
                .iter()
                .find(|turn| turn.turn_number() == number)
                .ok_or_else(|| task_error("TASK_LOG_NOT_FOUND", "turn log was not found"))
        },
    )
}

fn render_agent_log(
    bytes: &[u8],
    agent: AgentKind,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    let text = String::from_utf8_lossy(bytes);
    let adapter = adapter_for(agent);
    for line in text.lines() {
        if let Some(event) = adapter.parse_event(line) {
            let rendered = match event {
                crate::agent::AgentEvent::AssistantMessage { text } => text,
                crate::agent::AgentEvent::ToolCall { name, summary } => {
                    format!("{name}: {summary}")
                }
                crate::agent::AgentEvent::FileChange { paths } => paths.join(", "),
                crate::agent::AgentEvent::Command { summary, exit_code } => {
                    format!("{summary} ({exit_code:?})")
                }
                crate::agent::AgentEvent::Usage { .. } => "usage".into(),
                crate::agent::AgentEvent::SessionStarted { session_ref } => {
                    format!("session {session_ref}")
                }
                crate::agent::AgentEvent::TurnEnd { reason } => reason,
            };
            writeln!(stdout, "{rendered}")?;
        }
    }
    Ok(())
}

fn task_worker<'a>(
    config: &'a Config,
    status: &TaskStatus,
) -> Result<&'a WorkerEntry, WorkerError> {
    let name = status
        .worker()
        .ok_or_else(|| task_error("WORKER_NOT_FOUND", "task has not selected a worker"))?;
    config
        .worker(name)
        .ok_or_else(|| task_error("WORKER_NOT_FOUND", "recorded worker is not configured"))
}

fn require_project_match(project: &ProjectState, meta: &TaskMeta) -> Result<(), WorkerError> {
    if project.context.project_id != meta.project_id()
        || project.context.worktree_id != meta.worktree_id()
    {
        return Err(task_error(
            "PROJECT_MISMATCH",
            "current project is not the task's project",
        ));
    }
    Ok(())
}

fn pending_turn_id(status: &TaskStatus) -> Option<TurnId> {
    status
        .turns()
        .last()
        .filter(|turn| turn.terminal().is_none())
        .map(TurnSummary::turn_id)
}

fn is_wait_terminal(state: TaskState) -> bool {
    matches!(
        state,
        TaskState::Closed | TaskState::Open | TaskState::Abandoned | TaskState::Lost
    )
}

fn abandoned_status(status: &TaskStatus, reason: &str) -> Result<TaskStatus, WorkerError> {
    TaskStatus::new(
        TaskState::Abandoned,
        Some(TaskOutcome::failed(reason)),
        status.worker().map(str::to_owned),
        status.session_present(),
        status.head_oid().cloned(),
        status.summary().map(str::to_owned),
        status.questions().to_vec(),
        status.files_changed().to_vec(),
        status.diff_stat().map(str::to_owned),
        status.turns().to_vec(),
        current_time_millis()?,
    )
}

fn task_requirements(project: &[String], agent: AgentKind, profile: Option<&str>) -> Vec<String> {
    let mut requirements = project.to_vec();
    let name = agent_name(agent);
    let requirement = profile.map_or_else(
        || format!("{TASK_CAPABILITY_PREFIX}{name}"),
        |profile| format!("{TASK_CAPABILITY_PREFIX}{name}@{profile}"),
    );
    if !requirements.iter().any(|item| item == &requirement) {
        requirements.push(requirement);
    }
    requirements
}

fn effective_task_limits(
    requested: &TaskLimits,
    settings: &TaskSettings,
) -> Result<TaskLimits, WorkerError> {
    let defaults = TaskLimits::default();
    let timeout_millis = if requested.turn.timeout_millis == defaults.turn.timeout_millis {
        u64::try_from(settings.timeout.as_millis()).map_err(|_| {
            task_error(
                "TASK_CONFIG_INVALID",
                "task timeout is outside the supported range",
            )
        })?
    } else {
        requested.turn.timeout_millis
    };
    let turn = TurnLimits::new(
        timeout_millis,
        requested.turn.max_turns,
        requested.turn.max_budget_usd_cents,
    )
    .map_err(|error| task_error("TASK_CONFIG_INVALID", error.to_string()))?;
    let max_followups = if requested.max_followups == defaults.max_followups {
        settings.max_followups
    } else {
        requested.max_followups
    };
    TaskLimits::new(turn, max_followups)
}

fn compose_turn_prompt(
    task_id: TaskId,
    turn_number: u32,
    base_oid: &BaseOid,
    branch: Option<&str>,
    user_prompt: &str,
    resume: bool,
) -> String {
    let branch = branch.unwrap_or("(detached HEAD)");
    let continuation = if resume {
        "This is a follow-up turn. Continue from the existing task workspace and bound agent session.\n\n"
    } else {
        ""
    };
    format!(
        "mac-worker task context\n\nTask ID: {task_id}\nTurn: {turn_number}\nBase commit: {base_oid}\nBase branch: {branch}\n\n{continuation}You are working in an isolated task worktree. Make changes only there. Do not switch branches, push, or modify the user's repository. Leave your changes in the task worktree for publication.\n\nUser request:\n{user_prompt}\n"
    )
}

fn permission_policy(settings: &TaskSettings, agent: AgentKind) -> PermissionPolicy {
    match settings
        .permissions
        .get(agent_name(agent))
        .map(String::as_str)
    {
        Some("unattended") => PermissionPolicy::Unattended,
        _ => PermissionPolicy::Workspace,
    }
}

fn parse_agent(value: &str) -> Result<AgentKind, WorkerError> {
    match value {
        "codex" => Ok(AgentKind::Codex),
        "claude" => Ok(AgentKind::Claude),
        "cursor" => Ok(AgentKind::Cursor),
        "opencode" => Ok(AgentKind::Opencode),
        _ => Err(task_error("AGENT_UNSUPPORTED", "unknown agent")),
    }
}

fn agent_name(agent: AgentKind) -> &'static str {
    match agent {
        AgentKind::Codex => "codex",
        AgentKind::Claude => "claude",
        AgentKind::Cursor => "cursor",
        AgentKind::Opencode => "opencode",
    }
}

fn parse_close_policy(value: Option<&str>) -> Result<ClosePolicy, WorkerError> {
    match value.unwrap_or("never") {
        "done" => Ok(ClosePolicy::Done),
        "never" => Ok(ClosePolicy::Never),
        _ => Err(task_error(
            "TASK_CONFIG_INVALID",
            "close_on must be done or never",
        )),
    }
}

fn validate_preference(config: &Config, preference: &WorkerPreference) -> Result<(), WorkerError> {
    if let WorkerPreference::Pinned { worker } = preference
        && config.worker(worker).is_none()
    {
        return Err(task_error("WORKER_NOT_FOUND", "worker is not configured"));
    }
    Ok(())
}

fn validate_prompt(prompt: &str) -> Result<(), WorkerError> {
    if prompt.is_empty() {
        return Err(task_error("TASK_CONFIG_INVALID", "prompt is empty"));
    }
    if prompt.len() > crate::task::MAX_PROMPT_BYTES {
        return Err(task_error("TASK_PROMPT_TOO_LARGE", "prompt is too large"));
    }
    Ok(())
}

fn capacity_busy() -> WorkerError {
    WorkerError::Capacity {
        code: "CAPACITY_BUSY",
        message: "no eligible worker currently has an available heavy slot".into(),
    }
}

fn capability_missing(worker: &str, missing: &[String]) -> WorkerError {
    WorkerError::Capacity {
        code: "CAPABILITY_MISSING",
        message: format!(
            "pinned worker {worker} is missing required capabilities: {}",
            missing.join(", ")
        ),
    }
}

fn task_error(code: &'static str, message: impl Into<String>) -> WorkerError {
    WorkerError::Task {
        code,
        message: message.into(),
    }
}

fn current_time_millis() -> Result<u64, WorkerError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkerError::Io(io::Error::other("system clock predates Unix epoch")))?
        .as_millis();
    u64::try_from(value)
        .map_err(|_| WorkerError::Io(io::Error::other("system clock is out of range")))
}

fn current_process_identity() -> Result<ProcessIdentity, WorkerError> {
    let pid = std::process::id();
    SystemProcessInspector
        .identity_for_pid(pid)
        .or_else(|_| fallback_process_identity(pid))
}

fn fallback_process_identity(pid: u32) -> Result<ProcessIdentity, WorkerError> {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkerError::Io(io::Error::other("system clock predates Unix epoch")))?
        .as_micros();
    ProcessIdentity::new(
        pid,
        u64::try_from(micros)
            .map_err(|_| WorkerError::Io(io::Error::other("process start time is out of range")))?,
    )
}

fn relative_path(value: &str) -> Result<crate::inputs::RelativePath, WorkerError> {
    crate::inputs::RelativePath::parse(value.as_bytes())
        .map_err(|_| task_error("TASK_STATE_INVALID", "task state path is invalid"))
}

fn default_agent_name() -> String {
    "codex".into()
}

fn default_base_name() -> String {
    "HEAD".into()
}
