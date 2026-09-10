use std::{
    borrow::Cow,
    collections::BTreeSet,
    io::{self, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer};

use crate::{
    agent::{AgentKind, PermissionPolicy, TurnLimits, result_instruction},
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    error::WorkerError,
    git_transport::GitTransport,
    job::{
        CommandSpec, ProcessIdentity, QueueEntry, QueueEntryKind, QueueRunReference, QueueSnapshot,
        QueueState,
    },
    paths::PathLayout,
    process::ProcessRunner,
    project_config::TaskSettings,
    project_state::ProjectState,
    scheduler::{CandidateObservation, SchedulerPolicy, Selection, WorkerPreference},
    supervisor::{ProcessObservation, SystemProcessInspector},
    task::{
        BaseOid, BranchName, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, PushTarget,
        RunId, RunRecord, RunnerIdentity, RunnerState, TaskId, TaskLimits, TaskMeta, TaskMetaInput,
        TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
    },
    task_view::{
        TaskFreshness, TaskListProjection, TaskListRow, TaskRunProjection, TaskViewError,
        filter_task_list, project_task_list_with_blocking_codes,
    },
    transfer::RemoteJobClient,
    transfer_repo::TransferRepo,
    turn_runner::{DetachedRunnerExecutor, RunnerExecutor, TurnRunner, adopt_row_with_retry},
};

const TASK_COMMAND: &str = "task-turn";
const TASK_CAPABILITY_PREFIX: &str = "agent:";
const DEFAULT_GIT_NAME: &str = "mac-worker";
const DEFAULT_GIT_EMAIL: &str = "mac-worker@localhost";
const SUBMISSION_ROLLBACK_INCOMPLETE: &str = "SUBMISSION_ROLLBACK_INCOMPLETE";
const WAIT_POLL: Duration = Duration::from_millis(100);
const WAIT_MAX_POLL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct TaskSubmitRequest {
    pub agent: AgentKind,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub prompt: String,
    pub project: PathBuf,
    pub base: String,
    pub wip: bool,
    pub source: Option<String>,
    pub publish: Option<Vec<String>>,
    pub publish_branch: Option<String>,
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
    /// Canonical `TaskOutcome::kind` name; `needs_input` is the orchestrator's
    /// query for tasks waiting on an answer.
    pub outcome: Option<String>,
    pub full: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskReport {
    task_id: TaskId,
    run_id: Option<RunId>,
    status: TaskStatus,
    warnings: Vec<String>,
    events: Vec<serde_json::Value>,
    runner: Option<RunnerState>,
    exit_code: Option<u8>,
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

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub fn events(&self) -> &[serde_json::Value] {
        &self.events
    }

    pub fn runner(&self) -> Option<RunnerState> {
        self.runner
    }

    pub fn exit_code(&self) -> Option<u8> {
        self.exit_code
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskListReport {
    projection: TaskListProjection,
}

impl TaskListReport {
    pub fn projection(&self) -> &TaskListProjection {
        &self.projection
    }

    pub fn tasks(&self) -> &[TaskListRow] {
        &self.projection.tasks
    }

    pub fn runs(&self) -> &[TaskRunProjection] {
        &self.projection.runs
    }

    pub fn progress(&self) -> crate::task::RunProgress {
        self.projection.progress
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
    repaired_rows: usize,
}

impl ReconcileReport {
    pub fn replaced_runners(&self) -> usize {
        self.replaced_runners
    }

    pub fn started_runners(&self) -> usize {
        self.started_runners
    }

    pub fn repaired_rows(&self) -> usize {
        self.repaired_rows
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

#[derive(Debug, Clone)]
pub struct BatchFile {
    pub version: u32,
    pub defaults: BatchDefaults,
    pub tasks: Vec<BatchTask>,
}

impl<'de> Deserialize<'de> for BatchFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(default = "default_batch_version")]
            version: u32,
            #[serde(default)]
            defaults: Option<BatchDefaults>,
            #[serde(default)]
            agent: Option<String>,
            #[serde(default)]
            model: Option<String>,
            #[serde(default)]
            effort: Option<String>,
            #[serde(default)]
            base: Option<String>,
            #[serde(default)]
            wip: Option<bool>,
            #[serde(default)]
            timeout: Option<String>,
            #[serde(default)]
            max_turns: Option<u32>,
            #[serde(default)]
            max_budget_usd_cents: Option<u64>,
            #[serde(default)]
            max_followups: Option<u32>,
            #[serde(default)]
            close_on: Option<String>,
            #[serde(default)]
            env_profile: Option<String>,
            #[serde(default)]
            worker: Option<String>,
            #[serde(default)]
            source: Option<String>,
            #[serde(default)]
            publish: Option<Vec<String>>,
            #[serde(default)]
            publish_branch: Option<String>,
            tasks: Vec<BatchTask>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let has_flat_defaults = [
            wire.agent.is_some(),
            wire.model.is_some(),
            wire.effort.is_some(),
            wire.base.is_some(),
            wire.wip.is_some(),
            wire.timeout.is_some(),
            wire.max_turns.is_some(),
            wire.max_budget_usd_cents.is_some(),
            wire.max_followups.is_some(),
            wire.close_on.is_some(),
            wire.env_profile.is_some(),
            wire.worker.is_some(),
            wire.source.is_some(),
            wire.publish.is_some(),
            wire.publish_branch.is_some(),
        ]
        .into_iter()
        .any(|present| present);
        if wire.defaults.is_some() && has_flat_defaults {
            return Err(serde::de::Error::custom(
                "batch defaults must use either a [defaults] table or top-level keys, not both",
            ));
        }

        let mut defaults = wire.defaults.unwrap_or_default();
        if let Some(agent) = wire.agent {
            defaults.agent = agent;
        }
        if wire.model.is_some() {
            defaults.model = wire.model;
        }
        if wire.effort.is_some() {
            defaults.effort = wire.effort;
        }
        if let Some(base) = wire.base {
            defaults.base = base;
        }
        if let Some(wip) = wire.wip {
            defaults.wip = wip;
        }
        if wire.timeout.is_some() {
            defaults.timeout = wire.timeout;
        }
        if wire.max_turns.is_some() {
            defaults.max_turns = wire.max_turns;
        }
        if wire.max_budget_usd_cents.is_some() {
            defaults.max_budget_usd_cents = wire.max_budget_usd_cents;
        }
        if wire.max_followups.is_some() {
            defaults.max_followups = wire.max_followups;
        }
        if wire.close_on.is_some() {
            defaults.close_on = wire.close_on;
        }
        if wire.env_profile.is_some() {
            defaults.env_profile = wire.env_profile;
        }
        if wire.worker.is_some() {
            defaults.worker = wire.worker;
        }
        if let Some(source) = wire.source {
            defaults.source = source;
        }
        if let Some(publish) = wire.publish {
            defaults.publish = publish;
        }
        if wire.publish_branch.is_some() {
            defaults.publish_branch = wire.publish_branch;
        }

        Ok(Self {
            version: wire.version,
            defaults,
            tasks: wire.tasks,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchDefaults {
    #[serde(default = "default_agent_name")]
    pub agent: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
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
    #[serde(default = "default_source_name")]
    pub source: String,
    #[serde(default = "default_publish_modes")]
    pub publish: Vec<String>,
    #[serde(default)]
    pub publish_branch: Option<String>,
}

impl Default for BatchDefaults {
    fn default() -> Self {
        Self {
            agent: default_agent_name(),
            model: None,
            effort: None,
            base: default_base_name(),
            wip: false,
            timeout: None,
            max_turns: None,
            max_budget_usd_cents: None,
            max_followups: None,
            close_on: None,
            env_profile: None,
            worker: None,
            source: default_source_name(),
            publish: default_publish_modes(),
            publish_branch: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchTask {
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub prompt_file: Option<PathBuf>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
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
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub publish: Option<Vec<String>>,
    #[serde(default)]
    pub publish_branch: Option<String>,
}

pub struct TaskClient<'a> {
    pub(crate) runner: &'a dyn ProcessRunner,
    pub(crate) config: &'a Config,
    pub(crate) paths: &'a PathLayout,
    pub(crate) client_state: &'a ClientStateStore,
    pub(crate) executor: &'a dyn RunnerExecutor,
    pub(crate) herdr_notifier: Option<crate::herdr::HerdrSocket>,
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
            herdr_notifier: None,
        }
    }

    /// Herdr session the runners this client starts inline notify about
    /// finished turns.  Detached runners receive theirs from the CLI.
    pub fn with_herdr_notifier(mut self, socket: Option<crate::herdr::HerdrSocket>) -> Self {
        self.herdr_notifier = socket;
        self
    }

    pub fn with_detached_executor(
        runner: &'a dyn ProcessRunner,
        config: &'a Config,
        paths: &'a PathLayout,
        client_state: &'a ClientStateStore,
    ) -> Self {
        Self::new(runner, config, paths, client_state, &DetachedRunnerExecutor)
    }

    pub fn default_task_agent(&self, project: &Path) -> Result<AgentKind, WorkerError> {
        let settings = ProjectState::load(self.runner, project, &[])?.settings.task;
        let agent = parse_agent(&settings.default_agent)?;
        validate_task_agent(agent)?;
        Ok(agent)
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
        validate_task_agent(request.agent)?;
        validate_preference(self.config, &request.preference)?;

        let initial = ProjectState::load(self.runner, &request.project, &request.cli_includes)?;
        let settings = &initial.settings.task;
        let limits = effective_task_limits(&request.limits, settings)?;
        let env_profile = request
            .env_profile
            .clone()
            .or_else(|| settings.env_profile.clone());
        let model = request.model.clone().or_else(|| settings.model.clone());
        let effort = request.effort.clone().or_else(|| settings.effort.clone());
        let publish_names = request.publish.as_deref().unwrap_or(&settings.publish);
        let publish = parse_publish_modes(publish_names)?;
        let source_name = request.source.as_deref().unwrap_or(&settings.source);
        let needs_origin = source_name == "origin" || publish.contains(&PublishMode::Push);
        let origin_url = if needs_origin {
            initial
                .origin
                .clone()
                .ok_or_else(|| task_error("INVALID_ORIGIN", "project origin is not configured"))?
        } else {
            String::new()
        };
        let source = parse_task_source(
            source_name,
            request.wip,
            &origin_url,
            publish.contains(&PublishMode::Push),
        )?;
        let publish_branch = parse_publish_branch(request.publish_branch.as_deref(), &publish)?;
        if request.wip && publish.contains(&PublishMode::Push) {
            return Err(task_error(
                "PUBLISH_REQUIRES_COMMITTED_BASE",
                "publish push requires a committed base",
            ));
        }
        let requirements = task_requirements(
            &initial.requirements,
            request.agent,
            env_profile.as_deref(),
            source.origin_requirement()?.as_deref(),
        );
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
        let reserved_branch = publish.contains(&PublishMode::Push).then(|| {
            publish_branch
                .clone()
                .unwrap_or_else(|| BranchName::for_task(task_id))
        });
        let origin_base = if let TaskSource::Origin { url } = &source {
            let oid = TransferRepo::resolve_base_oid(self.runner, &initial.context, &request.base)?;
            GitTransport::new(self.runner).preflight_origin(url, &oid)?;
            Some(crate::transfer_repo::BaseCommit::from_origin(oid))
        } else {
            None
        };
        let transfer =
            TransferRepo::open_or_create(&self.paths.cache, &initial.context.common_dir)?;
        let base = if let Some(base) = origin_base {
            base
        } else if request.wip {
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
        if !matches!(source, TaskSource::Origin { .. })
            && let Err(error) =
                transfer.check_sensitive_tree(self.runner, base.oid(), &initial.settings)
        {
            let _ = transfer.release_base(self.runner, task_id);
            return Err(error);
        }
        let composed_prompt = compose_turn_prompt(
            task_id,
            1,
            request.agent,
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
        let meta = TaskMeta::new(TaskMetaInput {
            task_id,
            run_id: request.run_id,
            project_id: initial.context.project_id.clone(),
            worktree_id: initial.context.worktree_id.clone(),
            agent: request.agent,
            model,
            effort,
            policy,
            source,
            publish,
            publish_branch: publish_branch.clone(),
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
        )?
        // The intent is written in the initial task-record creation, before
        // any prompt, queue, report, or marker update can fail. Reconciliation
        // may compensate only while this marker remains present.
        .with_submission_intent_turn_id(turn_id)?;
        let record_for_rollback = record.clone();
        let reservation = match (request.run_id, reserved_branch.as_ref()) {
            (Some(run_id), Some(branch)) => match self
                .client_state
                .reserve_run_publish_branch_for_task(run_id, task_id, branch.clone())
            {
                Ok(run) => Some(run),
                Err(error) => {
                    let _ = transfer.release_base(self.runner, task_id);
                    return Err(error);
                }
            },
            _ => None,
        };
        if let Err(error) = self.client_state.create_task(record) {
            if reservation.is_some()
                && let (Some(run_id), Some(branch)) = (request.run_id, reserved_branch.as_ref())
            {
                let _ = self
                    .client_state
                    .release_run_publish_branch_for_task(run_id, task_id, branch);
            }
            let _ = transfer.release_base(self.runner, task_id);
            return Err(error);
        }
        let (mut report, should_start, queued_turn) =
            match (|| -> Result<(TaskReport, bool, TurnId), WorkerError> {
                self.client_state
                    .write_task_project_path(&record_for_rollback, &initial.context.root)?;
                self.client_state
                    .write_turn_prompt(task_id, turn_id, &composed_prompt)?;
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
                let entry = self.client_state.enqueue(entry)?;

                let should_start =
                    request.attached || self.live_runner_count()? < self.config.workers.len();
                self.client_state.submission_report_fault()?;
                let report = self.report_for(task_id)?;
                Ok((report, should_start, entry.job_id()))
            })() {
                Ok(report) => report,
                Err(error) => {
                    self.rollback_submission(turn_id, &record_for_rollback, &transfer);
                    return Err(error);
                }
            };
        // Intent clearance is the submission handoff boundary. Its atomic
        // replacement can publish the clear before the final directory sync
        // reports an error, so this caller cannot safely compensate from its
        // pre-handoff snapshot. Retain the complete durable submission for
        // reconciliation rather than overwriting a runner that may already
        // have adopted the published row.
        self.client_state
            .clear_submission_intent(record_for_rollback.without_submission_intent()?)?;
        if !should_start {
            // Parking publishes eligibility to an independent runner. A park
            // error may therefore mean ownership already transferred, so
            // preserve durable state for reconciliation.
            self.client_state.park_row(queued_turn)?;
        }
        if should_start {
            // A detached child may claim the row as soon as it is started.
            // Once that hand-off is attempted, retain the complete durable
            // submission for reconciliation instead of compensating it away.
            self.start_runner(task_id, turn_id)?;
            report.runner = self.client_state.runner_liveness(task_id)?;
        }
        // The submitter no longer needs the repository after the queue row is
        // handed off.  Its shared lock would not block the attached runner,
        // but a handle that outlives its use is a handle GC has to wait for.
        drop(transfer);
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
            .with_notifier(self.herdr_notifier.clone())
            .run(task_id, turn_id, Some(&mut follow))?;
            report.status = outcome.status().clone();
            report.events.extend(outcome.events().iter().cloned());
            report.exit_code = Some(outcome.exit_code());
        }
        Ok(report)
    }

    pub fn status(&self, task_id: TaskId) -> Result<TaskReport, WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        self.report_for_readonly(&record)
    }

    /// Resolves `--run` as a canonical UUID first, then as an exact stored name.
    ///
    /// `task batch --name` prints a human name next to the UUID. List and wait
    /// have to accept that name without treating a stored UUID-shaped name as
    /// a second way to spell a different run.
    pub fn resolve_run(&self, identifier: &str) -> Result<RunId, WorkerError> {
        self.client_state.resolve_run(identifier)
    }

    pub fn list(&self, filter: TaskListFilter) -> Result<TaskListReport, WorkerError> {
        let records = self
            .client_state
            .list_tasks()?
            .into_iter()
            .filter(|task| {
                filter
                    .run_id
                    .is_none_or(|run| task.meta().run_id() == Some(run))
            })
            .collect::<Vec<_>>();

        let runner_states = records
            .iter()
            .map(|task| {
                Ok((
                    task.meta().task_id(),
                    self.client_state.runner_liveness(task.meta().task_id())?,
                ))
            })
            .collect::<Result<std::collections::HashMap<_, _>, WorkerError>>()?;
        let freshness = records
            .iter()
            .map(|task| (task.meta().task_id(), TaskFreshness::Current))
            .collect();
        let runs = self
            .client_state
            .list_runs()?
            .into_iter()
            .filter(|run| filter.run_id.is_none_or(|run_id| run.run_id() == run_id))
            .collect::<Vec<_>>();
        let blocking_codes = self.client_state.task_blocking_codes(self.config)?;
        let projection = project_task_list_with_blocking_codes(
            &records,
            &runs,
            &runner_states,
            &freshness,
            &blocking_codes,
        )
        .map_err(task_view_error)?;
        Ok(TaskListReport {
            projection: filter_task_list(projection, filter.state, filter.outcome.as_deref()),
        })
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
        let turn_id = if record.status().turns().is_empty() && turn.is_none_or(|number| number == 1)
        {
            // Before host acceptance the first turn has no summary yet. Its
            // private turn directory still identifies an early runner log.
            // UUID order does not establish chronology, so never guess when
            // more than one directory exists.
            match self.client_state.turn_ids_for_task(task_id)?.as_slice() {
                [turn_id] => *turn_id,
                _ => return Err(task_error("TASK_LOG_NOT_FOUND", "task has no turn logs")),
            }
        } else {
            select_turn(record.status(), turn)?.turn_id()
        };
        let mut offset = 0;
        let mut reported_failure = None;
        loop {
            // Observe completion before reading bytes so a final diagnostic
            // written just before the status update is included in this read.
            record = self.client_state.load_task(task_id)?;
            let checkpoint = match crate::runner_log::snapshot(&self.paths.state, task_id, turn_id)
            {
                Err(WorkerError::Io(error)) if error.raw_os_error() == Some(libc::ESTALE) => {
                    continue;
                }
                result => result?,
            };
            let finished = !follow || checkpoint.as_ref().is_some_and(|s| s.completion.is_some());
            let may_initialize = matches!(
                record.status().state(),
                TaskState::Queued | TaskState::Active
            ) && record
                .status()
                .turns()
                .iter()
                .find(|turn| turn.turn_id() == turn_id)
                .is_none_or(|turn| turn.terminal().is_none());
            let failure = if raw {
                None
            } else {
                record
                    .status()
                    .turns()
                    .iter()
                    .find(|turn| turn.turn_id() == turn_id)
                    .and_then(|turn| {
                        checkpoint
                            .as_ref()
                            .and_then(|s| s.completion.as_ref())
                            .map_or_else(
                                || turn_failure(turn),
                                |c| outcome_failure(turn.turn_number(), &c.outcome),
                            )
                    })
            };
            let read = match &checkpoint {
                Some(checkpoint) => crate::runner_log::read_committed(
                    &self.paths.state,
                    task_id,
                    turn_id,
                    checkpoint,
                ),
                None => self.read_runner_log(task_id, turn_id),
            };
            let bytes = match read {
                Ok(mut bytes) => {
                    if let Some(checkpoint) = &checkpoint {
                        let len = usize::try_from(checkpoint.len).map_err(|_| {
                            task_error("LOG_CHECKPOINT_INVALID", "committed length is out of range")
                        })?;
                        if bytes.len() < len {
                            return Err(task_error(
                                "LOG_CHECKPOINT_INVALID",
                                "log is shorter than its committed length",
                            ));
                        }
                        bytes.truncate(len);
                    } else if follow && (!bytes.is_empty() || !may_initialize) {
                        return Err(task_error(
                            "LOG_COMPLETION_UNKNOWN",
                            "legacy log has no provable completion",
                        ));
                    }
                    Some(bytes)
                }
                // A cancelled parked turn may never have started a runner.
                // Its recorded failure remains useful without a log file.
                Err(WorkerError::Io(error))
                    if error.kind() == io::ErrorKind::NotFound && checkpoint.is_none() =>
                {
                    if follow && !may_initialize {
                        return Err(task_error(
                            "LOG_COMPLETION_UNKNOWN",
                            "legacy turn has no provable completion",
                        ));
                    }
                    if !follow && failure.is_none() {
                        return Err(WorkerError::Io(error));
                    }
                    None
                }
                Err(WorkerError::Io(error)) if error.raw_os_error() == Some(libc::ESTALE) => {
                    continue;
                }
                Err(error) => return Err(error),
            };
            if let Some(bytes) = bytes {
                let end = if raw || finished {
                    bytes.len()
                } else {
                    // A poll may end inside JSON or a UTF-8 code point. Leave
                    // the incomplete line for the next poll; the final read
                    // flushes any remaining tail.
                    bytes
                        .iter()
                        .rposition(|byte| *byte == b'\n')
                        .map_or(0, |index| index + 1)
                };
                if raw {
                    stdout.write_all(&bytes[offset..end])?;
                } else {
                    crate::turn_log::render_agent_log(
                        &bytes[offset..end],
                        record.meta().agent(),
                        stdout,
                    )?;
                }
                offset = end;
            }
            // Completed turns normally leave the task Open. Report their
            // failure now while still following later publication updates.
            if failure != reported_failure {
                if let Some(line) = &failure {
                    writeln!(stdout, "{line}")?;
                }
                reported_failure = failure;
            }
            stdout.flush()?;
            if finished {
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
        let project = ProjectState::load_for_task(
            self.runner,
            &self.current_project(Some(&record))?,
            &[],
            record.meta(),
        )?;
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
        if let Some(reason) = self.operator_busy_reason(task_id, &record)? {
            return Err(task_error("TASK_BUSY", reason));
        }
        if let Some(entry) = self.client_state.queue_entry_for_task_turn(task_id)? {
            let mut log =
                crate::runner_log::RunnerLog::open(&self.paths.state, task_id, entry.job_id())?;
            if log.current_entry(self.client_state)?.as_ref() != Some(&entry) {
                return Err(task_error("TASK_BUSY", "task turn changed before close"));
            }
            let record = self.client_state.load_task(task_id)?;
            let retained = self
                .client_state
                .retain_task_turn_cancel(entry.job_id(), current_time_millis()?)?
                .ok_or_else(|| task_error("TASK_INCONSISTENT", "task queue turn disappeared"))?;
            if matches!(retained.state(), QueueState::Dispatching { .. }) {
                return Err(task_error("TASK_BUSY", "task turn is being dispatched"));
            }
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
            self.finish_waiting_cancellation_locked(task_id, &retained, &mut log)?;
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
        let warnings = response.warnings().to_vec();
        self.client_state
            .update_task(record.with_status(response.status().clone())?)?;
        self.release_task_base(&record)?;
        let mut report = self.report_for(task_id)?;
        report.warnings = warnings;
        Ok(report)
    }

    pub fn reconcile_runners(&self) -> Result<ReconcileReport, WorkerError> {
        let mut report = ReconcileReport::default();
        let owner = current_process_identity()?;
        self.client_state.recover_replacement_residue()?;

        // A submit can fail while compensating its locally-created state. The
        // pre-handoff intent is durable from initial task creation; the
        // rollback marker is a stronger terminal form of that intent. Both
        // are safe to compensate only while the turn is still pre-handoff.
        for record in self.client_state.list_tasks()? {
            if !submission_recovery_pending(&record) {
                continue;
            }
            let expected_turn_id = submission_recovery_turn_id(&record);
            self.client_state
                .submission_intent_reconciliation_before_transfer_lock();
            let Ok(transfer) = self.transfer_for_record(&record) else {
                continue;
            };
            self.client_state
                .submission_intent_reconciliation_after_transfer_lock();
            // The transfer lock can wait behind the submitter. Never act on
            // the pre-lock snapshot: the submitter may have cleared its
            // intent and handed this exact row to a live runner meanwhile.
            let Ok(current) = self.client_state.load_task(record.meta().task_id()) else {
                continue;
            };
            if !submission_recovery_pending(&current)
                || expected_turn_id.is_some()
                    && submission_recovery_turn_id(&current) != expected_turn_id
            {
                continue;
            }
            let turn_id = self
                .client_state
                .queue_entry_for_task_turn(current.meta().task_id())?
                .map(|entry| entry.job_id())
                .or(expected_turn_id)
                .or_else(|| {
                    self.client_state
                        .turn_ids_for_task(current.meta().task_id())
                        .ok()?
                        .last()
                        .copied()
                });
            if !self.submission_rollback_is_safe(&current, turn_id)? {
                continue;
            }
            let _ = self.complete_submission_rollback(turn_id, &current, &transfer);
        }

        for entry in self.client_state.queue_snapshot()?.entries() {
            if entry.kind() == QueueEntryKind::TaskTurn
                && entry.is_cancel_requested()
                && !matches!(entry.state(), QueueState::Dispatching { .. })
                && let Some(task_id) = self.client_state.task_id_for_turn(entry.job_id())?
            {
                let record = self.client_state.load_task(task_id)?;
                if !submission_recovery_pending(&record) {
                    self.finish_waiting_cancellation(&record, entry)?;
                    report.repaired_rows += 1;
                }
            }
        }

        // A task-turn row is owned by its runner in both waiting and
        // dispatching states.  A dead owner is normally adopted so the next
        // runner can resume the accepted turn or retry the pre-acceptance
        // handoff. A dispatching row whose journal proves completion is handed
        // to the shared finalizer: journal completion is not proof that local
        // Failed status, this-turn import, or pin release finished.
        for entry in self.client_state.queue_snapshot()?.entries().iter() {
            if entry.kind() != QueueEntryKind::TaskTurn
                || matches!(entry.state(), QueueState::Parked)
            {
                continue;
            }
            if self
                .submission_recovery_task_for_turn(entry.job_id())?
                .is_some()
            {
                continue;
            }
            if let Some(task_id) = self.client_state.task_id_for_turn(entry.job_id())?
                && self.task_has_submission_recovery_pending(task_id)?
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
                let Some(task_id) = self.client_state.task_id_for_turn(entry.job_id())? else {
                    continue;
                };
                let Some(log) = crate::runner_log::RunnerLog::try_open(
                    &self.paths.state,
                    task_id,
                    entry.job_id(),
                )?
                else {
                    continue;
                };
                let Some(current) = log.current_entry(self.client_state)? else {
                    continue;
                };
                if current != *entry
                    || !matches!(
                        self.client_state.process_observation(row_owner),
                        ProcessObservation::Absent | ProcessObservation::Reused
                    )
                {
                    continue;
                }
                self.client_state.adopt_row(entry.job_id(), owner)?;
                if matches!(entry.state(), QueueState::Dispatching { .. })
                    && let Some(completion) = log.completion()
                {
                    crate::turn_runner::finalize_completed_turn(
                        self.client_state,
                        self.runner,
                        self.config,
                        self.paths,
                        task_id,
                        entry.job_id(),
                        owner,
                        completion,
                    )?;
                    report.repaired_rows += 1;
                }
            }
        }

        // Refresh only the task records referenced by the local queue.  This
        // is intentionally a mutating-command path; status/list remain
        // read-only projections.  Probe failures are not evidence that a
        // worker lost a task, so a failed refresh leaves the local record
        // untouched for a later reconciliation.
        for record in self.client_state.list_tasks()? {
            if submission_recovery_pending(&record) {
                continue;
            }
            if matches!(
                record.status().state(),
                TaskState::Closed | TaskState::Abandoned | TaskState::Lost
            ) {
                continue;
            }
            // Refreshing a queued task also writes task-scoped metadata. Use
            // the same fence so a stale refresh cannot resurrect an old runner
            // after another finalizer retires its row.
            if let Some(entry) = self
                .client_state
                .queue_entry_for_task_turn(record.meta().task_id())?
            {
                let Ok(Some(log)) = crate::runner_log::RunnerLog::try_open(
                    &self.paths.state,
                    record.meta().task_id(),
                    entry.job_id(),
                ) else {
                    continue;
                };
                if log.current_entry(self.client_state)?.as_ref() != Some(&entry) {
                    continue;
                }
                self.refresh_task_status(&self.client_state.load_task(record.meta().task_id())?)?;
            } else {
                self.refresh_task_status(&record)?;
            }
        }

        // A crash can occur after the task record is durable but before the
        // queue publication. Recreate only the task-turn row, retaining the
        // original task and turn identifiers. Both recovery phases share one
        // snapshot so they agree on which tasks look row-less; re-enqueue is
        // idempotent if that snapshot is already stale.
        let snapshot = self.client_state.queue_snapshot()?;
        let records = self.client_state.list_tasks()?;
        for record in &records {
            if submission_recovery_pending(record) {
                continue;
            }
            if matches!(
                record.status().state(),
                TaskState::Queued | TaskState::Active
            ) {
                let turn_ids = self
                    .client_state
                    .turn_ids_for_task(record.meta().task_id())?;
                if task_turn_in_snapshot(&snapshot, record, &turn_ids).is_none() {
                    let _ = self.enqueue_missing_turn(record, owner)?;
                }
            }
        }

        for record in &records {
            if submission_recovery_pending(record) {
                continue;
            }
            let turn_ids = self
                .client_state
                .turn_ids_for_task(record.meta().task_id())?;
            let queued = task_turn_in_snapshot(&snapshot, record, &turn_ids);
            let Some(turn_id) = queued
                .as_ref()
                .map(QueueEntry::job_id)
                .or_else(|| pending_turn_id(record.status()))
            else {
                continue;
            };
            let mut entry = queued;
            if entry.is_none()
                && matches!(
                    record.status().state(),
                    TaskState::Queued | TaskState::Active
                )
            {
                if let Some(recovered) = self.enqueue_missing_turn(record, owner)? {
                    entry = Some(recovered);
                } else {
                    entry = self.client_state.queue_entry(turn_id)?;
                }
            }
            let Some(entry) = entry else { continue };
            if matches!(entry.state(), QueueState::Parked) {
                continue;
            }
            let liveness = self.client_state.runner_liveness(record.meta().task_id())?;
            if liveness == Some(RunnerState::Dead) {
                let Some(log) = crate::runner_log::RunnerLog::try_open(
                    &self.paths.state,
                    record.meta().task_id(),
                    turn_id,
                )?
                else {
                    continue;
                };
                if log.current_entry(self.client_state)?.as_ref() != Some(&entry) {
                    continue;
                }
                if self.client_state.runner_liveness(record.meta().task_id())?
                    == Some(RunnerState::Dead)
                {
                    self.client_state
                        .record_runner(record.meta().task_id(), None)?;
                    report.replaced_runners += 1;
                }
            }
            if self
                .client_state
                .runner_liveness(record.meta().task_id())?
                .is_none()
            {
                // Queue publication precedes runner metadata during a
                // handoff. Its live owner remains authoritative throughout
                // that window, including a dispatch already reserved by the
                // departing runner.
                let Some(current_entry) = self.client_state.queue_entry(turn_id)? else {
                    continue;
                };
                if matches!(current_entry.state(), QueueState::Parked)
                    || current_entry.owner_opt().is_some_and(|row_owner| {
                        *row_owner != owner
                            && matches!(
                                self.client_state.process_observation(*row_owner),
                                ProcessObservation::Matching { .. } | ProcessObservation::Ambiguous
                            )
                    })
                {
                    continue;
                }
                if self.live_runner_count()? < self.config.workers.len() {
                    self.start_runner(record.meta().task_id(), turn_id)?;
                    report.started_runners += 1;
                }
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
        if self
            .client_state
            .queue_entry_for_task_turn(task_id)?
            .is_some()
        {
            // The previous turn still owns a queue row, including the window
            // where an undrainable finalizer has finished the journal but not
            // yet published Failed / imported / retired. A follow-up would
            // keep fetched_head from that earlier turn; refuse until the row
            // is gone so expected-record CAS in the finalizer stays valid.
            return Err(task_error("TASK_BUSY", "previous turn is still finalizing"));
        }
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
        let followups = record.status().turns().len().saturating_sub(1) as u32;
        if followups >= record.meta().limits().max_followups {
            return Err(task_error(
                "FOLLOWUP_LIMIT",
                "task follow-up limit has been reached",
            ));
        }
        let worker = task_worker(self.config, record.status())?.name.clone();
        let turn_id = TurnId::generate();
        let turn_number = u32::try_from(record.status().turns().len())
            .ok()
            .and_then(|turns| turns.checked_add(1))
            .ok_or_else(|| task_error("TASK_INCONSISTENT", "turn history is too long"))?;
        let base_oid = record
            .status()
            .head_oid()
            .cloned()
            .unwrap_or_else(|| record.meta().base_oid().clone());
        let composed_prompt = compose_turn_prompt(
            task_id,
            turn_number,
            record.meta().agent(),
            &base_oid,
            None,
            &message,
            true,
        );
        validate_prompt(&composed_prompt)?;
        self.client_state
            .write_turn_prompt(task_id, turn_id, &composed_prompt)?;
        let pending = TurnSummary::new(
            turn_number,
            turn_id,
            None,
            None,
            None,
            false,
            Some(current_time_millis()?),
            None,
        );
        let active = TaskStatus::new(
            TaskState::Active,
            record.status().last_outcome().cloned(),
            Some(worker.clone()),
            true,
            Some(base_oid),
            record.status().summary().map(str::to_owned),
            record.status().questions().to_vec(),
            record.status().files_changed().to_vec(),
            record.status().diff_stat().map(str::to_owned),
            record
                .status()
                .turns()
                .iter()
                .cloned()
                .chain([pending])
                .collect(),
            current_time_millis()?,
        )?;
        let active_record = record
            .with_status(active)?
            .with_runner(None)?
            .with_abandon_code(None)?;
        if let Err(error) = self.client_state.update_task(active_record) {
            let _ = self.client_state.remove_turn_prompt(task_id, turn_id);
            return Err(error);
        }
        let entry = match self.enqueue_followup(&record, turn_id, worker) {
            Ok(entry) => entry,
            Err(error) => {
                self.rollback_followup(&record, task_id, turn_id);
                return Err(error);
            }
        };
        let should_start = attached || self.live_runner_count()? < self.config.workers.len();
        if should_start {
            if let Err(error) = self.start_runner(task_id, turn_id) {
                return Err(self.fail_handoff(
                    task_id,
                    turn_id,
                    self.transfer_for_record(&record)?,
                    error,
                ));
            }
        } else {
            if let Err(error) = self.client_state.park_row(entry.job_id()) {
                self.rollback_followup(&record, task_id, turn_id);
                return Err(error);
            }
        }
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
            .with_notifier(self.herdr_notifier.clone())
            .run(task_id, entry.job_id(), Some(stdout))?;
            report.status = outcome.status().clone();
            report.events.extend(outcome.events().iter().cloned());
            report.exit_code = Some(outcome.exit_code());
        }
        Ok(report)
    }

    pub fn cancel(&self, task_id: TaskId) -> Result<TaskReport, WorkerError> {
        self.reconcile_runners()?;
        let record = self.client_state.load_task(task_id)?;
        if matches!(
            record.status().state(),
            TaskState::Closed | TaskState::Abandoned | TaskState::Lost
        ) {
            return self.report_for(task_id);
        }

        let queue_entry = self.client_state.queue_entry_for_task_turn(task_id)?;
        let turn_id = pending_turn_id(record.status())
            .or_else(|| queue_entry.as_ref().map(QueueEntry::job_id));
        let Some(turn_id) = turn_id else {
            return self.report_for(task_id);
        };

        // Keep the legacy path available for older local records; task turns
        // never create one of these records and use the task-specific host
        // cancellation protocol below.
        if let Some(job) = self.client_state.load_job_optional(turn_id)? {
            let worker = task_worker(self.config, record.status())?;
            let response = RemoteJobClient::new(self.runner)
                .cancel(worker, &crate::job::CancelRequest::from_local_record(&job)?)?;
            self.client_state
                .update_observation(turn_id, response.status().status().clone())?;
            let status = TaskStatus::new(
                TaskState::Open,
                Some(TaskOutcome::Cancelled),
                Some(response.status().meta().worker_name().to_owned()),
                record.status().session_present(),
                record.status().head_oid().cloned(),
                record.status().summary().map(str::to_owned),
                record.status().questions().to_vec(),
                record.status().files_changed().to_vec(),
                record.status().diff_stat().map(str::to_owned),
                record.status().turns().to_vec(),
                current_time_millis()?,
            )?;
            self.client_state
                .update_task(record.with_status(status)?.with_runner(None)?)?;
            return self.report_for(task_id);
        }

        let entry = self.client_state.queue_entry(turn_id)?;
        let prompt_exists = self.client_state.read_turn_prompt(task_id, turn_id).is_ok();
        if entry.as_ref().is_some_and(|entry| {
            matches!(
                entry.state(),
                QueueState::Waiting { .. } | QueueState::Parked
            )
        }) || prompt_exists
        {
            match self
                .client_state
                .retain_task_turn_cancel(turn_id, current_time_millis()?)?
            {
                Some(entry) if !matches!(entry.state(), QueueState::Dispatching { .. }) => {
                    self.finish_waiting_cancellation(&record, &entry)?;
                    return self.report_for(task_id);
                }
                Some(_) => return Err(task_error("TASK_BUSY", "task turn is being dispatched")),
                None if entry.is_some() => {
                    return Err(task_error(
                        "TASK_INCONSISTENT",
                        "task queue turn disappeared",
                    ));
                }
                None => {}
            }
        }

        if record.status().state() == TaskState::Active {
            let worker = task_worker(self.config, record.status())?;
            let response = RemoteJobClient::new(self.runner).task_cancel(
                worker,
                &crate::task_store::TaskCancelRequest::new(
                    record.meta().project_id(),
                    task_id,
                    turn_id,
                ),
            )?;
            let status = response.status().clone();
            // The remote cancellation targets this selected turn, but its
            // response can arrive after publication or a subsequent say.
            self.client_state
                .update_task_if_current(&record, record.with_status(status)?)?;
        }
        self.report_for(task_id)
    }

    /// Blocks until every selected task is wait-terminal and quiescent.
    ///
    /// A turn can already be Open while the detached runner is still
    /// finishing: its queue row may still be Dispatching and a runner identity
    /// may still be recorded. `close`, `say`, and `fetch` refuse that window
    /// with TASK_BUSY, so wait must not return until those commands would
    /// succeed.
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
            match self.reconcile_runners() {
                Ok(_) => {}
                Err(error) if is_queue_job_conflict(&error) => {}
                Err(error) => return Err(error),
            }
            let records = task_ids
                .iter()
                .map(|task_id| self.client_state.load_task(*task_id))
                .collect::<Result<Vec<_>, _>>()?;
            if self.tasks_are_quiescent(&records)? {
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
                return Err(task_error(
                    "WAIT_TIMEOUT",
                    "task wait timed out without cancelling the task",
                ));
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
        if batch.version != 1 {
            return Err(task_error(
                "TASK_CONFIG_INVALID",
                format!("unsupported batch version {}; expected 1", batch.version),
            ));
        }
        if batch.tasks.is_empty() {
            return Err(task_error("TASK_CONFIG_INVALID", "batch has no tasks"));
        }
        let max_parallel = resolve_batch_max_parallel(max_parallel, self.config.workers.len())?;
        let run_id = RunId::generate();
        let created = current_time_millis()?;
        let batch_dir = file.parent().unwrap_or_else(|| Path::new("."));
        let project = self.current_project(None)?;
        let mut requests = Vec::with_capacity(batch.tasks.len());
        for task in &batch.tasks {
            requests.push((
                self.batch_request(&batch.defaults, task, batch_dir, &project, run_id)?,
                task.title.clone(),
            ));
        }
        let settings = ProjectState::load(self.runner, &project, &[])?
            .settings
            .task;
        for (request, _) in &requests {
            validate_prompt(&request.prompt)?;
            validate_preference(self.config, &request.preference)?;
            let _ = effective_task_limits(&request.limits, &settings)?;
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
        batch_dir: &Path,
        project: &Path,
        run_id: RunId,
    ) -> Result<TaskSubmitRequest, WorkerError> {
        validate_batch_scope(defaults, task)?;
        let agent = parse_agent(task.agent.as_deref().unwrap_or(&defaults.agent))?;
        validate_task_agent(agent)?;
        let timeout = task
            .timeout
            .as_deref()
            .or(defaults.timeout.as_deref())
            .map(humantime::parse_duration)
            .transpose()
            .map_err(|_| task_error("TASK_CONFIG_INVALID", "batch timeout is invalid"))?;
        let default_limits = TaskLimits::default();
        let timeout_millis = timeout
            .map(|timeout| {
                timeout
                    .as_millis()
                    .try_into()
                    .map_err(|_| task_error("TASK_CONFIG_INVALID", "batch timeout is too large"))
            })
            .transpose()?
            .unwrap_or(default_limits.turn.timeout_millis);
        let limits = TaskLimits::new(
            TurnLimits::new(
                timeout_millis,
                task.max_turns.or(defaults.max_turns),
                task.max_budget_usd_cents.or(defaults.max_budget_usd_cents),
            )
            .map_err(WorkerError::from)?,
            task.max_followups
                .or(defaults.max_followups)
                .unwrap_or(default_limits.max_followups),
        )?;
        Ok(TaskSubmitRequest {
            agent,
            model: task.model.clone().or_else(|| defaults.model.clone()),
            effort: task.effort.clone().or_else(|| defaults.effort.clone()),
            prompt: read_batch_prompt(task, batch_dir)?,
            project: project.to_path_buf(),
            base: task.base.clone().unwrap_or_else(|| defaults.base.clone()),
            wip: task.wip.unwrap_or(defaults.wip),
            source: task
                .source
                .clone()
                .or_else(|| Some(defaults.source.clone())),
            publish: Some(
                task.publish
                    .clone()
                    .unwrap_or_else(|| defaults.publish.clone()),
            ),
            publish_branch: task
                .publish_branch
                .clone()
                .or_else(|| defaults.publish_branch.clone()),
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
        let project = ProjectState::load_for_task(
            self.runner,
            &self.current_project(Some(record))?,
            &[],
            record.meta(),
        )?;
        require_project_match(&project, record.meta())?;
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
            task_requirements(
                &project.requirements,
                record.meta().agent(),
                record.meta().env_profile(),
                task_origin_requirement(record.meta()).as_deref(),
            ),
            preference,
            QueueEntryKind::TaskTurn,
            run,
            current_process_identity()?,
            now,
        )?)
    }

    fn finish_waiting_cancellation(
        &self,
        record: &LocalTaskRecord,
        entry: &QueueEntry,
    ) -> Result<(), WorkerError> {
        let task = record.meta().task_id();
        let turn = entry.job_id();
        let Some(mut log) = crate::runner_log::RunnerLog::try_open(&self.paths.state, task, turn)?
        else {
            return Ok(());
        };
        let Some(current) = log.current_entry(self.client_state)? else {
            return Ok(());
        };
        if current != *entry {
            return Ok(());
        }
        self.finish_waiting_cancellation_locked(task, &current, &mut log)
    }

    fn finish_waiting_cancellation_locked(
        &self,
        task: TaskId,
        entry: &QueueEntry,
        log: &mut crate::runner_log::RunnerLog,
    ) -> Result<(), WorkerError> {
        let turn = entry.job_id();
        let record = self.client_state.load_task(task)?;
        if !entry.is_cancel_requested() || matches!(entry.state(), QueueState::Dispatching { .. }) {
            return Err(task_error(
                "TASK_BUSY",
                "cancellation is not proven preacceptance",
            ));
        }
        let status = match record.status().state() {
            TaskState::Queued => abandoned_status(record.status(), "CANCELLED")?,
            TaskState::Active => cancelled_followup_status(record.status())?,
            _ => record.status().clone(),
        };
        self.client_state.update_task(record.with_status(status)?)?;
        let outcome = if record.abandon_code() == Some("RUNNER_HANDOFF_FAILED") {
            TaskOutcome::failed("RUNNER_HANDOFF_FAILED")
        } else {
            TaskOutcome::Cancelled
        };
        log.finish_local(task, turn, outcome)?;
        self.release_task_base(&record)?;
        self.client_state.record_runner(task, None)?;
        self.client_state.remove_task_turn_after_terminal(
            turn,
            entry
                .owner_opt()
                .copied()
                .unwrap_or(current_process_identity()?),
        )?;
        self.client_state.remove_turn_prompt(task, turn)?;
        Ok(())
    }

    fn rollback_followup(&self, record: &LocalTaskRecord, task_id: TaskId, turn_id: TurnId) {
        let _ = self.client_state.remove_queued(turn_id);
        let _ = self.client_state.remove_turn_prompt(task_id, turn_id);
        let _ = self.client_state.update_task(record.clone());
    }

    fn rollback_submission(
        &self,
        turn_id: TurnId,
        record: &LocalTaskRecord,
        transfer: &TransferRepo,
    ) {
        let Ok(pending) = self.mark_submission_rollback(record, turn_id) else {
            return;
        };
        let _ = self.complete_submission_rollback(Some(turn_id), &pending, transfer);
    }

    fn mark_submission_rollback(
        &self,
        record: &LocalTaskRecord,
        turn_id: TurnId,
    ) -> Result<LocalTaskRecord, WorkerError> {
        if record.abandon_code() == Some(SUBMISSION_ROLLBACK_INCOMPLETE) {
            return Ok(record.clone());
        }
        let status = abandoned_status(record.status(), SUBMISSION_ROLLBACK_INCOMPLETE)?;
        let pending = record
            .clone()
            .with_status(status)?
            .with_abandon_code(Some(SUBMISSION_ROLLBACK_INCOMPLETE.to_owned()))?
            .with_submission_rollback_turn_id(turn_id)?;
        // Do not begin compensation until the durable recovery marker exists.
        // A transient replacement race is safe to retry here; if it persists,
        // this returns with the original complete submission untouched.
        if self.client_state.update_task(pending.clone()).is_err() {
            self.client_state.update_task(pending.clone())?;
        }
        Ok(pending)
    }

    fn complete_submission_rollback(
        &self,
        turn_id: Option<TurnId>,
        record: &LocalTaskRecord,
        transfer: &TransferRepo,
    ) -> Result<(), WorkerError> {
        // Keep the durable intent or marker, prompt, and queue row together
        // until all externally-referenced resources are released. Any failure
        // leaves a recovery record for the next reconcile.
        if let Some(run_id) = record.meta().run_id()
            && record.meta().publish().contains(&PublishMode::Push)
        {
            let branch = record
                .meta()
                .publish_branch()
                .cloned()
                .unwrap_or_else(|| BranchName::for_task(record.meta().task_id()));
            self.client_state.release_run_publish_branch_for_task(
                run_id,
                record.meta().task_id(),
                &branch,
            )?;
        }
        transfer.release_base(self.runner, record.meta().task_id())?;
        let task_id = record.meta().task_id();
        let removed_queue = if let Some(turn_id) = turn_id
            .or(record.submission_rollback_turn_id())
            .or(record.submission_intent_turn_id())
        {
            self.client_state
                .remove_task_turn_for_submission_rollback(turn_id)?
        } else {
            None
        };
        if let Err(error) = self.client_state.remove_task_submission_turns(task_id) {
            if let Some(entry) = removed_queue {
                self.client_state
                    .restore_task_turn_for_submission_rollback(entry)?;
            }
            return Err(error);
        }
        // The rollback marker stays durable until queue and prompt retirement
        // complete. If this final removal faults after its rename, no queue or
        // prompt artifact remains without a discoverable task record.
        self.client_state.remove_task_submission_record(task_id)?;
        Ok(())
    }

    fn transfer_for_record(&self, record: &LocalTaskRecord) -> Result<TransferRepo, WorkerError> {
        let project = ProjectState::load_for_task(
            self.runner,
            &self.current_project(Some(record))?,
            &[],
            record.meta(),
        )?;
        require_project_match(&project, record.meta())?;
        TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)
    }

    fn refresh_task_status(&self, record: &LocalTaskRecord) -> Result<(), WorkerError> {
        if record.retains_log_drain_unavailable() {
            return Ok(());
        }
        let Some(worker_name) = record.status().worker() else {
            return Ok(());
        };
        let Some(worker) = self.config.worker(worker_name) else {
            return Ok(());
        };
        let Ok(response) = RemoteJobClient::new(self.runner).task_status(
            worker,
            &crate::task_store::TaskStatusRequest::new(
                record.meta().project_id(),
                record.meta().task_id(),
            ),
        ) else {
            // A failed remote observation is not evidence of task loss.
            return Ok(());
        };
        if let Some(pending) = pending_turn_id(record.status())
            && record.status().state() == TaskState::Active
            && self
                .client_state
                .read_turn_prompt(record.meta().task_id(), pending)
                .is_ok()
            && !response
                .status()
                .turns()
                .last()
                .is_some_and(|turn| turn.turn_id() == pending)
        {
            // A follow-up is locally active before the host accepts its
            // task-turn job. The host still reports the previous Open turn
            // during that interval; do not erase the pending local turn.
            return Ok(());
        }
        let observed_at = response.status().updated_at_millis();
        self.client_state.update_task_if_current(
            record,
            record
                .with_status(response.status().clone())?
                .with_status_observed_at(Some(observed_at))?,
        )?;
        Ok(())
    }

    fn enqueue_missing_turn(
        &self,
        record: &LocalTaskRecord,
        owner: ProcessIdentity,
    ) -> Result<Option<QueueEntry>, WorkerError> {
        if submission_recovery_pending(record) {
            return Err(task_error(
                "TASK_SUBMISSION_RECOVERY_PENDING",
                "task submission recovery is incomplete",
            ));
        }
        let turn_id = pending_turn_id(record.status())
            .or_else(|| {
                self.client_state
                    .turn_ids_for_task(record.meta().task_id())
                    .ok()?
                    .last()
                    .copied()
            })
            .ok_or_else(|| task_error("TASK_INCONSISTENT", "queued task has no turn prompt"))?;
        if record
            .status()
            .turns()
            .iter()
            .any(|turn| turn.turn_id() == turn_id && turn.terminal().is_some())
        {
            // The last turn already finished locally. Re-publishing it would
            // resurrect a row `wait` is trying to let the runner retire.
            return Ok(None);
        }
        let project = ProjectState::load_for_task(
            self.runner,
            &self.current_project(Some(record))?,
            &[],
            record.meta(),
        )?;
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
        Ok(Some(self.client_state.enqueue_if_absent(
            QueueEntry::new(
                turn_id,
                self.client_state.client_id(),
                record.meta().project_id().to_owned(),
                record.meta().worktree_id().to_owned(),
                command.summary()?,
                task_requirements(
                    &project.requirements,
                    record.meta().agent(),
                    record.meta().env_profile(),
                    task_origin_requirement(record.meta()).as_deref(),
                ),
                preference,
                QueueEntryKind::TaskTurn,
                run,
                owner,
                current_time_millis()?,
            )?,
        )?))
    }

    fn start_runner(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<RunnerIdentity, WorkerError> {
        if self.task_has_submission_recovery_pending(task_id)? {
            return Err(task_error(
                "TASK_SUBMISSION_RECOVERY_PENDING",
                "task submission recovery is incomplete",
            ));
        }
        let expected = self
            .client_state
            .queue_entry_for_task_turn(task_id)?
            .filter(|entry| entry.job_id() == turn_id)
            .ok_or_else(|| task_error("TASK_BUSY", "task turn was retired before handoff"))?;
        let identity = self.executor.start(self.paths, task_id, turn_id)?;
        let log = crate::runner_log::RunnerLog::open(&self.paths.state, task_id, turn_id)?;
        if log.current_entry(self.client_state)?.as_ref() != Some(&expected) {
            return Err(task_error("TASK_BUSY", "task turn changed during handoff"));
        }
        let caller = current_process_identity()?;
        if expected.owner_opt().is_some_and(|owner| {
            *owner != caller
                && matches!(
                    self.client_state.process_observation(*owner),
                    ProcessObservation::Matching { .. } | ProcessObservation::Ambiguous
                )
        }) {
            return Err(task_error(
                "TASK_BUSY",
                "task turn has a live handoff owner",
            ));
        }
        self.client_state
            .record_runner(task_id, Some(identity.clone()))?;
        if let Err(error) =
            adopt_row_with_retry(self.client_state, turn_id, identity.process_identity())
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
            .submission_recovery_task_for_turn(entry.job_id())?
            .or(self.client_state.task_id_for_turn(entry.job_id())?)
            .ok_or_else(|| {
                task_error("TASK_INCONSISTENT", "parked task turn has no task record")
            })?;
        if self.task_has_submission_recovery_pending(task_id)? {
            self.client_state.park_row(entry.job_id())?;
            return Ok(false);
        }
        if let Err(error) = self.start_runner(task_id, entry.job_id()) {
            if let Ok(record) = self.client_state.load_task(task_id)
                && let Ok(transfer) = self.transfer_for_record(&record)
            {
                return Err(self.fail_handoff(task_id, entry.job_id(), transfer, error));
            }
            return Err(task_error(
                "RUNNER_HANDOFF_FAILED",
                format!("runner handoff failed: {error}"),
            ));
        }
        Ok(true)
    }

    fn task_has_submission_recovery_pending(&self, task_id: TaskId) -> Result<bool, WorkerError> {
        Ok(submission_recovery_pending(
            &self.client_state.load_task(task_id)?,
        ))
    }

    /// Finds a pre-handoff task turn from the durable submission marker. A
    /// rollback can retire the prompt tree before a later cleanup fault, so
    /// directory-only turn lookup cannot establish ownership in that window.
    fn submission_recovery_task_for_turn(
        &self,
        turn_id: TurnId,
    ) -> Result<Option<TaskId>, WorkerError> {
        Ok(self
            .client_state
            .list_tasks()?
            .into_iter()
            .find(|record| {
                submission_recovery_pending(record)
                    && submission_recovery_turn_id(record) == Some(turn_id)
            })
            .map(|record| record.meta().task_id()))
    }

    fn submission_rollback_is_safe(
        &self,
        record: &LocalTaskRecord,
        turn_id: Option<TurnId>,
    ) -> Result<bool, WorkerError> {
        if record.runner().is_some() || record.status().state() == TaskState::Active {
            return Ok(false);
        }
        let Some(turn_id) = turn_id else {
            return Ok(true);
        };
        let Some(entry) = self.client_state.queue_entry(turn_id)? else {
            return Ok(true);
        };
        Ok(matches!(entry.state(), QueueState::Waiting { .. })
            && entry.owner_opt() == Some(entry.enqueue_owner()))
    }

    fn report_for(&self, task_id: TaskId) -> Result<TaskReport, WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        Ok(TaskReport {
            task_id,
            run_id: record.meta().run_id(),
            status: record.status().clone(),
            warnings: Vec::new(),
            events: Vec::new(),
            runner: self.client_state.runner_liveness(task_id)?,
            exit_code: None,
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
            warnings: Vec::new(),
            events: Vec::new(),
            runner: self.client_state.runner_liveness(record.meta().task_id())?,
            exit_code: None,
        })
    }

    fn fail_handoff(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        transfer: TransferRepo,
        error: WorkerError,
    ) -> WorkerError {
        let _cleanup = (|| -> Result<(), WorkerError> {
            let Some(mut log) =
                crate::runner_log::RunnerLog::try_open(&self.paths.state, task_id, turn_id)?
            else {
                return Ok(());
            };
            log.require_owner(self.client_state, current_process_identity()?)?;
            let Some(entry) = self.client_state.cancel_unstarted_handoff(
                turn_id,
                current_process_identity()?,
                current_time_millis()?,
            )?
            else {
                return Ok(());
            };
            let record = self.client_state.load_task(task_id)?;
            let status = abandoned_status(record.status(), "RUNNER_HANDOFF_FAILED")?;
            let record = record
                .with_status(status)?
                .with_abandon_code(Some("RUNNER_HANDOFF_FAILED".into()))?;
            self.client_state.update_task(record)?;
            log.finish_local(
                task_id,
                turn_id,
                TaskOutcome::failed("RUNNER_HANDOFF_FAILED"),
            )?;
            transfer.release_base(self.runner, task_id)?;
            self.client_state.record_runner(task_id, None)?;
            self.client_state.remove_task_turn_after_terminal(
                turn_id,
                *entry.owner_opt().expect("waiting entry has owner"),
            )?;
            self.client_state.remove_turn_prompt(task_id, turn_id)?;
            Ok(())
        })();
        task_error(
            "RUNNER_HANDOFF_FAILED",
            format!("runner handoff failed: {error}"),
        )
    }

    fn release_task_base(&self, record: &LocalTaskRecord) -> Result<(), WorkerError> {
        let project = ProjectState::load_for_task(
            self.runner,
            &self.current_project(Some(record))?,
            &[],
            record.meta(),
        )?;
        require_project_match(&project, record.meta())?;
        let transfer =
            TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)?;
        transfer.release_base(self.runner, record.meta().task_id())
    }

    fn live_runner_count(&self) -> Result<usize, WorkerError> {
        let mut identities = BTreeSet::new();
        for task in self.client_state.list_tasks()? {
            if task.status().state().is_terminal()
                && self
                    .client_state
                    .queue_entry_for_task_turn(task.meta().task_id())?
                    .is_none()
            {
                continue;
            }
            let Some(runner) = task.runner() else {
                continue;
            };
            let identity = runner.process_identity();
            if matches!(
                self.client_state.process_observation(identity),
                ProcessObservation::Matching { .. } | ProcessObservation::Ambiguous
            ) {
                identities.insert((identity.pid(), identity.start_time_micros()));
            }
        }
        Ok(identities.len())
    }

    fn observe_admission(
        &self,
        preference: &WorkerPreference,
    ) -> Result<Vec<CandidateObservation>, WorkerError> {
        crate::admission::observe_admission(self.runner, self.config, self.client_state, preference)
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

    fn current_project(&self, record: Option<&LocalTaskRecord>) -> Result<PathBuf, WorkerError> {
        if let Some(record) = record
            && let Some(project_path) = self.client_state.task_project_path(record)?
        {
            return Ok(project_path);
        }
        std::env::current_dir().map_err(WorkerError::Io)
    }

    /// `close`, `say`, and `fetch` refuse while a detached runner is still
    /// finishing. `wait` uses the same check so it does not return in that
    /// window.
    fn operator_busy_reason(
        &self,
        task_id: TaskId,
        record: &LocalTaskRecord,
    ) -> Result<Option<&'static str>, WorkerError> {
        if record.status().state() == TaskState::Active {
            return Ok(Some("task has an active turn"));
        }
        if let Some(entry) = self.client_state.queue_entry_for_task_turn(task_id)?
            && matches!(entry.state(), QueueState::Dispatching { .. })
        {
            return Ok(Some("task turn is being dispatched"));
        }
        if record.runner().is_some() {
            return Ok(Some("task runner is still finishing"));
        }
        Ok(None)
    }

    fn tasks_are_quiescent(&self, records: &[LocalTaskRecord]) -> Result<bool, WorkerError> {
        for record in records {
            if !is_wait_terminal(record.status().state())
                || self
                    .operator_busy_reason(record.meta().task_id(), record)?
                    .is_some()
            {
                return Ok(false);
            }
        }
        Ok(true)
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

fn turn_failure(turn: &TurnSummary) -> Option<String> {
    outcome_failure(turn.turn_number(), turn.outcome()?)
}

fn outcome_failure(number: u32, outcome: &TaskOutcome) -> Option<String> {
    let description = match outcome {
        TaskOutcome::Failed { reason } => format!("failed: {reason}"),
        outcome @ (TaskOutcome::Cancelled | TaskOutcome::TimedOut | TaskOutcome::Lost) => {
            outcome.kind().to_owned()
        }
        _ => return None,
    };
    Some(format!("turn {number} {description}"))
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

fn task_turn_in_snapshot(
    snapshot: &QueueSnapshot,
    record: &LocalTaskRecord,
    turn_ids: &[TurnId],
) -> Option<QueueEntry> {
    let pending = pending_turn_id(record.status());
    snapshot
        .entries()
        .iter()
        .find(|entry| {
            entry.kind() == QueueEntryKind::TaskTurn
                && (turn_ids.contains(&entry.job_id()) || pending == Some(entry.job_id()))
        })
        .cloned()
}

fn is_queue_job_conflict(error: &WorkerError) -> bool {
    matches!(
        error,
        WorkerError::Queue {
            code: "QUEUE_JOB_CONFLICT",
            ..
        }
    )
}

fn submission_recovery_turn_id(record: &LocalTaskRecord) -> Option<TurnId> {
    record
        .submission_intent_turn_id()
        .or(record.submission_rollback_turn_id())
}

fn submission_recovery_pending(record: &LocalTaskRecord) -> bool {
    record.abandon_code() == Some(SUBMISSION_ROLLBACK_INCOMPLETE)
        || submission_recovery_turn_id(record).is_some()
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

fn cancelled_followup_status(status: &TaskStatus) -> Result<TaskStatus, WorkerError> {
    let Some(last) = status.turns().last() else {
        return Err(task_error(
            "TASK_INCONSISTENT",
            "cancelled follow-up has no turn record",
        ));
    };
    if last.terminal().is_some() {
        return Ok(status.clone());
    }
    let ended_at = current_time_millis()?;
    let replacement = TurnSummary::new(
        last.turn_number(),
        last.turn_id(),
        Some(crate::task::TurnTerminal::Cancelled),
        Some(TaskOutcome::Cancelled),
        Some(false),
        false,
        last.started_at_millis().or(Some(ended_at)),
        Some(ended_at),
    );
    let mut turns = status.turns().to_vec();
    let _ = turns.pop();
    turns.push(replacement);
    TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Cancelled),
        status.worker().map(str::to_owned),
        status.session_present(),
        status.head_oid().cloned(),
        status.summary().map(str::to_owned),
        status.questions().to_vec(),
        status.files_changed().to_vec(),
        status.diff_stat().map(str::to_owned),
        turns,
        ended_at,
    )
}

fn parse_task_source(
    source: &str,
    wip: bool,
    origin_url: &str,
    pushing: bool,
) -> Result<TaskSource, WorkerError> {
    match source {
        "local" => Ok(TaskSource::Local {
            wip,
            push_target: pushing
                .then(|| PushTarget::new(origin_url.to_owned()))
                .transpose()?,
        }),
        "origin" if !wip && !origin_url.is_empty() => Ok(TaskSource::Origin {
            url: origin_url.to_owned(),
        }),
        "origin" => Err(task_error(
            "TASK_CONFIG_INVALID",
            "source origin requires a configured origin and a committed base",
        )),
        _ => Err(task_error(
            "TASK_CONFIG_INVALID",
            "source must be local or origin",
        )),
    }
}

fn parse_publish_modes(values: &[String]) -> Result<Vec<PublishMode>, WorkerError> {
    if values.is_empty() {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "publish fetch is required for every task",
        ));
    }
    let mut modes = Vec::with_capacity(values.len());
    for value in values {
        let mode = match value.as_str() {
            "fetch" => PublishMode::Fetch,
            "push" => PublishMode::Push,
            _ => {
                return Err(task_error(
                    "TASK_CONFIG_INVALID",
                    "publish must be fetch or push",
                ));
            }
        };
        if modes.contains(&mode) {
            return Err(task_error(
                "TASK_CONFIG_INVALID",
                "publish modes must be unique",
            ));
        }
        modes.push(mode);
    }
    if !modes.contains(&PublishMode::Fetch) {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "publish fetch is required for every task",
        ));
    }
    Ok(modes)
}

fn parse_publish_branch(
    value: Option<&str>,
    publish: &[PublishMode],
) -> Result<Option<BranchName>, WorkerError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !publish.contains(&PublishMode::Push) {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "publish_branch requires publish push",
        ));
    }
    value.parse().map(Some).map_err(|_| {
        task_error(
            "TASK_CONFIG_INVALID",
            "publish branch is not a valid Git branch name",
        )
    })
}

fn task_origin_requirement(meta: &TaskMeta) -> Option<String> {
    meta.origin_requirement()
}

fn task_requirements(
    project: &[String],
    agent: AgentKind,
    profile: Option<&str>,
    origin_requirement: Option<&str>,
) -> Vec<String> {
    let mut requirements = project.to_vec();
    let name = agent_name(agent);
    let requirement = profile.map_or_else(
        || format!("{TASK_CAPABILITY_PREFIX}{name}"),
        |profile| format!("{TASK_CAPABILITY_PREFIX}{name}@{profile}"),
    );
    if !requirements.iter().any(|item| item == &requirement) {
        requirements.push(requirement);
    }
    if let Some(requirement) = origin_requirement
        && !requirements.iter().any(|item| item == requirement)
    {
        requirements.push(requirement.to_owned());
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
    agent: AgentKind,
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
    let result_instruction = result_instruction(agent)
        .map(|instruction| format!("\n\n{instruction}"))
        .unwrap_or_default();
    format!(
        "mac-worker task context\n\nTask ID: {task_id}\nTurn: {turn_number}\nBase commit: {base_oid}\nBase branch: {branch}\n\n{continuation}You are working in an isolated task worktree. Make changes only there. Do not switch branches, push, or modify the user's repository. Leave your changes in the task worktree for publication.\n\nUser request:\n{user_prompt}{result_instruction}\n"
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

fn read_batch_prompt(task: &BatchTask, batch_dir: &Path) -> Result<String, WorkerError> {
    match (&task.prompt, &task.prompt_file) {
        (Some(prompt), None) => Ok(prompt.clone()),
        (None, Some(path)) => {
            std::fs::read_to_string(batch_dir.join(path)).map_err(WorkerError::Io)
        }
        _ => Err(task_error(
            "TASK_CONFIG_INVALID",
            "each batch task requires exactly one of prompt or prompt_file",
        )),
    }
}

fn validate_batch_scope(defaults: &BatchDefaults, task: &BatchTask) -> Result<(), WorkerError> {
    let source = task.source.as_deref().unwrap_or(&defaults.source);
    if !matches!(source, "local" | "origin") {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "source must be local or origin (TASK_CONFIG_INVALID)",
        ));
    }

    let publish = task.publish.as_ref().unwrap_or(&defaults.publish);
    if publish
        .iter()
        .any(|mode| !matches!(mode.as_str(), "fetch" | "push"))
    {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "publish must be fetch or push (TASK_CONFIG_INVALID)",
        ));
    }
    if publish
        .iter()
        .filter(|mode| mode.as_str() == "fetch")
        .count()
        > 1
        || publish
            .iter()
            .filter(|mode| mode.as_str() == "push")
            .count()
            > 1
    {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "publish modes must be unique (TASK_CONFIG_INVALID)",
        ));
    }
    if !publish.iter().any(|mode| mode == "fetch") {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "publish fetch is required for every task (TASK_CONFIG_INVALID)",
        ));
    }
    if task.publish_branch.is_some() && !publish.iter().any(|mode| mode == "push") {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "publish_branch requires publish push (TASK_CONFIG_INVALID)",
        ));
    }
    if task.wip.unwrap_or(defaults.wip) && publish.iter().any(|mode| mode == "push") {
        return Err(task_error(
            "PUBLISH_REQUIRES_COMMITTED_BASE",
            "publish push requires a committed base",
        ));
    }
    Ok(())
}

fn resolve_batch_max_parallel(
    requested: Option<u32>,
    configured_workers: usize,
) -> Result<u32, WorkerError> {
    let max_parallel = match requested {
        Some(value) => value,
        None => u32::try_from(configured_workers).map_err(|_| {
            task_error(
                "TASK_CONFIG_INVALID",
                "configured worker count is too large for batch max_parallel",
            )
        })?,
    };
    if max_parallel == 0 {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "batch max_parallel must be positive",
        ));
    }
    Ok(max_parallel)
}

fn agent_name(agent: AgentKind) -> &'static str {
    match agent {
        AgentKind::Codex => "codex",
        AgentKind::Claude => "claude",
        AgentKind::Cursor => "cursor",
        AgentKind::Opencode => "opencode",
    }
}

fn validate_task_agent(agent: AgentKind) -> Result<(), WorkerError> {
    match agent {
        AgentKind::Codex | AgentKind::Claude | AgentKind::Cursor | AgentKind::Opencode => Ok(()),
    }
}

fn parse_close_policy(value: Option<&str>) -> Result<ClosePolicy, WorkerError> {
    match value.unwrap_or("done") {
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
    WorkerError::capacity(
        "CAPACITY_BUSY",
        "no eligible worker currently has an available heavy slot",
    )
}

fn capability_missing(worker: &str, missing: &[String]) -> WorkerError {
    WorkerError::capacity_public(
        "CAPABILITY_MISSING",
        format!(
            "pinned worker {worker} is missing required capabilities: {}",
            missing.join(", ")
        ),
    )
}

fn task_error(code: &'static str, message: impl Into<Cow<'static, str>>) -> WorkerError {
    WorkerError::task(code, message)
}

fn task_view_error(error: TaskViewError) -> WorkerError {
    WorkerError::task(error.code(), error.message().to_owned())
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

fn default_batch_version() -> u32 {
    1
}

fn default_base_name() -> String {
    "HEAD".into()
}

fn default_source_name() -> String {
    "local".into()
}

fn default_publish_modes() -> Vec<String> {
    vec!["fetch".into()]
}

#[cfg(test)]
mod tests {
    use super::resolve_batch_max_parallel;

    #[test]
    fn batch_parallelism_defaults_to_the_configured_worker_count() {
        assert_eq!(resolve_batch_max_parallel(None, 3).unwrap(), 3);
        assert_eq!(resolve_batch_max_parallel(Some(2), 3).unwrap(), 2);
    }

    #[test]
    fn batch_parallelism_rejects_zero_workers_or_a_zero_override() {
        assert!(resolve_batch_max_parallel(None, 0).is_err());
        assert!(resolve_batch_max_parallel(Some(0), 3).is_err());
    }
}
