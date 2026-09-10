use std::{
    borrow::Cow,
    cell::Cell,
    collections::{BTreeMap, HashSet},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Condvar, Mutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    agent::{AgentKind, PermissionPolicy, TurnLimits, result_instruction},
    client_state::{ClientStateConcurrencyPoint, ClientStateStore},
    config::{Config, WorkerEntry},
    dag::{
        DagBase, DagFrozenSpec, DagNode, DagNodeState, DagRecord, GraphNode, ParentGate,
        accepted_import_oid, dag_pin_ref, dag_run_is_quiescent, merge_pending_into_projection,
        parent_gate, parse_from_base, validate_batch_graph,
    },
    error::WorkerError,
    git_transport::GitTransport,
    job::{
        ClientId, CommandSpec, ProcessIdentity, QueueEntry, QueueEntryKind, QueueRunReference,
        QueueSnapshot, QueueState,
    },
    paths::PathLayout,
    process::ProcessRunner,
    project::ProjectInspector,
    project_config::{SetupSettings, TaskSettings},
    project_state::ProjectState,
    scheduler::{CandidateObservation, SchedulerPolicy, Selection, WorkerPreference},
    supervisor::{ProcessObservation, SystemProcessInspector},
    task::{
        BaseOid, BranchName, ClosePolicy, GitIdentity, LocalTaskRecord, OriginDelivery,
        PublishMode, PushTarget, RunId, RunRecord, RunnerState, TaskCloseIntent, TaskId,
        TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus,
        TurnId, TurnSummary, merge_origin_deliveries,
    },
    task_view::{
        TaskFreshness, TaskListProjection, TaskListRow, TaskRunProjection, TaskViewError,
        filter_task_list, project_task_list_with_blocking_codes,
    },
    transfer::RemoteJobClient,
    transfer_repo::TransferRepo,
    turn_runner::{
        DetachedRunnerExecutor, RunnerExecutor, RunnerStart, TurnRunner,
        last_post_acceptance_public_code, start_runner_with_reservation,
    },
};

const TASK_COMMAND: &str = "task-turn";
const TASK_CAPABILITY_PREFIX: &str = "agent:";
const DEFAULT_GIT_NAME: &str = "mac-worker";
const DEFAULT_GIT_EMAIL: &str = "mac-worker@localhost";
const SUBMISSION_ROLLBACK_INCOMPLETE: &str = "SUBMISSION_ROLLBACK_INCOMPLETE";
const CLOSE_IN_PROGRESS: &str = "task close is in progress";
const SUBMISSION_ROLLBACK_RECOVER_ATTEMPTS: usize = 2;

fn operator_revision_matches(current: &LocalTaskRecord, expected: &LocalTaskRecord) -> bool {
    current.meta().task_id() == expected.meta().task_id()
        && current.status().state() == expected.status().state()
        && current.status().updated_at_millis() == expected.status().updated_at_millis()
        && current.status().head_oid() == expected.status().head_oid()
        && current.status().turns().len() == expected.status().turns().len()
        && current.status().turns().last().map(TurnSummary::turn_id)
            == expected.status().turns().last().map(TurnSummary::turn_id)
}

fn same_close_target(current: &LocalTaskRecord, expected: &LocalTaskRecord) -> bool {
    current.meta().task_id() == expected.meta().task_id()
        && current.status().head_oid() == expected.status().head_oid()
        && current.status().turns().len() == expected.status().turns().len()
        && current.status().turns().last().map(TurnSummary::turn_id)
            == expected.status().turns().last().map(TurnSummary::turn_id)
}
const WAIT_POLL: Duration = Duration::from_millis(100);
const WAIT_MAX_POLL: Duration = Duration::from_secs(1);

thread_local! {
    static ADVANCING_PENDING_DAGS: Cell<bool> = const { Cell::new(false) };
}

struct InProcessSubmitLocks {
    held: Mutex<HashSet<(ClientId, TaskId)>>,
    cond: Condvar,
}

fn in_process_submit_locks() -> &'static InProcessSubmitLocks {
    static LOCKS: OnceLock<InProcessSubmitLocks> = OnceLock::new();
    LOCKS.get_or_init(|| InProcessSubmitLocks {
        held: Mutex::new(HashSet::new()),
        cond: Condvar::new(),
    })
}

struct InProcessSubmitGuard {
    client_id: ClientId,
    task_id: TaskId,
}

impl Drop for InProcessSubmitGuard {
    fn drop(&mut self) {
        let locks = in_process_submit_locks();
        let mut held = locks.held.lock().expect("in-process submit guard");
        held.remove(&(self.client_id, self.task_id));
        locks.cond.notify_all();
    }
}

/// Blocks same-store same-ID submits in this process. Keyed by client/task so
/// unrelated stores and ordinary unique-ID submits do not wait. Acquire
/// before TransferRepo; drop before attached runner reentry.
fn acquire_in_process_submit_guard(client_id: ClientId, task_id: TaskId) -> InProcessSubmitGuard {
    let locks = in_process_submit_locks();
    let mut held = locks.held.lock().expect("in-process submit guard");
    while !held.insert((client_id, task_id)) {
        held = locks.cond.wait(held).expect("in-process submit guard");
    }
    InProcessSubmitGuard { client_id, task_id }
}

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
    delivery: Option<OriginDelivery>,
    deliveries: Vec<OriginDelivery>,
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

    pub fn delivery(&self) -> Option<&OriginDelivery> {
        self.delivery.as_ref()
    }

    pub fn deliveries(&self) -> &[OriginDelivery] {
        &self.deliveries
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
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub acceptance: Vec<String>,
}

const MAX_BATCH_ID_BYTES: usize = 64;
const MAX_BATCH_FILES: usize = 32;
const MAX_BATCH_ACCEPTANCE: usize = 16;
const MAX_BATCH_ACCEPTANCE_BYTES: usize = 512;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BatchPreview {
    pub preview_version: u32,
    pub dag: DagPreview,
    pub setup: SetupPreview,
    pub tasks: Vec<TaskPreview>,
    pub issues: Vec<PreviewIssue>,
}

impl BatchPreview {
    pub fn has_config_errors(&self) -> bool {
        self.issues.iter().any(|issue| issue.severity == "error")
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DagPreview {
    pub enforced: bool,
    pub status: &'static str,
    pub message: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SetupPreview {
    pub present: bool,
    pub timeout: Option<String>,
    pub commands: Vec<String>,
    pub check: Option<String>,
    pub lockfiles: Vec<String>,
    pub inputs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TaskPreview {
    pub index: usize,
    pub id: String,
    pub title: Option<String>,
    pub agent: String,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub base: String,
    pub wip: bool,
    pub source: String,
    pub publish: Vec<String>,
    pub worker: Option<String>,
    pub env_profile: Option<String>,
    pub depends_on: Vec<String>,
    pub files: Vec<String>,
    pub acceptance: Vec<String>,
    pub acceptance_role: &'static str,
    pub setup_required: bool,
    pub overlaps: Vec<PreviewOverlap>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PreviewOverlap {
    pub with: usize,
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PreviewIssue {
    pub severity: &'static str,
    pub kind: &'static str,
    pub message: String,
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
        self.submit_with_ids(request, None, None, None, stdout, stderr, false, None, None)
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
        self.submit_with_ids(
            request, None, None, title, stdout, stderr, false, None, None,
        )
    }

    /// FLOW/controller entry for a claimed DAG node. Reuses the frozen
    /// `submit_with_ids` resume path on this `ClientStateStore` and TransferRepo;
    /// it does not create another queue or store. FLOW owns the non-DAG
    /// `run_id = None` wrapper around this same inner transaction and must not
    /// copy the metadata/queue/rollback body; this DAG path always passes
    /// `Some(run_id)`.
    pub fn submit_prepared_dag_node(
        &self,
        run_id: RunId,
        node: &DagNode,
        skip_reconcile: bool,
    ) -> Result<TaskReport, WorkerError> {
        let request = request_from_frozen_node(node, Some(run_id))?;
        self.submit_with_ids(
            request,
            Some(node.task_id),
            Some(node.turn_id),
            node.frozen.title.clone(),
            &mut io::sink(),
            &mut io::sink(),
            skip_reconcile,
            Some(node),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    /// Shared guarded submit transaction (full metadata, queue, intent, rollback
    /// CAS). `submit_prepared_dag_node` is the DAG wrapper. FLOW's non-DAG
    /// adapter must call this with `request.run_id = None` and the durable
    /// `original_created_at` instead of copying this body.
    pub(crate) fn submit_with_ids(
        &self,
        request: TaskSubmitRequest,
        task_id_override: Option<TaskId>,
        turn_id_override: Option<TurnId>,
        title: Option<String>,
        stdout: &mut dyn Write,
        _stderr: &mut dyn Write,
        skip_reconcile: bool,
        frozen: Option<&DagNode>,
        original_created_at: Option<u64>,
    ) -> Result<TaskReport, WorkerError> {
        if !skip_reconcile {
            self.reconcile_runners()?;
        }
        validate_prompt(&request.prompt)?;
        validate_task_agent(request.agent)?;
        validate_preference(self.config, &request.preference)?;

        let identity = GitIdentity::new(DEFAULT_GIT_NAME, DEFAULT_GIT_EMAIL)?;
        let (
            context,
            limits,
            env_profile,
            model,
            effort,
            publish,
            publish_branch,
            source,
            requirements,
            policy,
            prepared_base,
            settings,
            task_id,
            turn_id,
            title,
        ) = if let Some(node) = frozen {
            let spec = &node.frozen;
            let context = ProjectInspector::new(self.runner)
                .inspect_with_pinned_project_id(Path::new(&spec.project_path), &spec.project_id)?;
            if context.worktree_id != spec.worktree_id {
                return Err(task_error(
                    "TASK_CONFIG_INVALID",
                    "frozen DAG project identity does not match the worktree",
                ));
            }
            let limits = spec.limits()?;
            let env_profile = spec.env_profile.clone();
            let model = spec.model.clone();
            let effort = spec.effort.clone();
            let publish = parse_publish_modes(&spec.publish)?;
            let origin_url = spec.origin_url.clone().unwrap_or_default();
            let source = parse_task_source(
                &spec.source,
                spec.wip,
                &origin_url,
                publish.contains(&PublishMode::Push),
            )?;
            let publish_branch = parse_publish_branch(spec.publish_branch.as_deref(), &publish)?;
            if spec.wip && publish.contains(&PublishMode::Push) {
                return Err(task_error(
                    "PUBLISH_REQUIRES_COMMITTED_BASE",
                    "publish push requires a committed base",
                ));
            }
            let requirements = spec.requires.clone();
            let policy = spec.permission_policy()?;
            let observations = self.observe_admission(&request.preference)?;
            let affinity = self
                .client_state
                .affinity_hints(&context.project_id, &context.worktree_id)?;
            if let Selection::NoEligible { rejections } = SchedulerPolicy::select(
                &observations,
                &requirements,
                &request.preference,
                &affinity,
            ) {
                if let WorkerPreference::Pinned { worker } = &request.preference
                    && let Some(missing) = rejections.iter().find_map(|rejection| match rejection {
                        crate::scheduler::CandidateRejection::MissingCapabilities {
                            name,
                            missing,
                        } if name == worker => Some(missing.as_slice()),
                        _ => None,
                    })
                {
                    return Err(capability_missing(worker, missing));
                }
                if !request.wait_for_capacity {
                    return Err(capacity_busy());
                }
            }
            let oid = node.execution_oid().cloned().ok_or_else(|| {
                task_error(
                    "TASK_CONFIG_INVALID",
                    "DAG node has no frozen or bound base OID",
                )
            })?;
            // Bound from: objects live in the local TransferRepo. Rewrite Origin
            // to Local so host prepare consumes the pushed cache object instead of
            // fetching an unpublished OID. Keep push_target and frozen requires.
            let imported_from_parent = node.from_parent().is_some() && node.bound_oid.is_some();
            let source = if imported_from_parent {
                execution_source_for_bound_from(source, &publish)?
            } else {
                if let TaskSource::Origin { url } = &source {
                    GitTransport::new(self.runner).preflight_origin(url, &oid)?;
                }
                source
            };
            let prepared_base = PreparedSubmitBase::Ready(
                crate::transfer_repo::BaseCommit::from_pinned(oid, spec.wip),
            );
            let settings = ProjectState::settings_from_frozen(spec);
            (
                context,
                limits,
                env_profile,
                model,
                effort,
                publish,
                publish_branch,
                source,
                requirements,
                policy,
                prepared_base,
                settings,
                node.task_id,
                node.turn_id,
                spec.title.clone().or(title),
            )
        } else {
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
                initial.origin.clone().ok_or_else(|| {
                    task_error("INVALID_ORIGIN", "project origin is not configured")
                })?
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
            if let Selection::NoEligible { rejections } = SchedulerPolicy::select(
                &observations,
                &requirements,
                &request.preference,
                &affinity,
            ) {
                if let WorkerPreference::Pinned { worker } = &request.preference
                    && let Some(missing) = rejections.iter().find_map(|rejection| match rejection {
                        crate::scheduler::CandidateRejection::MissingCapabilities {
                            name,
                            missing,
                        } if name == worker => Some(missing.as_slice()),
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
            let turn_id = turn_id_override.unwrap_or_else(TurnId::generate);
            let prepared_base = if let TaskSource::Origin { url } = &source {
                let oid =
                    TransferRepo::resolve_base_oid(self.runner, &initial.context, &request.base)?;
                GitTransport::new(self.runner).preflight_origin(url, &oid)?;
                PreparedSubmitBase::Ready(crate::transfer_repo::BaseCommit::from_origin(oid))
            } else {
                PreparedSubmitBase::Resolve {
                    wip: request.wip,
                    request_base: request.base.clone(),
                }
            };
            let policy = permission_policy(settings, request.agent);
            (
                initial.context,
                limits,
                env_profile,
                model,
                effort,
                publish,
                publish_branch,
                source,
                requirements,
                policy,
                prepared_base,
                initial.settings,
                task_id,
                turn_id,
                title,
            )
        };
        let now = current_time_millis()?;
        let created_at = original_created_at.unwrap_or(now);
        let submit_guard = acquire_in_process_submit_guard(self.client_state.client_id(), task_id);
        self.client_state
            .reach_concurrency_point(ClientStateConcurrencyPoint::DagSubmit);
        let transfer = TransferRepo::open_or_create(&self.paths.cache, &context.common_dir)?;
        let built_local_base = matches!(prepared_base, PreparedSubmitBase::Resolve { .. });
        let base = match prepared_base {
            PreparedSubmitBase::Ready(base) => base,
            PreparedSubmitBase::Resolve { wip, request_base } => {
                if wip {
                    transfer.build_wip_base(self.runner, &context, task_id, &settings, &identity)?
                } else {
                    transfer.resolve_base(self.runner, &context, &request_base)?
                }
            }
        };
        if !matches!(source, TaskSource::Origin { .. })
            && let Err(error) = transfer.check_sensitive_tree(self.runner, base.oid(), &settings)
        {
            if built_local_base {
                let _ = transfer.release_base(self.runner, task_id);
            }
            return Err(error);
        }
        let reserved_branch = publish.contains(&PublishMode::Push).then(|| {
            publish_branch
                .clone()
                .unwrap_or_else(|| BranchName::for_task(task_id))
        });
        let mut resuming = false;
        let mut record_for_rollback =
            if let Some(existing) = self.client_state.load_task_optional(task_id)? {
                let existing = self.recover_existing_submit_record(existing)?;
                self.require_matching_submit_record(
                    &existing,
                    &request,
                    task_id,
                    turn_id,
                    base.oid(),
                    &context.project_id,
                    &context.worktree_id,
                    &source,
                    &publish,
                    publish_branch.as_ref(),
                    &limits,
                    policy,
                    request.close_policy,
                    model.as_deref(),
                    effort.as_deref(),
                    env_profile.as_deref(),
                    title.as_deref(),
                    original_created_at,
                )?;
                resuming = true;
                existing
            } else {
                let meta = TaskMeta::new(TaskMetaInput {
                    task_id,
                    run_id: request.run_id,
                    project_id: context.project_id.clone(),
                    worktree_id: context.worktree_id.clone(),
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
                    created_at_millis: created_at,
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
                LocalTaskRecord::new(
                    meta,
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
                .with_submission_intent_turn_id(turn_id)?
            };
        let composed_prompt = {
            let branch = match frozen {
                Some(node) => node.frozen.branch.as_deref(),
                None => context.branch.as_deref(),
            };
            match self.client_state.read_turn_prompt(task_id, turn_id) {
                Ok(existing) => {
                    let expected = compose_turn_prompt(
                        task_id,
                        1,
                        request.agent,
                        base.oid(),
                        branch,
                        &request.prompt,
                        false,
                    );
                    if existing != expected {
                        let _ = transfer.release_base(self.runner, task_id);
                        return Err(task_error(
                            "TASK_ID_CONFLICT",
                            "task ID is already present with different metadata",
                        ));
                    }
                    existing
                }
                Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    compose_turn_prompt(
                        task_id,
                        1,
                        request.agent,
                        base.oid(),
                        branch,
                        &request.prompt,
                        false,
                    )
                }
                Err(error) => {
                    let _ = transfer.release_base(self.runner, task_id);
                    return Err(error);
                }
            }
        };
        if let Err(error) = validate_prompt(&composed_prompt) {
            let _ = transfer.release_base(self.runner, task_id);
            return Err(error);
        }
        let mut preserve_durable = resuming || frozen.is_some();
        if resuming
            && crate::dag::dag_submission_complete(&record_for_rollback)
            && self.client_state.read_turn_prompt(task_id, turn_id).is_ok()
            && self.client_state.queue_entry(turn_id)?.is_some()
        {
            if let Some(entry) = self.client_state.queue_entry(turn_id)? {
                self.require_matching_queue_row(
                    &entry,
                    turn_id,
                    &context.project_id,
                    &context.worktree_id,
                    &requirements,
                    &request.preference,
                    request.run_id,
                )?;
            }
            drop(transfer);
            let mut report = self.report_for(task_id)?;
            report.events.push(event_task_created(&report));
            return Ok(report);
        }
        let reservation = if resuming {
            None
        } else {
            match (request.run_id, reserved_branch.as_ref()) {
                (Some(run_id), Some(branch)) => {
                    match self.client_state.reserve_run_publish_branch_for_task(
                        run_id,
                        task_id,
                        branch.clone(),
                    ) {
                        Ok(run) => Some(run),
                        Err(error) => {
                            let _ = transfer.release_base(self.runner, task_id);
                            return Err(error);
                        }
                    }
                }
                _ => None,
            }
        };
        if !resuming {
            match self.client_state.create_task(record_for_rollback.clone()) {
                Ok(()) => {}
                Err(error) if error.public_code() == "TASK_ID_CONFLICT" => {
                    let existing =
                        self.recover_existing_submit_record(self.client_state.load_task(task_id)?)?;
                    self.require_matching_submit_record(
                        &existing,
                        &request,
                        task_id,
                        turn_id,
                        base.oid(),
                        &context.project_id,
                        &context.worktree_id,
                        record_for_rollback.meta().source(),
                        record_for_rollback.meta().publish(),
                        record_for_rollback.meta().publish_branch(),
                        record_for_rollback.meta().limits(),
                        record_for_rollback.meta().policy(),
                        record_for_rollback.meta().close_policy(),
                        record_for_rollback.meta().model(),
                        record_for_rollback.meta().effort(),
                        record_for_rollback.meta().env_profile(),
                        Some(record_for_rollback.meta().title().as_str()),
                        original_created_at,
                    )?;
                    record_for_rollback = existing;
                    resuming = true;
                    preserve_durable = true;
                }
                Err(error) => {
                    if reservation.is_some()
                        && let (Some(run_id), Some(branch)) =
                            (request.run_id, reserved_branch.as_ref())
                    {
                        let _ = self
                            .client_state
                            .release_run_publish_branch_for_task(run_id, task_id, branch);
                    }
                    let _ = transfer.release_base(self.runner, task_id);
                    return Err(error);
                }
            }
        }
        let (mut report, queued_turn) = match (|| -> Result<(TaskReport, TurnId), WorkerError> {
            self.client_state
                .write_task_project_path(&record_for_rollback, &context.root)?;
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
                context.project_id.clone(),
                context.worktree_id.clone(),
                command.summary()?,
                requirements.clone(),
                request.preference.clone(),
                QueueEntryKind::TaskTurn,
                run_reference,
                owner,
                now,
            )?;
            let entry = match self.client_state.enqueue(entry) {
                Ok(entry) => entry,
                Err(error) if error.public_code() == "QUEUE_JOB_CONFLICT" => {
                    let existing = self.client_state.queue_entry(turn_id)?.ok_or(error)?;
                    self.require_matching_queue_row(
                        &existing,
                        turn_id,
                        &context.project_id,
                        &context.worktree_id,
                        &requirements,
                        &request.preference,
                        request.run_id,
                    )?;
                    existing
                }
                Err(error) => return Err(error),
            };

            self.client_state.submission_report_fault()?;
            let report = self.report_for(task_id)?;
            Ok((report, entry.job_id()))
        })() {
            Ok(report) => report,
            Err(error) => {
                if !preserve_durable {
                    self.rollback_submission(turn_id, &record_for_rollback, &transfer);
                }
                return Err(error);
            }
        };
        // Intent clearance is the submission handoff boundary. Its atomic
        // replacement can publish the clear before the final directory sync
        // reports an error, so this caller cannot safely compensate from its
        // pre-handoff snapshot. Retain the complete durable submission for
        // reconciliation rather than overwriting a runner that may already
        // have adopted the published row.
        let current = self.client_state.load_task(task_id)?;
        if current.submission_intent_turn_id().is_some() {
            self.client_state
                .clear_submission_intent(current.without_submission_intent()?)?;
        }
        if !(resuming && self.submission_handoff_already_adopted(task_id, turn_id)?) {
            match self.start_runner(task_id, turn_id, request.attached, false)? {
                RunnerStart::Started(_) => {
                    report.runner = self.client_state.runner_liveness(task_id)?;
                }
                RunnerStart::Pending => {}
                RunnerStart::Saturated => {
                    // Parking publishes eligibility to an independent runner. A park
                    // error may therefore mean ownership already transferred, so
                    // preserve durable state for reconciliation.
                    if self
                        .client_state
                        .queue_entry(queued_turn)?
                        .is_some_and(|entry| matches!(entry.state(), QueueState::Waiting { .. }))
                    {
                        self.client_state.park_row(queued_turn)?;
                    }
                }
            }
        } else {
            report.runner = self.client_state.runner_liveness(task_id)?;
        }
        // The submitter no longer needs the repository after the queue row is
        // handed off.  Its shared lock would not block the attached runner,
        // but a handle that outlives its use is a handle GC has to wait for.
        drop(transfer);
        drop(submit_guard);
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

    fn recover_existing_submit_record(
        &self,
        existing: LocalTaskRecord,
    ) -> Result<LocalTaskRecord, WorkerError> {
        let mut current = existing;
        for _ in 0..SUBMISSION_ROLLBACK_RECOVER_ATTEMPTS {
            if current.abandon_code() != Some(SUBMISSION_ROLLBACK_INCOMPLETE) {
                return Ok(current);
            }
            self.client_state
                .reach_concurrency_point(ClientStateConcurrencyPoint::SubmissionRollbackRecover);
            let recovered = current.without_submission_rollback()?;
            if self
                .client_state
                .update_task_if_current(&current, recovered.clone())?
            {
                return Ok(recovered);
            }
            current = self.client_state.load_task(current.meta().task_id())?;
        }
        if current.abandon_code() != Some(SUBMISSION_ROLLBACK_INCOMPLETE) {
            return Ok(current);
        }
        Err(task_error(
            "TASK_ID_CONFLICT",
            "submission rollback recovery raced with a newer task record",
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn require_matching_submit_record(
        &self,
        existing: &LocalTaskRecord,
        request: &TaskSubmitRequest,
        task_id: TaskId,
        turn_id: TurnId,
        base_oid: &crate::task::BaseOid,
        project_id: &str,
        worktree_id: &str,
        source: &TaskSource,
        publish: &[PublishMode],
        publish_branch: Option<&crate::task::BranchName>,
        limits: &TaskLimits,
        policy: PermissionPolicy,
        close_policy: ClosePolicy,
        model: Option<&str>,
        effort: Option<&str>,
        env_profile: Option<&str>,
        title: Option<&str>,
        original_created_at: Option<u64>,
    ) -> Result<(), WorkerError> {
        if existing.meta().task_id() != task_id
            || existing.meta().base_oid() != base_oid
            || existing.meta().project_id() != project_id
            || existing.meta().worktree_id() != worktree_id
            || existing.meta().run_id() != request.run_id
            || existing.meta().agent() != request.agent
            || existing.preference() != request.preference
            || existing.meta().source() != source
            || existing.meta().publish() != publish
            || existing.meta().publish_branch() != publish_branch
            || existing.meta().limits() != limits
            || existing.meta().policy() != policy
            || existing.meta().close_policy() != close_policy
            || existing.meta().model() != model
            || existing.meta().effort() != effort
            || existing.meta().env_profile() != env_profile
            || title.is_some_and(|title| existing.meta().title().as_str() != title)
            || original_created_at
                .is_some_and(|created_at| existing.meta().created_at_millis() != created_at)
        {
            return Err(task_error(
                "TASK_ID_CONFLICT",
                "task ID is already present with different metadata",
            ));
        }
        if existing
            .submission_intent_turn_id()
            .is_some_and(|intent| intent != turn_id)
            || existing
                .submission_rollback_turn_id()
                .is_some_and(|rollback| rollback != turn_id)
        {
            return Err(task_error(
                "TASK_ID_CONFLICT",
                "task ID is already present with different metadata",
            ));
        }
        let turns = self.client_state.turn_ids_for_task(task_id)?;
        if turns.iter().any(|id| *id != turn_id) {
            return Err(task_error(
                "TASK_ID_CONFLICT",
                "task ID is already present with different metadata",
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn require_matching_queue_row(
        &self,
        existing: &QueueEntry,
        turn_id: TurnId,
        project_id: &str,
        worktree_id: &str,
        requirements: &[String],
        preference: &WorkerPreference,
        run_id: Option<RunId>,
    ) -> Result<(), WorkerError> {
        let expected_run = run_id.map(|run_id| run_id.to_string());
        let actual_run = existing.run().map(|run| run.run_id().as_str().to_owned());
        if existing.job_id() != turn_id
            || existing.client_id() != self.client_state.client_id()
            || existing.project_id() != project_id
            || existing.worktree_id() != worktree_id
            || existing.kind() != QueueEntryKind::TaskTurn
            || existing.requirements() != requirements
            || existing.preference() != preference
            || expected_run != actual_run
        {
            return Err(task_error(
                "TASK_ID_CONFLICT",
                "task turn queue row does not match this submission",
            ));
        }
        Ok(())
    }

    fn submission_handoff_already_adopted(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<bool, WorkerError> {
        if self.client_state.runner_liveness(task_id)? != Some(RunnerState::Live) {
            return Ok(false);
        }
        Ok(self
            .client_state
            .queue_entry(turn_id)?
            .is_some_and(|entry| matches!(entry.state(), QueueState::Dispatching { .. })))
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
        let mut projection = project_task_list_with_blocking_codes(
            &records,
            &runs,
            &runner_states,
            &freshness,
            &blocking_codes,
        )
        .map_err(task_view_error)?;
        for dag in self.client_state.list_run_dags()? {
            if filter.run_id.is_none_or(|run_id| dag.run_id == run_id) {
                merge_pending_into_projection(
                    &mut projection,
                    dag.run_id,
                    &dag,
                    dag.created_at_millis,
                );
            }
        }
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
        self.client_state
            .reach_concurrency_point(ClientStateConcurrencyPoint::AfterTaskFetchLoad);
        if record.status().state() == TaskState::Abandoned {
            return Err(task_error(
                "TASK_CLOSED",
                "discarded tasks cannot be fetched",
            ));
        }
        let worker = task_worker(self.config, record.status())?;
        let project = self.load_project_for_record(&record)?;
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
        let _ = self
            .client_state
            .update_fetched_head_for_current_turn(&record, imported.head().clone())?;
        Ok(FetchReport {
            task_id,
            head: imported.head().clone(),
            local_ref: imported.local_ref().to_owned(),
        })
    }

    pub fn close(&self, task_id: TaskId, discard: bool) -> Result<TaskReport, WorkerError> {
        self.reconcile_runners()?;
        let record = self.client_state.load_task(task_id)?;
        self.close_from_expected(&record, discard)
    }

    /// Closes against a caller-supplied expected snapshot. Dashboard mutations
    /// pass the revision they already validated; CLI `close` loads after
    /// reconcile and then uses this path.
    pub fn close_from_expected(
        &self,
        expected: &LocalTaskRecord,
        discard: bool,
    ) -> Result<TaskReport, WorkerError> {
        let task_id = expected.meta().task_id();
        let record = self.client_state.load_task(task_id)?;
        if matches!(
            record.status().state(),
            TaskState::Closed | TaskState::Abandoned
        ) {
            if same_close_target(&record, expected)
                && record
                    .close_intent()
                    .is_none_or(|intent| intent.matches_record(&record))
            {
                let _ = self.release_task_base(&record);
                return self.report_for(task_id);
            }
            return Err(task_error("TASK_CLOSED", "task is terminal"));
        }
        if record.status().state() == TaskState::Lost {
            return Err(task_error("TASK_CLOSED", "task is terminal"));
        }
        if !operator_revision_matches(&record, expected) {
            return Err(task_error(
                "TASK_REVISION_CONFLICT",
                "task changed before close",
            ));
        }
        if let Some(reason) = self.operator_busy_reason(task_id, &record)?
            && !(record.close_intent().is_some() && reason == CLOSE_IN_PROGRESS)
        {
            return Err(task_error("TASK_BUSY", reason));
        }
        if record.close_intent().is_none()
            && let Some(entry) = self.client_state.queue_entry_for_task_turn(task_id)?
        {
            let mut log =
                crate::runner_log::RunnerLog::open(&self.paths.state, task_id, entry.job_id())?;
            if log.current_entry(self.client_state)?.as_ref() != Some(&entry) {
                return Err(task_error("TASK_BUSY", "task turn changed before close"));
            }
            let record = self.client_state.load_task(task_id)?;
            if !operator_revision_matches(&record, expected) {
                return Err(task_error(
                    "TASK_REVISION_CONFLICT",
                    "task changed before close",
                ));
            }
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
            )?
            .copying_reported_checks(record.status())?;
            let closed = record
                .with_status(status)?
                .with_abandon_code(discard.then_some("TASK_CLOSED".to_owned()))?;
            if !self
                .client_state
                .update_task_if_current(&record, closed.clone())?
            {
                return Err(task_error(
                    "TASK_REVISION_CONFLICT",
                    "task changed before close",
                ));
            }
            self.finish_waiting_cancellation_locked(task_id, &retained, &mut log)?;
            let report = self.report_for(task_id)?;
            self.advance_pending_dags()?;
            return Ok(report);
        }
        if record.close_intent().is_none() && record.status().state() == TaskState::Queued {
            return Err(task_error(
                "TASK_INCONSISTENT",
                "queued task has no queue turn",
            ));
        }
        let fenced = if record.close_intent().is_some() {
            record
        } else {
            if expected.status().state() != TaskState::Open {
                return Err(task_error("TASK_BUSY", "task is not ready to close"));
            }
            let intent = TaskCloseIntent::from_record(expected, discard)?;
            let fenced = expected.with_close_intent(intent)?;
            if !self
                .client_state
                .update_task_if_current(expected, fenced.clone())?
            {
                return Err(task_error(
                    "TASK_REVISION_CONFLICT",
                    "task changed before close",
                ));
            }
            fenced
        };
        let discard = fenced
            .close_intent()
            .map(TaskCloseIntent::discard)
            .unwrap_or(discard);
        let worker = task_worker(self.config, fenced.status())?;
        let response = RemoteJobClient::new(self.runner).task_close(
            worker,
            &crate::task_store::TaskCloseRequest::new(fenced.meta().project_id(), task_id, discard),
        )?;
        let warnings = response.warnings().to_vec();
        let current = self.client_state.load_task(task_id)?;
        if matches!(
            current.status().state(),
            TaskState::Closed | TaskState::Abandoned
        ) {
            let _ = self.release_task_base(&current);
            return self.report_for(task_id);
        }
        let closed_status = response
            .status()
            .clone()
            .copying_reported_checks_if_empty(current.status())?;
        let closed = current
            .without_close_intent()?
            .with_status(closed_status)?
            .with_deliveries(merge_origin_deliveries(
                current.deliveries(),
                response.deliveries(),
            ))?;
        if !self.client_state.update_task_if_current(&current, closed)? {
            let after = self.client_state.load_task(task_id)?;
            if matches!(
                after.status().state(),
                TaskState::Closed | TaskState::Abandoned
            ) {
                let _ = self.release_task_base(&after);
                return self.report_for(task_id);
            }
            return Err(task_error(
                "TASK_REVISION_CONFLICT",
                "task changed before close completed",
            ));
        }
        self.release_task_base(&fenced)?;
        let mut report = self.report_for(task_id)?;
        report.warnings = warnings;
        self.advance_pending_dags()?;
        Ok(report)
    }

    pub fn reconcile_runners(&self) -> Result<ReconcileReport, WorkerError> {
        self.reconcile_runners_inner(false)
    }

    /// Operator-driven `worker task reconcile`: clears the restart budget and
    /// retries once instead of waiting out backoff or leaving a parked row.
    pub fn operator_reconcile(&self) -> Result<ReconcileReport, WorkerError> {
        self.reconcile_runners_inner(true)
    }

    fn reconcile_runners_inner(
        &self,
        reset_failure_budget: bool,
    ) -> Result<ReconcileReport, WorkerError> {
        let mut report = ReconcileReport::default();
        let owner = current_process_identity()?;
        if reset_failure_budget {
            self.client_state.reset_replacement_failures(owner)?;
        }
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
            if record.close_intent().is_some() {
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
            if record
                .status()
                .turns()
                .iter()
                .any(|turn| turn.turn_id() == turn_id && turn.terminal().is_some())
            {
                continue;
            }
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
                // An explicit operator reconcile just cleared the budget so
                // this pass can retry. Old diagnostic lines must not rebuild
                // it and put the row back into backoff.
                if !reset_failure_budget {
                    self.observe_replacement_failure(record.meta().task_id(), turn_id)?;
                }
                let Some(current_entry) = self.client_state.queue_entry(turn_id)? else {
                    continue;
                };
                if let Some(budget) = current_entry.replacement_failure() {
                    if budget.should_park() {
                        self.park_repeated_replacement_failure(turn_id, record.meta().task_id())?;
                        continue;
                    }
                    if !budget.backoff_elapsed(current_time_millis()?) {
                        continue;
                    }
                }
                match self.start_runner(record.meta().task_id(), turn_id, false, false)? {
                    RunnerStart::Started(_) => report.started_runners += 1,
                    RunnerStart::Pending | RunnerStart::Saturated => {}
                }
            }
        }

        while self.start_oldest_parked_runner(owner)? {
            report.started_runners += 1;
        }
        self.advance_pending_dags()?;
        Ok(report)
    }

    pub fn say(
        &self,
        task_id: TaskId,
        message: String,
        attached: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<TaskReport, WorkerError> {
        self.reconcile_runners()?;
        let record = self.client_state.load_task(task_id)?;
        self.say_from_expected(&record, message, attached, stdout, stderr)
    }

    /// Follows up against a caller-supplied expected snapshot. Dashboard reply
    /// passes the revision it already validated so a later reload cannot win.
    pub fn say_from_expected(
        &self,
        expected: &LocalTaskRecord,
        message: String,
        attached: bool,
        stdout: &mut dyn Write,
        _stderr: &mut dyn Write,
    ) -> Result<TaskReport, WorkerError> {
        let task_id = expected.meta().task_id();
        let current = self.client_state.load_task(task_id)?;
        if !operator_revision_matches(&current, expected) {
            return Err(task_error(
                "TASK_REVISION_CONFLICT",
                "task changed before follow-up",
            ));
        }
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
        if expected.close_intent().is_some() || current.close_intent().is_some() {
            return Err(task_error("TASK_BUSY", CLOSE_IN_PROGRESS));
        }
        match expected.status().state() {
            TaskState::Active => return Err(task_error("TASK_BUSY", "task has an active turn")),
            TaskState::Closed | TaskState::Abandoned | TaskState::Lost => {
                return Err(task_error("TASK_CLOSED", "task is terminal"));
            }
            TaskState::Queued => {
                return Err(task_error("TASK_BUSY", "task has not reached an open turn"));
            }
            TaskState::Open => {}
        }
        let followups = expected.status().turns().len().saturating_sub(1) as u32;
        if followups >= expected.meta().limits().max_followups {
            return Err(task_error(
                "FOLLOWUP_LIMIT",
                "task follow-up limit has been reached",
            ));
        }
        let worker = task_worker(self.config, expected.status())?.name.clone();
        let turn_id = TurnId::generate();
        let turn_number = u32::try_from(expected.status().turns().len())
            .ok()
            .and_then(|turns| turns.checked_add(1))
            .ok_or_else(|| task_error("TASK_INCONSISTENT", "turn history is too long"))?;
        let base_oid = expected
            .status()
            .head_oid()
            .cloned()
            .unwrap_or_else(|| expected.meta().base_oid().clone());
        let composed_prompt = compose_turn_prompt(
            task_id,
            turn_number,
            expected.meta().agent(),
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
            expected.status().last_outcome().cloned(),
            Some(worker.clone()),
            true,
            Some(base_oid),
            expected.status().summary().map(str::to_owned),
            expected.status().questions().to_vec(),
            expected.status().files_changed().to_vec(),
            expected.status().diff_stat().map(str::to_owned),
            expected
                .status()
                .turns()
                .iter()
                .cloned()
                .chain([pending])
                .collect(),
            current_time_millis()?,
        )?
        .copying_reported_checks(expected.status())?;
        let active_record = expected
            .with_status(active)?
            .with_runner(None)?
            .with_abandon_code(None)?;
        if !self
            .client_state
            .update_task_if_current(expected, active_record.clone())?
        {
            let _ = self.client_state.remove_turn_prompt(task_id, turn_id);
            return Err(task_error(
                "TASK_REVISION_CONFLICT",
                "task changed before follow-up",
            ));
        }
        let entry = match self.enqueue_followup(expected, turn_id, worker) {
            Ok(entry) => entry,
            Err(error) => {
                self.rollback_followup(&active_record, expected, task_id, turn_id);
                return Err(error);
            }
        };
        match self.start_runner(task_id, turn_id, attached, false) {
            Ok(RunnerStart::Started(_)) | Ok(RunnerStart::Pending) => {}
            Ok(RunnerStart::Saturated) => {
                if let Err(error) = self.client_state.park_row(entry.job_id()) {
                    self.rollback_followup(&active_record, expected, task_id, turn_id);
                    return Err(error);
                }
            }
            Err(error) => {
                return Err(self.fail_handoff(
                    task_id,
                    turn_id,
                    self.transfer_for_record(expected)?,
                    error,
                ));
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
                    // An accepted turn parked after repeated replacement deaths
                    // still has a remote job. Local pre-acceptance finish would
                    // reject its journal; cancel it on the worker instead.
                    if !entry
                        .replacement_failure()
                        .is_some_and(|budget| budget.should_park())
                    {
                        self.finish_waiting_cancellation(&record, &entry)?;
                        return self.report_for(task_id);
                    }
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
        if self
            .client_state
            .queue_entry(turn_id)?
            .is_some_and(|entry| {
                matches!(entry.state(), QueueState::Parked)
                    && entry
                        .replacement_failure()
                        .is_some_and(|budget| budget.should_park())
            })
        {
            let _ = self.client_state.record_runner(task_id, None);
            self.client_state
                .remove_task_turn_after_terminal(turn_id, current_process_identity()?)?;
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
        loop {
            match self.reconcile_runners() {
                Ok(_) => {}
                Err(error) if is_queue_job_conflict(&error) => {}
                Err(error) => return Err(error),
            }
            let (task_ids, dag_pending) = match selector {
                WaitSelector::Task(task_id) => (vec![task_id], false),
                WaitSelector::Run(run_id) => {
                    let pending = self
                        .client_state
                        .load_run_dag(run_id)?
                        .is_some_and(|dag| !dag_run_is_quiescent(&dag));
                    (
                        self.client_state.load_run(run_id)?.task_ids().to_vec(),
                        pending,
                    )
                }
            };
            if let Some(error) = self.wait_blocked_error(&task_ids)? {
                return Err(error);
            }
            let records = task_ids
                .iter()
                .map(|task_id| self.client_state.load_task(*task_id))
                .collect::<Result<Vec<_>, _>>()?;
            if !dag_pending && self.tasks_are_quiescent(&records)? {
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

    pub fn preview_batch(&self, file: &Path) -> Result<BatchPreview, WorkerError> {
        preview_batch_plan(
            self.runner,
            self.config,
            file,
            &std::env::current_dir().map_err(WorkerError::Io)?,
        )
    }
}

/// Resolves a batch file the same way submit does, without opening client
/// state or talking to workers.
pub fn preview_batch_plan(
    runner: &dyn ProcessRunner,
    config: &Config,
    file: &Path,
    project: &Path,
) -> Result<BatchPreview, WorkerError> {
    let batch = load_batch_file(file)?;
    let state = ProjectState::load(runner, project, &[])?;
    let batch_dir = file.parent().unwrap_or_else(|| Path::new("."));
    let mut issues = Vec::new();
    let graph: Vec<GraphNode<'_>> = batch
        .tasks
        .iter()
        .map(|task| GraphNode {
            id: task.id.as_deref(),
            depends_on: &task.depends_on,
            base: task.base.as_deref().unwrap_or(&batch.defaults.base),
        })
        .collect();
    issues.extend(
        validate_batch_graph(&graph)
            .into_iter()
            .map(|issue| PreviewIssue {
                severity: "error",
                kind: issue.kind,
                message: issue.message,
            }),
    );
    let mut tasks = Vec::with_capacity(batch.tasks.len());
    for (index, task) in batch.tasks.iter().enumerate() {
        let files = task.files.clone();
        let mut overlaps = Vec::new();
        for (other_index, other) in batch.tasks.iter().enumerate() {
            if other_index <= index {
                continue;
            }
            let paths = overlapping_paths(&files, &other.files);
            if !paths.is_empty() {
                overlaps.push(PreviewOverlap {
                    with: other_index,
                    paths,
                });
            }
        }
        let resolved = resolve_batch_task(
            config,
            &batch.defaults,
            task,
            batch_dir,
            project,
            &state.settings.task,
        );
        let (agent, model, effort, mut base, wip, source, publish, worker, env_profile) =
            match &resolved {
                Ok(request) => (
                    agent_name(request.agent).to_owned(),
                    request.model.clone(),
                    request.effort.clone(),
                    request.base.clone(),
                    request.wip,
                    request
                        .source
                        .clone()
                        .unwrap_or_else(|| batch.defaults.source.clone()),
                    request
                        .publish
                        .clone()
                        .unwrap_or_else(|| batch.defaults.publish.clone()),
                    match &request.preference {
                        WorkerPreference::Pinned { worker } => Some(worker.clone()),
                        WorkerPreference::Automatic => None,
                    },
                    request.env_profile.clone(),
                ),
                Err(error) => {
                    issues.push(preview_issue_from_error(index, error));
                    (
                        task.agent
                            .clone()
                            .unwrap_or_else(|| batch.defaults.agent.clone()),
                        task.model.clone().or_else(|| batch.defaults.model.clone()),
                        task.effort
                            .clone()
                            .or_else(|| batch.defaults.effort.clone()),
                        task.base
                            .clone()
                            .unwrap_or_else(|| batch.defaults.base.clone()),
                        task.wip.unwrap_or(batch.defaults.wip),
                        task.source
                            .clone()
                            .unwrap_or_else(|| batch.defaults.source.clone()),
                        task.publish
                            .clone()
                            .unwrap_or_else(|| batch.defaults.publish.clone()),
                        task.worker
                            .clone()
                            .or_else(|| batch.defaults.worker.clone()),
                        task.env_profile
                            .clone()
                            .or_else(|| batch.defaults.env_profile.clone()),
                    )
                }
            };
        if !wip && parse_from_base(&base).is_none() {
            match TransferRepo::resolve_base_oid(runner, &state.context, &base) {
                Ok(oid) => {
                    base = oid.to_string();
                    if source == "origin" {
                        base.push_str(" (origin validation remains for submit)");
                    }
                }
                Err(_) if source == "origin" => {
                    base = format!("{base} (origin validation remains for submit)");
                }
                Err(error) => issues.push(preview_issue_from_error(index, &error)),
            }
        }
        tasks.push(TaskPreview {
            index,
            id: task_preview_id(task, index),
            title: task.title.clone(),
            agent,
            model,
            effort,
            base,
            wip,
            source,
            publish,
            worker,
            env_profile,
            depends_on: task.depends_on.clone(),
            files,
            acceptance: task.acceptance.clone(),
            acceptance_role: "declared_agent_instruction",
            setup_required: state.settings.setup.is_some(),
            overlaps,
        });
    }
    Ok(BatchPreview {
        preview_version: 1,
        dag: DagPreview {
            enforced: true,
            status: "enforced",
            message: "Dependencies execute when parents are Closed and Done.",
        },
        setup: setup_preview(state.settings.setup.as_ref()),
        tasks,
        issues,
    })
}

impl<'a> TaskClient<'a> {
    pub fn batch(
        &self,
        file: &Path,
        run_name: Option<String>,
        max_parallel: Option<u32>,
        _stdout: &mut dyn Write,
    ) -> Result<RunReport, WorkerError> {
        let batch = load_batch_file(file)?;
        let graph: Vec<GraphNode<'_>> = batch
            .tasks
            .iter()
            .map(|task| GraphNode {
                id: task.id.as_deref(),
                depends_on: &task.depends_on,
                base: task.base.as_deref().unwrap_or(&batch.defaults.base),
            })
            .collect();
        if let Some(issue) = validate_batch_graph(&graph).into_iter().next() {
            return Err(task_error("TASK_CONFIG_INVALID", issue.message));
        }
        if batch.tasks.is_empty() {
            return Err(task_error("TASK_CONFIG_INVALID", "batch has no tasks"));
        }
        self.reconcile_runners()?;
        if batch_has_dag_edges(&batch.defaults, &batch.tasks) {
            return self.submit_dependent_batch(file, &batch, run_name, max_parallel);
        }
        let max_parallel =
            resolve_batch_max_parallel(max_parallel, self.config.configured_runner_slots())?;
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
                false,
                None,
                None,
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
        let settings = ProjectState::load(self.runner, project, &[])?.settings.task;
        let mut request =
            resolve_batch_task(self.config, defaults, task, batch_dir, project, &settings)?;
        request.run_id = Some(run_id);
        Ok(request)
    }

    fn submit_dependent_batch(
        &self,
        file: &Path,
        batch: &BatchFile,
        run_name: Option<String>,
        max_parallel: Option<u32>,
    ) -> Result<RunReport, WorkerError> {
        if batch.tasks.iter().any(|task| task.id.is_none()) {
            return Err(task_error(
                "TASK_CONFIG_INVALID",
                "DAG batches require a task id on every task",
            ));
        }
        let max_parallel =
            resolve_batch_max_parallel(max_parallel, configured_slot_count(self.config))?;
        let run_id = RunId::generate();
        let created = current_time_millis()?;
        let batch_dir = file.parent().unwrap_or_else(|| Path::new("."));
        let project = self.current_project(None)?;
        let project_state = ProjectState::load(self.runner, &project, &[])?;
        let identity = GitIdentity::new(DEFAULT_GIT_NAME, DEFAULT_GIT_EMAIL)?;
        let transfer =
            TransferRepo::open_or_create(&self.paths.cache, &project_state.context.common_dir)?;
        let mut nodes = BTreeMap::new();
        let mut pins = Vec::new();
        let freeze_result = (|| -> Result<BTreeMap<String, DagNode>, WorkerError> {
            for task in &batch.tasks {
                let request = resolve_batch_task(
                    self.config,
                    &batch.defaults,
                    task,
                    batch_dir,
                    &project,
                    &project_state.settings.task,
                )?;
                let batch_id = task.id.clone().expect("id checked");
                let mut depends_on = task.depends_on.clone();
                if let Some(parent) = parse_from_base(&request.base)
                    && !depends_on.iter().any(|dep| dep == parent)
                {
                    depends_on.push(parent.to_owned());
                }
                let task_id = TaskId::generate();
                let turn_id = TurnId::generate();
                let mut frozen = freeze_spec(&request, &project_state)?;
                frozen.title = task.title.clone();
                let base = if let Some(parent) = parse_from_base(&request.base) {
                    DagBase::From {
                        parent: parent.to_owned(),
                    }
                } else {
                    let pin_ref = dag_pin_ref(run_id, &batch_id);
                    let (oid, wip) = if request.wip {
                        let commit = transfer.build_wip_base(
                            self.runner,
                            &project_state.context,
                            task_id,
                            &project_state.settings,
                            &identity,
                        )?;
                        (commit.oid().clone(), true)
                    } else {
                        (
                            TransferRepo::resolve_base_oid(
                                self.runner,
                                &project_state.context,
                                &request.base,
                            )?,
                            false,
                        )
                    };
                    transfer.pin_object(self.runner, &pin_ref, &oid)?;
                    pins.push(pin_ref.clone());
                    DagBase::Frozen { oid, pin_ref, wip }
                };
                nodes.insert(
                    batch_id.clone(),
                    DagNode {
                        batch_id,
                        task_id,
                        turn_id,
                        depends_on,
                        base,
                        frozen,
                        state: DagNodeState::Waiting,
                        bound_oid: None,
                        bound_turn_id: None,
                        pin_ref: None,
                        blocked_by: None,
                        claimed_by: None,
                        claimed_at_millis: None,
                    },
                );
            }
            Ok(nodes)
        })();
        let nodes = match freeze_result {
            Ok(nodes) => nodes,
            Err(error) => {
                for pin in &pins {
                    let _ = transfer.unpin_object(self.runner, pin);
                }
                return Err(error);
            }
        };
        let dag = match DagRecord::new(run_id, nodes, max_parallel, run_name.clone(), created) {
            Ok(dag) => dag,
            Err(error) => {
                for pin in &pins {
                    let _ = transfer.unpin_object(self.runner, pin);
                }
                return Err(error);
            }
        };
        let run = RunRecord::new(run_id, run_name, Vec::new(), max_parallel, created)?;
        if let Err(error) = self.client_state.create_run_with_dag(run, dag) {
            // DAG file may already be durable. Do not unpin a published graph
            // because the RunRecord write failed or crashed.
            if matches!(self.client_state.load_run_dag(run_id), Ok(None)) {
                for pin in &pins {
                    let _ = transfer.unpin_object(self.runner, pin);
                }
            }
            return Err(error);
        }
        drop(transfer);
        self.advance_pending_dags_inner(true)?;
        let task_ids = self.client_state.load_run(run_id)?.task_ids().to_vec();
        Ok(RunReport { run_id, task_ids })
    }

    pub fn advance_pending_dags(&self) -> Result<(), WorkerError> {
        let entered = ADVANCING_PENDING_DAGS.with(|flag| {
            if flag.get() {
                false
            } else {
                flag.set(true);
                true
            }
        });
        if !entered {
            return Ok(());
        }
        struct AdvancingGuard;
        impl Drop for AdvancingGuard {
            fn drop(&mut self) {
                ADVANCING_PENDING_DAGS.with(|flag| flag.set(false));
            }
        }
        let _guard = AdvancingGuard;
        self.advance_pending_dags_inner(true)
    }

    fn advance_pending_dags_inner(&self, skip_reconcile: bool) -> Result<(), WorkerError> {
        self.bind_ready_from_nodes()?;
        let caller = current_process_identity()?;
        let now = current_time_millis()?;
        for dag in self.client_state.list_pending_dags()? {
            while let Some(claim) = self
                .client_state
                .claim_next_eligible_dag_node(dag.run_id, caller, now)?
            {
                self.submit_prepared_dag_node(dag.run_id, &claim.node, skip_reconcile)?;
                self.client_state.mark_dag_node_submitted(
                    dag.run_id,
                    &claim.batch_id,
                    claim.node.task_id,
                )?;
            }
        }
        Ok(())
    }

    /// Pins `from:` children to the parent's exact last imported turn, then
    /// publishes the immutable DAG binding. TransferRepo is dropped before
    /// the state write that reacquires StateLock.
    fn bind_ready_from_nodes(&self) -> Result<(), WorkerError> {
        for dag in self.client_state.list_pending_dags()? {
            let candidates: Vec<(String, DagNode)> = dag
                .nodes
                .iter()
                .filter(|(_, node)| {
                    node.from_parent().is_some()
                        && node.bound_oid.is_none()
                        && node.state != DagNodeState::Blocked
                })
                .map(|(batch_id, node)| (batch_id.clone(), node.clone()))
                .collect();
            for (batch_id, node) in candidates {
                let Some(parent_id) = node.from_parent() else {
                    continue;
                };
                let Some(parent_node) = dag.nodes.get(parent_id) else {
                    continue;
                };
                let Some(parent_record) =
                    self.client_state.load_task_optional(parent_node.task_id)?
                else {
                    continue;
                };
                if parent_gate(&parent_record) != ParentGate::Ready {
                    continue;
                }
                let Some(last) = parent_record.status().turns().last() else {
                    continue;
                };
                let last_turn_id = last.turn_id();
                let queue_busy = self
                    .client_state
                    .queue_entry_for_task_turn(parent_record.meta().task_id())?
                    .is_some();
                let journal_complete = match crate::runner_log::RunnerLog::try_open_existing(
                    &self.paths.state,
                    parent_record.meta().task_id(),
                    last_turn_id,
                )? {
                    Some(log) => {
                        let complete = log.completion().is_some();
                        drop(log);
                        complete
                    }
                    None => false,
                };
                let transfer = self.transfer_for_record(&parent_record)?;
                let object_exists = parent_record
                    .fetched_head()
                    .is_some_and(|oid| transfer.has_object(oid));
                let Some(oid) = accepted_import_oid(
                    &parent_record,
                    last_turn_id,
                    queue_busy,
                    journal_complete,
                    object_exists,
                ) else {
                    drop(transfer);
                    continue;
                };
                let pin_ref = dag_pin_ref(dag.run_id, &batch_id);
                transfer.pin_object(self.runner, &pin_ref, &oid)?;
                drop(transfer);
                self.client_state.publish_dag_binding(
                    dag.run_id,
                    &batch_id,
                    oid,
                    last_turn_id,
                    pin_ref,
                )?;
            }
        }
        Ok(())
    }

    fn enqueue_followup(
        &self,
        record: &LocalTaskRecord,
        turn_id: TurnId,
        worker: String,
    ) -> Result<QueueEntry, WorkerError> {
        let project = self.load_project_for_record(record)?;
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

    fn rollback_followup(
        &self,
        committed_active: &LocalTaskRecord,
        previous_open: &LocalTaskRecord,
        task_id: TaskId,
        turn_id: TurnId,
    ) {
        let _ = self.client_state.remove_queued(turn_id);
        let _ = self.client_state.remove_turn_prompt(task_id, turn_id);
        let _ = self
            .client_state
            .update_task_if_current(committed_active, previous_open.clone());
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
        let project = self.load_project_for_record(record)?;
        TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)
    }

    fn refresh_task_status(&self, record: &LocalTaskRecord) -> Result<(), WorkerError> {
        self.persist_remote_task(record)
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
        let project = self.load_project_for_record(record)?;
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
        attached: bool,
        exclude_reserver: bool,
    ) -> Result<RunnerStart, WorkerError> {
        if self.task_has_submission_recovery_pending(task_id)? {
            return Err(task_error(
                "TASK_SUBMISSION_RECOVERY_PENDING",
                "task submission recovery is incomplete",
            ));
        }
        let expected = self
            .client_state
            .queue_entry(turn_id)?
            .filter(|entry| entry.kind() == QueueEntryKind::TaskTurn && entry.job_id() == turn_id)
            .ok_or_else(|| task_error("TASK_BUSY", "task turn was retired before handoff"))?;
        let caller = current_process_identity()?;
        if expected.owner_opt().is_some_and(|owner| {
            *owner != caller
                && matches!(
                    self.client_state.process_observation(*owner),
                    ProcessObservation::Matching { .. } | ProcessObservation::Ambiguous
                )
        }) {
            return Ok(RunnerStart::Pending);
        }
        let slot_limit = if attached {
            usize::MAX
        } else {
            self.config.configured_runner_slots()
        };
        start_runner_with_reservation(
            self.client_state,
            self.executor,
            self.paths,
            task_id,
            turn_id,
            slot_limit,
            exclude_reserver,
        )
    }

    /// Initializes the row's restart budget from the journal when a replacement
    /// already wrote `exited after acceptance:` but the queue field is still
    /// empty. The runner also writes the field; this covers tests that plant
    /// only the diagnostic and a runner that died after the log line.
    fn observe_replacement_failure(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<(), WorkerError> {
        let Some(entry) = self.client_state.queue_entry(turn_id)? else {
            return Ok(());
        };
        if entry.replacement_failure().is_some() {
            return Ok(());
        }
        let bytes = match self.read_runner_log(task_id, turn_id) {
            Ok(bytes) => bytes,
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let Some(code) = last_post_acceptance_public_code(&bytes) else {
            return Ok(());
        };
        self.client_state
            .record_replacement_failure(turn_id, code, current_time_millis()?)?;
        Ok(())
    }

    fn park_repeated_replacement_failure(
        &self,
        turn_id: TurnId,
        task_id: TaskId,
    ) -> Result<(), WorkerError> {
        let _ = self.client_state.record_runner(task_id, None);
        self.client_state.park_row(turn_id)?;
        Ok(())
    }

    fn wait_blocked_error(&self, task_ids: &[TaskId]) -> Result<Option<WorkerError>, WorkerError> {
        for task_id in task_ids {
            let Some(entry) = self.client_state.queue_entry_for_task_turn(*task_id)? else {
                continue;
            };
            if let Some(budget) = entry.replacement_failure()
                && budget.should_park()
            {
                return Ok(Some(task_error("WAIT_BLOCKED", "RUNNER_REPEATED_FAILURE")));
            }
        }
        Ok(None)
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
        // Reconcile is not a finishing runner replacing its own slot.
        // exclude_reserver belongs only to TurnRunner::start_next_parked.
        match self.start_runner(task_id, entry.job_id(), false, false) {
            Ok(RunnerStart::Started(_)) => Ok(true),
            Ok(RunnerStart::Pending) => Ok(false),
            Ok(RunnerStart::Saturated) => {
                self.client_state.park_row(entry.job_id())?;
                Ok(false)
            }
            Err(error) => {
                if let Ok(record) = self.client_state.load_task(task_id)
                    && let Ok(transfer) = self.transfer_for_record(&record)
                {
                    return Err(self.fail_handoff(task_id, entry.job_id(), transfer, error));
                }
                Err(task_error(
                    "RUNNER_HANDOFF_FAILED",
                    format!("runner handoff failed: {error}"),
                ))
            }
        }
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
            delivery: record.delivery().cloned(),
            deliveries: record.deliveries().to_vec(),
        })
    }

    fn report_for_readonly(&self, record: &LocalTaskRecord) -> Result<TaskReport, WorkerError> {
        let observed = self.project_remote_task(record)?;
        Ok(TaskReport {
            task_id: observed.meta().task_id(),
            run_id: observed.meta().run_id(),
            status: observed.status().clone(),
            warnings: Vec::new(),
            events: Vec::new(),
            runner: self
                .client_state
                .runner_liveness(observed.meta().task_id())?,
            exit_code: None,
            delivery: observed.delivery().cloned(),
            deliveries: observed.deliveries().to_vec(),
        })
    }

    fn project_remote_task(
        &self,
        record: &LocalTaskRecord,
    ) -> Result<LocalTaskRecord, WorkerError> {
        if record.close_intent().is_some() || record.abandon_code() == Some("LOG_DRAIN_UNAVAILABLE")
        {
            return Ok(record.clone());
        }
        if !record.needs_remote_observation() {
            return Ok(record.clone());
        }
        let Some(worker_name) = record.status().worker() else {
            return Ok(record.clone());
        };
        let Some(worker) = self.config.worker(worker_name) else {
            return Ok(record.clone());
        };
        let Ok(remote) = RemoteJobClient::new(self.runner).task_status(
            worker,
            &crate::task_store::TaskStatusRequest::new(
                record.meta().project_id(),
                record.meta().task_id(),
            ),
        ) else {
            return Ok(record.clone());
        };
        if let Some(pending) = pending_turn_id(record.status())
            && record.status().state() == TaskState::Active
            && self
                .client_state
                .read_turn_prompt(record.meta().task_id(), pending)
                .is_ok()
            && !remote
                .status()
                .turns()
                .last()
                .is_some_and(|turn| turn.turn_id() == pending)
        {
            return Ok(record.clone());
        }
        record.with_remote_observation(remote.status(), remote.deliveries())
    }

    fn persist_remote_task(&self, record: &LocalTaskRecord) -> Result<(), WorkerError> {
        if !record.needs_remote_status_refresh() {
            return Ok(());
        }
        let projected = self.project_remote_task(record)?;
        if projected.status() == record.status() && projected.deliveries() == record.deliveries() {
            return Ok(());
        }
        let observed = if record.needs_remote_status_refresh() {
            projected.with_status_observed_at(Some(projected.status().updated_at_millis()))?
        } else {
            projected
        };
        self.client_state
            .update_task_if_current(record, observed)
            .map(|_| ())
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
        let project = self.load_project_for_record(record)?;
        let transfer =
            TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)?;
        transfer.release_base(self.runner, record.meta().task_id())
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

    fn load_project_for_record(
        &self,
        record: &LocalTaskRecord,
    ) -> Result<ProjectState, WorkerError> {
        let frozen = self.client_state.frozen_spec_for_record(record)?;
        let project = ProjectState::load_validated_for_task(self.runner, frozen.as_ref(), || {
            ProjectState::load_for_task(
                self.runner,
                &self.current_project(Some(record))?,
                &[],
                record.meta(),
            )
        })?;
        require_project_match(&project, record.meta())?;
        Ok(project)
    }

    /// `close`, `say`, and `fetch` refuse while a detached runner is still
    /// finishing. `wait` uses the same check so it does not return in that
    /// window.
    fn operator_busy_reason(
        &self,
        task_id: TaskId,
        record: &LocalTaskRecord,
    ) -> Result<Option<&'static str>, WorkerError> {
        if record.close_intent().is_some() {
            return Ok(Some(CLOSE_IN_PROGRESS));
        }
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
    )?
    .copying_reported_checks(status)
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
    )?
    .copying_reported_checks(status)
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

/// Bound `from:` children execute from the local TransferRepo cache. Origin
/// stays the publish destination via `push_target`; frozen `requires` keep
/// the origin host requirement. Ordinary origin tasks are unchanged.
fn execution_source_for_bound_from(
    source: TaskSource,
    publish: &[PublishMode],
) -> Result<TaskSource, WorkerError> {
    match source {
        TaskSource::Origin { url } => Ok(TaskSource::Local {
            wip: false,
            push_target: publish
                .contains(&PublishMode::Push)
                .then(|| PushTarget::new(url))
                .transpose()?,
        }),
        local @ TaskSource::Local { .. } => Ok(local),
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

fn batch_has_dag_edges(defaults: &BatchDefaults, tasks: &[BatchTask]) -> bool {
    tasks.iter().any(|task| {
        !task.depends_on.is_empty()
            || parse_from_base(task.base.as_deref().unwrap_or(&defaults.base)).is_some()
    })
}

fn freeze_spec(
    request: &TaskSubmitRequest,
    state: &ProjectState,
) -> Result<DagFrozenSpec, WorkerError> {
    let settings = &state.settings.task;
    let limits = effective_task_limits(&request.limits, settings)?;
    let env_profile = request
        .env_profile
        .clone()
        .or_else(|| settings.env_profile.clone());
    let model = request.model.clone().or_else(|| settings.model.clone());
    let effort = request.effort.clone().or_else(|| settings.effort.clone());
    let publish = request
        .publish
        .clone()
        .unwrap_or_else(|| settings.publish.clone());
    let source = request
        .source
        .clone()
        .unwrap_or_else(|| settings.source.clone());
    let parsed_publish = parse_publish_modes(&publish)?;
    let origin_url = if source == "origin" || parsed_publish.contains(&PublishMode::Push) {
        state.origin.clone()
    } else {
        None
    };
    let parsed_source = parse_task_source(
        &source,
        request.wip,
        origin_url.as_deref().unwrap_or(""),
        parsed_publish.contains(&PublishMode::Push),
    )?;
    let requirements = task_requirements(
        &state.requirements,
        request.agent,
        env_profile.as_deref(),
        parsed_source.origin_requirement()?.as_deref(),
    );
    let permissions = match permission_policy(settings, request.agent) {
        PermissionPolicy::Unattended => "unattended",
        PermissionPolicy::Workspace => "workspace",
    };
    Ok(DagFrozenSpec {
        prompt: request.prompt.clone(),
        title: None,
        agent: agent_name(request.agent).to_owned(),
        model,
        effort,
        source,
        origin_url,
        publish,
        publish_branch: request.publish_branch.clone(),
        close_on: request.close_policy,
        env_profile,
        worker: match &request.preference {
            WorkerPreference::Pinned { worker } => Some(worker.clone()),
            WorkerPreference::Automatic => None,
        },
        wip: request.wip,
        project_path: state.context.root.to_string_lossy().into_owned(),
        project_id: state.context.project_id.clone(),
        worktree_id: state.context.worktree_id.clone(),
        timeout_millis: limits.turn.timeout_millis,
        max_turns: limits.turn.max_turns,
        max_budget_usd_cents: limits.turn.max_budget_usd_cents,
        max_followups: limits.max_followups,
        permissions: permissions.to_owned(),
        requires: requirements,
        include_untracked: state.settings.snapshot.include_untracked.clone(),
        include_empty_dirs: state.settings.snapshot.include_empty_dirs.clone(),
        allow_sensitive: state.settings.snapshot.allow_sensitive.clone(),
        cli_includes: request.cli_includes.clone(),
        branch: state.context.branch.clone(),
    })
}

fn request_from_frozen_node(
    node: &DagNode,
    run_id: Option<RunId>,
) -> Result<TaskSubmitRequest, WorkerError> {
    let spec = &node.frozen;
    Ok(TaskSubmitRequest {
        agent: parse_agent(&spec.agent)?,
        model: spec.model.clone(),
        effort: spec.effort.clone(),
        prompt: spec.prompt.clone(),
        project: PathBuf::from(&spec.project_path),
        base: node
            .execution_oid()
            .map(|oid| oid.to_string())
            .unwrap_or_default(),
        wip: false,
        source: Some(spec.source.clone()),
        publish: Some(spec.publish.clone()),
        publish_branch: spec.publish_branch.clone(),
        cli_includes: spec.cli_includes.clone(),
        limits: spec.limits()?,
        close_policy: spec.close_on,
        env_profile: spec.env_profile.clone(),
        preference: match &spec.worker {
            Some(worker) => WorkerPreference::Pinned {
                worker: worker.clone(),
            },
            None => WorkerPreference::Automatic,
        },
        wait_for_capacity: true,
        attached: false,
        run_id,
    })
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

fn load_batch_file(file: &Path) -> Result<BatchFile, WorkerError> {
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
    Ok(batch)
}

fn resolve_batch_task(
    config: &Config,
    defaults: &BatchDefaults,
    task: &BatchTask,
    batch_dir: &Path,
    project: &Path,
    settings: &TaskSettings,
) -> Result<TaskSubmitRequest, WorkerError> {
    validate_batch_scope(defaults, task)?;
    validate_batch_metadata(task)?;
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
    let request = TaskSubmitRequest {
        agent,
        model: task.model.clone().or_else(|| defaults.model.clone()),
        effort: task.effort.clone().or_else(|| defaults.effort.clone()),
        prompt: append_declared_acceptance(read_batch_prompt(task, batch_dir)?, &task.acceptance)?,
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
        run_id: None,
    };
    validate_prompt(&request.prompt)?;
    validate_preference(config, &request.preference)?;
    let _ = effective_task_limits(&request.limits, settings)?;
    Ok(request)
}

fn preview_issue_from_error(index: usize, error: &WorkerError) -> PreviewIssue {
    let code = error.public_code();
    let kind = match code.as_str() {
        "AGENT_UNSUPPORTED" => "AGENT_UNSUPPORTED",
        "WORKER_NOT_FOUND" => "WORKER_NOT_FOUND",
        "TASK_PROMPT_TOO_LARGE" => "TASK_PROMPT_TOO_LARGE",
        "PUBLISH_REQUIRES_COMMITTED_BASE" => "PUBLISH_REQUIRES_COMMITTED_BASE",
        "BASE_UNAVAILABLE" => "BASE_UNAVAILABLE",
        "TASK_CONFIG_INVALID" => "TASK_CONFIG_INVALID",
        _ => "TASK_CONFIG_INVALID",
    };
    PreviewIssue {
        severity: "error",
        kind,
        message: format!("task {index}: {error}"),
    }
}

fn setup_preview(setup: Option<&SetupSettings>) -> SetupPreview {
    match setup {
        Some(setup) => SetupPreview {
            present: true,
            timeout: Some(humantime::format_duration(setup.timeout).to_string()),
            commands: setup.commands.clone(),
            check: setup.check.clone(),
            lockfiles: setup.lockfiles.clone(),
            inputs: setup.inputs.clone(),
        },
        None => SetupPreview {
            present: false,
            timeout: None,
            commands: Vec::new(),
            check: None,
            lockfiles: Vec::new(),
            inputs: Vec::new(),
        },
    }
}

fn task_preview_id(task: &BatchTask, index: usize) -> String {
    task.id.clone().unwrap_or_else(|| index.to_string())
}

fn overlapping_paths(left: &[String], right: &[String]) -> Vec<String> {
    let mut paths = Vec::new();
    for path in left {
        if right.iter().any(|other| paths_overlap(path, other)) && !paths.contains(path) {
            paths.push(path.clone());
        }
    }
    paths
}

fn paths_overlap(left: &str, right: &str) -> bool {
    left == right
        || left.starts_with(&format!("{right}/"))
        || right.starts_with(&format!("{left}/"))
}

fn validate_batch_metadata(task: &BatchTask) -> Result<(), WorkerError> {
    if let Some(id) = &task.id {
        validate_batch_id(id).map_err(|message| task_error("TASK_CONFIG_INVALID", message))?;
    }
    if task.files.len() > MAX_BATCH_FILES {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "batch task declares too many files",
        ));
    }
    for path in &task.files {
        crate::project_config::validate_preview_path(path).map_err(|_| {
            task_error(
                "TASK_CONFIG_INVALID",
                "batch files path must be a precise relative path",
            )
        })?;
    }
    if task.acceptance.len() > MAX_BATCH_ACCEPTANCE {
        return Err(task_error(
            "TASK_CONFIG_INVALID",
            "batch task declares too many acceptance commands",
        ));
    }
    for command in &task.acceptance {
        if command.is_empty()
            || command.len() > MAX_BATCH_ACCEPTANCE_BYTES
            || command.chars().any(char::is_control)
        {
            return Err(task_error(
                "TASK_CONFIG_INVALID",
                "acceptance command is empty, too long, or contains a control character",
            ));
        }
    }
    Ok(())
}

fn validate_batch_id(id: &str) -> Result<(), String> {
    if id.is_empty()
        || id.len() > MAX_BATCH_ID_BYTES
        || !id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
    {
        return Err("batch task id must be a short lowercase identifier".into());
    }
    Ok(())
}

fn append_declared_acceptance(
    prompt: String,
    acceptance: &[String],
) -> Result<String, WorkerError> {
    Ok(format!(
        "{prompt}{}",
        crate::agent::declared_acceptance_instructions(acceptance)?
    ))
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

enum PreparedSubmitBase {
    Ready(crate::transfer_repo::BaseCommit),
    Resolve { wip: bool, request_base: String },
}

fn configured_slot_count(config: &Config) -> usize {
    config.configured_runner_slots()
}

fn resolve_batch_max_parallel(
    requested: Option<u32>,
    configured_slots: usize,
) -> Result<u32, WorkerError> {
    let max_parallel = match requested {
        Some(value) => value,
        None => u32::try_from(configured_slots).map_err(|_| {
            task_error(
                "TASK_CONFIG_INVALID",
                "configured slot count is too large for batch max_parallel",
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
    use super::*;

    #[test]
    fn batch_parallelism_defaults_to_the_configured_slot_count() {
        assert_eq!(resolve_batch_max_parallel(None, 3).unwrap(), 3);
        assert_eq!(resolve_batch_max_parallel(Some(2), 3).unwrap(), 2);
    }

    #[test]
    fn batch_parallelism_rejects_zero_slots_or_a_zero_override() {
        assert!(resolve_batch_max_parallel(None, 0).is_err());
        assert!(resolve_batch_max_parallel(Some(0), 3).is_err());
    }

    #[test]
    fn declared_acceptance_is_appended_as_an_unverified_agent_instruction() {
        let prompt =
            append_declared_acceptance("Do the work".into(), &["cargo test -p login".into()])
                .unwrap();
        assert!(prompt.contains("Do the work"));
        assert!(prompt.contains("cargo test -p login"));
        assert!(prompt.contains("Declared acceptance criteria"));
        assert!(
            prompt.contains("mac-worker will not treat agent-reported pass as laptop-verified")
        );
    }

    #[test]
    fn overlapping_declared_files_are_advisory() {
        let overlap = overlapping_paths(
            &["src/login.rs".into(), "src/lib.rs".into()],
            &["src/login.rs".into(), "src/other.rs".into()],
        );
        assert_eq!(overlap, vec!["src/login.rs"]);
        assert!(paths_overlap("src", "src/login.rs"));
        assert!(!paths_overlap("src/a.rs", "src/b.rs"));
    }

    #[test]
    fn cyclic_depends_on_is_reported_as_preview_metadata() {
        let a = vec!["b".to_owned()];
        let b = vec!["a".to_owned()];
        let issues = validate_batch_graph(&[
            GraphNode {
                id: Some("a"),
                depends_on: &a,
                base: "HEAD",
            },
            GraphNode {
                id: Some("b"),
                depends_on: &b,
                base: "HEAD",
            },
        ]);
        assert!(
            issues.iter().any(|issue| issue.kind == "cycle"),
            "{issues:?}"
        );
    }
}
