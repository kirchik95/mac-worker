//! Detached driver seam. T3 owns execution and recovery.
use super::{contracts::*, coordinator::IntegrationCoordinator};
use crate::{error::WorkerError, task::TaskId};
pub struct IntegrationRunner<'a> {
    coordinator: IntegrationCoordinator<'a>,
}

use crate::{
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    inputs::RelativePath,
    paths::PathLayout,
    process::ProcessRunner,
    rooted_fs::RootedDir,
    task::{BaseOid, TaskState, TurnId},
    task_client::TaskClient,
    turn_runner::{DetachedRunnerExecutor, RunnerExecutor},
};
use std::{
    fs::File,
    io,
    os::{fd::AsRawFd, unix::process::CommandExt},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// The same rooted reservation domain is used by RPC, leader and detached/direct recovery.
pub(crate) struct OwnerIntegration<'a> {
    state: super::store::RootedIntegrationState,
    runtime: Arc<OwnerRuntime>,
    ports: OwnerPorts<'a>,
}

pub(crate) struct OwnerRuntime {
    paths: PathLayout,
    client: ClientStateStore,
    actor: crate::job::ProcessIdentity,
    helper_unavailable: AtomicBool,
}
impl OwnerRuntime {
    pub(crate) fn new(paths: &PathLayout, client: &ClientStateStore) -> Result<Self, WorkerError> {
        Ok(Self {
            paths: paths.clone(),
            client: client.reopen_until(None)?,
            actor: crate::turn_runner::current_process_identity()?,
            helper_unavailable: AtomicBool::new(false),
        })
    }
}
impl IntegrationRuntime for OwnerRuntime {
    fn now_millis(&self) -> u64 {
        self.client
            .admission_time(crate::controller::leader::now_millis)
            .unwrap_or(0)
    }
    fn actor(&self) -> crate::job::ProcessIdentity {
        self.actor
    }
    fn actor_verdict(
        &self,
        actor: crate::job::ProcessIdentity,
    ) -> crate::client_state::RunnerLivenessVerdict {
        self.client.runner_identity_verdict(actor)
    }
    fn begin_phase(
        &self,
        key: &IntegrationPhaseKey,
    ) -> Result<IntegrationDriveAdmission, WorkerError> {
        if self.helper_unavailable.load(Ordering::Acquire) {
            return Ok(IntegrationDriveAdmission::Park(IntegrationPauseEvidence {
                reason: IntegrationPauseReason::HelperUnavailable,
                effective_at_millis: self.now_millis(),
            }));
        }
        match crate::controller::drain::integration_permit(
            &self.paths.controller_state_root(),
            self.client.wait_deadline(),
        )? {
            Some(permit) => Ok(IntegrationDriveAdmission::Permit(
                IntegrationPhasePermit::with_guard(key.clone(), Box::new(permit)),
            )),
            None => Ok(IntegrationDriveAdmission::Park(IntegrationPauseEvidence {
                reason: IntegrationPauseReason::ControllerDrained,
                effective_at_millis: self.now_millis(),
            })),
        }
    }
    fn reach(&self, _: IntegrationHook) {}
}

struct OwnerPorts<'a> {
    runner: &'a dyn ProcessRunner,
    config: &'a Config,
    paths: &'a PathLayout,
    client: &'a ClientStateStore,
    executor: &'a dyn RunnerExecutor,
    runtime: Arc<OwnerRuntime>,
}
impl OwnerPorts<'_> {
    fn client(&self) -> TaskClient<'_> {
        TaskClient::new(
            self.runner,
            self.config,
            self.paths,
            self.client,
            self.executor,
        )
    }
    fn worker(&self, task: TaskId) -> Result<&WorkerEntry, WorkerError> {
        let local = self.client.load_task(task)?;
        local
            .status()
            .worker()
            .and_then(|name| self.config.worker(name))
            .ok_or_else(|| IntegrationCode::IntegrationWorkerOffline.error())
    }
    fn require_helper(&self, worker: &WorkerEntry) -> Result<(), WorkerError> {
        let probe = crate::transport::SshTransport::new(self.runner).probe(worker);
        let Some(probe) = probe.probe else {
            return Err(IntegrationCode::IntegrationWorkerOffline.error());
        };
        let unavailable = !probe.features.as_ref().is_some_and(|features| {
            features
                .iter()
                .any(|feature| feature == crate::features::HOST_FEATURE_INTEGRATION)
        });
        self.runtime
            .helper_unavailable
            .store(unavailable, Ordering::Release);
        if unavailable {
            Err(integration_unavailable())
        } else {
            Ok(())
        }
    }
}
impl IntegrationHost for OwnerPorts<'_> {
    fn execute(
        &self,
        request: &HostIntegrationRequest,
    ) -> Result<HostIntegrationResponse, WorkerError> {
        let worker = self.worker(request.task_id)?;
        self.require_helper(worker)?;
        crate::transfer::RemoteJobClient::new(self.runner).task_integration(worker, request)
    }
}
impl IntegrationObserver for OwnerPorts<'_> {
    fn facts(&self, task: TaskId) -> Result<IntegrationTaskFacts, WorkerError> {
        let mut ordinary = self.client.load_task(task)?;
        if let Ok(worker) = self.worker(task) {
            let _ = self.require_helper(worker);
            if let Ok(remote) = crate::transfer::RemoteJobClient::new(self.runner).task_status(
                worker,
                &crate::task_store::TaskStatusRequest::new(ordinary.meta().project_id(), task),
            ) {
                // Do not replace a locally prepared turn with the preceding remote turn.
                let local_pending = ordinary
                    .status()
                    .turns()
                    .last()
                    .filter(|turn| turn.terminal().is_none());
                if local_pending.is_none_or(|pending| {
                    remote
                        .status()
                        .turns()
                        .iter()
                        .any(|turn| turn.turn_id() == pending.turn_id())
                }) {
                    let next =
                        ordinary.with_remote_observation(remote.status(), remote.deliveries())?;
                    self.client.update_task_if_current(&ordinary, next)?;
                    ordinary = self.client.load_task(task)?;
                }
            }
        }
        let last = ordinary.status().turns().last().map(|turn| turn.turn_id());
        let auxiliary_purpose = last
            .map(|turn| {
                super::store::RootedIntegrationState::read_auxiliary(self.paths, task, turn)
            })
            .transpose()?
            .flatten()
            .map(|p| p.purpose);
        let queue = self.client.queue_entry_for_task_turn(task)?;
        let cycle_base = last
            .map(|turn| read_source_base(self.paths, task, turn))
            .transpose()?
            .flatten()
            .unwrap_or_else(|| ordinary.meta().base_oid().clone());
        Ok(IntegrationTaskFacts {
            cycle_base,
            result_imported: ordinary.fetched_head().is_some()
                && ordinary.fetched_head() == ordinary.status().head_oid(),
            session_import_complete: ordinary.meta().session_import().is_none()
                || (ordinary.status().session_present() && !ordinary.status().turns().is_empty()),
            continuation_pending: ordinary.auto_continue_intent().is_some(),
            runner_present: queue.is_some() || ordinary.runner().is_some(),
            stop_requested: queue
                .as_ref()
                .is_some_and(|entry| entry.is_cancel_requested()),
            close_pending: ordinary.close_intent().is_some(),
            submission_pending: ordinary.submission_intent_turn_id().is_some()
                || ordinary.submission_rollback_turn_id().is_some(),
            auxiliary_purpose,
            ordinary,
        })
    }
}
impl IntegrationTurns for OwnerPorts<'_> {
    fn enqueue(&self, prepared: &PreparedIntegrationTurn) -> Result<TurnId, WorkerError> {
        self.client().say_integration_prepared(prepared)?;
        Ok(prepared.followup.turn_id())
    }
    fn observe(&self, turn: TurnId) -> Result<IntegrationTurnObservation, WorkerError> {
        let Some(task) = self.client.task_id_for_turn(turn)? else {
            return Ok(IntegrationTurnObservation {
                turn_id: turn,
                queue_position: None,
                accepted: false,
                completed: false,
            });
        };
        let entry = self.client.queue_entry(turn)?;
        let position = read_position(self.paths, task, turn)?;
        if entry.as_ref().is_some_and(|entry| {
            position.is_some_and(|position| position != entry.queue_id().value())
        }) {
            return Err(invalid());
        }
        let journal = crate::runner_log::snapshot(&self.paths.state, task, turn)?;
        let accepted = journal.as_ref().is_some_and(|journal| journal.accepted);
        let completed = accepted
            && journal
                .as_ref()
                .is_some_and(|journal| journal.completion.is_some());
        let observation = IntegrationTurnObservation {
            turn_id: turn,
            queue_position: position.or_else(|| entry.map(|entry| entry.queue_id().value())),
            accepted,
            completed,
        };
        observation.validate()?;
        Ok(observation)
    }
    fn import_receipt(
        &self,
        task: TaskId,
        receipt: &IntegrationReceipt,
    ) -> Result<IntegrationReceipt, WorkerError> {
        receipt.validate()?;
        let (_, stored) = super::store::RootedIntegrationState::read_task(self.paths, task)?;
        let mut stored = stored.ok_or_else(invalid)?;
        let mut imported = receipt.clone();
        imported.imported = true;
        if stored.snapshot.integration_id != receipt.integration_id
            || stored.snapshot.epoch != receipt.epoch
        {
            return if stored.archived_receipts.iter().any(|old| old == &imported) {
                Ok(imported)
            } else {
                Err(invalid())
            };
        }
        if self.client.queue_entry_for_task_turn(task)?.is_some() {
            return Err(WorkerError::task(
                "TASK_BUSY",
                "task runner is still finishing",
            ));
        }
        let local = self.client.load_task(task)?;
        let worker = self.worker(task)?;
        let project = self.client().load_project_for_record(&local)?;
        let transfer = crate::controller::registry::open_transfer_repo_until(
            self.runner,
            self.paths,
            &project,
            local.meta(),
            self.client.wait_deadline(),
        )?;
        let result = transfer.result_import(task)?;
        crate::git_transport::GitTransport::new(self.runner).fetch_result(
            worker,
            self.client.client_id(),
            local.meta().project_id(),
            task,
            transfer.path(),
        )?;
        let head = result
            .import_result(self.runner, &project.context.common_dir, &worker.name)?
            .head()
            .clone();
        let accepted = receipt.merge_oid.as_ref().unwrap_or(&receipt.target_head);
        if &head != accepted {
            return Err(invalid());
        }
        let remote = crate::transfer::RemoteJobClient::new(self.runner).task_status(
            worker,
            &crate::task_store::TaskStatusRequest::new(local.meta().project_id(), task),
        )?;
        if remote.status().head_oid() != Some(accepted)
            || !matches!(remote.status().state(), TaskState::Open | TaskState::Closed)
        {
            return Err(invalid());
        }
        let next = local
            .with_remote_observation(remote.status(), remote.deliveries())?
            .with_fetched_head(Some(head))?;
        if !self.client.update_task_if_current(&local, next)? {
            return Err(WorkerError::task(
                "TASK_REVISION_CONFLICT",
                "task changed during integration import",
            ));
        }
        drop(result);
        drop(transfer);
        // Repair with the identical imported receipt is the host's sticky acknowledgement.
        stored.receipt = Some(imported.clone());
        let request = HostIntegrationRequest {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            task_id: task,
            integration_id: Some(receipt.integration_id),
            epoch: receipt.epoch,
            revision: stored.snapshot.revision,
            action: HostIntegrationAction::Step {
                step: IntegrationStep::Repair,
                record: Box::new(stored),
            },
        };
        match self.execute(&request)? {
            HostIntegrationResponse::Integrated { receipt: ack, .. } if ack == imported => {
                Ok(imported)
            }
            _ => Err(invalid()),
        }
    }
    fn close_integrated(
        &self,
        task: TaskId,
        receipt: &IntegrationReceipt,
    ) -> Result<(), WorkerError> {
        self.client()
            .close_imported_integration(task, receipt)
            .map(|_| ())
    }
}

impl<'a> OwnerIntegration<'a> {
    pub(crate) fn new(
        runner: &'a dyn ProcessRunner,
        config: &'a Config,
        paths: &'a PathLayout,
        client: &'a ClientStateStore,
        executor: &'a dyn RunnerExecutor,
    ) -> Result<Self, WorkerError> {
        let runtime = Arc::new(OwnerRuntime::new(paths, client)?);
        let state = super::store::RootedIntegrationState::open(paths, runtime.clone())?;
        Ok(Self {
            state,
            runtime: runtime.clone(),
            ports: OwnerPorts {
                runner,
                config,
                paths,
                client,
                executor,
                runtime,
            },
        })
    }
    pub(crate) fn coordinator(&self) -> IntegrationCoordinator<'_> {
        IntegrationCoordinator::new(
            &self.state,
            &self.ports,
            &self.ports,
            self.runtime.as_ref(),
            &self.ports,
        )
    }
    pub(crate) fn stage(&self, task: TaskId) -> Result<(), WorkerError> {
        if !self.coordinator().configured(task)? {
            return Ok(());
        }
        if let Some(last) = self.ports.client.load_task(task)?.status().turns().last() {
            self.coordinator().on_terminal(task, last.turn_id())?;
            crate::task_client::stamp_integration_run_position(
                self.ports.client,
                &self.coordinator(),
                task,
            )?;
        }
        self.schedule(task)
    }
    fn schedule(&self, task: TaskId) -> Result<(), WorkerError> {
        let Some(record) = self.state.load(task)? else {
            return Ok(());
        };
        if matches!(
            record.snapshot.state,
            IntegrationStatus::Integrated | IntegrationStatus::Blocked | IntegrationStatus::Revoked
        ) && record.tombstone.as_ref().is_none_or(|t| t.acknowledged)
        {
            return Ok(());
        }
        let root = task_root(self.ports.paths, task)?;
        let Some(_driver) = driver_lock(&root, true)? else {
            return Ok(());
        };
        if let Some(bytes) = read(&root, "driver.json")? {
            let binding: DriverBinding = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            binding.actor.validate()?;
            if binding.task != task {
                return Err(invalid());
            }
            if self.runtime.actor_verdict(binding.actor)
                != crate::client_state::RunnerLivenessVerdict::Exited
            {
                return Ok(());
            }
        }
        let key = IntegrationPhaseKey {
            task,
            intent: record.snapshot.integration_id,
            epoch: record.snapshot.epoch,
            revision: record.snapshot.revision,
            phase: IntegrationPhase::Drive,
        };
        let permit = match self.runtime.begin_phase(&key)? {
            IntegrationDriveAdmission::Permit(permit) => permit,
            IntegrationDriveAdmission::Park(pause) => {
                self.coordinator().park_for_runtime(task, pause)?;
                return Ok(());
            }
        };
        let mut command = Command::new(std::env::current_exe()?);
        command
            .arg("--config")
            .arg(&self.ports.paths.config)
            .arg("integration-runner")
            .arg(task.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        let child = command.spawn()?;
        use crate::supervisor::SystemProcessInspector;
        let actor = SystemProcessInspector.identity_for_pid(child.id())?;
        write(
            &root,
            "driver.json",
            &serde_json::to_vec(&DriverBinding {
                task,
                intent: key.intent,
                epoch: key.epoch,
                actor,
            })
            .map_err(|_| invalid())?,
            false,
        )?;
        drop(child);
        drop(permit);
        Ok(())
    }
    pub(crate) fn run_child(&self, task: TaskId) -> Result<IntegrationSnapshot, WorkerError> {
        let root = task_root(self.ports.paths, task)?;
        let _driver = driver_lock(&root, false)?.ok_or_else(invalid)?;
        if let Some(bytes) = read(&root, "driver.json")? {
            let binding: DriverBinding = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if binding.task != task || binding.actor != self.runtime.actor() {
                return Err(invalid());
            }
        }
        let snapshot = IntegrationRunner::new(self.coordinator()).run(task)?;
        if snapshot.state == IntegrationStatus::Integrated {
            self.ports.client().advance_pending_dags()?;
        }
        Ok(snapshot)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverBinding {
    task: TaskId,
    intent: IntegrationId,
    epoch: u32,
    actor: crate::job::ProcessIdentity,
}
fn invalid() -> WorkerError {
    IntegrationCode::IntegrationStateInvalid.error()
}
fn relative(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes()).map_err(|_| invalid())
}
fn task_root(paths: &PathLayout, task: TaskId) -> Result<RootedDir, WorkerError> {
    Ok(RootedDir::open_anchored_absolute(&paths.state)?
        .open_child_directory(&relative(&format!("integrations/tasks/{task}"))?, false)?)
}
fn read(root: &RootedDir, name: &str) -> Result<Option<Vec<u8>>, WorkerError> {
    match root.read_private_regular(name, MAX_PRIVATE_RECORD_BYTES as u64) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
fn write(root: &RootedDir, name: &str, bytes: &[u8], exact: bool) -> Result<(), WorkerError> {
    match read(root, name)? {
        Some(old) if old == bytes => Ok(()),
        Some(_) if exact => Err(invalid()),
        Some(old) => root
            .replace_private_regular_exact(name, &old, bytes)
            .map_err(WorkerError::Io),
        None => root
            .write_private_atomic_no_replace(name, bytes)
            .map_err(WorkerError::Io),
    }
}
fn driver_lock(root: &RootedDir, nonblocking: bool) -> Result<Option<File>, WorkerError> {
    let lock = root.open_private_lock("driver.lock")?;
    if unsafe {
        libc::flock(
            lock.as_raw_fd(),
            libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 },
        )
    } != 0
    {
        let error = io::Error::last_os_error();
        if nonblocking && error.kind() == io::ErrorKind::WouldBlock {
            return Ok(None);
        }
        return Err(error.into());
    }
    let identity = root.private_entry_identity("driver.lock")?;
    root.validate_private_regular_binding("driver.lock", &lock, identity)?;
    Ok(Some(lock))
}
pub(crate) fn record_source_base(
    paths: &PathLayout,
    task: TaskId,
    turn: TurnId,
    base: &BaseOid,
) -> Result<(), WorkerError> {
    if super::store::RootedIntegrationState::read_task(paths, task)?
        .0
        .is_none()
        || super::store::RootedIntegrationState::read_auxiliary(paths, task, turn)?.is_some()
    {
        return Ok(());
    }
    let root = task_root(paths, task)?.open_child_directory(&relative("sources")?, true)?;
    write(
        &root,
        &format!("{turn}.json"),
        &serde_json::to_vec(base).map_err(|_| invalid())?,
        true,
    )
}
fn read_source_base(
    paths: &PathLayout,
    task: TaskId,
    turn: TurnId,
) -> Result<Option<BaseOid>, WorkerError> {
    let root = task_root(paths, task)?;
    match root.open_child_directory(&relative("sources")?, false) {
        Ok(root) => read(&root, &format!("{turn}.json"))?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|_| invalid()))
            .transpose(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
pub(crate) fn record_position(
    paths: &PathLayout,
    task: TaskId,
    turn: TurnId,
    position: u64,
) -> Result<(), WorkerError> {
    if super::store::RootedIntegrationState::read_auxiliary(paths, task, turn)?.is_none() {
        return Ok(());
    }
    let root = task_root(paths, task)?.open_child_directory(&relative("queue")?, true)?;
    write(
        &root,
        &format!("{turn}.json"),
        &serde_json::to_vec(&position).map_err(|_| invalid())?,
        true,
    )
}
fn read_position(
    paths: &PathLayout,
    task: TaskId,
    turn: TurnId,
) -> Result<Option<u64>, WorkerError> {
    let root = task_root(paths, task)?;
    match root.open_child_directory(&relative("queue")?, false) {
        Ok(root) => read(&root, &format!("{turn}.json"))?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|_| invalid()))
            .transpose(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn recover_selected(
    runner: &dyn ProcessRunner,
    config: &Config,
    paths: &PathLayout,
    client: &ClientStateStore,
    executor: &dyn RunnerExecutor,
    tasks: &[TaskId],
) -> Result<(), WorkerError> {
    let configured = tasks
        .iter()
        .map(|task| {
            super::store::RootedIntegrationState::read_task(paths, *task)
                .map(|(policy, _)| (*task, policy.is_some()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !configured.iter().any(|(_, configured)| *configured) {
        return Ok(());
    }
    let owner = OwnerIntegration::new(runner, config, paths, client, executor)?;
    for (task, configured) in configured {
        if configured {
            owner.stage(task)?;
        }
    }
    Ok(())
}

pub(crate) fn run_native_child(
    runner: &dyn ProcessRunner,
    config: &Config,
    paths: &PathLayout,
    client: &ClientStateStore,
    task: TaskId,
) -> Result<IntegrationSnapshot, WorkerError> {
    if super::store::RootedIntegrationState::read_task(paths, task)?
        .0
        .is_none()
    {
        return Err(integration_unavailable());
    }
    OwnerIntegration::new(runner, config, paths, client, &DetachedRunnerExecutor)?.run_child(task)
}
impl<'a> IntegrationRunner<'a> {
    pub fn new(coordinator: IntegrationCoordinator<'a>) -> Self {
        Self { coordinator }
    }
    pub fn run(&self, task: TaskId) -> Result<IntegrationSnapshot, WorkerError> {
        let mut snapshot = self
            .coordinator
            .snapshot(task)?
            .ok_or_else(integration_unavailable)?;
        // Yield to selected recovery rather than wait for clocks, queue slots or agents.
        // A detached child also bounds a non-progressing capable peer.
        for _ in 0..32 {
            let revision = snapshot.revision;
            snapshot = self.coordinator.drive_once(task)?;
            if snapshot.revision == revision || !self.coordinator.ready_to_drive(task)? {
                break;
            }
        }
        Ok(snapshot)
    }
}
