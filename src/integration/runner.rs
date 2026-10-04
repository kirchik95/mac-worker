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
    source: OwnerSource<'a>,
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
            client: client.with_wait_deadline(client.wait_deadline()),
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
        match crate::controller::drain::integration_admission(
            &self.paths.controller_state_root(),
            self.client.wait_deadline(),
            self.now_millis(),
        )? {
            Ok(permit) => {
                if self.helper_unavailable.load(Ordering::Acquire) {
                    drop(permit);
                    Ok(IntegrationDriveAdmission::Park(IntegrationPauseEvidence {
                        reason: IntegrationPauseReason::HelperUnavailable,
                        effective_at_millis: self.now_millis(),
                    }))
                } else {
                    Ok(IntegrationDriveAdmission::Permit(
                        IntegrationPhasePermit::with_guard(key.clone(), Box::new(permit)),
                    ))
                }
            }
            Err(pause) => Ok(IntegrationDriveAdmission::Park(pause)),
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
        let revoking = matches!(request.action, HostIntegrationAction::Revoke { .. });
        if revoking && let Some(entry) = self.client.queue_entry_for_task_turn(request.task_id)? {
            self.client
                .retain_task_turn_cancel(entry.job_id(), self.runtime.now_millis())?;
        }
        self.require_helper(worker)?;
        let response =
            crate::transfer::RemoteJobClient::new(self.runner).task_integration(worker, request);
        if revoking
            && !matches!(response, Ok(HostIntegrationResponse::Integrated { .. }))
            && !self
                .client()
                .settle_integration_stop(request.task_id)
                .unwrap_or(false)
        {
            return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
        }
        response
    }
}
/// Admission witnesses only the current owner record, queue and prepared sidecars.
/// Remote observation stays at the normal drive/finalizer boundary.
struct OwnerSource<'a> {
    paths: &'a PathLayout,
    client: &'a ClientStateStore,
}
impl IntegrationObserver for OwnerSource<'_> {
    fn facts(&self, task: TaskId) -> Result<IntegrationTaskFacts, WorkerError> {
        owner_facts(self.paths, self.client, self.client.load_task(task)?)
    }
}
fn owner_facts(
    paths: &PathLayout,
    client: &ClientStateStore,
    ordinary: crate::task::LocalTaskRecord,
) -> Result<IntegrationTaskFacts, WorkerError> {
    let task = ordinary.meta().task_id();
    let last = ordinary.status().turns().last().map(|turn| turn.turn_id());
    let auxiliary_purpose = last
        .map(|turn| super::store::RootedIntegrationState::read_auxiliary(paths, task, turn))
        .transpose()?
        .flatten()
        .map(|p| p.purpose);
    let queue = client.queue_entry_for_task_turn(task)?;
    let cycle_base = last
        .map(|turn| read_source_base(paths, task, turn))
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
                if crate::task_view::remote_observation_allowed(&ordinary)
                    && local_pending.is_none_or(|pending| {
                        remote
                            .status()
                            .turns()
                            .iter()
                            .any(|turn| turn.turn_id() == pending.turn_id())
                    })
                {
                    let next =
                        ordinary.with_remote_observation(remote.status(), remote.deliveries())?;
                    self.client.update_task_if_current(&ordinary, next)?;
                    ordinary = self.client.load_task(task)?;
                }
            }
        }
        owner_facts(self.paths, self.client, ordinary)
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
        // A host close can finish before its owner clears the close intent.
        // Keep that intent for the existing close recovery, including re-import.
        let observed = if local.close_intent().is_some() {
            local.with_deliveries(crate::task::merge_origin_deliveries(
                local.deliveries(),
                remote.deliveries(),
            ))?
        } else if local.status().state() == TaskState::Closed
            && remote.status().state() == TaskState::Closed
        {
            // Ordinary observation freezes terminal tasks. A validated receipt
            // authorizes this retained result-head repair while preserving Closed.
            local
                .with_status(remote.status().clone())?
                .with_deliveries(crate::task::merge_origin_deliveries(
                    local.deliveries(),
                    remote.deliveries(),
                ))?
        } else {
            local.with_remote_observation(remote.status(), remote.deliveries())?
        };
        let next = observed.with_fetched_head(Some(head))?;
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
        let state = super::store::RootedIntegrationState::open(paths, runtime.clone())?
            .with_event_sink(client.event_sink());
        Ok(Self {
            state,
            runtime: runtime.clone(),
            source: OwnerSource { paths, client },
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
        .with_source_observer(&self.source)
        .with_owner_gate(self.ports.paths)
    }
    pub(crate) fn stage(&self, task: TaskId) -> Result<(), WorkerError> {
        let _hints = self.ports.client.event_scope();
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
        self.settle_parked_auxiliary(task)?;
        self.schedule(task)
    }
    fn settle_parked_auxiliary(&self, task: TaskId) -> Result<(), WorkerError> {
        let Some(entry) = self.ports.client.queue_entry_for_task_turn(task)? else {
            return Ok(());
        };
        let Some(record) = self.state.load(task)? else {
            return Ok(());
        };
        if !matches!(entry.state(), crate::job::QueueState::Parked)
            || !record
                .auxiliaries
                .iter()
                .any(|aux| aux.turn_id == entry.job_id())
        {
            return Ok(());
        }
        if record.tombstone.is_none() && record.snapshot.state != IntegrationStatus::Blocked {
            let admission =
                auxiliary_launch_permit(self.ports.paths, self.ports.client, task, entry.job_id());
            if let Err(error) = admission {
                let latest = self.state.load(task)?.ok_or_else(invalid)?;
                if latest.tombstone.is_none() && latest.snapshot.state != IntegrationStatus::Blocked
                {
                    return Err(error);
                }
            }
        }
        let latest = self.state.load(task)?.ok_or_else(invalid)?;
        if latest.tombstone.is_some() || latest.snapshot.state == IntegrationStatus::Blocked {
            record_position(
                self.ports.paths,
                task,
                entry.job_id(),
                entry.queue_id().value(),
            )?;
            // Only this owner's auxiliary is settled. Accepted work still
            // needs remote cancellation/completion proof before retirement.
            self.ports.client().settle_integration_stop(task)?;
        }
        Ok(())
    }
    pub(crate) fn redrive(
        &self,
        request: &IntegrationRedriveRequest,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        let _hints = self.ports.client.event_scope();
        request.validate()?;
        let task = request.task_id;
        let root = task_root(self.ports.paths, task)?;
        let _request = private_lock(&root, "redrive.lock", true)?
            .ok_or_else(|| WorkerError::task("TASK_BUSY", "integration redrive is in progress"))?;
        let name = format!("redrive-{}.json", request.request_id);
        let mut record = self.state.load(task)?.ok_or_else(integration_unavailable)?;
        let mut binding: RedriveBinding = match read(&root, &name)? {
            Some(bytes) => {
                let saved: RedriveBinding =
                    serde_json::from_slice(&bytes).map_err(|_| invalid())?;
                if saved.request != *request
                    || serde_json::to_vec(&saved).map_err(|_| invalid())? != bytes
                {
                    return Err(invalid());
                }
                saved
            }
            None => {
                if record.snapshot.revision != request.expected {
                    return Err(WorkerError::task(
                        "TASK_REVISION_CONFLICT",
                        "integration revision changed",
                    ));
                }
                let ordinary = self.ports.client.load_task(task)?;
                if ordinary.status().state() != TaskState::Open {
                    return Err(IntegrationCode::IntegrationDependencyNotIntegrated.error());
                }
                let result = if record.snapshot.state == IntegrationStatus::Integrated {
                    record.validate()?;
                    if !self
                        .coordinator()
                        .covers_latest_ordinary_work(&ordinary, &record.snapshot)?
                    {
                        return Err(IntegrationCode::IntegrationDependencyNotIntegrated.error());
                    }
                    let receipt = record.receipt.as_ref().ok_or_else(invalid)?;
                    let accepted = receipt.merge_oid.as_ref().unwrap_or(&receipt.target_head);
                    if !receipt.imported
                        || receipt.integration_id != record.snapshot.integration_id
                        || receipt.epoch != record.snapshot.epoch
                        || receipt.source_turn_id != record.snapshot.source_turn_id
                        || receipt.source_head != record.snapshot.source_head
                        || Some(&receipt.target_head)
                            != record.snapshot.observed_target_oid.as_ref()
                        || receipt.merge_oid != record.snapshot.merge_oid
                        || Some(receipt.disposition) != record.snapshot.disposition
                        || ordinary.status().head_oid() != Some(accepted)
                        || ordinary.fetched_head() != Some(accepted)
                        || ordinary.runner().is_some()
                        || self.ports.client.queue_entry_for_task_turn(task)?.is_some()
                    {
                        return Err(invalid());
                    }
                    Some(record.snapshot.clone())
                } else if record.snapshot.state == IntegrationStatus::Blocked {
                    self.ports.require_helper(self.ports.worker(task)?)?;
                    None
                } else {
                    return Err(WorkerError::task("TASK_BUSY", "INTEGRATION_IN_PROGRESS"));
                };
                let saved = RedriveBinding {
                    request: request.clone(),
                    intent: record.snapshot.integration_id,
                    epoch: record.snapshot.epoch,
                    result,
                };
                write(
                    &root,
                    &name,
                    &serde_json::to_vec(&saved).map_err(|_| invalid())?,
                    true,
                )?;
                saved
            }
        };
        if let Some(result) = binding.result {
            self.schedule(task)?;
            return Ok(result);
        }
        if record.snapshot.integration_id != binding.intent {
            return Err(WorkerError::task(
                "TASK_REVISION_CONFLICT",
                "integration cycle changed",
            ));
        }
        let result = if binding.epoch.checked_add(1) == Some(record.snapshot.epoch) {
            // Epoch publication survived a crash before saving the operation result.
            record.snapshot
        } else if record.snapshot.epoch == binding.epoch {
            self.ports.require_helper(self.ports.worker(task)?)?;
            match self
                .coordinator()
                .resume_redrive(task, binding.intent, binding.epoch)
            {
                Ok(snapshot) => snapshot,
                Err(error) if error.public_code() == "INTEGRATION_ALREADY_COMMITTED" => {
                    record = self.state.load(task)?.ok_or_else(invalid)?;
                    if record.snapshot.state != IntegrationStatus::Integrated {
                        return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
                    }
                    record.snapshot
                }
                Err(error) => return Err(error),
            }
        } else {
            return Err(WorkerError::task(
                "TASK_REVISION_CONFLICT",
                "integration epoch changed",
            ));
        };
        binding.result = Some(result.clone());
        write(
            &root,
            &name,
            &serde_json::to_vec(&binding).map_err(|_| invalid())?,
            false,
        )?;
        drop(_request);
        self.schedule(task)?;
        Ok(result)
    }
    fn schedule(&self, task: TaskId) -> Result<(), WorkerError> {
        let _hints = self.ports.client.event_scope();
        let Some(record) = self.state.load(task)? else {
            return Ok(());
        };
        let closed = self.ports.client.load_task(task)?.status().state() == TaskState::Closed;
        let closed_observation = closed
            && record.snapshot.state == IntegrationStatus::Blocked
            && !record
                .phase_retries
                .iter()
                .any(|retry| retry.phase == IntegrationPhase::Repair);
        if matches!(
            record.snapshot.state,
            IntegrationStatus::Integrated | IntegrationStatus::Blocked | IntegrationStatus::Revoked
        ) && !closed_observation
            && record.tombstone.as_ref().is_none_or(|t| t.acknowledged)
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
        if !self.coordinator().reclaim_exited_actor(task)? {
            return Ok(());
        }
        let record = self.state.load(task)?.ok_or_else(invalid)?;
        let key = IntegrationPhaseKey {
            task,
            intent: record.snapshot.integration_id,
            epoch: record.snapshot.epoch,
            revision: record.snapshot.revision,
            phase: IntegrationPhase::Drive,
        };
        // Revoke and settlement remain available while the launch gate is shut.
        let permit = if closed
            || record
                .tombstone
                .as_ref()
                .is_some_and(|stop| !stop.acknowledged)
        {
            None
        } else {
            Some(match self.runtime.begin_phase(&key)? {
                IntegrationDriveAdmission::Permit(permit) => permit,
                IntegrationDriveAdmission::Park(pause) => {
                    self.coordinator().park_for_runtime(task, pause)?;
                    return Ok(());
                }
            })
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
        let _hints = self.ports.client.event_scope();
        let root = task_root(self.ports.paths, task)?;
        let _driver = driver_lock(&root, false)?.ok_or_else(invalid)?;
        if let Some(bytes) = read(&root, "driver.json")? {
            let binding: DriverBinding = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            let record = self.state.load(task)?.ok_or_else(invalid)?;
            if binding.task != task
                || binding.actor != self.runtime.actor()
                || binding.intent != record.snapshot.integration_id
                || binding.epoch != record.snapshot.epoch
            {
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RedriveBinding {
    request: IntegrationRedriveRequest,
    intent: IntegrationId,
    epoch: u32,
    result: Option<IntegrationSnapshot>,
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
    private_lock(root, "driver.lock", nonblocking)
}
fn private_lock(
    root: &RootedDir,
    name: &str,
    nonblocking: bool,
) -> Result<Option<File>, WorkerError> {
    let lock = root.open_private_lock(name)?;
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
    let identity = root.private_entry_identity(name)?;
    root.validate_private_regular_binding(name, &lock, identity)?;
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

/// The guard spans only local purpose/budget ownership and queue handoff.
/// Ordinary turns retain their existing valve and wire behavior.
pub(crate) struct AuxiliaryLaunchPermit {
    _phase: Option<IntegrationPhasePermit>,
}
/// Donor scans must not initialize deadlines, resume records or acquire phase
/// ownership for another task. The claimed row gets the full mutating valve.
pub(crate) fn auxiliary_queue_eligible(
    paths: &PathLayout,
    client: &ClientStateStore,
    task: TaskId,
    turn: TurnId,
) -> Result<bool, WorkerError> {
    let Some(prepared) = super::store::RootedIntegrationState::read_auxiliary(paths, task, turn)?
    else {
        return Ok(true);
    };
    let (_, record) = super::store::RootedIntegrationState::read_task(paths, task)?;
    let record = record.ok_or_else(invalid)?;
    prepared.validate_for(&record)?;
    if record.tombstone.is_some()
        || !matches!(
            record.snapshot.state,
            IntegrationStatus::Resolving | IntegrationStatus::Verifying
        ) && !(record.snapshot.state == IntegrationStatus::Parked
            && matches!(
                record.snapshot.resume_state,
                Some(IntegrationStatus::Resolving | IntegrationStatus::Verifying)
            ))
    {
        return Ok(false);
    }
    Ok(crate::controller::drain::integration_admission(
        &paths.controller_state_root(),
        client.wait_deadline(),
        client.admission_time(crate::controller::leader::now_millis)?,
    )?
    .is_ok())
}
pub(crate) fn auxiliary_launch_permit(
    paths: &PathLayout,
    client: &ClientStateStore,
    task: TaskId,
    turn: TurnId,
) -> Result<Option<AuxiliaryLaunchPermit>, WorkerError> {
    let Some(prepared) = super::store::RootedIntegrationState::read_auxiliary(paths, task, turn)?
    else {
        return Ok(Some(AuxiliaryLaunchPermit { _phase: None }));
    };
    let runtime = Arc::new(OwnerRuntime::new(paths, client)?);
    let state = super::store::RootedIntegrationState::open(paths, runtime.clone())?
        .with_event_sink(client.event_sink());
    let mut record = state.load(task)?.ok_or_else(invalid)?;
    prepared.validate_for(&record)?;
    if record.tombstone.is_some() {
        return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
    }
    if record.snapshot.state == IntegrationStatus::Blocked {
        return Err(record
            .snapshot
            .blocked_code
            .unwrap_or(IntegrationCode::IntegrationStateInvalid)
            .error());
    }
    let key = IntegrationPhaseKey {
        task,
        intent: record.snapshot.integration_id,
        epoch: record.snapshot.epoch,
        revision: record.snapshot.revision,
        phase: IntegrationPhase::AuxiliaryAdmission,
    };
    let permit = match runtime.begin_phase(&key)? {
        IntegrationDriveAdmission::Permit(p) => p,
        IntegrationDriveAdmission::Park(pause) => {
            super::coordinator::park_record(
                &state,
                runtime.as_ref(),
                Some(paths),
                &mut record,
                pause,
            )?;
            return Ok(None);
        }
    };
    super::coordinator::extend_elapsed_pauses(&state, runtime.as_ref(), paths, &mut record)?;
    if record.snapshot.state == IntegrationStatus::Parked {
        super::coordinator::resume_record(&state, runtime.as_ref(), Some(paths), &mut record)?;
    }
    if !matches!(
        record.snapshot.state,
        IntegrationStatus::Resolving | IntegrationStatus::Verifying
    ) {
        return Err(invalid());
    }
    let accepted =
        crate::runner_log::snapshot(&paths.state, task, turn)?.is_some_and(|s| s.accepted);
    if !accepted {
        let now = runtime.now_millis();
        let deadline = *record
            .admission_deadline_millis
            .get_or_insert(now.saturating_add(AUXILIARY_ADMISSION_MILLIS));
        if deadline <= now {
            record.snapshot.state = IntegrationStatus::Blocked;
            record.snapshot.blocked_code = Some(IntegrationCode::IntegrationTurnQueueTimeout);
            record.snapshot.resume_state = None;
            record.admission_deadline_millis = None;
            record.remaining_admission_millis = None;
            super::coordinator::persist_record(&state, runtime.as_ref(), &mut record)?;
            return Err(IntegrationCode::IntegrationTurnQueueTimeout.error());
        }
        super::coordinator::persist_record(&state, runtime.as_ref(), &mut record)?;
    }
    Ok(Some(AuxiliaryLaunchPermit {
        _phase: Some(permit),
    }))
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

/// Observe retained process identities before an operator's confirmation wait.
/// A fresh CLI must prove death in its own cache before selected recovery can
/// release a phase actor or replace a driver. This read creates no sidecars.
pub(crate) fn retained_owner_identities(
    paths: &PathLayout,
    tasks: &[TaskId],
) -> Result<Vec<crate::job::ProcessIdentity>, WorkerError> {
    let mut actors = Vec::new();
    for task in tasks {
        let (_, record) = super::store::RootedIntegrationState::read_task(paths, *task)?;
        let Some(record) = record else { continue };
        if let Some(actor) = record.actor {
            actor.validate()?;
            actors.push(actor);
        }
        if let Some(bytes) = read(&task_root(paths, *task)?, "driver.json")? {
            let binding: DriverBinding = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            binding.actor.validate()?;
            if binding.task != *task {
                return Err(invalid());
            }
            actors.push(binding.actor);
        }
    }
    Ok(actors)
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

#[cfg(test)]
pub(crate) mod native_launch_tests {
    use super::*;
    use crate::{
        integration::testing::*,
        job::{CommandSpec, QueueEntry, QueueEntryKind},
        scheduler::WorkerPreference,
        task::RunnerIdentity,
    };
    use std::sync::atomic::AtomicU64;

    struct NeverSpawn;
    impl RunnerExecutor for NeverSpawn {
        fn start(
            &self,
            _: &PathLayout,
            _: TaskId,
            _: TurnId,
        ) -> Result<RunnerIdentity, WorkerError> {
            panic!("expired auxiliary reached the executor")
        }
    }
    fn paths(base: &std::path::Path) -> PathLayout {
        PathLayout {
            config: base.join("config"),
            state: base.join("state"),
            data: base.join("data"),
            cache: base.join("cache"),
        }
    }

    struct NoProcesses;
    impl ProcessRunner for NoProcesses {
        fn run(
            &self,
            _: &crate::process::ProcessRequest,
        ) -> Result<crate::process::ProcessResult, WorkerError> {
            panic!("a saved redrive result must not contact Git or the helper")
        }
    }

    #[test]
    fn native_owner_releases_companion_hints_after_the_outer_state_fence() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(&root.path().canonicalize().unwrap());
        let sink = Arc::new(crate::controller::events::testing::RecordingSink::new());
        let client = ClientStateStore::open(&paths.state)
            .unwrap()
            .with_event_sink(sink.clone());
        let config = Config::parse("version = 1\n").unwrap();
        let owner =
            OwnerIntegration::new(&NoProcesses, &config, &paths, &client, &NeverSpawn).unwrap();
        let record = sample_record(fixture_task(), fixture_source(), "main");
        owner
            .state
            .publish_policy(record.task_id, &record.policy)
            .unwrap();
        let scope = client.event_scope();
        let fence = crate::controller::drain::integration_admission(
            &paths.controller_state_root(),
            client.wait_deadline(),
            1000,
        )
        .unwrap()
        .unwrap();
        assert!(
            owner
                .state
                .replace(record.task_id, IntegrationRevision(0), &record)
                .unwrap()
        );
        assert!(sink.batches().is_empty());
        drop(fence);
        drop(scope);
        let batches = sink.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].events(),
            &[crate::controller::events::NewEvent::IntegrationChanged {
                task_id: record.task_id,
                integration: record.snapshot.annotation().unwrap(),
            }]
        );
    }

    #[test]
    fn native_observer_keeps_close_intent_until_close_recovery_retires_it() {
        struct ClosedHost(crate::task_store::TaskStatusResponse);
        impl ProcessRunner for ClosedHost {
            fn run(
                &self,
                request: &crate::process::ProcessRequest,
            ) -> Result<crate::process::ProcessResult, WorkerError> {
                use std::os::unix::process::ExitStatusExt;
                if request
                    .args
                    .last()
                    .is_some_and(|arg| arg.to_string_lossy().ends_with(" host task-status"))
                {
                    return Ok(crate::process::ProcessResult {
                        status: std::process::ExitStatus::from_raw(0),
                        stdout: serde_json::to_vec(&self.0).unwrap(),
                        stderr: vec![],
                    });
                }
                Err(integration_unavailable())
            }
        }
        let root = tempfile::tempdir().unwrap();
        let paths = paths(&root.path().canonicalize().unwrap());
        let client = ClientStateStore::open(&paths.state).unwrap();
        let ordinary = sample_ordinary(fixture_task(), fixture_source());
        let closing = ordinary
            .with_close_intent(crate::task::TaskCloseIntent::from_record(&ordinary, false).unwrap())
            .unwrap();
        client.create_task(closing.clone()).unwrap();
        let mut wire = serde_json::to_value(ordinary.status()).unwrap();
        wire["state"] = serde_json::json!("closed");
        let remote = ClosedHost(crate::task_store::TaskStatusResponse::new(
            serde_json::from_value(wire).unwrap(),
        ));
        let config = Config::parse("version = 1\n[[workers]]\nname = 'fixture-worker'\nssh = 'fixture.invalid'\nslots = 1\n").unwrap();
        let owner = OwnerIntegration::new(&remote, &config, &paths, &client, &NeverSpawn).unwrap();
        owner
            .state
            .publish_policy(fixture_task(), &sample_policy("main"))
            .unwrap();
        let facts = owner.ports.facts(fixture_task()).unwrap();
        assert!(facts.close_pending);
        assert_eq!(facts.ordinary, closing);
        assert_eq!(client.load_task(fixture_task()).unwrap(), closing);
    }

    #[test]
    fn native_redrive_recovers_the_published_epoch_without_advancing_again() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(&root.path().canonicalize().unwrap());
        let client = ClientStateStore::open(&paths.state).unwrap();
        client
            .create_task(sample_ordinary(fixture_task(), fixture_source()))
            .unwrap();
        let config = Config::parse("version = 1\n[[workers]]\nname = 'fixture-worker'\nssh = 'fixture.invalid'\nslots = 1\n").unwrap();
        let owner =
            OwnerIntegration::new(&NoProcesses, &config, &paths, &client, &NeverSpawn).unwrap();
        let mut record = sample_record(fixture_task(), fixture_source(), "main");
        record.snapshot.state = IntegrationStatus::Blocked;
        record.snapshot.blocked_code = Some(IntegrationCode::IntegrationWorkerOffline);
        owner
            .state
            .publish_policy(record.task_id, &record.policy)
            .unwrap();
        owner
            .state
            .replace(record.task_id, IntegrationRevision(0), &record)
            .unwrap();
        record.snapshot.revision = IntegrationRevision(2);
        record.snapshot.state = IntegrationStatus::Revoked;
        record.tombstone = Some(IntegrationTombstone {
            epoch: 0,
            revision: IntegrationRevision(2),
            requested_at_millis: 1001,
            acknowledged: true,
        });
        owner
            .state
            .replace(record.task_id, IntegrationRevision(1), &record)
            .unwrap();
        record.snapshot.epoch = 1;
        record.snapshot.revision = IntegrationRevision(3);
        record.snapshot.state = IntegrationStatus::Pending;
        record.snapshot.blocked_code = None;
        record.tombstone = None;
        owner
            .state
            .replace(record.task_id, IntegrationRevision(2), &record)
            .unwrap();
        crate::controller::drain::set_drained(&paths.controller_state_root(), true).unwrap();
        let request = IntegrationRedriveRequest {
            task_id: record.task_id,
            expected: IntegrationRevision(1),
            request_id: "00000000000000000000000000000064".into(),
        };
        let binding = RedriveBinding {
            request: request.clone(),
            intent: record.snapshot.integration_id,
            epoch: 0,
            result: None,
        };
        let task_root = task_root(&paths, record.task_id).unwrap();
        write(
            &task_root,
            &format!("redrive-{}.json", request.request_id),
            &serde_json::to_vec(&binding).unwrap(),
            true,
        )
        .unwrap();
        let recovered = owner.redrive(&request).unwrap();
        assert_eq!(recovered.epoch, 1);
        assert_eq!(recovered.integration_id, record.snapshot.integration_id);
        assert_eq!(
            owner
                .state
                .load(record.task_id)
                .unwrap()
                .unwrap()
                .snapshot
                .state,
            IntegrationStatus::Parked
        );
        assert_eq!(owner.redrive(&request).unwrap(), recovered);
        assert_eq!(
            owner
                .state
                .load(record.task_id)
                .unwrap()
                .unwrap()
                .snapshot
                .epoch,
            1
        );
        assert!(client.queue_snapshot().unwrap().entries().is_empty());
    }

    #[test]
    fn native_integrated_redrive_refuses_inconsistent_confirmation() {
        for bad in ["import", "epoch", "source", "target", "fetched", "latest"] {
            let temp = tempfile::tempdir().unwrap();
            let paths = paths(&temp.path().canonicalize().unwrap());
            let client = ClientStateStore::open(&paths.state).unwrap();
            let ordinary = match bad {
                "fetched" => sample_ordinary(fixture_task(), fixture_source())
                    .with_fetched_head(None)
                    .unwrap(),
                "latest" => sample_ordinary_followup(
                    fixture_task(),
                    fixture_source(),
                    Some(crate::task::TaskOutcome::Done),
                ),
                _ => sample_ordinary(fixture_task(), fixture_source()),
            };
            client.create_task(ordinary.clone()).unwrap();
            let config = Config::parse("version = 1\n[[workers]]\nname = 'fixture-worker'\nssh = 'never-connect'\nslots = 1\n").unwrap();
            let owner =
                OwnerIntegration::new(&NoProcesses, &config, &paths, &client, &NeverSpawn).unwrap();
            let mut record = sample_record(fixture_task(), fixture_source(), "main");
            record.snapshot.state = IntegrationStatus::Integrated;
            record.snapshot.disposition = Some(IntegrationDisposition::AlreadyIntegrated);
            record.snapshot.observed_target_oid = Some(fixture_head());
            let mut receipt = IntegrationReceipt {
                integration_id: record.snapshot.integration_id,
                epoch: 0,
                source_turn_id: fixture_source(),
                source_head: fixture_head(),
                target_head: fixture_head(),
                merge_oid: None,
                disposition: IntegrationDisposition::AlreadyIntegrated,
                imported: true,
                recorded_at_millis: 1002,
            };
            match bad {
                "import" => receipt.imported = false,
                "epoch" => receipt.epoch = 1,
                "source" => receipt.source_turn_id = TurnId::generate(),
                "target" => receipt.target_head = "e".repeat(40).parse().unwrap(),
                _ => {}
            }
            record.receipt = Some(receipt);
            owner
                .state
                .publish_policy(record.task_id, &record.policy)
                .unwrap();
            owner
                .state
                .replace(record.task_id, IntegrationRevision(0), &record)
                .unwrap();
            let request = IntegrationRedriveRequest {
                task_id: record.task_id,
                expected: record.snapshot.revision,
                request_id: uuid::Uuid::new_v4().simple().to_string(),
            };
            let error = owner.redrive(&request).unwrap_err();
            assert_eq!(
                error.public_code(),
                if bad == "latest" {
                    "INTEGRATION_DEPENDENCY_NOT_INTEGRATED"
                } else {
                    "INTEGRATION_STATE_INVALID"
                },
                "{bad}"
            );
            assert_eq!(owner.state.load(record.task_id).unwrap().unwrap(), record);
            assert_eq!(client.load_task(record.task_id).unwrap(), ordinary);
            assert!(
                read(
                    &task_root(&paths, record.task_id).unwrap(),
                    &format!("redrive-{}.json", request.request_id)
                )
                .unwrap()
                .is_none()
            );
        }
    }

    #[test]
    fn native_phase_and_auxiliary_permits_serialize_both_sides_of_drain_ack() {
        use std::sync::mpsc;
        for auxiliary in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let paths = paths(&root.path().canonicalize().unwrap());
            let client = ClientStateStore::open(&paths.state)
                .unwrap()
                .with_admission_clock(Arc::new(|| Ok(2001)));
            let (state, record, prepared, entry) = queued_auxiliary(&paths, &client, 601001);
            let gate = paths.controller_state_root();
            crate::controller::drain::set_drained_at(&gate, false, 1001).unwrap();
            let runtime = OwnerRuntime::new(&paths, &client).unwrap();
            let key = IntegrationPhaseKey {
                task: record.task_id,
                intent: record.snapshot.integration_id,
                epoch: 0,
                revision: record.snapshot.revision,
                phase: IntegrationPhase::Fetch,
            };
            let permit: Box<dyn Send> = if auxiliary {
                Box::new(
                    auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id())
                        .unwrap()
                        .unwrap(),
                )
            } else {
                let IntegrationDriveAdmission::Permit(permit) = runtime.begin_phase(&key).unwrap()
                else {
                    panic!("open gate refused phase")
                };
                Box::new(permit)
            };
            let probe = File::open(gate.join("drain.lock")).unwrap();
            assert_ne!(
                unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
                0
            );
            assert_eq!(io::Error::last_os_error().kind(), io::ErrorKind::WouldBlock);
            let (begin, begun) = mpsc::channel();
            let (ack, acknowledged) = mpsc::channel();
            let writer_gate = gate.clone();
            let writer = std::thread::spawn(move || {
                begin.send(()).unwrap();
                crate::controller::drain::set_drained_at(&writer_gate, true, 2001).unwrap();
                ack.send(()).unwrap();
            });
            begun.recv().unwrap();
            assert!(matches!(
                acknowledged.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            drop(permit);
            acknowledged.recv().unwrap();
            writer.join().unwrap();
            assert!(matches!(
                runtime.begin_phase(&key).unwrap(),
                IntegrationDriveAdmission::Park(IntegrationPauseEvidence {
                    effective_at_millis: 2001,
                    ..
                })
            ));
            assert!(
                auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id())
                    .unwrap()
                    .is_none()
            );
            let parked = state.load(record.task_id).unwrap().unwrap();
            assert_eq!(parked.followups_spent, 1);
            assert_eq!(
                state.load_prepared(record.task_id, entry.job_id()).unwrap(),
                Some(prepared)
            );
            assert_eq!(client.queue_snapshot().unwrap().entries().len(), 1);
            assert_eq!(
                client
                    .queue_entry(entry.job_id())
                    .unwrap()
                    .unwrap()
                    .queue_id(),
                entry.queue_id()
            );
        }
    }

    #[test]
    fn native_helper_pause_after_undrain_preserves_only_the_active_remainder() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(&root.path().canonicalize().unwrap());
        let clock = Arc::new(AtomicU64::new(1001));
        let read_clock = clock.clone();
        let client = ClientStateStore::open(&paths.state)
            .unwrap()
            .with_admission_clock(Arc::new(move || Ok(read_clock.load(Ordering::SeqCst))));
        let (state, record, prepared, entry) = queued_auxiliary(&paths, &client, 601001);
        let gate = paths.controller_state_root();
        crate::controller::drain::set_drained_at(&gate, true, 121001).unwrap();
        clock.store(1500001, Ordering::SeqCst);
        assert!(
            auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id())
                .unwrap()
                .is_none()
        );
        crate::controller::drain::set_drained_at(&gate, false, 1900001).unwrap();
        clock.store(1923001, Ordering::SeqCst);
        let config = Config::parse("version = 1\n[[workers]]\nname = 'fixture-worker'\nssh = 'fixture.invalid'\nslots = 1\n").unwrap();
        let owner =
            OwnerIntegration::new(&NoProcesses, &config, &paths, &client, &NeverSpawn).unwrap();
        owner
            .runtime
            .helper_unavailable
            .store(true, Ordering::Release);
        let key = IntegrationPhaseKey {
            task: record.task_id,
            intent: record.snapshot.integration_id,
            epoch: 0,
            revision: record.snapshot.revision,
            phase: IntegrationPhase::Drive,
        };
        let IntegrationDriveAdmission::Park(pause) = owner.runtime.begin_phase(&key).unwrap()
        else {
            panic!("unavailable helper admitted")
        };
        owner
            .coordinator()
            .park_for_runtime(record.task_id, pause)
            .unwrap();
        let parked = state.load(record.task_id).unwrap().unwrap();
        assert_eq!(
            parked.snapshot.pause_reason,
            Some(IntegrationPauseReason::HelperUnavailable)
        );
        assert_eq!(parked.remaining_admission_millis, Some(457000));
        clock.store(2500001, Ordering::SeqCst);
        let IntegrationDriveAdmission::Park(pause) = owner.runtime.begin_phase(&key).unwrap()
        else {
            panic!("unavailable helper admitted")
        };
        owner
            .coordinator()
            .park_for_runtime(record.task_id, pause)
            .unwrap();
        assert_eq!(
            state
                .load(record.task_id)
                .unwrap()
                .unwrap()
                .pause
                .unwrap()
                .effective_at_millis,
            1923001
        );
        drop(owner);
        // A capable helper returns after a new owner reopens the same stores.
        clock.store(2900001, Ordering::SeqCst);
        assert!(
            auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id())
                .unwrap()
                .is_some()
        );
        let restored = state.load(record.task_id).unwrap().unwrap();
        assert_eq!(restored.admission_deadline_millis, Some(3357001));
        assert_eq!(restored.followups_spent, 1);
        assert_eq!(
            state.load_prepared(record.task_id, entry.job_id()).unwrap(),
            Some(prepared)
        );
        assert_eq!(client.queue_snapshot().unwrap().entries().len(), 1);
    }

    #[test]
    fn late_native_phase_observes_the_persisted_pause_time() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(&root.path().canonicalize().unwrap());
        let client = ClientStateStore::open(&paths.state).unwrap();
        crate::controller::drain::set_drained(&paths.controller_state_root(), true).unwrap();
        let metadata: serde_json::Value = serde_json::from_slice(
            &std::fs::read(paths.controller_state_root().join("integration-gate.json")).unwrap(),
        )
        .unwrap();
        let effective = metadata["windows"][0]["effective_at_millis"]
            .as_u64()
            .unwrap();
        let client = client.with_admission_clock(Arc::new(move || Ok(effective + 900_000)));
        let runtime = OwnerRuntime::new(&paths, &client).unwrap();
        let record = sample_record(fixture_task(), fixture_source(), "main");
        let key = IntegrationPhaseKey {
            task: record.task_id,
            intent: record.snapshot.integration_id,
            epoch: 0,
            revision: record.snapshot.revision,
            phase: IntegrationPhase::Fetch,
        };
        match runtime.begin_phase(&key).unwrap() {
            IntegrationDriveAdmission::Park(pause) => {
                assert_eq!(pause.effective_at_millis, effective)
            }
            _ => panic!("drained phase admitted"),
        }
        crate::controller::drain::set_drained(&paths.controller_state_root(), true).unwrap();
        let again: serde_json::Value = serde_json::from_slice(
            &std::fs::read(paths.controller_state_root().join("integration-gate.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(again, metadata, "repeated drain renewed its effective time");
    }

    pub(crate) fn queued_auxiliary(
        paths: &PathLayout,
        client: &ClientStateStore,
        deadline: u64,
    ) -> (
        super::super::store::RootedIntegrationState,
        IntegrationRecord,
        PreparedIntegrationTurn,
        QueueEntry,
    ) {
        let runtime = Arc::new(OwnerRuntime::new(paths, client).unwrap());
        let state = super::super::store::RootedIntegrationState::open(paths, runtime).unwrap();
        let ordinary = sample_ordinary(fixture_task(), fixture_source());
        client.create_task(ordinary.clone()).unwrap();
        let mut record = sample_record(fixture_task(), fixture_source(), "main");
        record.snapshot.state = IntegrationStatus::Resolving;
        record.snapshot.attempts = 1;
        record.candidates.push(sample_candidate(&record));
        let prepared = PreparedIntegrationTurn::prepare(
            &ordinary,
            &record,
            IntegrationTurnPurpose::Resolve,
            1,
            1,
        )
        .unwrap();
        record.auxiliaries.push(prepared.intent().unwrap());
        record.followups_spent = 1;
        record.snapshot.resolve_turns = 1;
        record.admission_deadline_millis = Some(deadline);
        state
            .publish_policy(record.task_id, &record.policy)
            .unwrap();
        state.publish_prepared(record.task_id, &prepared).unwrap();
        state
            .replace(record.task_id, IntegrationRevision(0), &record)
            .unwrap();
        let pending = crate::task::TurnSummary::new(
            2,
            prepared.followup.turn_id(),
            None,
            None,
            None,
            false,
            Some(1001),
            None,
        );
        let status = crate::task::TaskStatus::new(
            TaskState::Active,
            ordinary.status().last_outcome().cloned(),
            Some("fixture-worker".into()),
            true,
            Some(prepared.followup.base_oid().clone()),
            None,
            vec![],
            vec![],
            None,
            ordinary
                .status()
                .turns()
                .iter()
                .cloned()
                .chain([pending])
                .collect(),
            1001,
        )
        .unwrap();
        client
            .update_task_if_current(&ordinary, ordinary.with_status(status).unwrap())
            .unwrap();
        client
            .write_turn_prompt(
                record.task_id,
                prepared.followup.turn_id(),
                prepared.followup.composed_prompt(),
            )
            .unwrap();
        client
            .write_turn_prepared_binding(
                record.task_id,
                prepared.followup.turn_id(),
                &prepared.followup.binding(),
            )
            .unwrap();
        let entry = client
            .enqueue(
                QueueEntry::new(
                    prepared.followup.turn_id(),
                    client.client_id(),
                    ordinary.meta().project_id().into(),
                    ordinary.meta().worktree_id().into(),
                    CommandSpec::argv(vec!["task".into()])
                        .unwrap()
                        .summary()
                        .unwrap(),
                    vec![],
                    WorkerPreference::Pinned {
                        worker: "fixture-worker".into(),
                    },
                    QueueEntryKind::TaskTurn,
                    None,
                    crate::turn_runner::current_process_identity().unwrap(),
                    1001,
                )
                .unwrap(),
            )
            .unwrap();
        (state, record, prepared, entry)
    }

    #[test]
    fn expired_auxiliary_is_refused_at_the_real_runner_handoff() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(&root.path().canonicalize().unwrap());
        let clock = Arc::new(AtomicU64::new(700_001));
        let read_clock = clock.clone();
        let client = ClientStateStore::open(&paths.state)
            .unwrap()
            .with_admission_clock(Arc::new(move || Ok(read_clock.load(Ordering::SeqCst))));
        let (state, record, _, entry) = queued_auxiliary(&paths, &client, 600_001);
        assert_eq!(
            crate::turn_runner::start_runner_with_reservation(
                &client,
                &NeverSpawn,
                &paths,
                record.task_id,
                entry.job_id(),
                1,
                false
            )
            .unwrap_err()
            .public_code(),
            "INTEGRATION_TURN_QUEUE_TIMEOUT"
        );
        assert_eq!(
            state.load(record.task_id).unwrap().unwrap().snapshot.state,
            IntegrationStatus::Blocked
        );
        assert_eq!(
            client
                .queue_entry(entry.job_id())
                .unwrap()
                .unwrap()
                .queue_id(),
            entry.queue_id()
        );
    }

    #[test]
    fn queued_auxiliary_keeps_eight_active_minutes_and_its_position_after_restart() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(&root.path().canonicalize().unwrap());
        let clock = Arc::new(AtomicU64::new(1001));
        let read_clock = clock.clone();
        let client = ClientStateStore::open(&paths.state)
            .unwrap()
            .with_admission_clock(Arc::new(move || Ok(read_clock.load(Ordering::SeqCst))));
        let (state, record, prepared, entry) = queued_auxiliary(&paths, &client, 601001);
        let gate = paths.controller_state_root();
        crate::controller::drain::set_drained_at(&gate, true, 121001).unwrap();
        clock.store(1_500_001, Ordering::SeqCst);
        assert_eq!(
            crate::turn_runner::start_runner_with_reservation(
                &client,
                &NeverSpawn,
                &paths,
                record.task_id,
                entry.job_id(),
                1,
                false
            )
            .unwrap(),
            crate::turn_runner::RunnerStart::Drained
        );
        let parked = state.load(record.task_id).unwrap().unwrap();
        assert_eq!(parked.pause.as_ref().unwrap().effective_at_millis, 121001);
        assert_eq!(parked.remaining_admission_millis, Some(480000));
        assert_eq!(parked.snapshot.state, IntegrationStatus::Parked);
        let reopened = client.reopen_until(None).unwrap();
        clock.store(1_900_001, Ordering::SeqCst);
        crate::controller::drain::set_drained_at(&gate, false, 1_900_001).unwrap();
        assert!(matches!(
            crate::turn_runner::start_runner_with_reservation(
                &reopened,
                &crate::turn_runner::InlineRunnerExecutor,
                &paths,
                record.task_id,
                entry.job_id(),
                1,
                false
            )
            .unwrap(),
            crate::turn_runner::RunnerStart::Started(_)
        ));
        let resumed = state.load(record.task_id).unwrap().unwrap();
        assert_eq!(resumed.admission_deadline_millis, Some(2_380_001));
        assert_eq!(
            resumed.snapshot.integration_id,
            parked.snapshot.integration_id
        );
        assert_eq!(resumed.auxiliaries, parked.auxiliaries);
        assert_eq!(resumed.followups_spent, 1);
        assert_eq!(
            state.load_prepared(record.task_id, entry.job_id()).unwrap(),
            Some(prepared)
        );
        assert_eq!(reopened.queue_snapshot().unwrap().entries().len(), 1);
        assert_eq!(
            reopened
                .queue_entry(entry.job_id())
                .unwrap()
                .unwrap()
                .queue_id(),
            entry.queue_id()
        );
        clock.store(2_380_000, Ordering::SeqCst);
        assert!(matches!(
            crate::turn_runner::start_runner_with_reservation(
                &reopened,
                &NeverSpawn,
                &paths,
                record.task_id,
                entry.job_id(),
                1,
                false
            )
            .unwrap(),
            crate::turn_runner::RunnerStart::Pending
        ));
        clock.store(2_380_001, Ordering::SeqCst);
        assert_eq!(
            crate::turn_runner::start_runner_with_reservation(
                &reopened,
                &NeverSpawn,
                &paths,
                record.task_id,
                entry.job_id(),
                1,
                false
            )
            .unwrap_err()
            .public_code(),
            "INTEGRATION_TURN_QUEUE_TIMEOUT"
        );
    }

    #[test]
    fn queued_auxiliary_history_pruning_is_exact_below_cap_and_conservative_above_it() {
        for cycles in [200, 257] {
            let root = tempfile::tempdir().unwrap();
            let paths = paths(&root.path().canonicalize().unwrap());
            let clock = Arc::new(AtomicU64::new(1001));
            let read_clock = clock.clone();
            let client = ClientStateStore::open(&paths.state)
                .unwrap()
                .with_admission_clock(Arc::new(move || Ok(read_clock.load(Ordering::SeqCst))));
            let deadline = if cycles == 200 { 601001 } else { 130001 };
            let (state, record, prepared, entry) = queued_auxiliary(&paths, &client, deadline);
            let gate = paths.controller_state_root();
            crate::controller::drain::set_drained_at(&gate, true, 1001).unwrap();
            assert!(
                auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id())
                    .unwrap()
                    .is_none()
            );
            // Restore completed windows as a delayed/restarted owner would
            // find them; the operator write below performs actual pruning.
            let windows = (0..cycles)
                .map(|cycle| {
                    serde_json::json!({
                        "reason": "controller_drained", "effective_at_millis": 1001 + cycle * 2000,
                        "resumed_at_millis": 2501 + cycle * 2000,
                    })
                })
                .collect::<Vec<_>>();
            std::fs::write(
                gate.join("integration-gate.json"),
                serde_json::to_vec(&serde_json::json!({"version":1,"windows":windows})).unwrap(),
            )
            .unwrap();
            crate::controller::drain::set_drained_at(&gate, false, 1001 + cycles * 2000).unwrap();
            clock.store(1001 + cycles * 2000, Ordering::SeqCst);
            let result = auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id());
            let restored = state.load(record.task_id).unwrap().unwrap();
            if cycles == 200 {
                assert!(result.unwrap().is_some());
                assert_eq!(restored.admission_deadline_millis, Some(901001));
                assert_eq!(
                    restored.admission_deadline_millis.unwrap() - clock.load(Ordering::SeqCst),
                    500000
                );
            } else {
                assert_eq!(
                    result.err().unwrap().public_code(),
                    "INTEGRATION_TURN_QUEUE_TIMEOUT"
                );
                assert_eq!(restored.snapshot.state, IntegrationStatus::Blocked);
                assert!(
                    restored.tombstone.is_none(),
                    "budget expiry must stay re-drivable"
                );
            }
            assert_eq!(
                restored.snapshot.integration_id,
                record.snapshot.integration_id
            );
            assert_eq!(restored.snapshot.epoch, record.snapshot.epoch);
            assert_eq!(restored.followups_spent, 1);
            assert_eq!(
                state.load_prepared(record.task_id, entry.job_id()).unwrap(),
                Some(prepared)
            );
            assert_eq!(client.queue_snapshot().unwrap().entries().len(), 1);
            assert_eq!(
                client
                    .queue_entry(entry.job_id())
                    .unwrap()
                    .unwrap()
                    .queue_id(),
                entry.queue_id()
            );
        }
    }

    #[test]
    fn native_disable_pause_survives_enable_without_blocking_ordinary_admission() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(&root.path().canonicalize().unwrap());
        let now = crate::controller::leader::now_millis().unwrap();
        let clock = Arc::new(AtomicU64::new(now));
        let read_clock = clock.clone();
        let client = ClientStateStore::open(&paths.state)
            .unwrap()
            .with_admission_clock(Arc::new(move || Ok(read_clock.load(Ordering::SeqCst))));
        let (state, record, prepared, entry) = queued_auxiliary(&paths, &client, now + 600000);
        let gate = paths.controller_state_root();
        let _ = crate::controller::drain::close_for_disable(&gate).unwrap();
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(gate.join("integration-gate.json")).unwrap())
                .unwrap();
        let effective = metadata["windows"][0]["effective_at_millis"]
            .as_u64()
            .unwrap();
        clock.store(effective + 900000, Ordering::SeqCst);
        assert!(
            auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id())
                .unwrap()
                .is_none()
        );
        let parked = state.load(record.task_id).unwrap().unwrap();
        assert_eq!(
            parked.pause.unwrap().reason,
            IntegrationPauseReason::ControllerDisabled
        );
        assert!(!crate::controller::drain::is_drained(&gate).unwrap());
        crate::controller::ControllerStore::open(&gate).unwrap();
        assert!(
            crate::controller::drain::launch_permit(
                &gate,
                crate::client_state::WaitDeadline::new(None)
            )
            .unwrap()
            .is_some()
        );
        assert!(
            auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id())
                .unwrap()
                .is_none()
        );
        let resume = effective + 1100000;
        crate::controller::drain::set_drained_at(&gate, false, resume).unwrap();
        clock.store(resume, Ordering::SeqCst);
        assert!(
            auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id())
                .unwrap()
                .is_some()
        );
        let restored = state.load(record.task_id).unwrap().unwrap();
        assert_eq!(
            restored.admission_deadline_millis,
            Some(resume + parked.remaining_admission_millis.unwrap())
        );
        assert_eq!(
            restored.snapshot.integration_id,
            record.snapshot.integration_id
        );
        assert_eq!(restored.snapshot.epoch, record.snapshot.epoch);
        assert_eq!(restored.followups_spent, 1);
        assert_eq!(
            state.load_prepared(record.task_id, entry.job_id()).unwrap(),
            Some(prepared)
        );
        assert_eq!(
            client
                .queue_entry(entry.job_id())
                .unwrap()
                .unwrap()
                .queue_id(),
            entry.queue_id()
        );
        assert_eq!(client.queue_snapshot().unwrap().entries().len(), 1);
    }

    #[test]
    fn corrupt_integration_gate_allows_ordinary_permit_and_refuses_auxiliary() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp.path().canonicalize().unwrap());
        let client = ClientStateStore::open(&paths.state).unwrap();
        let (state, record, prepared, entry) = queued_auxiliary(&paths, &client, u64::MAX);
        let gate = paths.controller_state_root();
        crate::controller::drain::set_drained_at(&gate, false, 1001).unwrap();
        let bad = b"invalid integration gate";
        let path = gate.join("integration-gate.json");
        std::fs::write(&path, bad).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let queue = client.queue_snapshot().unwrap();
        assert!(
            crate::controller::drain::launch_permit(&gate, client.wait_deadline())
                .unwrap()
                .is_some()
        );
        assert_eq!(
            auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id())
                .err()
                .unwrap()
                .public_code(),
            "CONTROLLER_TRANSPORT"
        );
        assert_eq!(state.load(record.task_id).unwrap().unwrap(), record);
        assert_eq!(
            state.load_prepared(record.task_id, entry.job_id()).unwrap(),
            Some(prepared)
        );
        assert_eq!(client.queue_snapshot().unwrap(), queue);
        assert_eq!(std::fs::read(path).unwrap(), bad);
    }

    #[test]
    #[ignore] // 1,000 fsynced cycles run in the nightly stress selection.
    fn native_pause_history_1000_cycles_stress() {
        let root = tempfile::tempdir().unwrap();
        let gate = root.path().canonicalize().unwrap().join("controller");
        for cycle in 0..1000 {
            let start = 1000 + cycle * 2000;
            crate::controller::drain::set_drained_at(&gate, true, start).unwrap();
            assert!(crate::controller::drain::is_drained(&gate).unwrap());
            crate::controller::drain::set_drained_at(&gate, false, start + 1500).unwrap();
            assert!(!crate::controller::drain::is_drained(&gate).unwrap());
            if cycle == 199 {
                assert_eq!(
                    crate::controller::drain::elapsed_pause_time(&gate, 1000, 401000).unwrap(),
                    300000
                );
            }
        }
        let bytes = std::fs::read(gate.join("integration-gate.json")).unwrap();
        let metadata: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(metadata["windows"].as_array().unwrap().len() <= 256);
        assert!(bytes.len() <= MAX_PRIVATE_RECORD_BYTES);
        let pause = crate::controller::drain::elapsed_pause_time(&gate, 1000, 2001000).unwrap();
        assert!((601000u64 + pause).saturating_sub(2001000) <= 100000);
    }

    #[test]
    fn a_completed_pause_is_accounted_once_even_when_no_owner_observed_the_drain() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(&root.path().canonicalize().unwrap());
        let clock = Arc::new(AtomicU64::new(1001));
        let read_clock = clock.clone();
        let client = ClientStateStore::open(&paths.state)
            .unwrap()
            .with_admission_clock(Arc::new(move || Ok(read_clock.load(Ordering::SeqCst))));
        let (state, record, _, entry) = queued_auxiliary(&paths, &client, 601001);
        let gate = paths.controller_state_root();
        crate::controller::drain::set_drained_at(&gate, true, 121001).unwrap();
        crate::controller::drain::set_drained_at(&gate, false, 1_900_001).unwrap();
        clock.store(1_923_001, Ordering::SeqCst);
        let permit = auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id())
            .unwrap()
            .unwrap();
        drop(permit);
        assert_eq!(
            state
                .load(record.task_id)
                .unwrap()
                .unwrap()
                .admission_deadline_millis,
            Some(2_380_001)
        );
        drop(auxiliary_launch_permit(&paths, &client, record.task_id, entry.job_id()).unwrap());
        assert_eq!(
            state
                .load(record.task_id)
                .unwrap()
                .unwrap()
                .admission_deadline_millis,
            Some(2_380_001)
        );
    }

    #[test]
    fn late_auxiliary_observation_preserves_a_completed_pause_before_updating_its_revision() {
        struct NoProcesses;
        impl ProcessRunner for NoProcesses {
            fn run(
                &self,
                _: &crate::process::ProcessRequest,
            ) -> Result<crate::process::ProcessResult, WorkerError> {
                panic!("auxiliary observation started a process")
            }
        }
        let root = tempfile::tempdir().unwrap();
        let paths = paths(&root.path().canonicalize().unwrap());
        let clock = Arc::new(AtomicU64::new(1001));
        let read_clock = clock.clone();
        let client = ClientStateStore::open(&paths.state)
            .unwrap()
            .with_admission_clock(Arc::new(move || Ok(read_clock.load(Ordering::SeqCst))));
        let (state, record, _, entry) = queued_auxiliary(&paths, &client, 601001);
        let gate = paths.controller_state_root();
        crate::controller::drain::set_drained_at(&gate, true, 121001).unwrap();
        crate::controller::drain::set_drained_at(&gate, false, 1_900_001).unwrap();
        clock.store(1_923_001, Ordering::SeqCst);
        let config = Config::parse(
            "version=1\n[[workers]]\nname='fixture-worker'\nssh='never-connect'\nslots=1\n",
        )
        .unwrap();
        let owner =
            OwnerIntegration::new(&NoProcesses, &config, &paths, &client, &NeverSpawn).unwrap();
        owner
            .coordinator()
            .on_terminal(record.task_id, entry.job_id())
            .unwrap();
        let observed = state.load(record.task_id).unwrap().unwrap();
        assert_eq!(
            observed.auxiliaries[0].queue_position,
            Some(entry.queue_id().value())
        );
        assert_eq!(observed.admission_deadline_millis, Some(2_380_001));
    }
}
