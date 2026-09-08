use super::*;

impl ClientStateStore {
    /// Reuses a waiting detached runner without allocating another process.
    /// The queue reservation is authoritative; runner metadata is published
    /// recipient-first while the same state/queue lock is still held.
    pub fn claim_parked_for_waiting_runner(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
        observations: &[CandidateObservation],
        now_millis: u64,
    ) -> Result<Option<(TaskId, QueueClaim)>, WorkerError> {
        owner.validate()?;
        if now_millis == 0 {
            return Err(queue_error(
                "QUEUE_CLAIM_INVALID",
                "queue claim timestamp must be positive",
            ));
        }
        let mut names = BTreeSet::new();
        for observation in observations {
            validate_state_worker_name(observation.worker_name())?;
            if !names.insert(observation.worker_name()) {
                return Err(queue_error(
                    "QUEUE_CANDIDATES_INVALID",
                    "worker observations contain a duplicate",
                ));
            }
        }
        let _lock = QueueLock::acquire(
            self.inner.root.as_raw_fd(),
            self.inner.queue.as_raw_fd(),
            &self.inner.sync_counts,
        )?;
        let (mut snapshot, identity) = read_queue_snapshot(self.inner.queue.as_raw_fd())?;
        require_queue_client(&snapshot, self.inner.client_id)?;
        let source_index = snapshot
            .entries
            .iter()
            .position(|entry| entry.job_id() == turn_id)
            .ok_or_else(|| queue_error("TASK_QUEUE_MISSING", "waiting runner has no queue row"))?;
        let source = &snapshot.entries[source_index];
        if source.kind() != QueueEntryKind::TaskTurn
            || !matches!(source.state(), QueueState::Waiting { owner: row_owner } if *row_owner == owner)
        {
            return Err(queue_error(
                "QUEUE_OWNER_MISMATCH",
                "only the current waiting runner can yield its turn",
            ));
        }
        let tasks = self.queued_task_records_locked(&snapshot)?;
        let source_record = tasks
            .get(&turn_id)
            .filter(|record| record.meta().task_id() == task_id)
            .ok_or_else(|| queue_error("TASK_INCONSISTENT", "waiting turn has no matching task"))?;
        if !self.task_turn_is_runnable(source, &tasks)? {
            return Ok(None);
        }
        // An eligible donor keeps its turn. This is checked again under the
        // lock because another process may have released capacity since its
        // previous claim attempt.
        if self
            .worker_for_reassignment(&snapshot, source_index, source, owner, observations, &tasks)?
            .is_some()
        {
            return Ok(None);
        }

        let mut selected = None;
        for (index, entry) in snapshot.entries.iter().enumerate() {
            if !matches!(entry.state(), QueueState::Parked)
                || !self.task_turn_is_runnable(entry, &tasks)?
                || !self.task_context_available_from(entry, source, &tasks)?
            {
                continue;
            }
            if let Some(worker) =
                self.worker_for_reassignment(&snapshot, index, source, owner, observations, &tasks)?
            {
                selected = Some((index, worker));
                break;
            }
        }
        let Some((target_index, worker)) = selected else {
            return Ok(None);
        };
        let recipient = &tasks[&snapshot.entries[target_index].job_id()];
        if self.task_project_path(recipient)?.is_none() {
            let source_path = self.task_project_path(source_record)?.ok_or_else(|| {
                queue_error(
                    "TASK_PROJECT_CONTEXT_MISSING",
                    "legacy recipient requires a saved donor project context",
                )
            })?;
            self.write_task_project_path_locked(recipient, &source_path)?;
        }
        snapshot.entries[source_index].park()?;
        snapshot.entries[target_index].unpark(owner)?;
        snapshot.entries[target_index].dispatch(owner, worker, now_millis)?;
        snapshot.validate()?;
        self.reach_concurrency_point(ClientStateConcurrencyPoint::QueuePublication);
        publish_queue_snapshot(self, &snapshot, identity)?;
        if self.take_fault(ClientStateWritePoint::AfterRunnerYieldQueuePublication) {
            return Err(injected_failure(
                ClientStateWritePoint::AfterRunnerYieldQueuePublication,
            ));
        }
        self.update_task_locked_before_final_sync(
            recipient.with_runner(Some(RunnerIdentity::new(owner)))?,
            || Ok(()),
        )?;
        self.update_task_locked_before_final_sync(source_record.with_runner(None)?, || Ok(()))?;
        Ok(Some((
            recipient.meta().task_id(),
            QueueClaim::new(snapshot.entries[target_index].clone()),
        )))
    }

    /// Builds the private task/turn mapping once while StateLock is held.
    /// Unrelated historical turn directories do not participate in dispatch.
    pub(super) fn queued_task_records_locked(
        &self,
        snapshot: &QueueSnapshot,
    ) -> Result<HashMap<TurnId, LocalTaskRecord>, WorkerError> {
        let queued = snapshot
            .entries
            .iter()
            .filter(|entry| entry.kind() == QueueEntryKind::TaskTurn)
            .map(QueueEntry::job_id)
            .collect::<HashSet<_>>();
        let mut records = HashMap::new();
        if queued.is_empty() {
            return Ok(records);
        }
        for record in self.list_tasks_locked()? {
            for turn in self.turn_ids_for_task(record.meta().task_id())? {
                if queued.contains(&turn) && records.insert(turn, record.clone()).is_some() {
                    return Err(queue_error(
                        "TASK_INCONSISTENT",
                        "queued turn belongs to more than one task",
                    ));
                }
            }
        }
        Ok(records)
    }

    fn task_turn_is_runnable(
        &self,
        entry: &QueueEntry,
        tasks: &HashMap<TurnId, LocalTaskRecord>,
    ) -> Result<bool, WorkerError> {
        let Some(record) = tasks.get(&entry.job_id()) else {
            return Ok(false);
        };
        if entry.kind() != QueueEntryKind::TaskTurn
            || entry.is_cancel_requested()
            || entry.preacceptance_abandonment_proof().is_some()
            || record.submission_intent_turn_id().is_some()
            || record.submission_rollback_turn_id().is_some()
            || record.abandon_code() == Some("SUBMISSION_ROLLBACK_INCOMPLETE")
            || !matches!(
                record.status().state(),
                crate::task::TaskState::Queued | crate::task::TaskState::Active
            )
        {
            return Ok(false);
        }
        if entry.project_id() != record.meta().project_id()
            || entry.worktree_id() != record.meta().worktree_id()
        {
            return Err(queue_error(
                "TASK_INCONSISTENT",
                "queued turn and task project identities differ",
            ));
        }
        match self.read_turn_prompt(record.meta().task_id(), entry.job_id()) {
            Ok(_) => Ok(true),
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn task_context_available_from(
        &self,
        entry: &QueueEntry,
        runner_entry: &QueueEntry,
        tasks: &HashMap<TurnId, LocalTaskRecord>,
    ) -> Result<bool, WorkerError> {
        let Some(record) = tasks.get(&entry.job_id()) else {
            return Ok(false);
        };
        if self.task_project_path(record)?.is_some() {
            return Ok(true);
        }
        if entry.project_id() != runner_entry.project_id()
            || entry.worktree_id() != runner_entry.worktree_id()
        {
            return Ok(false);
        }
        let Some(donor) = tasks.get(&runner_entry.job_id()) else {
            return Ok(false);
        };
        Ok(self.task_project_path(donor)?.is_some())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn blocks_queue_claim(
        &self,
        snapshot: &QueueSnapshot,
        older: &QueueEntry,
        runner_entry: &QueueEntry,
        worker: &str,
        capabilities: Option<&[String]>,
        caller: ProcessIdentity,
        tasks: &HashMap<TurnId, LocalTaskRecord>,
    ) -> Result<bool, WorkerError> {
        if older.is_cancel_requested()
            || !older.eligible_for(worker, capabilities)
            || !self.run_has_capacity(snapshot, older)?
        {
            return Ok(false);
        }
        match older.state() {
            QueueState::Waiting { owner } => Ok(self.owner_is_live_or_ambiguous(owner, caller)),
            QueueState::Parked => Ok(self.task_turn_is_runnable(older, tasks)?
                && self.task_context_available_from(older, runner_entry, tasks)?),
            QueueState::Dispatching { .. } => Ok(false),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn worker_for_reassignment(
        &self,
        snapshot: &QueueSnapshot,
        index: usize,
        runner_entry: &QueueEntry,
        owner: ProcessIdentity,
        observations: &[CandidateObservation],
        tasks: &HashMap<TurnId, LocalTaskRecord>,
    ) -> Result<Option<String>, WorkerError> {
        let entry = &snapshot.entries[index];
        if !self.run_has_capacity(snapshot, entry)? {
            return Ok(None);
        }
        let affinity = self.affinity_hints_locked(entry.project_id(), entry.worktree_id())?;
        for candidate in SchedulerPolicy::rank(observations, entry.requirements(), &affinity) {
            let worker = candidate.worker_name();
            let capabilities = observations
                .iter()
                .find(|observation| observation.worker_name() == worker)
                .map(|observation| observation.capabilities());
            if !entry.eligible_for(worker, capabilities)
                || snapshot.entries.iter().any(|other| matches!(other.state(), QueueState::Dispatching { selected_worker, .. } if selected_worker == worker))
            { continue; }
            let mut blocked = false;
            for older in &snapshot.entries[..index] {
                if self.blocks_queue_claim(
                    snapshot,
                    older,
                    runner_entry,
                    worker,
                    capabilities,
                    owner,
                    tasks,
                )? {
                    blocked = true;
                    break;
                }
            }
            if !blocked {
                return Ok(Some(worker.to_owned()));
            }
        }
        Ok(None)
    }
}
