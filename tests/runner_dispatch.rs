use std::sync::{Arc, Barrier};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    client_state::{ClientStateStore, ClientStateWritePoint},
    error::WorkerError,
    job::{
        AdmissionObservation, CommandSummary, ProcessIdentity, QueueEntry, QueueEntryKind,
        QueueRunReference, QueueState,
    },
    scheduler::{CandidateObservation, CandidateSlot, WorkerPreference},
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunnerIdentity, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskSource, TaskState, TaskStatus, TurnId,
    },
};
use uuid::Uuid;

struct LiveOwners;
impl ProcessInspector for LiveOwners {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        Ok(owner(pid))
    }
    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        ProcessObservation::Matching {
            process_group: expected.pid(),
        }
    }
    fn observe_group(&self, _: u32) -> ProcessGroupObservation {
        ProcessGroupObservation::Ambiguous
    }
    fn observe_group_members(&self, _: u32) -> ProcessGroupMembership {
        ProcessGroupMembership::Ambiguous
    }
}

fn owner(pid: u32) -> ProcessIdentity {
    ProcessIdentity::new(pid, u64::from(pid) * 1000 + 1).unwrap()
}
fn pin(worker: &str) -> WorkerPreference {
    WorkerPreference::Pinned {
        worker: worker.into(),
    }
}
fn turn(number: u128) -> TurnId {
    TurnId::new(Uuid::from_u128(number + 1000))
}
fn task(number: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(number))
}

struct Fixture {
    directory: tempfile::TempDir,
    store: ClientStateStore,
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = ClientStateStore::open_with_owner_inspector(
            &directory.path().canonicalize().unwrap().join("state"),
            LiveOwners,
        )
        .unwrap();
        Self { directory, store }
    }

    fn enqueue(
        &self,
        number: u128,
        preference: WorkerPreference,
        parked: bool,
        row_owner: ProcessIdentity,
        run: Option<QueueRunReference>,
    ) -> QueueEntry {
        self.enqueue_in_worktree(number, preference, parked, row_owner, run, 'b')
    }

    fn enqueue_in_worktree(
        &self,
        number: u128,
        preference: WorkerPreference,
        parked: bool,
        row_owner: ProcessIdentity,
        run: Option<QueueRunReference>,
        worktree: char,
    ) -> QueueEntry {
        let base = "a".repeat(40).parse().unwrap();
        let meta = TaskMeta::new(TaskMetaInput {
            task_id: task(number),
            run_id: run.as_ref().map(|r| r.run_id().as_str().parse().unwrap()),
            project_id: "a".repeat(64),
            worktree_id: worktree.to_string().repeat(64),
            agent: AgentKind::Codex,
            model: None,
            effort: None,
            policy: PermissionPolicy::Workspace,
            source: TaskSource::Local {
                wip: false,
                push_target: None,
            },
            publish: vec![PublishMode::Fetch],
            publish_branch: None,
            base_oid: base,
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
            title: None,
            prompt: "private prompt".into(),
            created_at_millis: number as u64,
        })
        .unwrap();
        let status = TaskStatus::new(
            TaskState::Queued,
            None,
            None,
            false,
            Some(meta.base_oid().clone()),
            None,
            vec![],
            vec![],
            None,
            vec![],
            number as u64,
        )
        .unwrap();
        let pinned = match &preference {
            WorkerPreference::Pinned { worker } => Some(worker.clone()),
            WorkerPreference::Automatic => None,
        };
        let record = LocalTaskRecord::new(
            meta,
            status,
            None,
            (!parked).then(|| RunnerIdentity::new(row_owner)),
            None,
            "c".repeat(64),
            pinned,
            true,
            None,
        )
        .unwrap();
        self.store.create_task(record.clone()).unwrap();
        self.store
            .write_task_project_path(&record, self.directory.path())
            .unwrap();
        self.store
            .write_turn_prompt(task(number), turn(number), "private prompt")
            .unwrap();
        let entry = QueueEntry::new(
            turn(number),
            self.store.client_id(),
            record.meta().project_id().into(),
            record.meta().worktree_id().into(),
            CommandSummary::argv(2).unwrap(),
            vec!["agent:codex".into()],
            preference,
            QueueEntryKind::TaskTurn,
            run,
            row_owner,
            number as u64,
        )
        .unwrap();
        self.store.enqueue(entry).unwrap();
        if parked {
            self.store.park_row(turn(number)).unwrap();
        }
        self.store.queue_entry(turn(number)).unwrap().unwrap()
    }

    fn forget_context(&self, number: u128) {
        std::fs::remove_file(
            self.directory
                .path()
                .join("state/turns")
                .join(task(number).to_string())
                .join("project.json"),
        )
        .unwrap();
    }

    fn observations(
        &self,
        mini1: CandidateSlot,
        mini2: CandidateSlot,
    ) -> Vec<CandidateObservation> {
        [("mini-1", mini1), ("mini-2", mini2)]
            .into_iter()
            .map(|(name, slot)| {
                let observation = AdmissionObservation::new(
                    name.into(),
                    true,
                    slot,
                    vec!["agent:codex".into()],
                    Some(16 << 30),
                    64 << 30,
                    100,
                )
                .unwrap();
                self.store
                    .admission_observation(name, 100, || Ok(observation))
                    .unwrap();
                CandidateObservation::new(
                    name.into(),
                    true,
                    slot,
                    vec!["agent:codex".into()],
                    Some(16 << 30),
                    64 << 30,
                )
                .unwrap()
            })
            .collect()
    }

    fn claim(
        &self,
        number: u128,
        owner: ProcessIdentity,
        observations: &[CandidateObservation],
    ) -> Result<Option<(TaskId, mac_worker::job::QueueClaim)>, WorkerError> {
        self.store.claim_parked_for_waiting_runner(
            task(number),
            turn(number),
            owner,
            observations,
            101,
        )
    }
}

#[test]
fn waiting_runner_claims_idle_peer_without_changing_queue_identity_or_prompt() {
    let fixture = Fixture::new();
    let donor = fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
    let recipient = fixture.enqueue(2, WorkerPreference::Automatic, true, owner(2), None);
    let observations = fixture.observations(CandidateSlot::Busy, CandidateSlot::Idle);
    let (selected_task, claim) = fixture
        .claim(1, owner(1), &observations)
        .unwrap()
        .expect("idle mini-2 must serve the parked task despite a pinned live runner");
    assert_eq!(selected_task, task(2));
    assert_eq!(claim.entry().job_id(), recipient.job_id());
    assert_eq!(claim.entry().queue_id(), recipient.queue_id());
    assert!(
        matches!(claim.entry().state(), QueueState::Dispatching { dispatch_owner, selected_worker, .. } if *dispatch_owner == owner(1) && selected_worker == "mini-2")
    );
    let parked = fixture.store.queue_entry(turn(1)).unwrap().unwrap();
    assert_eq!(parked.queue_id(), donor.queue_id());
    assert_eq!(parked.preference(), donor.preference());
    assert!(matches!(parked.state(), QueueState::Parked));
    assert_eq!(
        fixture.store.read_turn_prompt(task(1), turn(1)).unwrap(),
        "private prompt"
    );
    assert!(fixture.store.load_task(task(1)).unwrap().runner().is_none());
    assert_eq!(
        fixture
            .store
            .load_task(task(2))
            .unwrap()
            .runner()
            .unwrap()
            .process_identity(),
        owner(1)
    );
}

#[test]
fn runnable_parked_predecessor_keeps_fifo_after_yield() {
    let fixture = Fixture::new();
    fixture.enqueue(1, pin("mini-1"), true, owner(1), None);
    fixture.enqueue(2, pin("mini-1"), false, owner(2), None);
    let observations = fixture.observations(CandidateSlot::Idle, CandidateSlot::Idle);
    assert!(
        fixture
            .store
            .claim_next(owner(2), &["mini-1".into()], 101)
            .unwrap()
            .is_none(),
        "a displaced older task must retain priority"
    );
    assert_eq!(
        fixture
            .claim(2, owner(2), &observations)
            .unwrap()
            .unwrap()
            .0,
        task(1)
    );
}

#[test]
fn runnable_donor_retains_its_place_and_cannot_be_stolen_from_another_owner() {
    let fixture = Fixture::new();
    fixture.enqueue(1, WorkerPreference::Automatic, false, owner(1), None);
    fixture.enqueue(2, WorkerPreference::Automatic, true, owner(2), None);
    let observations = fixture.observations(CandidateSlot::Idle, CandidateSlot::Idle);
    assert!(fixture.claim(1, owner(9), &observations).is_err());
    assert!(fixture.claim(1, owner(1), &observations).unwrap().is_none());
    assert!(matches!(
        fixture.store.queue_entry(turn(1)).unwrap().unwrap().state(),
        QueueState::Waiting { .. }
    ));
}

#[test]
fn incomplete_or_cancelled_head_does_not_hide_an_executable_parked_task() {
    for incomplete in [true, false] {
        let fixture = Fixture::new();
        fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
        fixture.enqueue(2, WorkerPreference::Automatic, true, owner(2), None);
        fixture.enqueue(3, WorkerPreference::Automatic, true, owner(3), None);
        if incomplete {
            let record = fixture
                .store
                .load_task(task(2))
                .unwrap()
                .with_submission_intent_turn_id(turn(2))
                .unwrap();
            fixture.store.update_task(record).unwrap();
        } else {
            fixture.store.request_queue_cancel(turn(2), 100).unwrap();
        }
        let observations = fixture.observations(CandidateSlot::Busy, CandidateSlot::Idle);
        assert_eq!(
            fixture
                .claim(1, owner(1), &observations)
                .unwrap()
                .unwrap()
                .0,
            task(3)
        );
        if incomplete {
            assert!(matches!(
                fixture.store.queue_entry(turn(2)).unwrap().unwrap().state(),
                QueueState::Parked
            ));
        }
    }
}

#[test]
fn reserved_worker_and_run_limit_cannot_be_bypassed() {
    for limited_run in [true, false] {
        let fixture = Fixture::new();
        let run = QueueRunReference::new(
            mac_worker::job::RunId::new(task(99).to_string()).unwrap(),
            1,
        )
        .unwrap();
        fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
        fixture.enqueue(
            2,
            pin("mini-2"),
            false,
            owner(2),
            limited_run.then(|| run.clone()),
        );
        let observations = fixture.observations(CandidateSlot::Busy, CandidateSlot::Idle);
        fixture
            .store
            .claim_next(owner(2), &["mini-2".into()], 100)
            .unwrap()
            .unwrap();
        fixture.enqueue(
            3,
            WorkerPreference::Automatic,
            true,
            owner(3),
            limited_run.then(|| run.clone()),
        );
        let observations = if limited_run {
            vec![
                observations[0].clone(),
                CandidateObservation::new(
                    "mini-3".into(),
                    true,
                    CandidateSlot::Idle,
                    vec!["agent:codex".into()],
                    Some(16 << 30),
                    64 << 30,
                )
                .unwrap(),
            ]
        } else {
            observations
        };
        assert!(fixture.claim(1, owner(1), &observations).unwrap().is_none());
    }
}

#[test]
fn legacy_other_worktree_requires_saved_context() {
    let fixture = Fixture::new();
    fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
    fixture.enqueue_in_worktree(2, WorkerPreference::Automatic, true, owner(2), None, 'd');
    fixture.forget_context(2);
    let observations = fixture.observations(CandidateSlot::Busy, CandidateSlot::Idle);
    assert!(fixture.claim(1, owner(1), &observations).unwrap().is_none());
    let record = fixture.store.load_task(task(2)).unwrap();
    fixture
        .store
        .write_task_project_path(&record, fixture.directory.path())
        .unwrap();
    assert_eq!(
        fixture
            .claim(1, owner(1), &observations)
            .unwrap()
            .unwrap()
            .0,
        task(2)
    );
}

#[test]
fn contextless_legacy_donor_cannot_permanently_bind_its_unverified_cwd() {
    let fixture = Fixture::new();
    fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
    fixture.enqueue(2, pin("mini-2"), true, owner(2), None);
    fixture.forget_context(1);
    fixture.forget_context(2);
    let observations = fixture.observations(CandidateSlot::Busy, CandidateSlot::Idle);
    assert!(fixture.claim(1, owner(1), &observations).unwrap().is_none());
    assert!(
        fixture
            .store
            .task_project_path(&fixture.store.load_task(task(2)).unwrap())
            .unwrap()
            .is_none()
    );
}

#[test]
fn legacy_other_worktree_head_cannot_block_a_runner_that_cannot_execute_it() {
    let fixture = Fixture::new();
    fixture.enqueue_in_worktree(1, pin("mini-2"), true, owner(1), None, 'd');
    fixture.forget_context(1);
    fixture.enqueue(2, pin("mini-2"), false, owner(2), None);
    fixture.observations(CandidateSlot::Busy, CandidateSlot::Idle);
    assert!(
        fixture
            .store
            .claim_task_turn(owner(2), turn(2), &["mini-2".into()], 101)
            .unwrap()
            .is_some(),
        "an unexecutable legacy head must not prevent the owned task from using mini-2"
    );
}

#[test]
fn skipped_legacy_head_does_not_block_a_later_recipient_with_saved_context() {
    let fixture = Fixture::new();
    fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
    fixture.enqueue_in_worktree(2, pin("mini-2"), true, owner(2), None, 'd');
    fixture.forget_context(2);
    fixture.enqueue_in_worktree(3, pin("mini-2"), true, owner(3), None, 'd');
    let later = fixture.store.load_task(task(3)).unwrap();
    fixture
        .store
        .write_task_project_path(&later, fixture.directory.path())
        .unwrap();
    let observations = fixture.observations(CandidateSlot::Busy, CandidateSlot::Idle);
    assert_eq!(
        fixture
            .claim(1, owner(1), &observations)
            .unwrap()
            .unwrap()
            .0,
        task(3)
    );
}

#[test]
fn concurrent_waiters_cannot_claim_the_same_recipient_or_worker() {
    let fixture = Fixture::new();
    fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
    fixture.enqueue(2, pin("mini-1"), false, owner(2), None);
    fixture.enqueue(3, WorkerPreference::Automatic, true, owner(3), None);
    let observations = fixture.observations(CandidateSlot::Busy, CandidateSlot::Idle);
    let barrier = Arc::new(Barrier::new(2));
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for number in [1, 2] {
            let (fixture, observations, barrier) = (&fixture, &observations, Arc::clone(&barrier));
            handles.push(scope.spawn(move || {
                barrier.wait();
                fixture
                    .claim(number, owner(number as u32), observations)
                    .unwrap()
                    .is_some()
            }));
        }
        assert_eq!(
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .filter(|claimed| *claimed)
                .count(),
            1
        );
    });
}

#[test]
fn interrupted_metadata_publication_keeps_a_single_durable_claim() {
    let fixture = Fixture::new();
    fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
    fixture.enqueue(2, WorkerPreference::Automatic, true, owner(2), None);
    fixture.forget_context(2);
    let observations = fixture.observations(CandidateSlot::Busy, CandidateSlot::Idle);
    fixture
        .store
        .inject_write_failure_once(ClientStateWritePoint::AfterRunnerYieldQueuePublication);
    assert!(fixture.claim(1, owner(1), &observations).is_err());
    let snapshot = fixture.store.queue_snapshot().unwrap();
    assert!(matches!(snapshot.entries()[0].state(), QueueState::Parked));
    assert!(
        matches!(snapshot.entries()[1].state(), QueueState::Dispatching { dispatch_owner, .. } if *dispatch_owner == owner(1))
    );
    assert!(fixture.store.load_task(task(1)).unwrap().runner().is_some());
    assert!(fixture.store.load_task(task(2)).unwrap().runner().is_none());
    assert!(
        fixture
            .store
            .task_project_path(&fixture.store.load_task(task(2)).unwrap())
            .unwrap()
            .is_some(),
        "legacy recipient context must survive a crash immediately after queue publication"
    );
}

#[test]
fn requested_turn_claim_does_not_dispatch_another_row_with_the_same_pid() {
    let fixture = Fixture::new();
    fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
    fixture.enqueue(2, pin("mini-1"), false, owner(1), None);
    fixture.observations(CandidateSlot::Idle, CandidateSlot::Idle);
    assert!(
        fixture
            .store
            .claim_task_turn(owner(1), turn(2), &["mini-1".into()], 101)
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .store
            .queue_snapshot()
            .unwrap()
            .entries()
            .iter()
            .all(|entry| matches!(entry.state(), QueueState::Waiting { .. }))
    );
}

#[test]
fn reassignment_requires_ready_idle_and_capable_workers() {
    let fixture = Fixture::new();
    fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
    fixture.enqueue(2, pin("mini-2"), true, owner(2), None);
    for (ready, slot, capabilities) in [
        (false, CandidateSlot::Idle, vec!["agent:codex".into()]),
        (true, CandidateSlot::Busy, vec!["agent:codex".into()]),
        (true, CandidateSlot::Idle, vec!["agent:claude".into()]),
    ] {
        let observations = vec![
            CandidateObservation::new(
                "mini-2".into(),
                ready,
                slot,
                capabilities,
                Some(16 << 30),
                64 << 30,
            )
            .unwrap(),
        ];
        assert!(fixture.claim(1, owner(1), &observations).unwrap().is_none());
    }
    assert!(matches!(
        fixture.store.queue_entry(turn(1)).unwrap().unwrap().state(),
        QueueState::Waiting { .. }
    ));
    assert!(matches!(
        fixture.store.queue_entry(turn(2)).unwrap().unwrap().state(),
        QueueState::Parked
    ));
}

#[test]
fn source_cancellation_or_dispatch_prevents_a_late_yield() {
    for cancel in [true, false] {
        let fixture = Fixture::new();
        fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
        fixture.enqueue(2, pin("mini-2"), true, owner(2), None);
        let observations = fixture.observations(CandidateSlot::Idle, CandidateSlot::Idle);
        if cancel {
            fixture.store.request_queue_cancel(turn(1), 100).unwrap();
        } else {
            fixture
                .store
                .claim_task_turn(owner(1), turn(1), &["mini-1".into()], 100)
                .unwrap()
                .unwrap();
        }
        assert!(fixture.claim(1, owner(1), &observations).is_err());
        assert!(matches!(
            fixture.store.queue_entry(turn(2)).unwrap().unwrap().state(),
            QueueState::Parked
        ));
        assert!(fixture.store.load_task(task(2)).unwrap().runner().is_none());
    }
}

#[test]
fn failed_recipient_record_sync_retains_both_copies_of_the_same_identity() {
    let fixture = Fixture::new();
    fixture.enqueue(1, pin("mini-1"), false, owner(1), None);
    fixture.enqueue(2, pin("mini-2"), true, owner(2), None);
    let observations = fixture.observations(CandidateSlot::Busy, CandidateSlot::Idle);
    fixture
        .store
        .inject_task_replacement_after_exchange_failure_once();
    assert!(fixture.claim(1, owner(1), &observations).is_err());
    let reopened = ClientStateStore::open_with_owner_inspector(
        &fixture
            .directory
            .path()
            .canonicalize()
            .unwrap()
            .join("state"),
        LiveOwners,
    )
    .unwrap();
    assert!(matches!(
        reopened.queue_entry(turn(1)).unwrap().unwrap().state(),
        QueueState::Parked
    ));
    assert!(
        matches!(reopened.queue_entry(turn(2)).unwrap().unwrap().state(), QueueState::Dispatching { dispatch_owner, .. } if *dispatch_owner == owner(1))
    );
    for number in [1, 2] {
        assert_eq!(
            reopened
                .load_task(task(number))
                .unwrap()
                .runner()
                .unwrap()
                .process_identity(),
            owner(1)
        );
    }
}
