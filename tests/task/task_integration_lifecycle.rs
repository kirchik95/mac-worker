use mac_worker::test_support::integration::*;

pub(crate) mod native_owner {
    use super::*;
    use mac_worker::test_support::{
        core::error::WorkerError,
        host::process::SystemProcessRunner,
        task::{model::LocalTaskRecord, store::TaskStore},
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    pub fn observed(f: &GitIntegrationFixture) -> IntegrationTaskFacts {
        let tasks = TaskStore::new(&f.store, &SystemProcessRunner);
        let ordinary = LocalTaskRecord::new(
            tasks
                .load_meta(&f.record.policy.project_id, f.record.task_id)
                .unwrap(),
            tasks
                .load_status(&f.record.policy.project_id, f.record.task_id)
                .unwrap(),
            Some(1001),
            None,
            Some(f.record.snapshot.source_head.clone()),
            "c".repeat(64),
            Some("fixture-worker".into()),
            true,
            None,
        )
        .unwrap();
        IntegrationTaskFacts::from_record(&ordinary, false)
    }

    pub fn state(f: &GitIntegrationFixture) -> MemoryIntegrationState {
        let state = MemoryIntegrationState::default();
        state
            .publish_policy(f.record.task_id, &f.record.policy)
            .unwrap();
        state
            .replace(f.record.task_id, IntegrationRevision(0), &f.record)
            .unwrap();
        state
    }

    pub fn retention_close(f: &GitIntegrationFixture) {
        use mac_worker::test_support::task::model::TaskStatus;
        let path = f.workspace().parent().unwrap().join("status.json");
        let mut wire: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        wire["state"] = "closed".into();
        let closed: TaskStatus = serde_json::from_value(wire).unwrap();
        std::fs::write(path, serde_json::to_vec(&closed).unwrap()).unwrap();
        std::fs::remove_dir_all(f.workspace()).unwrap();
    }

    pub struct Host<'a> {
        service: HostIntegrationService<'a>,
        lost_fetches: AtomicUsize,
        pub calls: Mutex<Vec<IntegrationStep>>,
    }
    impl<'a> Host<'a> {
        pub fn new(f: &'a GitIntegrationFixture, lost_fetches: usize) -> Self {
            Self {
                service: HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime),
                lost_fetches: AtomicUsize::new(lost_fetches),
                calls: Mutex::new(vec![]),
            }
        }
    }
    impl IntegrationHost for Host<'_> {
        fn execute(
            &self,
            request: &HostIntegrationRequest,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            if let HostIntegrationAction::Step { step, .. } = request.action {
                self.calls.lock().unwrap().push(step);
            }
            let response = self.service.execute(request)?;
            if matches!(
                request.action,
                HostIntegrationAction::Step {
                    step: IntegrationStep::Fetch,
                    ..
                }
            ) && self
                .lost_fetches
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
            {
                return Err(IntegrationCode::IntegrationNetwork.error());
            }
            Ok(response)
        }
    }

    pub fn assert_released(f: &GitIntegrationFixture, state: &MemoryIntegrationState) {
        let record = state.load(f.record.task_id).unwrap().unwrap();
        assert_eq!(record.actor, None);
        let other = IntegrationId::derive(
            mac_worker::test_support::task::model::TaskId::generate(),
            record.snapshot.source_turn_id,
            &record.snapshot.source_head,
            &record.target_key,
        )
        .unwrap();
        let reservation = state
            .reserve(
                &record.target_key,
                other,
                0,
                ProcessIdentity::new(5_001_000, 999).unwrap(),
            )
            .unwrap();
        let reservation = reservation
            .expect("an error must leave the target available to a different integration");
        state.release(&reservation).unwrap();
    }

    pub fn queued(
        coordinator: &IntegrationCoordinator<'_>,
        state: &MemoryIntegrationState,
        task: mac_worker::test_support::task::model::TaskId,
    ) -> mac_worker::test_support::task::model::TurnId {
        for _ in 0..8 {
            coordinator.drive_once(task).unwrap();
            let record = state.load(task).unwrap().unwrap();
            assert!(
                !matches!(
                    record.snapshot.state,
                    IntegrationStatus::Blocked | IntegrationStatus::Integrated
                ),
                "{:?}",
                record.snapshot
            );
            if let Some(auxiliary) = record.auxiliaries.last()
                && auxiliary.queue_position.is_some()
                && !auxiliary.completed
            {
                return auxiliary.turn_id;
            }
        }
        panic!("native host did not request an auxiliary");
    }

    pub fn complete(
        f: &GitIntegrationFixture,
        state: &MemoryIntegrationState,
        turns: &FakeIntegrationTurns,
        observer: &FakeIntegrationObserver,
        turn: mac_worker::test_support::task::model::TurnId,
    ) {
        use mac_worker::test_support::task::model::{
            TaskOutcome, TaskStatus, TurnSummary, TurnTerminal,
        };
        let prepared = state
            .load_prepared(f.record.task_id, turn)
            .unwrap()
            .unwrap();
        // Simulate the completed agent handoff inside this disposable host task.
        // Use the owner's exact payload, not a separately prepared host fixture.
        use std::os::unix::fs::PermissionsExt;
        let task = f.workspace().parent().unwrap();
        let preparation = task.join("integration").join(format!("turn-{turn}.json"));
        std::fs::write(&preparation, encode_prepared_turn(&prepared).unwrap()).unwrap();
        std::fs::set_permissions(&preparation, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut facts = observed(f);
        let mut wire = serde_json::to_value(facts.ordinary.status()).unwrap();
        wire["turns"].as_array_mut().unwrap().push(
            serde_json::to_value(TurnSummary::new(
                prepared.followup.turn_number(),
                turn,
                Some(TurnTerminal::Succeeded),
                Some(TaskOutcome::Done),
                Some(false),
                false,
                Some(1002),
                Some(1003),
            ))
            .unwrap(),
        );
        let status: TaskStatus = serde_json::from_value(wire).unwrap();
        std::fs::write(
            task.join("status.json"),
            serde_json::to_vec(&status).unwrap(),
        )
        .unwrap();
        facts.ordinary = facts.ordinary.with_status(status).unwrap();
        facts.auxiliary_purpose = Some(prepared.purpose);
        observer.insert(facts);
        turns
            .set_observation(IntegrationTurnObservation {
                turn_id: turn,
                queue_position: turns.queue_position(turn),
                accepted: true,
                completed: true,
            })
            .unwrap();
    }
}

#[test]
fn review_native_lost_fetch_and_target_move_accept_the_new_candidate() {
    use native_owner::*;
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let state = state(&f);
    let observer = FakeIntegrationObserver::default();
    observer.insert(observed(&f));
    let turns = FakeIntegrationTurns::default();
    let host = Host::new(&f, 1);
    let coordinator = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
    assert_eq!(
        coordinator.drive_once(f.record.task_id).unwrap().state,
        IntegrationStatus::RetryWait
    );
    let target = f.advance_target();
    f.runtime.advance(std::time::Duration::from_secs(2));
    let snapshot = coordinator.drive_once(f.record.task_id).unwrap();
    assert_eq!(snapshot.attempts, 2);
    assert_eq!(snapshot.state, IntegrationStatus::CommitReady);
    let record = state.load(f.record.task_id).unwrap().unwrap();
    assert_eq!(record.candidates.len(), 1);
    assert_eq!(record.candidates[0].target_head, target);
    assert_eq!(record.candidates[0].id.attempt, 2);
    assert_released(&f, &state);
    let completed = IntegrationRunner::new(coordinator)
        .run(f.record.task_id)
        .unwrap();
    assert_eq!(completed.state, IntegrationStatus::Integrated);
    assert_eq!(turns.imports(f.record.task_id).len(), 1);
}

#[test]
fn review_native_two_lost_candidates_spend_the_three_candidate_cap() {
    use native_owner::*;
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let state = state(&f);
    let observer = FakeIntegrationObserver::default();
    observer.insert(observed(&f));
    let turns = FakeIntegrationTurns::default();
    let host = Host::new(&f, 2);
    let coordinator = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
    assert_eq!(
        coordinator.drive_once(f.record.task_id).unwrap().state,
        IntegrationStatus::RetryWait
    );
    f.advance_target_with("target.txt", b"second\n");
    f.runtime.advance(std::time::Duration::from_secs(2));
    assert_eq!(
        coordinator.drive_once(f.record.task_id).unwrap().state,
        IntegrationStatus::RetryWait
    );
    f.advance_target_with("target.txt", b"third\n");
    f.runtime.advance(std::time::Duration::from_secs(10));
    assert_eq!(
        coordinator.drive_once(f.record.task_id).unwrap().attempts,
        3
    );
    let target = f.advance_target_with("target.txt", b"fourth\n");
    let snapshot = coordinator.drive_once(f.record.task_id).unwrap();
    assert_eq!(snapshot.state, IntegrationStatus::Blocked);
    assert_eq!(
        snapshot.blocked_code,
        Some(IntegrationCode::IntegrationTargetMovedExhausted)
    );
    assert_eq!(snapshot.attempts, 3);
    assert_eq!(f.origin_tip(), target);
    assert_released(&f, &state);
}

#[test]
fn review_native_invalid_replies_release_the_target_on_validation_and_application_errors() {
    use mac_worker::test_support::core::error::WorkerError;
    use native_owner::*;
    struct Corrupt<'a> {
        host: Host<'a>,
    }
    impl IntegrationHost for Corrupt<'_> {
        fn execute(
            &self,
            request: &HostIntegrationRequest,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            let mut response = self.host.execute(request)?;
            match &mut response {
                HostIntegrationResponse::CandidateReady { candidate, .. } => {
                    candidate.identity = mac_worker::test_support::task::model::GitIdentity::new(
                        "different identity",
                        candidate.identity.email(),
                    )
                    .unwrap()
                }
                HostIntegrationResponse::NeedTurn { candidate, .. } => {
                    candidate.identity = mac_worker::test_support::task::model::GitIdentity::new(
                        "different identity",
                        candidate.identity.email(),
                    )
                    .unwrap()
                }
                _ => panic!("expected native candidate"),
            }
            Ok(response)
        }
    }
    for conflict in [false, true] {
        let mut f = GitIntegrationFixture::new();
        f.write("payload.txt", b"base\n");
        f.commit_base();
        f.write("payload.txt", b"ours\n");
        f.commit_task();
        if conflict {
            f.advance_target_with("payload.txt", b"theirs\n");
        }
        let state = state(&f);
        let observer = FakeIntegrationObserver::default();
        observer.insert(observed(&f));
        let turns = FakeIntegrationTurns::default();
        let host = Corrupt {
            host: Host::new(&f, 0),
        };
        let coordinator = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
        assert_eq!(
            coordinator
                .drive_once(f.record.task_id)
                .unwrap_err()
                .public_code(),
            "INTEGRATION_STATE_INVALID"
        );
        assert_released(&f, &state);
        assert!(
            state
                .load(f.record.task_id)
                .unwrap()
                .unwrap()
                .candidates
                .is_empty()
        );
    }
}

#[test]
fn review_native_authentication_error_retains_its_catalog_code() {
    use mac_worker::test_support::{
        core::error::WorkerError,
        host::process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    };
    use native_owner::*;
    use std::{
        os::unix::process::ExitStatusExt,
        sync::atomic::{AtomicUsize, Ordering},
    };
    struct RejectPush(AtomicUsize);
    impl ProcessRunner for RejectPush {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.args.iter().any(|arg| arg == "push") {
                self.0.fetch_add(1, Ordering::SeqCst);
                return Ok(ProcessResult {
                    status: std::process::ExitStatus::from_raw(256),
                    stdout: vec![],
                    stderr: b"fatal: Authentication failed for fixture".to_vec(),
                });
            }
            SystemProcessRunner.run(request)
        }
    }
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    f.commit_task();
    let state = state(&f);
    let observer = FakeIntegrationObserver::default();
    observer.insert(observed(&f));
    let turns = FakeIntegrationTurns::default();
    let runner = RejectPush(AtomicUsize::new(0));
    let host = HostIntegrationService::new(&f.store, &runner, &f.runtime);
    let coordinator = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
    assert_eq!(
        coordinator.drive_once(f.record.task_id).unwrap().state,
        IntegrationStatus::CommitReady
    );
    let blocked = coordinator.drive_once(f.record.task_id).unwrap();
    assert_eq!(runner.0.load(Ordering::SeqCst), 1);
    assert_eq!(blocked.state, IntegrationStatus::Blocked);
    assert_eq!(
        blocked.blocked_code,
        Some(IntegrationCode::IntegrationAuthFailed)
    );
    assert_eq!(f.origin_tip(), target);
    assert!(turns.imports(f.record.task_id).is_empty());
    assert_released(&f, &state);
}

#[test]
fn review_native_closed_lost_push_imports_the_retained_merge_without_reopening() {
    use mac_worker::test_support::task::model::TaskState;
    use native_owner::*;
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let merge = f.prepare();
    f.push();
    f.record.snapshot.state = IntegrationStatus::Pushing;
    let candidate = f.record.candidates.last().unwrap();
    f.record.push_intent = Some(IntegrationPushIntent {
        candidate: candidate.id,
        expected_target: candidate.target_head.clone(),
        merge_oid: merge.clone(),
        started_at_millis: 1000,
        uncertain: true,
    });
    assert!(f.record.receipt.is_none()); // The owner's push reply never arrived.
    retention_close(&f);
    let state = state(&f);
    let observer = FakeIntegrationObserver::default();
    observer.insert(observed(&f));
    let turns = FakeIntegrationTurns::default();
    let host = Host::new(&f, 0);
    let coordinator = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
    let settled = coordinator.drive_once(f.record.task_id).unwrap();
    assert_eq!(settled.state, IntegrationStatus::Integrated);
    assert_eq!(settled.merge_oid, Some(merge.clone()));
    assert_eq!(settled.disposition, Some(IntegrationDisposition::Merged));
    assert_eq!(turns.accepted_head(f.record.task_id), Some(merge.clone()));
    assert_eq!(turns.imports(f.record.task_id).len(), 1);
    assert!(turns.closes(f.record.task_id).is_empty());
    assert_eq!(f.origin_tip(), merge);
    assert_eq!(observed(&f).ordinary.status().state(), TaskState::Closed);
    assert!(!f.workspace().exists());
    assert!(
        host.calls
            .lock()
            .unwrap()
            .iter()
            .all(|step| *step == IntegrationStep::Repair)
    );
}

#[test]
fn review_native_closed_unpublished_candidate_blocks_without_recreating_the_workspace() {
    native_closed_missing(false);
}

#[test]
fn review_native_closed_without_a_candidate_blocks_without_recreating_the_workspace() {
    native_closed_missing(true);
}

fn native_closed_missing(no_candidate: bool) {
    use mac_worker::test_support::task::model::TaskState;
    use native_owner::*;
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    f.commit_task();
    if !no_candidate {
        f.prepare();
    }
    retention_close(&f);
    let state = state(&f);
    let observer = FakeIntegrationObserver::default();
    observer.insert(observed(&f));
    let turns = FakeIntegrationTurns::default();
    let host = Host::new(&f, 0);
    let coordinator = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
    let settled = coordinator.drive_once(f.record.task_id).unwrap();
    assert_eq!(settled.state, IntegrationStatus::Blocked);
    assert_eq!(
        settled.blocked_code,
        Some(IntegrationCode::IntegrationWorkspaceMissing)
    );
    assert_eq!(
        state
            .load(f.record.task_id)
            .unwrap()
            .unwrap()
            .candidates
            .len(),
        usize::from(!no_candidate)
    );
    assert!(turns.imports(f.record.task_id).is_empty());
    assert!(turns.observations().is_empty());
    assert_eq!(f.origin_tip(), target);
    assert_eq!(observed(&f).ordinary.status().state(), TaskState::Closed);
    assert!(!f.workspace().exists());
    assert_eq!(*host.calls.lock().unwrap(), vec![IntegrationStep::Repair]);
}

#[test]
fn review_native_closed_reachable_source_without_a_candidate_imports_the_observed_target() {
    use mac_worker::test_support::task::model::TaskState;
    use native_owner::*;
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    let head = f.commit_task();
    f.git(&["push", &f.record.policy.origin, "HEAD:refs/heads/main"]);
    retention_close(&f);
    let state = state(&f);
    let observer = FakeIntegrationObserver::default();
    observer.insert(observed(&f));
    let turns = FakeIntegrationTurns::default();
    let host = Host::new(&f, 0);
    let coordinator = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
    let settled = coordinator.drive_once(f.record.task_id).unwrap();
    assert_eq!(settled.state, IntegrationStatus::Integrated);
    assert_eq!(
        settled.disposition,
        Some(IntegrationDisposition::AlreadyIntegrated)
    );
    assert_eq!(settled.merge_oid, None);
    assert_eq!(settled.observed_target_oid, Some(head.clone()));
    assert_eq!(turns.accepted_head(f.record.task_id), Some(head));
    assert!(
        state
            .load(f.record.task_id)
            .unwrap()
            .unwrap()
            .candidates
            .is_empty()
    );
    assert_eq!(observed(&f).ordinary.status().state(), TaskState::Closed);
    assert!(!f.workspace().exists());
    assert!(
        host.calls
            .lock()
            .unwrap()
            .iter()
            .all(|step| *step == IntegrationStep::Repair)
    );
}

#[test]
fn legacy_closed_blocked_cycle_observes_once_and_then_stays_terminal() {
    use native_owner::*;
    for reachable in [false, true] {
        let mut f = GitIntegrationFixture::new();
        f.commit_base();
        f.commit_task();
        f.prepare();
        if reachable {
            f.push();
        }
        retention_close(&f);
        let mut retained = f.record.clone();
        retained.snapshot.state = IntegrationStatus::Blocked;
        retained.snapshot.blocked_code = Some(IntegrationCode::IntegrationNetwork);
        let state = MemoryIntegrationState::default();
        state
            .publish_policy(retained.task_id, &retained.policy)
            .unwrap();
        state
            .replace(retained.task_id, IntegrationRevision(0), &retained)
            .unwrap();
        let observer = FakeIntegrationObserver::default();
        observer.insert(observed(&f));
        let turns = FakeIntegrationTurns::default();
        let host = Host::new(&f, 0);
        let coordinator = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
        let settled = coordinator.drive_once(retained.task_id).unwrap();
        assert_eq!(
            settled.state,
            if reachable {
                IntegrationStatus::Integrated
            } else {
                IntegrationStatus::Blocked
            }
        );
        if !reachable {
            assert_eq!(
                settled.blocked_code,
                Some(IntegrationCode::IntegrationWorkspaceMissing)
            );
        }
        assert!(!host.calls.lock().unwrap().is_empty());
        let calls = host.calls.lock().unwrap().len();
        coordinator.drive_once(retained.task_id).unwrap();
        assert_eq!(host.calls.lock().unwrap().len(), calls);
        assert!(
            host.calls
                .lock()
                .unwrap()
                .iter()
                .all(|step| *step == IntegrationStep::Repair)
        );
    }
}

#[test]
fn review_native_closed_observation_keeps_network_uncertainty_and_the_repair_retry() {
    use mac_worker::test_support::{
        core::error::{ProcessError, WorkerError},
        host::process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    };
    use native_owner::*;
    struct Offline;
    impl ProcessRunner for Offline {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.args.iter().any(|arg| arg == "ls-remote") {
                return Err(ProcessError::Cancelled.into());
            }
            SystemProcessRunner.run(request)
        }
    }
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.prepare();
    retention_close(&f);
    let state = state(&f);
    let observer = FakeIntegrationObserver::default();
    observer.insert(observed(&f));
    let turns = FakeIntegrationTurns::default();
    let host = HostIntegrationService::new(&f.store, &Offline, &f.runtime);
    let coordinator = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
    let uncertain = coordinator.drive_once(f.record.task_id).unwrap();
    assert_eq!(uncertain.state, IntegrationStatus::RetryWait);
    assert_eq!(
        uncertain.blocked_code,
        Some(IntegrationCode::IntegrationNetwork)
    );
    let record = state.load(f.record.task_id).unwrap().unwrap();
    assert_eq!(record.phase_retries[0].phase, IntegrationPhase::Drive);
    assert_eq!(
        record.phase_retries[0].code,
        IntegrationCode::IntegrationNetwork
    );
    assert!(record.receipt.is_none());
    assert!(turns.imports(f.record.task_id).is_empty());
    assert!(!f.workspace().exists());
}

#[test]
fn fixture_keeps_imported_result_through_partial_close_replay() {
    let f = IntegrationFixture::new();
    let record = sample_record(f.task(), f.source(), "main");
    let receipt = IntegrationReceipt {
        integration_id: record.snapshot.integration_id,
        epoch: 0,
        source_turn_id: f.source(),
        source_head: fixture_head(),
        target_head: record.cycle_base,
        merge_oid: Some("e".repeat(40).parse().unwrap()),
        disposition: IntegrationDisposition::Merged,
        imported: false,
        recorded_at_millis: 1004,
    };
    let imported = f.turns().import_receipt(f.task(), &receipt).unwrap();
    f.restart();
    assert_eq!(f.imports(f.task()), vec![imported.clone()]);
    assert!(f.closes(f.task()).is_empty());
    f.turns().close_integrated(f.task(), &imported).unwrap();
    f.restart();
    f.turns().close_integrated(f.task(), &imported).unwrap();
    assert_eq!(f.closes(f.task()), vec![imported]);
    assert_eq!(f.accepted_head(f.task()), receipt.merge_oid);
    assert!(f.host_calls().is_empty());
}

#[test]
fn final_source_stages_once_after_exact_import_and_retirement() {
    let f = IntegrationFixture::new();
    f.enable(f.task(), "main").unwrap();
    f.coordinator().on_terminal(f.task(), f.source()).unwrap();
    assert!(f.load(f.task()).unwrap().is_none());
    f.complete_source(f.task()).unwrap();
    let staged = f.load(f.task()).unwrap().unwrap();
    assert_eq!(staged.snapshot.state, IntegrationStatus::Pending);
    assert_eq!(staged.snapshot.revision, IntegrationRevision(1));
    assert_eq!(staged.source_revision.len(), 64);
    f.complete_source(f.task()).unwrap();
    f.restart();
    f.complete_source(f.task()).unwrap();
    assert_eq!(f.load(f.task()).unwrap(), Some(staged));
    assert!(f.host_calls().is_empty());
}

#[test]
fn final_source_refuses_incomplete_stopped_and_auxiliary_facts() {
    for reason in [
        "import",
        "head",
        "session",
        "continuation",
        "runner",
        "stop",
        "close",
        "submission",
        "auxiliary",
        "closed",
        "cancelled",
        "other_turn",
    ] {
        let f = IntegrationFixture::new();
        f.enable(f.task(), "main").unwrap();
        let mut facts = f.observer().facts(f.task()).unwrap();
        facts.ordinary = sample_ordinary(f.task(), f.source());
        facts.result_imported = true;
        match reason {
            "import" => facts.result_imported = false,
            "head" => {
                facts.ordinary = facts
                    .ordinary
                    .with_fetched_head(Some("f".repeat(40).parse().unwrap()))
                    .unwrap()
            }
            "session" => facts.session_import_complete = false,
            "continuation" => facts.continuation_pending = true,
            "runner" => facts.runner_present = true,
            "stop" => facts.stop_requested = true,
            "close" => facts.close_pending = true,
            "submission" => facts.submission_pending = true,
            "auxiliary" => facts.auxiliary_purpose = Some(IntegrationTurnPurpose::Resolve),
            "closed" | "cancelled" => {
                let mut wire = serde_json::to_value(facts.ordinary.status()).unwrap();
                if reason == "closed" {
                    wire["state"] = "closed".into();
                } else {
                    wire["last_outcome"] = serde_json::json!({"kind":"cancelled"});
                }
                facts.ordinary = facts
                    .ordinary
                    .with_status(serde_json::from_value(wire).unwrap())
                    .unwrap();
            }
            "other_turn" => {
                facts.ordinary = sample_ordinary(
                    f.task(),
                    mac_worker::test_support::task::model::TurnId::generate(),
                )
            }
            _ => unreachable!(),
        }
        f.observer().insert(facts);
        f.coordinator().on_terminal(f.task(), f.source()).unwrap();
        assert!(f.load(f.task()).unwrap().is_none(), "{reason}");
        assert!(f.host_calls().is_empty(), "{reason}");
    }
}

#[test]
fn disabled_terminal_has_no_sidecar_or_host_effect() {
    let f = IntegrationFixture::new();
    f.coordinator().on_terminal(f.task(), f.source()).unwrap();
    assert!(f.load(f.task()).unwrap().is_none());
    assert!(f.host_calls().is_empty());
}

#[test]
fn rooted_state_replays_cas_prepared_binding_and_bounded_due_page() {
    use mac_worker::test_support::{core::paths::PathLayout, task::model::TaskId};
    use std::sync::Arc;
    let f = IntegrationFixture::new();
    let paths = PathLayout {
        state: f.root().join("state"),
        data: f.root().join("data"),
        cache: f.root().join("cache"),
        config: f.root().join("config"),
    };
    let runtime = Arc::new(ManualIntegrationRuntime::default());
    let state = RootedIntegrationState::open(&paths, runtime.clone()).unwrap();
    let mut record = sample_record(f.task(), f.source(), "main");
    record.candidates.push(sample_candidate(&record));
    state.publish_policy(f.task(), &record.policy).unwrap();
    state.publish_policy(f.task(), &record.policy).unwrap();
    assert!(
        state
            .publish_policy(f.task(), &sample_policy("other"))
            .is_err()
    );
    assert!(
        state
            .replace(f.task(), IntegrationRevision(0), &record)
            .unwrap()
    );
    assert!(
        !state
            .replace(f.task(), IntegrationRevision(0), &record)
            .unwrap()
    );
    let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
    state.publish_prepared(f.task(), &prepared).unwrap();
    state.publish_prepared(f.task(), &prepared).unwrap();
    drop(state);
    let state = RootedIntegrationState::open(&paths, runtime).unwrap();
    assert_eq!(state.load(f.task()).unwrap(), Some(record));
    assert_eq!(
        state
            .load_prepared(f.task(), prepared.followup.turn_id())
            .unwrap(),
        Some(prepared)
    );
    for n in 4..44 {
        let task = TaskId::new(uuid::Uuid::from_u128(n));
        let mut record = sample_record(task, f.source(), "main");
        record.run_position = n as u64;
        state.publish_policy(task, &record.policy).unwrap();
        state
            .replace(task, IntegrationRevision(0), &record)
            .unwrap();
    }
    let due = state.due(2000, 100).unwrap();
    assert_eq!(due.len(), 32);
    assert_eq!(due[0], f.task());
    assert_eq!(due[1], TaskId::new(uuid::Uuid::from_u128(4)));
}

#[test]
fn rooted_reservations_require_confirmed_absence_and_cap_four_targets() {
    use mac_worker::test_support::core::paths::PathLayout;
    use std::sync::Arc;
    let f = IntegrationFixture::new();
    let paths = PathLayout {
        state: f.root().join("state"),
        data: f.root().join("data"),
        cache: f.root().join("cache"),
        config: f.root().join("config"),
    };
    let runtime = Arc::new(ManualIntegrationRuntime::default());
    let state = RootedIntegrationState::open(&paths, runtime.clone()).unwrap();
    let record = sample_record(f.task(), f.source(), "main");
    let first = runtime.actor();
    let reserved = state
        .reserve(&record.target_key, record.snapshot.integration_id, 0, first)
        .unwrap()
        .unwrap();
    runtime.set_actor_verdict(first, RunnerLivenessVerdict::Unverifiable);
    runtime.restart();
    runtime.set_actor_verdict(first, RunnerLivenessVerdict::Unverifiable);
    assert!(
        state
            .reserve(
                &record.target_key,
                record.snapshot.integration_id,
                0,
                runtime.actor()
            )
            .unwrap()
            .is_none()
    );
    runtime.set_actor_verdict(first, RunnerLivenessVerdict::Exited);
    let replacement = state
        .reserve(
            &record.target_key,
            record.snapshot.integration_id,
            0,
            runtime.actor(),
        )
        .unwrap()
        .unwrap();
    assert!(state.release(&reserved).is_err());
    for branch in ["one", "two", "three"] {
        let key = TargetKey::new(&record.target_key.origin, branch).unwrap();
        assert!(
            state
                .reserve(&key, record.snapshot.integration_id, 0, runtime.actor())
                .unwrap()
                .is_some()
        );
    }
    let key = TargetKey::new(&record.target_key.origin, "five").unwrap();
    assert!(
        state
            .reserve(&key, record.snapshot.integration_id, 0, runtime.actor())
            .unwrap()
            .is_none()
    );
    state.release(&replacement).unwrap();
    assert!(
        state
            .reserve(&key, record.snapshot.integration_id, 0, runtime.actor())
            .unwrap()
            .is_some()
    );
}

#[test]
fn private_record_rejects_duplicate_candidates_and_auxiliary_ids() {
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    let candidate = sample_candidate(&record);
    record.candidates = vec![candidate.clone(), candidate];
    assert!(encode_record(&record).is_err());
    record.candidates.clear();
    let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
    let auxiliary = prepared.intent().unwrap();
    record.auxiliaries = vec![auxiliary.clone(), auxiliary];
    assert!(encode_record(&record).is_err());
}

#[test]
fn private_record_enforces_candidate_auxiliary_receipt_and_clock_caps() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let receipt = IntegrationReceipt {
        integration_id: record.snapshot.integration_id,
        epoch: 0,
        source_turn_id: fixture_source(),
        source_head: fixture_head(),
        target_head: record.cycle_base.clone(),
        merge_oid: None,
        disposition: IntegrationDisposition::AlreadyIntegrated,
        imported: true,
        recorded_at_millis: 1001,
    };
    for (field, value) in [
        ("remaining_admission_millis", serde_json::json!(600001)),
        ("remaining_backoff_millis", serde_json::json!(30001)),
        ("source_summary", serde_json::json!("s".repeat(1025))),
        (
            "candidates",
            serde_json::json!(vec![sample_candidate(&record); 4]),
        ),
        (
            "auxiliaries",
            serde_json::json!(vec![
                sample_prepared_turn(
                    &record,
                    IntegrationTurnPurpose::Resolve,
                    1,
                    1
                )
                .intent()
                .unwrap();
                6
            ]),
        ),
        ("archived_receipts", serde_json::json!(vec![receipt; 9])),
    ] {
        let mut wire = serde_json::to_value(&record).unwrap();
        wire[field] = value;
        assert!(
            serde_json::from_value::<IntegrationRecord>(wire).is_err(),
            "{field}"
        );
    }
    let mut at = encode_record(&record).unwrap();
    at.resize(MAX_PRIVATE_RECORD_BYTES, b' ');
    assert_eq!(decode_record(&at).unwrap(), record);
    at.push(b' ');
    assert!(decode_record(&at).is_err());
    assert_eq!(MAX_ARCHIVED_RECEIPTS, 8);
    assert_eq!(TRANSPORT_RETRY_DELAYS_MILLIS, [2000, 10000, 30000]);
}

#[test]
fn fixture_retains_full_authoritative_branch_but_bounds_public_display() {
    let branch = format!("{}a", "é".repeat(127));
    let record = sample_record(fixture_task(), fixture_source(), &branch);
    record.validate().unwrap();
    assert_eq!(record.policy.target.as_str(), branch);
    assert!(record.snapshot.target.len() <= MAX_TARGET_DISPLAY_BYTES);
    assert!(record.snapshot.target.ends_with('…'));
}

pub(crate) mod driver_fixture {
    use super::*;
    use mac_worker::test_support::{
        core::error::WorkerError,
        task::model::{ClosePolicy, TaskOutcome, TaskState, TurnId, TurnSummary, TurnTerminal},
    };
    use std::sync::Arc;
    use std::sync::Mutex;
    #[derive(Clone, Copy)]
    pub enum Mode {
        Clean,
        Resolve,
        Verify,
        Offline,
        RepairOffline,
        Missing,
        Reachable,
    }
    pub struct Host {
        pub mode: Mutex<Mode>,
        pub calls: Mutex<Vec<HostIntegrationRequest>>,
        published: Mutex<Option<IntegrationReceipt>>,
        runtime: Arc<ManualIntegrationRuntime>,
        pub publications: std::sync::atomic::AtomicUsize,
    }
    impl IntegrationHost for Host {
        fn execute(
            &self,
            request: &HostIntegrationRequest,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            request.validate()?;
            self.calls.lock().unwrap().push(request.clone());
            let identity = IntegrationResponseIdentity::for_request(request);
            let mode = *self.mode.lock().unwrap();
            if matches!(mode, Mode::Offline) {
                return Err(IntegrationCode::IntegrationWorkerOffline.error());
            }
            if let Some(receipt) = self.published.lock().unwrap().clone()
                && matches!(
                    request.action,
                    HostIntegrationAction::Revoke { .. }
                        | HostIntegrationAction::Step {
                            step: IntegrationStep::Fetch | IntegrationStep::Repair,
                            ..
                        }
                )
            {
                if matches!(mode, Mode::RepairOffline)
                    && matches!(
                        request.action,
                        HostIntegrationAction::Step {
                            step: IntegrationStep::Repair,
                            ..
                        }
                    )
                {
                    return Err(IntegrationCode::IntegrationWorkerOffline.error());
                }
                return Ok(HostIntegrationResponse::Integrated { identity, receipt });
            }
            if matches!(
                request.action,
                HostIntegrationAction::Revoke { .. } | HostIntegrationAction::Read
            ) {
                return Ok(HostIntegrationResponse::Revoked { identity });
            }
            let HostIntegrationAction::Step { step, record } = &request.action else {
                panic!("unexpected arm");
            };
            if matches!(mode, Mode::RepairOffline) && *step == IntegrationStep::Repair {
                return Err(IntegrationCode::IntegrationWorkerOffline.error());
            }
            if matches!(mode, Mode::Missing) {
                return Ok(HostIntegrationResponse::TargetMoved {
                    identity,
                    observed_target: record.cycle_base.clone(),
                });
            }
            if *step == IntegrationStep::Push {
                self.runtime.reach(IntegrationHook::BeforeAdvertisement);
                self.runtime.reach(IntegrationHook::AfterAdvertisement);
            }
            let mut candidate = record
                .candidates
                .last()
                .filter(|c| c.id.attempt == record.snapshot.attempts)
                .cloned()
                .unwrap_or_else(|| {
                    let mut c = sample_candidate(record);
                    c.id.attempt = record.snapshot.attempts.max(1);
                    c.timestamp_millis = record.snapshot.updated_at_millis;
                    c.merge_oid = None;
                    if matches!(mode, Mode::Resolve) {
                        c.tree_oid = None;
                        c.conflict_paths = vec!["conflict.txt".into()];
                    }
                    c
                });
            if matches!(step, IntegrationStep::Push | IntegrationStep::Repair)
                || matches!(mode, Mode::Reachable)
            {
                let receipt = IntegrationReceipt {
                    integration_id: record.snapshot.integration_id,
                    epoch: record.snapshot.epoch,
                    source_turn_id: record.snapshot.source_turn_id,
                    source_head: record.snapshot.source_head.clone(),
                    target_head: candidate.target_head,
                    merge_oid: candidate.merge_oid.clone(),
                    disposition: if candidate.merge_oid.is_some() {
                        IntegrationDisposition::Merged
                    } else {
                        IntegrationDisposition::AlreadyIntegrated
                    },
                    imported: false,
                    recorded_at_millis: 1000,
                };
                if *step == IntegrationStep::Push {
                    *self.published.lock().unwrap() = Some(receipt.clone());
                    self.publications
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                return Ok(HostIntegrationResponse::Integrated { identity, receipt });
            }
            if matches!(mode, Mode::Resolve | Mode::Verify)
                && matches!(step, IntegrationStep::Fetch | IntegrationStep::Prepare)
                && !record
                    .auxiliaries
                    .iter()
                    .any(|a| a.completed && a.attempt == candidate.id.attempt)
            {
                return Ok(HostIntegrationResponse::NeedTurn {
                    identity,
                    candidate: Box::new(candidate),
                    purpose: if matches!(mode, Mode::Resolve) {
                        IntegrationTurnPurpose::Resolve
                    } else {
                        IntegrationTurnPurpose::Verify
                    },
                });
            }
            if matches!(step, IntegrationStep::AcceptTurn) {
                candidate.tree_oid = Some("c".repeat(40).parse().unwrap());
            }
            if matches!(step, IntegrationStep::Build) {
                candidate.merge_oid = Some("e".repeat(40).parse().unwrap());
            }
            Ok(HostIntegrationResponse::CandidateReady {
                identity,
                candidate: Box::new(candidate),
            })
        }
    }
    pub struct Rig {
        pub state: MemoryIntegrationState,
        pub host: Host,
        pub turns: FakeIntegrationTurns,
        pub runtime: Arc<ManualIntegrationRuntime>,
        pub observer: FakeIntegrationObserver,
    }
    impl Rig {
        pub fn new(mode: Mode, close: ClosePolicy) -> Self {
            let runtime = Arc::new(ManualIntegrationRuntime::default());
            let rig = Self {
                state: MemoryIntegrationState::default(),
                host: Host {
                    mode: Mutex::new(mode),
                    calls: Mutex::new(vec![]),
                    published: Mutex::new(None),
                    runtime: runtime.clone(),
                    publications: std::sync::atomic::AtomicUsize::new(0),
                },
                turns: FakeIntegrationTurns::default(),
                runtime,
                observer: FakeIntegrationObserver::default(),
            };
            let mut policy = sample_policy("main");
            policy.requested_close = close;
            if matches!(mode, Mode::Verify) {
                policy.verify = VerifyPolicy::MovedTarget;
            }
            rig.state.publish_policy(fixture_task(), &policy).unwrap();
            rig.observer.insert(IntegrationTaskFacts {
                ordinary: sample_ordinary(fixture_task(), fixture_source()),
                cycle_base: policy.base_oid.unwrap(),
                result_imported: true,
                session_import_complete: true,
                continuation_pending: false,
                runner_present: false,
                stop_requested: false,
                close_pending: false,
                submission_pending: false,
                auxiliary_purpose: None,
            });
            rig.coordinator()
                .on_terminal(fixture_task(), fixture_source())
                .unwrap();
            rig
        }
        pub fn coordinator(&self) -> IntegrationCoordinator<'_> {
            IntegrationCoordinator::new(
                &self.state,
                &self.host,
                &self.turns,
                self.runtime.as_ref(),
                &self.observer,
            )
        }
        pub fn record(&self) -> IntegrationRecord {
            self.state.load(fixture_task()).unwrap().unwrap()
        }
        pub fn drive(&self) -> IntegrationSnapshot {
            self.coordinator().drive_once(fixture_task()).unwrap()
        }
        pub fn queued(&self) -> TurnId {
            for _ in 0..8 {
                self.drive();
                if let Some(a) = self.record().auxiliaries.last()
                    && self.turns.queue_position(a.turn_id).is_some()
                {
                    return a.turn_id;
                }
            }
            panic!("auxiliary was not queued");
        }
        pub fn complete(
            &self,
            turn: TurnId,
            outcome: TaskOutcome,
            checks: Vec<mac_worker::test_support::agents::agent::ReportedCheck>,
        ) {
            let position = self.turns.queue_position(turn).unwrap();
            self.turns
                .set_observation(IntegrationTurnObservation {
                    turn_id: turn,
                    queue_position: Some(position),
                    accepted: true,
                    completed: true,
                })
                .unwrap();
            let mut facts = self.observer.facts(fixture_task()).unwrap();
            let old = facts.ordinary.status();
            let status = mac_worker::test_support::task::model::TaskStatus::new(
                TaskState::Open,
                Some(outcome.clone()),
                old.worker().map(str::to_owned),
                true,
                old.head_oid().cloned(),
                Some("auxiliary result".into()),
                vec![],
                vec![],
                None,
                old.turns()
                    .iter()
                    .cloned()
                    .chain([TurnSummary::new(
                        2,
                        turn,
                        Some(TurnTerminal::Succeeded),
                        Some(outcome),
                        Some(false),
                        false,
                        Some(1000),
                        Some(self.runtime.now_millis()),
                    )])
                    .collect(),
                self.runtime.now_millis(),
            )
            .unwrap()
            .with_reported_checks(checks)
            .unwrap();
            facts.ordinary = facts.ordinary.with_status(status).unwrap();
            facts.auxiliary_purpose = Some(IntegrationTurnPurpose::Resolve);
            self.observer.insert(facts);
        }
    }
}

#[test]
fn clean_driver_pins_before_push_imports_once_and_replays_requested_close() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::ClosePolicy;
    for close in [ClosePolicy::Done, ClosePolicy::Never] {
        let rig = Rig::new(Mode::Clean, close);
        let mut snapshot = rig.record().snapshot;
        for _ in 0..12 {
            snapshot = rig.drive();
            if snapshot.state == IntegrationStatus::Integrated {
                break;
            }
        }
        assert_eq!(snapshot.state, IntegrationStatus::Integrated);
        assert_eq!(rig.turns.imports(fixture_task()).len(), 1);
        assert_eq!(
            rig.turns.closes(fixture_task()).len(),
            usize::from(close == ClosePolicy::Done)
        );
        for _ in 0..3 {
            assert_eq!(rig.drive(), snapshot);
        }
        let calls = rig.host.calls.lock().unwrap();
        let pushes: Vec<_> = calls
            .iter()
            .filter(|r| {
                matches!(
                    r.action,
                    HostIntegrationAction::Step {
                        step: IntegrationStep::Push,
                        ..
                    }
                )
            })
            .collect();
        assert_eq!(pushes.len(), 1);
        let HostIntegrationAction::Step { record, .. } = &pushes[0].action else {
            unreachable!()
        };
        assert!(record.push_intent.as_ref().unwrap().uncertain);
        assert!(record.candidates.last().unwrap().merge_oid.is_some());
        assert_eq!(snapshot.attempts, 1);
    }
}

#[test]
fn offline_phase_has_three_durable_delays_then_exhausts_without_sleep() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::ClosePolicy;
    let rig = Rig::new(Mode::Offline, ClosePolicy::Never);
    for delay in [2000, 10000, 30000] {
        let snapshot = rig.drive();
        assert_eq!(snapshot.state, IntegrationStatus::RetryWait);
        assert_eq!(
            snapshot.retry_at_millis,
            Some(rig.runtime.now_millis() + delay)
        );
        rig.runtime.advance(std::time::Duration::from_millis(delay));
        rig.runtime.restart();
    }
    let snapshot = rig.drive();
    assert_eq!(snapshot.state, IntegrationStatus::Blocked);
    assert_eq!(
        snapshot.blocked_code,
        Some(IntegrationCode::IntegrationWorkerOffline)
    );
    assert!(snapshot.retry_exhausted);
    assert_eq!(rig.host.calls.lock().unwrap().len(), 4);
}

#[test]
fn uncertain_revoke_stays_nonterminal_and_reuses_the_same_tombstone() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::ClosePolicy;
    let rig = Rig::new(Mode::Offline, ClosePolicy::Never);
    let revision = rig.record().snapshot.revision;
    assert_eq!(
        rig.coordinator()
            .revoke(fixture_task(), revision)
            .unwrap_err()
            .public_code(),
        "INTEGRATION_STOP_UNCONFIRMED"
    );
    let tombstone = rig.record().tombstone.unwrap();
    assert!(!tombstone.acknowledged);
    *rig.host.mode.lock().unwrap() = Mode::Clean;
    let snapshot = rig
        .coordinator()
        .revoke(fixture_task(), rig.record().snapshot.revision)
        .unwrap();
    assert_eq!(snapshot.state, IntegrationStatus::Revoked);
    assert_eq!(
        rig.record().tombstone.unwrap().requested_at_millis,
        tombstone.requested_at_millis
    );
    let count = rig.host.calls.lock().unwrap().len();
    rig.drive();
    assert_eq!(rig.host.calls.lock().unwrap().len(), count);
}

#[test]
fn legacy_closed_only_observes_and_settles_retained_ancestry() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::ClosePolicy;
    for (mode, want) in [
        (Mode::Reachable, IntegrationStatus::Integrated),
        (Mode::Missing, IntegrationStatus::Blocked),
    ] {
        let rig = Rig::new(mode, ClosePolicy::Never);
        let mut facts = rig.observer.facts(fixture_task()).unwrap();
        let mut wire = serde_json::to_value(facts.ordinary.status()).unwrap();
        wire["state"] = "closed".into();
        facts.ordinary = facts
            .ordinary
            .with_status(serde_json::from_value(wire).unwrap())
            .unwrap();
        rig.observer.insert(facts);
        rig.runtime
            .set_drive_gate(Some(IntegrationPauseReason::HelperUnavailable));
        let snapshot = rig.drive();
        assert_eq!(snapshot.state, want);
        if want == IntegrationStatus::Blocked {
            assert_eq!(
                snapshot.blocked_code,
                Some(IntegrationCode::IntegrationWorkspaceMissing)
            );
        }
        assert_eq!(
            rig.observer
                .facts(fixture_task())
                .unwrap()
                .ordinary
                .status()
                .state(),
            mac_worker::test_support::task::model::TaskState::Closed
        );
        assert!(rig.host.calls.lock().unwrap().iter().all(|r| matches!(
            r.action,
            HostIntegrationAction::Step {
                step: IntegrationStep::Repair,
                ..
            }
        )));
    }
}

#[test]
fn detached_runner_drives_ready_phases_and_yields_for_admission_or_backoff() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::ClosePolicy;
    for (mode, want) in [
        (Mode::Clean, IntegrationStatus::Integrated),
        (Mode::Resolve, IntegrationStatus::Resolving),
        (Mode::Offline, IntegrationStatus::RetryWait),
    ] {
        let rig = Rig::new(mode, ClosePolicy::Never);
        let snapshot = IntegrationRunner::new(rig.coordinator())
            .run(fixture_task())
            .unwrap();
        assert_eq!(snapshot.state, want);
        if matches!(mode, Mode::Resolve) {
            let auxiliary = rig.record().auxiliaries.pop().unwrap();
            assert!(auxiliary.queue_position.is_some());
            assert_eq!(rig.turns.enqueue_count(auxiliary.turn_id), 1);
        }
        if matches!(mode, Mode::Offline) {
            assert_eq!(rig.host.calls.lock().unwrap().len(), 1);
        }
    }
}

#[test]
fn published_repair_retries_without_repeating_push_and_parks_backoff_once() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::ClosePolicy;
    use std::time::Duration;
    let rig = Rig::new(Mode::RepairOffline, ClosePolicy::Never);
    for _ in 0..3 {
        rig.drive();
    }
    let receipt = rig.record().receipt.unwrap();
    assert_eq!(rig.drive().state, IntegrationStatus::RetryWait);
    assert_eq!(
        rig.record().snapshot.resume_state,
        Some(IntegrationStatus::Published)
    );
    rig.runtime.advance(Duration::from_millis(500));
    rig.runtime
        .set_drive_gate(Some(IntegrationPauseReason::ControllerDisabled));
    rig.runtime.advance(Duration::from_secs(900));
    rig.runtime.restart();
    assert_eq!(rig.drive().state, IntegrationStatus::Parked);
    assert_eq!(rig.record().remaining_backoff_millis, Some(1500));
    rig.drive();
    assert_eq!(rig.record().remaining_backoff_millis, Some(1500));
    *rig.host.mode.lock().unwrap() = Mode::Clean;
    rig.runtime.set_drive_gate(None);
    assert_eq!(rig.drive().state, IntegrationStatus::RetryWait);
    rig.runtime.advance(Duration::from_millis(1500));
    assert_eq!(
        IntegrationRunner::new(rig.coordinator())
            .run(fixture_task())
            .unwrap()
            .state,
        IntegrationStatus::Integrated
    );
    let mut imported = receipt;
    imported.imported = true;
    assert_eq!(rig.turns.imports(fixture_task()), vec![imported]);
    assert_eq!(
        rig.host
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|r| matches!(
                r.action,
                HostIntegrationAction::Step {
                    step: IntegrationStep::Push,
                    ..
                }
            ))
            .count(),
        1
    );
}

#[test]
fn frozen_failed_source_checks_cannot_be_bypassed_by_redrive() {
    use driver_fixture::*;
    use mac_worker::test_support::{
        agents::agent::{ReportedCheck, ReportedCheckStatus},
        task::model::ClosePolicy,
    };
    let rig = Rig::new(Mode::Clean, ClosePolicy::Never);
    let mut record = rig.record();
    let revision = record.snapshot.revision;
    record.source_checks = vec![ReportedCheck::new(
        "source",
        "test",
        ReportedCheckStatus::Fail,
        "failure",
    )];
    record.snapshot.state = IntegrationStatus::Blocked;
    record.snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
    record.snapshot.revision = revision.next().unwrap();
    rig.state
        .replace(fixture_task(), revision, &record)
        .unwrap();
    rig.coordinator()
        .redrive(fixture_task(), record.snapshot.revision)
        .unwrap();
    let snapshot = rig.drive();
    assert_eq!(snapshot.state, IntegrationStatus::Blocked);
    assert_eq!(
        snapshot.blocked_code,
        Some(IntegrationCode::IntegrationChecksFailed)
    );
    assert!(
        rig.host
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|r| !matches!(r.action, HostIntegrationAction::Step { .. }))
    );
}

fn owner_config() -> mac_worker::test_support::core::config::Config {
    use mac_worker::test_support::core::config::{Config, WorkerEntry};
    Config {
        version: 1,
        notifications: Default::default(),
        controller: Default::default(),
        ssh: Default::default(),
        workers: vec![WorkerEntry {
            name: "fixture-worker".into(),
            ssh: "unused".into(),
            slots: 1,
            capabilities: vec![],
            remote_binary: "worker".into(),
            herdr: false,
        }],
    }
}

#[test]
fn enabled_mutations_revoke_before_reconcile_and_ambiguous_stop_keeps_the_task_open() {
    use crate::support::{recording_runner::RecordingRunner, task_harness::paths};
    use driver_fixture::*;
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        task::{
            client::TaskClient,
            model::{ClosePolicy, TaskState},
            turn_runner::InlineRunnerExecutor,
        },
    };
    for operation in ["close", "discard", "cancel", "blocked_say", "active_say"] {
        let f = IntegrationFixture::new();
        let paths = paths(f.root());
        let store = ClientStateStore::open(&paths.state).unwrap();
        let ordinary = sample_ordinary(fixture_task(), fixture_source());
        store.create_task(ordinary.clone()).unwrap();
        let rig = Rig::new(Mode::Offline, ClosePolicy::Never);
        if operation == "blocked_say" {
            let mut record = rig.record();
            let revision = record.snapshot.revision;
            record.snapshot.state = IntegrationStatus::Blocked;
            record.snapshot.blocked_code = Some(IntegrationCode::IntegrationResolveBlocked);
            record.snapshot.revision = revision.next().unwrap();
            rig.state
                .replace(fixture_task(), revision, &record)
                .unwrap();
        }
        let coordinator = rig.coordinator();
        let runner = RecordingRunner::default();
        let config = owner_config();
        let client = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor)
            .with_integration(&coordinator);
        let error = match operation {
            "close" | "discard" => client.close(fixture_task(), operation == "discard"),
            "cancel" => client.cancel(fixture_task()),
            _ => client.say(
                fixture_task(),
                "fix it".into(),
                false,
                &mut Vec::new(),
                &mut Vec::new(),
            ),
        }
        .unwrap_err();
        assert_eq!(
            error.public_code(),
            if operation == "active_say" {
                "TASK_BUSY"
            } else {
                "INTEGRATION_STOP_UNCONFIRMED"
            },
            "{operation}"
        );
        assert_eq!(store.load_task(fixture_task()).unwrap(), ordinary);
        assert_eq!(
            store.load_task(fixture_task()).unwrap().status().state(),
            TaskState::Open
        );
        assert!(
            runner.requests().is_empty(),
            "ordinary I/O before revoke: {operation}"
        );
        if operation != "active_say" {
            assert!(rig.record().tombstone.is_some_and(|t| !t.acknowledged));
            assert!(
                rig.host
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|r| matches!(r.action, HostIntegrationAction::Revoke { .. }))
            );
        } else {
            assert!(rig.host.calls.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn review_old_integrated_receipt_allows_cancel_of_later_needs_input_work() {
    old_integrated_cancel_of_later_terminal(
        mac_worker::test_support::task::model::TaskOutcome::NeedsInput,
    );
}

#[test]
fn review_old_integrated_receipt_allows_cancel_of_later_failed_work() {
    old_integrated_cancel_of_later_terminal(
        mac_worker::test_support::task::model::TaskOutcome::Failed {
            reason: "EXIT_CODE_3".into(),
        },
    );
}

#[test]
fn review_old_integrated_receipt_allows_cancel_replay_of_later_cancelled_work() {
    old_integrated_cancel_of_later_terminal(
        mac_worker::test_support::task::model::TaskOutcome::Cancelled,
    );
}

fn old_integrated_cancel_of_later_terminal(
    outcome: mac_worker::test_support::task::model::TaskOutcome,
) {
    use crate::support::{recording_runner::RecordingRunner, task_harness::paths};
    use driver_fixture::*;
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        task::{
            client::TaskClient,
            model::{
                ClosePolicy, TaskOutcome, TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal,
            },
            turn_runner::InlineRunnerExecutor,
        },
    };
    let rig = Rig::new(Mode::Clean, ClosePolicy::Never);
    IntegrationRunner::new(rig.coordinator())
        .run(fixture_task())
        .unwrap();
    let prior = rig.record();
    let calls = rig.host.calls.lock().unwrap().clone();
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    let mut history = ordinary.status().turns().to_vec();
    history.push(TurnSummary::new(
        2,
        TurnId::generate(),
        Some(match outcome {
            TaskOutcome::Cancelled => TurnTerminal::Cancelled,
            TaskOutcome::Failed { .. } => TurnTerminal::Failed,
            _ => TurnTerminal::Succeeded,
        }),
        Some(outcome.clone()),
        Some(false),
        false,
        Some(2000),
        Some(2001),
    ));
    let accepted = prior.receipt.as_ref().unwrap().merge_oid.clone().unwrap();
    let status = TaskStatus::new(
        TaskState::Open,
        Some(outcome),
        Some("fixture-worker".into()),
        true,
        Some(accepted.clone()),
        Some("later ordinary work".into()),
        vec![],
        vec![],
        None,
        history,
        2001,
    )
    .unwrap();
    let ordinary = ordinary
        .with_status(status)
        .unwrap()
        .with_fetched_head(Some(accepted))
        .unwrap();
    let f = IntegrationFixture::new();
    let paths = paths(f.root());
    let store = ClientStateStore::open(&paths.state).unwrap();
    store.create_task(ordinary.clone()).unwrap();
    let coordinator = rig.coordinator();
    let runner = RecordingRunner::default();
    let config = owner_config();
    let client = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor)
        .with_integration(&coordinator);
    let report = client.cancel_from_expected(&ordinary).unwrap();
    assert_eq!(report.status(), ordinary.status());
    assert_eq!(store.load_task(fixture_task()).unwrap(), ordinary);
    assert_eq!(rig.record(), prior);
    assert_eq!(*rig.host.calls.lock().unwrap(), calls);
    assert!(runner.requests().is_empty());
}

#[test]
fn review_old_integrated_receipt_allows_a_later_ordinary_say_and_waiting_turn_cancel() {
    use crate::support::{GitRepo, task_harness::paths};
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::drain::set_drained,
        host::process::SystemProcessRunner,
        task::{
            client::TaskClient,
            model::{LocalTaskRecord, TaskOutcome, TaskState},
            project_state::ProjectState,
            turn_runner::InlineRunnerExecutor,
        },
    };
    use std::sync::Arc;
    let f = IntegrationFixture::new();
    let paths = paths(f.root());
    let repo = GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    assert!(
        repo.git(&["remote", "add", "origin", "https://example.test/repo.git"])
            .status
            .success()
    );
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let store = ClientStateStore::open(&paths.state).unwrap();
    let mut wire = serde_json::to_value(sample_ordinary(fixture_task(), fixture_source())).unwrap();
    wire["meta"]["project_id"] = project.context.project_id.clone().into();
    wire["meta"]["worktree_id"] = project.context.worktree_id.clone().into();
    wire["status"]["head_oid"] = "e".repeat(40).into();
    wire["fetched_head"] = "e".repeat(40).into();
    let ordinary: LocalTaskRecord = serde_json::from_value(wire).unwrap();
    store.create_task(ordinary.clone()).unwrap();
    store
        .write_task_project_path(&ordinary, repo.root())
        .unwrap();
    let runtime = Arc::new(ManualIntegrationRuntime::default());
    let state = RootedIntegrationState::open(&paths, runtime.clone()).unwrap();
    let mut prior = sample_record(fixture_task(), fixture_source(), "main");
    prior.policy.project_id = project.context.project_id;
    prior.snapshot.state = IntegrationStatus::Integrated;
    prior.snapshot.merge_oid = Some("e".repeat(40).parse().unwrap());
    prior.snapshot.observed_target_oid = Some(prior.cycle_base.clone());
    prior.snapshot.disposition = Some(IntegrationDisposition::Merged);
    prior.receipt = Some(IntegrationReceipt {
        integration_id: prior.snapshot.integration_id,
        epoch: 0,
        source_turn_id: fixture_source(),
        source_head: prior.snapshot.source_head.clone(),
        target_head: prior.cycle_base.clone(),
        merge_oid: prior.snapshot.merge_oid.clone(),
        disposition: IntegrationDisposition::Merged,
        imported: true,
        recorded_at_millis: 1001,
    });
    state.publish_policy(fixture_task(), &prior.policy).unwrap();
    state
        .replace(fixture_task(), IntegrationRevision(0), &prior)
        .unwrap();
    let host = FakeIntegrationHost::default();
    let turns = FakeIntegrationTurns::default();
    let observer = FakeIntegrationObserver::default();
    let coordinator =
        IntegrationCoordinator::new(&state, &host, &turns, runtime.as_ref(), &observer);
    let config = owner_config();
    let client = TaskClient::new(
        &SystemProcessRunner,
        &config,
        &paths,
        &store,
        &InlineRunnerExecutor,
    )
    .with_integration(&coordinator);
    set_drained(&paths.controller_state_root(), true).unwrap();
    client
        .say(
            fixture_task(),
            "follow up".into(),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap();
    let queued = store.load_task(fixture_task()).unwrap();
    assert_eq!(queued.status().state(), TaskState::Active);
    assert_eq!(queued.status().turns().len(), 2);
    assert_eq!(
        client
            .say(
                fixture_task(),
                "another".into(),
                false,
                &mut Vec::new(),
                &mut Vec::new()
            )
            .unwrap_err()
            .public_code(),
        "TASK_BUSY"
    );
    let turn = queued.status().turns().last().unwrap().turn_id();
    let cancelled = client.cancel_from_expected(&queued).unwrap();
    assert_eq!(cancelled.status().state(), TaskState::Open);
    assert_eq!(
        cancelled.status().last_outcome(),
        Some(&TaskOutcome::Cancelled)
    );
    assert!(store.queue_entry(turn).unwrap().is_none());
    assert_eq!(state.load(fixture_task()).unwrap(), Some(prior));
    assert!(host.calls().is_empty());
}

#[test]
fn review_integrated_source_with_a_completed_auxiliary_still_refuses_cancellation() {
    use crate::support::{recording_runner::RecordingRunner, task_harness::paths};
    use driver_fixture::*;
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        task::{
            client::TaskClient,
            model::{ClosePolicy, TaskOutcome},
            turn_runner::InlineRunnerExecutor,
        },
    };
    let rig = Rig::new(Mode::Resolve, ClosePolicy::Never);
    let turn = rig.queued();
    rig.complete(turn, TaskOutcome::Done, vec![]);
    IntegrationRunner::new(rig.coordinator())
        .run(fixture_task())
        .unwrap();
    let prior = rig.record();
    let ordinary = rig.observer.facts(fixture_task()).unwrap().ordinary;
    assert_eq!(ordinary.status().turns().last().unwrap().turn_id(), turn);
    let f = IntegrationFixture::new();
    let paths = paths(f.root());
    let store = ClientStateStore::open(&paths.state).unwrap();
    store.create_task(ordinary.clone()).unwrap();
    let coordinator = rig.coordinator();
    let runner = RecordingRunner::default();
    let config = owner_config();
    let client = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor)
        .with_integration(&coordinator);
    assert_eq!(
        client
            .cancel_from_expected(&ordinary)
            .unwrap_err()
            .public_code(),
        "INTEGRATION_ALREADY_COMMITTED"
    );
    assert_eq!(rig.record(), prior);
    assert!(runner.requests().is_empty());
}

#[test]
fn integrated_cancel_cannot_relabel_committed_work() {
    use crate::support::{recording_runner::RecordingRunner, task_harness::paths};
    use driver_fixture::*;
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        task::{client::TaskClient, model::ClosePolicy, turn_runner::InlineRunnerExecutor},
    };
    let rig = Rig::new(Mode::Clean, ClosePolicy::Never);
    IntegrationRunner::new(rig.coordinator())
        .run(fixture_task())
        .unwrap();
    let f = IntegrationFixture::new();
    let paths = paths(f.root());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    store.create_task(ordinary.clone()).unwrap();
    let coordinator = rig.coordinator();
    let runner = RecordingRunner::default();
    let config = owner_config();
    let client = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor)
        .with_integration(&coordinator);
    assert_eq!(
        client.cancel(fixture_task()).unwrap_err().public_code(),
        "INTEGRATION_ALREADY_COMMITTED"
    );
    assert_eq!(store.load_task(fixture_task()).unwrap(), ordinary);
    assert!(runner.requests().is_empty());
}

#[test]
fn configured_dag_parent_requires_imported_current_receipt_and_block_is_reversible() {
    use crate::support::{recording_runner::RecordingRunner, task_harness::paths};
    use mac_worker::test_support::{
        client_state::{ClientStateStore, dag::ParentGate},
        task::{client::TaskClient, turn_runner::InlineRunnerExecutor},
    };
    use std::sync::Arc;
    let f = IntegrationFixture::new();
    let paths = paths(f.root());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let state = RootedIntegrationState::open(&paths, Arc::new(ManualIntegrationRuntime::default()))
        .unwrap();
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    state
        .publish_policy(fixture_task(), &record.policy)
        .unwrap();
    state
        .replace(fixture_task(), IntegrationRevision(0), &record)
        .unwrap();
    // The companion is an allowed rooted state entry after reopening the client.
    drop(store);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RecordingRunner::default();
    let config = owner_config();
    let client = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor);
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    for status in [
        IntegrationStatus::Pending,
        IntegrationStatus::Parked,
        IntegrationStatus::Blocked,
    ] {
        let expected = record.snapshot.revision;
        record.snapshot.revision = expected.next().unwrap();
        record.snapshot.state = status;
        record.snapshot.blocked_code = (status == IntegrationStatus::Blocked)
            .then_some(IntegrationCode::IntegrationResolveBlocked);
        if status == IntegrationStatus::Parked {
            record.snapshot.resume_state = Some(IntegrationStatus::Resolving);
            record.snapshot.pause_reason = Some(IntegrationPauseReason::ControllerDrained);
            record.pause = Some(IntegrationPauseEvidence {
                reason: IntegrationPauseReason::ControllerDrained,
                effective_at_millis: 1000,
            });
        } else {
            record.snapshot.resume_state = None;
            record.snapshot.pause_reason = None;
            record.pause = None;
        }
        state.replace(fixture_task(), expected, &record).unwrap();
        assert_eq!(
            client.integration_parent_gate(&ordinary).unwrap(),
            ParentGate::Waiting
        );
    }
    let expected = record.snapshot.revision;
    record.snapshot.revision = expected.next().unwrap();
    record.snapshot.state = IntegrationStatus::Integrated;
    record.snapshot.blocked_code = None;
    let merge: mac_worker::test_support::task::model::BaseOid = "e".repeat(40).parse().unwrap();
    record.snapshot.merge_oid = Some(merge.clone());
    record.snapshot.observed_target_oid = Some(record.cycle_base.clone());
    record.snapshot.disposition = Some(IntegrationDisposition::Merged);
    record.receipt = Some(IntegrationReceipt {
        integration_id: record.snapshot.integration_id,
        epoch: 0,
        source_turn_id: fixture_source(),
        source_head: fixture_head(),
        target_head: record.cycle_base.clone(),
        merge_oid: Some(merge.clone()),
        disposition: IntegrationDisposition::Merged,
        imported: true,
        recorded_at_millis: 1001,
    });
    state.replace(fixture_task(), expected, &record).unwrap();
    assert_eq!(
        client.integration_parent_gate(&ordinary).unwrap(),
        ParentGate::Waiting,
        "stale H is insufficient"
    );
    let mut wire = serde_json::to_value(ordinary.status()).unwrap();
    wire["head_oid"] = serde_json::to_value(&merge).unwrap();
    let accepted = ordinary
        .with_status(serde_json::from_value(wire).unwrap())
        .unwrap()
        .with_fetched_head(Some(merge))
        .unwrap();
    assert_eq!(
        client.integration_parent_gate(&accepted).unwrap(),
        ParentGate::Ready
    );
    let mut wire = serde_json::to_value(ordinary.status()).unwrap();
    wire["state"] = "closed".into();
    let closed_stale = ordinary
        .with_status(serde_json::from_value(wire).unwrap())
        .unwrap();
    assert_ne!(
        client.integration_parent_gate(&closed_stale).unwrap(),
        ParentGate::Ready
    );
    assert!(runner.requests().is_empty());
}

#[test]
fn selected_terminal_recovery_stages_once_after_all_ordinary_fences_are_dropped() {
    use crate::support::{recording_runner::RecordingRunner, task_harness::paths};
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        task::{client::TaskClient, turn_runner::InlineRunnerExecutor},
    };
    let f = IntegrationFixture::new();
    let paths = paths(f.root());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    store.create_task(ordinary.clone()).unwrap();
    f.enable(fixture_task(), "main").unwrap();
    let mut facts = f.observer().facts(fixture_task()).unwrap();
    facts.ordinary = ordinary;
    facts.result_imported = true;
    f.observer().insert(facts);
    let coordinator = f.coordinator();
    let runner = RecordingRunner::default();
    let config = owner_config();
    let client = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor)
        .with_integration(&coordinator);
    client.reconcile_selected(&[fixture_task()]).unwrap();
    let staged = f.load(fixture_task()).unwrap().unwrap();
    client.reconcile_selected(&[fixture_task()]).unwrap();
    assert_eq!(f.load(fixture_task()).unwrap(), Some(staged));
    assert!(runner.requests().is_empty());
    assert!(f.host_calls().is_empty());
}

#[test]
fn integrating_from_child_requires_an_enabled_parent_on_the_same_target_before_submission() {
    use mac_worker::test_support::task::{
        client::TaskClient,
        model::{RunId, TaskId, TurnId},
    };
    use std::collections::BTreeMap;
    let parent_task = fixture_task();
    let child_task = TaskId::generate();
    let frozen: DagFrozenSpec = serde_json::from_value(serde_json::json!({
        "prompt":"work", "agent":"codex", "source":"local", "publish":["fetch"],
        "close_on":"never", "wip":false, "project_path":"/fixture/project",
        "project_id":"a".repeat(64), "worktree_id":"b".repeat(64),
        "timeout_millis":2700000, "max_followups":10, "permissions":"workspace", "requires":[],
        "include_untracked":[], "include_empty_dirs":[], "allow_sensitive":[], "cli_includes":[]
    }))
    .unwrap();
    let node = |task: TaskId, base: DagBase| DagNode {
        batch_id: task.to_string(),
        task_id: task,
        turn_id: TurnId::generate(),
        depends_on: vec![],
        base,
        frozen: frozen.clone(),
        state: DagNodeState::Waiting,
        bound_oid: None,
        bound_turn_id: None,
        pin_ref: None,
        blocked_by: None,
        claimed_by: None,
        claimed_at_millis: None,
    };
    let mut child = sample_policy("main");
    child.base_kind = IntegrationBaseKind::FromTask;
    child.base_oid = None;
    child.base_task = Some(parent_task);
    child.base_preflight = IntegrationBasePreflight::Unknown;
    let parent = node(
        parent_task,
        DagBase::Frozen {
            oid: fixture_head(),
            pin_ref: "refs/worker/dag/fixture".into(),
            wip: false,
        },
    );
    let child_node = node(
        child_task,
        DagBase::From {
            parent: "parent".into(),
        },
    );
    let mut batch = FrozenIntegratingBatch {
        batch: FrozenBatchBody {
            kind: BatchKind::Dag,
            run_id: RunId::generate(),
            max_parallel: None,
            name: None,
            created_at_millis: 1000,
            nodes: BTreeMap::from([("parent".into(), parent), ("child".into(), child_node)]),
            sources: vec![],
        },
        integrations: BTreeMap::from([(parent_task, None), (child_task, Some(child))]),
    };
    assert_eq!(
        TaskClient::validate_integration_batch(&batch)
            .unwrap_err()
            .public_code(),
        "INTEGRATION_DEPENDENCY_NOT_INTEGRATED"
    );
    batch
        .integrations
        .insert(parent_task, Some(sample_policy("other")));
    assert_eq!(
        TaskClient::validate_integration_batch(&batch)
            .unwrap_err()
            .public_code(),
        "INTEGRATION_DEPENDENCY_NOT_INTEGRATED"
    );
    batch
        .integrations
        .insert(parent_task, Some(sample_policy("main")));
    TaskClient::validate_integration_batch(&batch).unwrap();
    batch.integrations.insert(child_task, None);
    batch.integrations.insert(parent_task, None);
    TaskClient::validate_integration_batch(&batch).unwrap();
}

#[test]
fn owner_and_host_boundary_crashes_preserve_ids_counters_and_one_publication() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::{ClosePolicy, TaskOutcome};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let clean = [
        IntegrationHook::BeforePhasePermit,
        IntegrationHook::TargetReserved,
        IntegrationHook::AfterPhaseAdmission,
        IntegrationHook::AfterFetchBeforePin,
        IntegrationHook::AfterTargetPin,
        IntegrationHook::AfterCommitBeforePin,
        IntegrationHook::AfterMergePin,
        IntegrationHook::AfterPushIntent,
        IntegrationHook::BeforePush,
        IntegrationHook::BeforeAdvertisement,
        IntegrationHook::AfterAdvertisement,
        IntegrationHook::AfterPushBeforeReceipt,
        IntegrationHook::AfterReceipt,
        IntegrationHook::AfterOwnerImport,
        IntegrationHook::AfterClose,
        IntegrationHook::AfterStateBeforeEvent,
    ];
    let auxiliary = [
        IntegrationHook::AfterWorkspaceManifest,
        IntegrationHook::DuringWorkspacePrepare,
        IntegrationHook::AfterAuxPrepared,
        IntegrationHook::AfterAuxPrompt,
        IntegrationHook::AfterAuxCas,
        IntegrationHook::AfterAuxEnqueue,
        IntegrationHook::AfterAuxAccepted,
        IntegrationHook::AfterAuxCompleted,
        IntegrationHook::BeforeAuxAdmission,
    ];
    for (mode, hook) in clean
        .into_iter()
        .map(|p| (Mode::Clean, p))
        .chain(auxiliary.into_iter().map(|p| (Mode::Resolve, p)))
    {
        let rig = Rig::new(mode, ClosePolicy::Done);
        let identity = rig.record().snapshot.integration_id;
        rig.runtime.crash_at(hook);
        let run = || {
            for _ in 0..32 {
                if rig.drive().state == IntegrationStatus::Integrated {
                    break;
                }
                if let Some(a) = rig.record().auxiliaries.last()
                    && rig.turns.queue_position(a.turn_id).is_some()
                    && !rig.turns.observe(a.turn_id).unwrap().completed
                {
                    rig.complete(a.turn_id, TaskOutcome::Done, vec![]);
                }
            }
        };
        assert!(
            catch_unwind(AssertUnwindSafe(run)).is_err(),
            "unreached hook: {hook:?}"
        );
        rig.runtime.restart(); // Explicitly confirms the crashed actor is absent.
        run();
        let record = rig.record();
        assert_eq!(
            record.snapshot.state,
            IntegrationStatus::Integrated,
            "{hook:?}"
        );
        assert_eq!(record.snapshot.integration_id, identity);
        assert_eq!(record.snapshot.epoch, 0);
        assert_eq!(record.snapshot.attempts, 1);
        assert_eq!(
            record.followups_spent,
            u32::from(matches!(mode, Mode::Resolve))
        );
        for a in &record.auxiliaries {
            assert_eq!(
                a.turn_id,
                auxiliary_turn_id(identity, 0, a.attempt, a.purpose, a.ordinal).unwrap()
            );
            assert_eq!(rig.turns.enqueue_count(a.turn_id), 1);
        }
        assert_eq!(rig.turns.imports(fixture_task()).len(), 1);
        assert_eq!(rig.turns.closes(fixture_task()).len(), 1);
        assert_eq!(
            rig.host
                .publications
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "{hook:?}"
        );
    }
}

#[test]
fn revoke_after_lost_push_reply_repairs_and_imports_committed_work_without_cancelling_it() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::ClosePolicy;
    let rig = Rig::new(Mode::Clean, ClosePolicy::Never);
    rig.drive();
    rig.drive();
    rig.runtime
        .crash_at(IntegrationHook::AfterPushBeforeReceipt);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rig.drive())).is_err());
    rig.runtime.restart();
    assert_eq!(
        rig.coordinator()
            .revoke(fixture_task(), rig.record().snapshot.revision)
            .unwrap_err()
            .public_code(),
        "INTEGRATION_ALREADY_COMMITTED"
    );
    assert_eq!(rig.record().snapshot.state, IntegrationStatus::Integrated);
    assert_eq!(rig.turns.imports(fixture_task()).len(), 1);
    assert!(rig.host.calls.lock().unwrap().iter().any(|r| matches!(
        r.action,
        HostIntegrationAction::Step {
            step: IntegrationStep::Repair,
            ..
        }
    )));
    let calls = rig.host.calls.lock().unwrap().len();
    rig.drive();
    assert_eq!(rig.host.calls.lock().unwrap().len(), calls);
}

#[test]
fn acknowledged_stop_at_pre_push_boundaries_has_no_later_push() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::ClosePolicy;
    for hook in [
        IntegrationHook::AfterPushIntent,
        IntegrationHook::BeforePush,
        IntegrationHook::BeforeAdvertisement,
        IntegrationHook::AfterAdvertisement,
        IntegrationHook::AfterRevoke,
        IntegrationHook::BeforeRevokeAck,
        IntegrationHook::AfterRevokeAck,
    ] {
        let rig = Rig::new(Mode::Clean, ClosePolicy::Never);
        rig.drive();
        rig.drive();
        rig.runtime.crash_at(hook);
        let revoke_hook = matches!(
            hook,
            IntegrationHook::AfterRevoke
                | IntegrationHook::BeforeRevokeAck
                | IntegrationHook::AfterRevokeAck
        );
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if revoke_hook {
                    rig.coordinator()
                        .revoke(fixture_task(), rig.record().snapshot.revision)
                        .unwrap();
                } else {
                    rig.drive();
                }
            }))
            .is_err()
        );
        rig.runtime.restart();
        rig.coordinator()
            .revoke(fixture_task(), rig.record().snapshot.revision)
            .unwrap();
        for _ in 0..3 {
            assert_eq!(rig.drive().state, IntegrationStatus::Revoked);
        }
        assert!(
            rig.host
                .calls
                .lock()
                .unwrap()
                .iter()
                .skip_while(|r| !matches!(r.action, HostIntegrationAction::Revoke { .. }))
                .all(|r| !matches!(
                    r.action,
                    HostIntegrationAction::Step {
                        step: IntegrationStep::Push,
                        ..
                    }
                )),
            "{hook:?}"
        );
    }
}

#[test]
fn source_staging_crashes_converge_and_policy_publication_is_durable_before_its_hook() {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Arc,
    };
    for hook in [
        IntegrationHook::AfterSourceImport,
        IntegrationHook::AfterRunnerRetirement,
        IntegrationHook::AfterIntent,
    ] {
        let f = IntegrationFixture::new();
        f.enable(f.task(), "main").unwrap();
        f.runtime().crash_at(hook);
        assert!(catch_unwind(AssertUnwindSafe(|| f.complete_source(f.task()))).is_err());
        f.restart();
        f.complete_source(f.task()).unwrap();
        let record = f.load(f.task()).unwrap().unwrap();
        f.complete_source(f.task()).unwrap();
        assert_eq!(f.load(f.task()).unwrap(), Some(record));
    }
    let f = IntegrationFixture::new();
    let paths = crate::support::task_harness::paths(f.root());
    let runtime = Arc::new(ManualIntegrationRuntime::default());
    let state = RootedIntegrationState::open(&paths, runtime.clone()).unwrap();
    runtime.crash_at(IntegrationHook::AfterPolicy);
    let policy = sample_policy("main");
    assert!(catch_unwind(AssertUnwindSafe(|| state.publish_policy(f.task(), &policy))).is_err());
    runtime.restart();
    assert_eq!(state.load_policy(f.task()).unwrap(), Some(policy));
}

#[test]
fn an_independent_ready_task_runs_while_another_target_waits_for_resolution() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::{ClosePolicy, TaskId, TurnId};
    let rig = Rig::new(Mode::Resolve, ClosePolicy::Never);
    let auxiliary = rig.queued();
    assert!(rig.record().actor.is_none());
    let task = TaskId::generate();
    let turn = TurnId::generate();
    let mut facts = rig.observer.facts(fixture_task()).unwrap();
    facts.ordinary = sample_ordinary(task, turn);
    rig.observer.insert(facts);
    rig.state
        .publish_policy(task, &sample_policy("other"))
        .unwrap();
    rig.coordinator().on_terminal(task, turn).unwrap();
    *rig.host.mode.lock().unwrap() = Mode::Clean;
    assert_eq!(
        IntegrationRunner::new(rig.coordinator())
            .run(task)
            .unwrap()
            .state,
        IntegrationStatus::Integrated
    );
    assert_eq!(rig.record().snapshot.state, IntegrationStatus::Resolving);
    assert_eq!(rig.turns.enqueue_count(auxiliary), 1);
}

#[test]
fn phase_gate_schedules_drop_permits_before_io_and_stop_chaining_after_acknowledgement() {
    use driver_fixture::*;
    use mac_worker::test_support::{core::error::WorkerError, task::model::ClosePolicy};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    struct Runtime<'a> {
        base: &'a ManualIntegrationRuntime,
        held: Arc<AtomicUsize>,
        at: IntegrationHook,
        paused: AtomicBool,
    }
    impl IntegrationRuntime for Runtime<'_> {
        fn now_millis(&self) -> u64 {
            self.base.now_millis()
        }
        fn actor(&self) -> ProcessIdentity {
            self.base.actor()
        }
        fn actor_verdict(&self, actor: ProcessIdentity) -> RunnerLivenessVerdict {
            self.base.actor_verdict(actor)
        }
        fn begin_phase(
            &self,
            key: &IntegrationPhaseKey,
        ) -> Result<IntegrationDriveAdmission, WorkerError> {
            match self.base.begin_phase(key)? {
                IntegrationDriveAdmission::Park(p) => Ok(IntegrationDriveAdmission::Park(p)),
                IntegrationDriveAdmission::Permit(_) => {
                    self.held.fetch_add(1, Ordering::SeqCst);
                    Ok(IntegrationDriveAdmission::Permit(
                        IntegrationPhasePermit::with_guard(
                            key.clone(),
                            Box::new(Guard(self.held.clone())),
                        ),
                    ))
                }
            }
        }
        fn reach(&self, point: IntegrationHook) {
            self.base.reach(point);
            if point == self.at && !self.paused.swap(true, Ordering::SeqCst) {
                self.base
                    .set_drive_gate(Some(IntegrationPauseReason::ControllerDrained));
            }
        }
    }
    struct HostPort<'a>(&'a Host, &'a AtomicUsize);
    impl IntegrationHost for HostPort<'_> {
        fn execute(
            &self,
            request: &HostIntegrationRequest,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            assert_eq!(
                self.1.load(Ordering::SeqCst),
                0,
                "host I/O under a phase permit"
            );
            self.0.execute(request)
        }
    }
    struct TurnsPort<'a>(&'a FakeIntegrationTurns, &'a AtomicUsize);
    impl IntegrationTurns for TurnsPort<'_> {
        fn enqueue(
            &self,
            prepared: &PreparedIntegrationTurn,
        ) -> Result<mac_worker::test_support::task::model::TurnId, WorkerError> {
            assert_eq!(
                self.1.load(Ordering::SeqCst),
                0,
                "queue handoff I/O under a phase permit"
            );
            self.0.enqueue(prepared)
        }
        fn observe(
            &self,
            turn: mac_worker::test_support::task::model::TurnId,
        ) -> Result<IntegrationTurnObservation, WorkerError> {
            self.0.observe(turn)
        }
        fn import_receipt(
            &self,
            task: mac_worker::test_support::task::model::TaskId,
            receipt: &IntegrationReceipt,
        ) -> Result<IntegrationReceipt, WorkerError> {
            assert_eq!(self.1.load(Ordering::SeqCst), 0);
            self.0.import_receipt(task, receipt)
        }
        fn close_integrated(
            &self,
            task: mac_worker::test_support::task::model::TaskId,
            receipt: &IntegrationReceipt,
        ) -> Result<(), WorkerError> {
            assert_eq!(self.1.load(Ordering::SeqCst), 0);
            self.0.close_integrated(task, receipt)
        }
    }
    for (mode, at, host_count) in [
        (Mode::Clean, IntegrationHook::BeforePhasePermit, 0),
        (Mode::Clean, IntegrationHook::AfterPhaseAdmission, 1),
        (Mode::Resolve, IntegrationHook::BeforeAuxAdmission, 2),
    ] {
        let rig = Rig::new(mode, ClosePolicy::Never);
        let runtime = Runtime {
            base: rig.runtime.as_ref(),
            held: Arc::new(AtomicUsize::new(0)),
            at,
            paused: AtomicBool::new(false),
        };
        let host = HostPort(&rig.host, &runtime.held);
        let turns = TurnsPort(&rig.turns, &runtime.held);
        let coordinator =
            IntegrationCoordinator::new(&rig.state, &host, &turns, &runtime, &rig.observer);
        assert_eq!(
            IntegrationRunner::new(coordinator)
                .run(fixture_task())
                .unwrap()
                .state,
            IntegrationStatus::Parked
        );
        assert_eq!(rig.host.calls.lock().unwrap().len(), host_count);
        assert!(
            rig.turns.observations().is_empty(),
            "auxiliary admitted after drain acknowledgement at {at:?}"
        );
        assert_eq!(runtime.held.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn a_revoked_blocked_say_waits_reversibly_but_explicit_give_up_blocks_configured_children() {
    use crate::support::{recording_runner::RecordingRunner, task_harness::paths};
    use driver_fixture::*;
    use mac_worker::test_support::{
        client_state::{ClientStateStore, dag::ParentGate},
        task::{client::TaskClient, turn_runner::InlineRunnerExecutor},
    };
    let rig = Rig::new(
        Mode::Clean,
        mac_worker::test_support::task::model::ClosePolicy::Never,
    );
    let f = IntegrationFixture::new();
    let paths = paths(f.root());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let state = RootedIntegrationState::open(&paths, rig.runtime.clone()).unwrap();
    let mut record = rig.record();
    record.snapshot.state = IntegrationStatus::Blocked;
    record.snapshot.blocked_code = Some(IntegrationCode::IntegrationResolveBlocked);
    state
        .publish_policy(fixture_task(), &record.policy)
        .unwrap();
    state
        .replace(fixture_task(), IntegrationRevision(0), &record)
        .unwrap();
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    store.create_task(ordinary.clone()).unwrap();
    let runner = RecordingRunner::default();
    let config = owner_config();
    let coordinator = IntegrationCoordinator::new(
        &state,
        &rig.host,
        &rig.turns,
        rig.runtime.as_ref(),
        &rig.observer,
    );
    let client = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor)
        .with_integration(&coordinator);
    coordinator
        .revoke(fixture_task(), record.snapshot.revision)
        .unwrap();
    assert_eq!(
        client.integration_parent_gate(&ordinary).unwrap(),
        ParentGate::Waiting,
        "revoke during a blocked repair say is not abandonment"
    );
    client.cancel(fixture_task()).unwrap();
    record = state.load(fixture_task()).unwrap().unwrap();
    assert_eq!(
        record.snapshot.blocked_code,
        Some(IntegrationCode::IntegrationDependencyNotIntegrated)
    );
    assert_eq!(
        client.integration_parent_gate(&ordinary).unwrap(),
        ParentGate::IntegrationFailed
    );
    assert!(runner.requests().is_empty());
}

#[test]
fn legacy_closed_recovery_does_not_reclaim_an_unconfirmed_actor_and_backs_off_failed_observation() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::ClosePolicy;
    let rig = Rig::new(Mode::Offline, ClosePolicy::Never);
    let actor = ProcessIdentity::new(5_000_099, 99).unwrap();
    let mut record = rig.record();
    rig.state
        .reserve(&record.target_key, record.snapshot.integration_id, 0, actor)
        .unwrap()
        .unwrap();
    let revision = record.snapshot.revision;
    record.actor = Some(actor);
    record.snapshot.revision = revision.next().unwrap();
    rig.state
        .replace(fixture_task(), revision, &record)
        .unwrap();
    let mut facts = rig.observer.facts(fixture_task()).unwrap();
    let mut wire = serde_json::to_value(facts.ordinary.status()).unwrap();
    wire["state"] = "closed".into();
    facts.ordinary = facts
        .ordinary
        .with_status(serde_json::from_value(wire).unwrap())
        .unwrap();
    rig.observer.insert(facts);
    for verdict in [
        RunnerLivenessVerdict::Unverifiable,
        RunnerLivenessVerdict::Live,
    ] {
        rig.runtime.set_actor_verdict(actor, verdict);
        assert_eq!(rig.drive(), record.snapshot);
        assert!(rig.host.calls.lock().unwrap().is_empty());
    }
    rig.runtime
        .set_actor_verdict(actor, RunnerLivenessVerdict::Exited);
    for delay in [2000, 10000, 30000] {
        assert_eq!(rig.drive().state, IntegrationStatus::RetryWait);
        let calls = rig.host.calls.lock().unwrap().len();
        rig.drive();
        assert_eq!(rig.host.calls.lock().unwrap().len(), calls);
        rig.runtime.advance(std::time::Duration::from_millis(delay));
    }
    assert_eq!(rig.drive().state, IntegrationStatus::Blocked);
    assert_eq!(rig.host.calls.lock().unwrap().len(), 4);
    assert!(rig.host.calls.lock().unwrap().iter().all(|r| matches!(
        r.action,
        HostIntegrationAction::Step {
            step: IntegrationStep::Repair,
            ..
        }
    )));
}

#[test]
fn the_next_ordinary_cycle_uses_the_previously_accepted_merge_as_its_base() {
    use driver_fixture::*;
    use mac_worker::test_support::task::model::{
        ClosePolicy, TaskOutcome, TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal,
    };
    let rig = Rig::new(Mode::Resolve, ClosePolicy::Never);
    let auxiliary = rig.queued();
    rig.complete(auxiliary, TaskOutcome::Done, vec![]);
    IntegrationRunner::new(rig.coordinator())
        .run(fixture_task())
        .unwrap();
    let old = rig.record();
    let mut spent = old.clone();
    let revision = spent.snapshot.revision;
    spent.snapshot.revision = revision.next().unwrap();
    spent.followups_spent = 2; // One materialized auxiliary and one retained reservation.
    rig.state.replace(fixture_task(), revision, &spent).unwrap();
    let accepted = old.receipt.as_ref().unwrap().merge_oid.clone().unwrap();
    let mut facts = rig.observer.facts(fixture_task()).unwrap();
    let prior = facts.ordinary.status();
    let source = TurnId::generate();
    let head: mac_worker::test_support::task::model::BaseOid = "f".repeat(40).parse().unwrap();
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        prior.worker().map(str::to_owned),
        true,
        Some(head.clone()),
        Some("new source".into()),
        vec![],
        vec![],
        None,
        prior
            .turns()
            .iter()
            .cloned()
            .chain([TurnSummary::new(
                prior.turns().len() as u32 + 1,
                source,
                Some(TurnTerminal::Succeeded),
                Some(TaskOutcome::Done),
                Some(true),
                false,
                Some(1002),
                Some(1003),
            )])
            .collect(),
        1003,
    )
    .unwrap();
    facts.ordinary = facts
        .ordinary
        .with_status(status)
        .unwrap()
        .with_fetched_head(Some(head))
        .unwrap();
    facts.auxiliary_purpose = None;
    rig.observer.insert(facts);
    rig.coordinator()
        .on_terminal(fixture_task(), source)
        .unwrap();
    let next = rig.record();
    assert_ne!(next.snapshot.integration_id, old.snapshot.integration_id);
    assert_eq!(next.snapshot.source_turn_id, source);
    assert_eq!(next.cycle_base, accepted);
    assert_eq!(next.archived_receipts, vec![old.receipt.unwrap()]);
    assert_eq!(next.source_summary, "new source");
    assert_eq!(next.followups_spent, 3);
}

#[test]
fn selected_recovery_uses_durable_run_position_as_a_readiness_tie_breaker() {
    use crate::support::{recording_runner::RecordingRunner, task_harness::paths};
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        task::{
            client::TaskClient,
            model::{LocalTaskRecord, RunId, RunRecord, TaskId},
            turn_runner::InlineRunnerExecutor,
        },
    };
    let f = IntegrationFixture::new();
    let paths = paths(f.root());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let run = RunId::generate();
    let mut wire = serde_json::to_value(sample_ordinary(fixture_task(), fixture_source())).unwrap();
    wire["meta"]["run_id"] = serde_json::to_value(run).unwrap();
    let ordinary: LocalTaskRecord = serde_json::from_value(wire).unwrap();
    store.create_task(ordinary.clone()).unwrap();
    store
        .create_run(
            RunRecord::new(run, None, vec![TaskId::generate(), fixture_task()], 2, 1000).unwrap(),
        )
        .unwrap();
    f.enable(fixture_task(), "main").unwrap();
    let mut facts = f.observer().facts(fixture_task()).unwrap();
    facts.ordinary = ordinary;
    facts.result_imported = true;
    f.observer().insert(facts);
    let coordinator = f.coordinator();
    let runner = RecordingRunner::default();
    let config = owner_config();
    let client = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor)
        .with_integration(&coordinator);
    client.reconcile_selected(&[fixture_task()]).unwrap();
    let staged = f.load(fixture_task()).unwrap().unwrap();
    assert_eq!(staged.run_position, 1);
    assert_eq!(staged.ready_at_millis, f.runtime().now_millis());
    client.reconcile_selected(&[fixture_task()]).unwrap();
    assert_eq!(f.load(fixture_task()).unwrap(), Some(staged));
}

#[test]
fn an_unmaterialized_auxiliary_reservation_spends_the_shared_ordinary_followup_allowance() {
    use crate::support::{recording_runner::RecordingRunner, task_harness::paths};
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        task::{client::TaskClient, model::LocalTaskRecord, turn_runner::InlineRunnerExecutor},
    };
    let f = IntegrationFixture::new();
    let paths = paths(f.root());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let mut wire = serde_json::to_value(sample_ordinary(fixture_task(), fixture_source())).unwrap();
    wire["meta"]["limits"]["max_followups"] = 1.into();
    let ordinary: LocalTaskRecord = serde_json::from_value(wire).unwrap();
    store.create_task(ordinary.clone()).unwrap();
    f.enable(fixture_task(), "main").unwrap();
    let mut facts = f.observer().facts(fixture_task()).unwrap();
    facts.ordinary = ordinary.clone();
    facts.result_imported = true;
    f.observer().insert(facts);
    f.coordinator()
        .on_terminal(fixture_task(), fixture_source())
        .unwrap();
    let mut record = f.load(fixture_task()).unwrap().unwrap();
    let revision = record.snapshot.revision;
    record.snapshot.revision = revision.next().unwrap();
    record.snapshot.state = IntegrationStatus::Blocked;
    record.snapshot.blocked_code = Some(IntegrationCode::IntegrationResolveBlocked);
    record.snapshot.resolve_turns = 1;
    record.followups_spent = 1;
    f.state()
        .replace(fixture_task(), revision, &record)
        .unwrap();
    let coordinator = f.coordinator();
    let runner = RecordingRunner::default();
    let config = owner_config();
    let client = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor)
        .with_integration(&coordinator);
    assert_eq!(
        client
            .say(
                fixture_task(),
                "fix".into(),
                false,
                &mut Vec::new(),
                &mut Vec::new()
            )
            .unwrap_err()
            .public_code(),
        "FOLLOWUP_LIMIT"
    );
    assert_eq!(store.load_task(fixture_task()).unwrap(), ordinary);
    assert!(f.host_calls().is_empty());
    assert!(runner.requests().is_empty());
}

mod rooted_git_concurrency {
    use super::*;
    use mac_worker::test_support::{
        core::{error::WorkerError, paths::PathLayout},
        host::process::SystemProcessRunner,
        task::model::TaskId,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    struct Actor {
        identity: ProcessIdentity,
        clock: ManualIntegrationRuntime,
        liveness: Arc<ManualIntegrationRuntime>,
    }
    impl IntegrationRuntime for Actor {
        fn now_millis(&self) -> u64 {
            self.clock.now_millis()
        }
        fn actor(&self) -> ProcessIdentity {
            self.identity
        }
        fn actor_verdict(&self, actor: ProcessIdentity) -> RunnerLivenessVerdict {
            self.liveness.actor_verdict(actor)
        }
        fn begin_phase(
            &self,
            key: &IntegrationPhaseKey,
        ) -> Result<IntegrationDriveAdmission, WorkerError> {
            self.clock.begin_phase(key)
        }
        fn reach(&self, hook: IntegrationHook) {
            self.clock.reach(hook);
        }
    }
    fn actor(n: u32, liveness: Arc<ManualIntegrationRuntime>) -> Actor {
        let identity = ProcessIdentity::new(6_000_000 + n, 1000).unwrap();
        liveness.set_actor_verdict(identity, RunnerLivenessVerdict::Live);
        Actor {
            identity,
            clock: ManualIntegrationRuntime::default(),
            liveness,
        }
    }
    struct HeldHost<'a> {
        fixture: &'a GitIntegrationFixture,
        ready: mpsc::Sender<TaskId>,
        release: Mutex<mpsc::Receiver<()>>,
        calls: AtomicUsize,
    }
    impl IntegrationHost for HeldHost<'_> {
        fn execute(
            &self,
            request: &HostIntegrationRequest,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.ready.send(request.task_id).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            HostIntegrationService::new(
                &self.fixture.store,
                &SystemProcessRunner,
                &self.fixture.runtime,
            )
            .execute(request)
        }
    }
    struct ReleaseAll(Vec<mpsc::Sender<()>>);
    impl Drop for ReleaseAll {
        fn drop(&mut self) {
            for sender in &self.0 {
                let _ = sender.send(());
            }
        }
    }
    fn paths(root: &std::path::Path) -> PathLayout {
        PathLayout {
            config: root.join("config"),
            state: root.join("owner"),
            cache: root.join("cache"),
            data: root.join("data"),
        }
    }
    fn install(
        state: &RootedIntegrationState,
        fixture: &GitIntegrationFixture,
        observer: &FakeIntegrationObserver,
    ) {
        state
            .publish_policy(fixture.record.task_id, &fixture.record.policy)
            .unwrap();
        state
            .replace(
                fixture.record.task_id,
                IntegrationRevision(0),
                &fixture.record,
            )
            .unwrap();
        observer.insert(super::native_owner::observed(fixture));
        let arm = HostIntegrationRequest {
            protocol_version: 7,
            task_id: fixture.record.task_id,
            integration_id: None,
            epoch: 0,
            revision: IntegrationRevision(0),
            action: HostIntegrationAction::Arm {
                policy: fixture.record.policy.clone(),
            },
        };
        HostIntegrationService::new(&fixture.store, &SystemProcessRunner, &fixture.runtime)
            .execute(&arm)
            .unwrap();
    }
    fn reservations(paths: &PathLayout) -> usize {
        std::fs::read_dir(paths.state.join("integrations/reservations"))
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "json")
            })
            .count()
    }

    #[test]
    fn rooted_owner_real_git_fences_concurrent_cycle_drivers() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp.path().canonicalize().unwrap());
        let liveness = Arc::new(ManualIntegrationRuntime::default());
        let state = RootedIntegrationState::open(&paths, liveness.clone()).unwrap();
        let mut f = GitIntegrationFixture::new();
        let target = f.commit_base();
        let head = f.commit_task();
        let observer = FakeIntegrationObserver::default();
        let turns = FakeIntegrationTurns::default();
        install(&state, &f, &observer);
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let host = HeldHost {
            fixture: &f,
            ready: ready_tx,
            release: Mutex::new(release_rx),
            calls: AtomicUsize::new(0),
        };
        let actors: Vec<_> = (1..=4).map(|n| actor(n, liveness.clone())).collect();
        std::thread::scope(|scope| {
            let _release_on_panic = ReleaseAll(vec![release_tx.clone()]);
            let first = scope.spawn(|| {
                IntegrationCoordinator::new(&state, &host, &turns, &actors[0], &observer)
                    .drive_once(f.record.task_id)
                    .unwrap()
            });
            assert_eq!(ready_rx.recv().unwrap(), f.record.task_id);
            let held = state.load(f.record.task_id).unwrap().unwrap();
            assert_eq!(held.actor, Some(actors[0].identity));
            let others: Vec<_> = actors[1..]
                .iter()
                .map(|actor| {
                    scope.spawn(|| {
                        IntegrationCoordinator::new(&state, &host, &turns, actor, &observer)
                            .drive_once(f.record.task_id)
                            .unwrap()
                    })
                })
                .collect();
            for other in others {
                assert_eq!(other.join().unwrap(), held.snapshot);
            }
            assert_eq!(host.calls.load(Ordering::SeqCst), 1);
            assert_eq!(reservations(&paths), 1);
            assert_eq!(f.origin_tip(), target);
            release_tx.send(()).unwrap();
            assert_eq!(first.join().unwrap().state, IntegrationStatus::CommitReady);
        });
        assert_eq!(reservations(&paths), 0);
        let ready = state.load(f.record.task_id).unwrap().unwrap();
        assert_eq!(
            f.parents(ready.candidates[0].merge_oid.as_ref().unwrap()),
            vec![target, head]
        );
        let host = HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime);
        let done = IntegrationRunner::new(IntegrationCoordinator::new(
            &state, &host, &turns, &actors[3], &observer,
        ))
        .run(f.record.task_id)
        .unwrap();
        assert_eq!(done.state, IntegrationStatus::Integrated);
        assert_eq!(f.origin_tip(), done.merge_oid.unwrap());
        assert_eq!(host_record_count(&state, f.record.task_id), 1);
    }
    #[test]
    fn distinct_rooted_tasks_share_one_canonical_target_and_preserve_both_git_results() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let paths = paths(&root);
        let live = Arc::new(ManualIntegrationRuntime::default());
        let state = RootedIntegrationState::open(&paths, live.clone()).unwrap();
        let mut first = GitIntegrationFixture::at_for_task(
            root.join("first"),
            TaskId::new(uuid::Uuid::from_u128(201)),
            None,
        );
        let target = first.commit_base();
        first.commit_task();
        let mut second = GitIntegrationFixture::at_for_task(
            root.join("second"),
            TaskId::new(uuid::Uuid::from_u128(202)),
            Some(first.origin()),
        );
        second.git(&["fetch", &second.record.policy.origin, target.as_str()]);
        second.git(&["reset", "--hard", target.as_str()]);
        second.record.policy.base_oid = Some(target.clone());
        second.record.cycle_base = target.clone();
        second.write("second.txt", b"second\n");
        let second_head = second.commit("second");
        second.freeze_source(&second_head);
        let observer = FakeIntegrationObserver::default();
        let turns = FakeIntegrationTurns::default();
        install(&state, &first, &observer);
        install(&state, &second, &observer);
        assert_eq!(first.record.target_key, second.record.target_key);
        let actor_a = actor(30, live.clone());
        let actor_b = actor(31, live);
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let held = HeldHost {
            fixture: &first,
            ready: ready_tx,
            release: Mutex::new(release_rx),
            calls: AtomicUsize::new(0),
        };
        let host_b =
            HostIntegrationService::new(&second.store, &SystemProcessRunner, &second.runtime);
        std::thread::scope(|scope| {
            let _release_on_panic = ReleaseAll(vec![release_tx.clone()]);
            let running = scope.spawn(|| {
                IntegrationCoordinator::new(&state, &held, &turns, &actor_a, &observer)
                    .drive_once(first.record.task_id)
                    .unwrap()
            });
            ready_rx.recv().unwrap();
            let before = state.load(second.record.task_id).unwrap().unwrap();
            assert_eq!(
                IntegrationCoordinator::new(&state, &host_b, &turns, &actor_b, &observer)
                    .drive_once(second.record.task_id)
                    .unwrap(),
                before.snapshot
            );
            assert_eq!(state.load(second.record.task_id).unwrap().unwrap(), before);
            assert_eq!(reservations(&paths), 1);
            release_tx.send(()).unwrap();
            running.join().unwrap();
        });
        let host_a =
            HostIntegrationService::new(&first.store, &SystemProcessRunner, &first.runtime);
        let a = IntegrationRunner::new(IntegrationCoordinator::new(
            &state, &host_a, &turns, &actor_a, &observer,
        ))
        .run(first.record.task_id)
        .unwrap();
        let b = IntegrationRunner::new(IntegrationCoordinator::new(
            &state, &host_b, &turns, &actor_b, &observer,
        ))
        .run(second.record.task_id)
        .unwrap();
        assert_eq!(a.state, IntegrationStatus::Integrated);
        assert_eq!(b.state, IntegrationStatus::Integrated);
        let merge = b.merge_oid.unwrap();
        assert_eq!(
            second.parents(&merge),
            vec![a.merge_oid.unwrap(), second_head]
        );
        assert_eq!(second.git(&["show", &format!("{merge}:task.txt")]), "task");
        assert_eq!(
            second.git(&["show", &format!("{merge}:second.txt")]),
            "second"
        );
        assert_eq!(reservations(&paths), 0);
    }

    fn host_record_count(state: &RootedIntegrationState, task: TaskId) -> usize {
        state.load(task).unwrap().unwrap().candidates.len()
    }

    #[test]
    fn four_real_git_targets_hold_the_shared_cap_and_resolution_releases_it_for_ready_work() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let paths = paths(&root);
        let liveness = Arc::new(ManualIntegrationRuntime::default());
        let state = RootedIntegrationState::open(&paths, liveness.clone()).unwrap();
        let observer = FakeIntegrationObserver::default();
        let turns = FakeIntegrationTurns::default();
        let mut fixtures = vec![];
        for n in 0..5 {
            let task = TaskId::new(uuid::Uuid::from_u128(100 + n));
            let mut f =
                GitIntegrationFixture::at_for_task(root.join(format!("git-{n}")), task, None);
            if n == 0 {
                f.write("payload.txt", b"base\n");
            }
            f.commit_base();
            if n == 0 {
                f.write("payload.txt", b"ours\n");
            }
            f.commit_task();
            if n == 0 {
                f.advance_target_with("payload.txt", b"theirs\n");
            }
            install(&state, &f, &observer);
            fixtures.push(f);
        }
        let actors: Vec<_> = (10..15).map(|n| actor(n, liveness.clone())).collect();
        let (ready_tx, ready_rx) = mpsc::channel();
        let mut releases = vec![];
        let mut hosts = vec![];
        for f in &fixtures {
            let (tx, rx) = mpsc::channel();
            releases.push(tx);
            hosts.push(HeldHost {
                fixture: f,
                ready: ready_tx.clone(),
                release: Mutex::new(rx),
                calls: AtomicUsize::new(0),
            });
        }
        let fifth = fixtures[4].record.task_id;
        std::thread::scope(|scope| {
            let _release_on_panic = ReleaseAll(releases.clone());
            let mut drivers = vec![];
            for n in 0..4 {
                let state = &state;
                let host = &hosts[n];
                let turns = &turns;
                let actor = &actors[n];
                let observer = &observer;
                let task = fixtures[n].record.task_id;
                drivers.push(scope.spawn(move || {
                    IntegrationCoordinator::new(state, host, turns, actor, observer)
                        .drive_once(task)
                        .unwrap()
                }));
            }
            let mut held = std::collections::BTreeSet::new();
            for _ in 0..4 {
                held.insert(ready_rx.recv().unwrap());
            }
            assert_eq!(held.len(), 4);
            assert_eq!(reservations(&paths), 4);
            let before = state.load(fifth).unwrap().unwrap();
            assert_eq!(
                IntegrationCoordinator::new(&state, &hosts[4], &turns, &actors[4], &observer)
                    .drive_once(fifth)
                    .unwrap(),
                before.snapshot
            );
            assert_eq!(state.load(fifth).unwrap().unwrap(), before);
            assert_eq!(hosts[4].calls.load(Ordering::SeqCst), 0);
            releases[0].send(()).unwrap();
            assert_eq!(
                drivers.remove(0).join().unwrap().state,
                IntegrationStatus::Resolving
            );
            assert_eq!(reservations(&paths), 3);
            let resumed = scope.spawn(|| {
                IntegrationCoordinator::new(&state, &hosts[4], &turns, &actors[4], &observer)
                    .drive_once(fifth)
                    .unwrap()
            });
            assert_eq!(ready_rx.recv().unwrap(), fifth);
            assert_eq!(reservations(&paths), 4);
            for release in releases.iter().skip(1) {
                release.send(()).unwrap();
            }
            for driver in drivers {
                assert_eq!(driver.join().unwrap().state, IntegrationStatus::CommitReady);
            }
            assert_eq!(
                resumed.join().unwrap().state,
                IntegrationStatus::CommitReady
            );
        });
        assert_eq!(reservations(&paths), 0);
        let first = fixtures[0].record.task_id;
        let host = HostIntegrationService::new(
            &fixtures[0].store,
            &SystemProcessRunner,
            &fixtures[0].runtime,
        );
        let owner = IntegrationCoordinator::new(&state, &host, &turns, &actors[0], &observer);
        for _ in 0..8 {
            owner.drive_once(first).unwrap();
            if state
                .load(first)
                .unwrap()
                .unwrap()
                .auxiliaries
                .last()
                .is_some_and(|intent| intent.queue_position.is_some())
            {
                break;
            }
        }
        let queued = state.load(first).unwrap().unwrap();
        let intent = queued.auxiliaries.last().unwrap();
        assert!(intent.queue_position.is_some());
        let deadline = queued.admission_deadline_millis.unwrap();
        actors[0]
            .clock
            .advance(std::time::Duration::from_millis(1234));
        actors[0]
            .clock
            .set_drive_gate(Some(IntegrationPauseReason::ControllerDrained));
        owner.drive_once(first).unwrap();
        let parked = state.load(first).unwrap().unwrap();
        let saved = parked.auxiliaries.last().unwrap();
        assert_eq!(saved.turn_id, intent.turn_id);
        assert_eq!(saved.queue_position, intent.queue_position);
        assert_eq!(
            parked.remaining_admission_millis,
            Some(deadline.saturating_sub(actors[0].now_millis()))
        );
        actors[0]
            .clock
            .advance(std::time::Duration::from_millis(900_000));
        actors[0].clock.set_drive_gate(None);
        owner.drive_once(first).unwrap();
        let restored = state.load(first).unwrap().unwrap();
        let next = restored.auxiliaries.last().unwrap();
        assert_eq!(next.turn_id, intent.turn_id);
        assert_eq!(next.queue_position, intent.queue_position);
        assert_eq!(
            restored.admission_deadline_millis,
            Some(actors[0].now_millis() + parked.remaining_admission_millis.unwrap())
        );
        assert_eq!(restored.followups_spent, parked.followups_spent);
    }
}

mod review_fixes {
    use super::*;
    use mac_worker::test_support::{
        client_state::{ActiveTaskConfig, ClientStateStore},
        core::{error::WorkerError, paths::PathLayout},
        host::process::SystemProcessRunner,
        task::model::TaskState,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn probe_paths(root: &std::path::Path) -> PathLayout {
        PathLayout {
            config: root.join("config"),
            state: root.join("state"),
            cache: root.join("cache"),
            data: root.join("data"),
        }
    }
    struct FailOnlyOpenRepair<'a> {
        fixture: &'a GitIntegrationFixture,
        repairs: AtomicUsize,
    }
    impl IntegrationHost for FailOnlyOpenRepair<'_> {
        fn execute(
            &self,
            request: &HostIntegrationRequest,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            if matches!(
                request.action,
                HostIntegrationAction::Step {
                    step: IntegrationStep::Repair,
                    ..
                }
            ) {
                self.repairs.fetch_add(1, Ordering::SeqCst);
                if self
                    .fixture
                    .store
                    .task_status(
                        &self.fixture.record.policy.project_id,
                        self.fixture.record.task_id,
                    )?
                    .state()
                    == TaskState::Open
                {
                    return Err(IntegrationCode::IntegrationNetwork.error());
                }
            }
            HostIntegrationService::new(
                &self.fixture.store,
                &SystemProcessRunner,
                &self.fixture.runtime,
            )
            .execute(request)
        }
    }

    #[test]
    fn review_closed_restore_does_not_confuse_an_open_repair_retry_with_closed_settlement() {
        let temp = tempfile::tempdir().unwrap();
        let paths = probe_paths(&temp.path().canonicalize().unwrap());
        let mut f = GitIntegrationFixture::at(temp.path().join("git"));
        f.commit_base();
        f.commit_task();
        let runtime = Arc::new(ManualIntegrationRuntime::default());
        let state = RootedIntegrationState::open(&paths, runtime.clone()).unwrap();
        state
            .publish_policy(f.record.task_id, &f.record.policy)
            .unwrap();
        state
            .replace(f.record.task_id, IntegrationRevision(0), &f.record)
            .unwrap();
        HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime)
            .execute(&HostIntegrationRequest {
                protocol_version: 7,
                task_id: f.record.task_id,
                integration_id: None,
                epoch: 0,
                revision: IntegrationRevision(0),
                action: HostIntegrationAction::Arm {
                    policy: f.record.policy.clone(),
                },
            })
            .unwrap();
        let ordinary = sample_ordinary(f.record.task_id, f.record.snapshot.source_turn_id)
            .with_status(
                f.store
                    .task_status(&f.record.policy.project_id, f.record.task_id)
                    .unwrap(),
            )
            .unwrap()
            .with_fetched_head(Some(f.record.snapshot.source_head.clone()))
            .unwrap();
        let mut facts = IntegrationTaskFacts::from_record(&ordinary, false);
        facts.cycle_base = f.record.cycle_base.clone();
        let observer = FakeIntegrationObserver::default();
        observer.insert(facts.clone());
        let turns = FakeIntegrationTurns::default();
        let host = FailOnlyOpenRepair {
            fixture: &f,
            repairs: AtomicUsize::new(0),
        };
        for _ in 0..4 {
            let saved = state.load(f.record.task_id).unwrap().unwrap();
            if let Some(due) = saved.snapshot.retry_at_millis {
                runtime.advance(std::time::Duration::from_millis(
                    due.saturating_sub(runtime.now_millis()),
                ));
            }
            IntegrationRunner::new(IntegrationCoordinator::new(
                &state,
                &host,
                &turns,
                runtime.as_ref(),
                &observer,
            ))
            .run(f.record.task_id)
            .unwrap();
        }
        let blocked = state.load(f.record.task_id).unwrap().unwrap();
        assert_eq!(blocked.snapshot.state, IntegrationStatus::Blocked);
        assert!(
            blocked
                .phase_retries
                .iter()
                .any(|r| r.phase == IntegrationPhase::Repair && r.retries == 3)
        );
        let receipt = blocked.receipt.as_ref().unwrap();
        assert!(!receipt.imported);
        assert_eq!(f.origin_tip(), receipt.merge_oid.clone().unwrap());
        assert_eq!(host.repairs.load(Ordering::SeqCst), 4);
        let close_at = f
            .store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .updated_at_millis()
            + mac_worker::test_support::host::store::TASK_RETENTION_MILLIS
            + 1;
        assert!(
            mac_worker::test_support::host::gc::apply_baseline_retention_close(
                &f.store,
                &SystemProcessRunner,
                &f.record.policy.project_id,
                f.record.task_id,
                close_at
            )
            .unwrap()
        );
        facts.ordinary = ordinary
            .with_status(
                f.store
                    .task_status(&f.record.policy.project_id, f.record.task_id)
                    .unwrap(),
            )
            .unwrap();
        observer.insert(facts.clone());
        let client = ClientStateStore::open(&paths.state).unwrap();
        client.create_task(facts.ordinary).unwrap();
        client.bootstrap_active_task_index().unwrap();
        let selected = client
            .select_active_task_ids(&ActiveTaskConfig::default())
            .unwrap()
            .selected;
        let settled =
            IntegrationCoordinator::new(&state, &host, &turns, runtime.as_ref(), &observer)
                .drive_once(f.record.task_id)
                .unwrap();
        println!(
            "real M={} on origin; host Closed/workspace gone; pre-close Repair retries=3; selected={selected:?}; after restore state={:?}, Repair calls={}, receipt imported={}",
            f.origin_tip(),
            settled.state,
            host.repairs.load(Ordering::SeqCst),
            state
                .load(f.record.task_id)
                .unwrap()
                .unwrap()
                .receipt
                .unwrap()
                .imported
        );
        assert_eq!(
            settled.state,
            IntegrationStatus::Integrated,
            "an Open repair retry is not evidence that Closed settlement was attempted"
        );
        assert_eq!(selected, vec![f.record.task_id]);
        assert_eq!(host.repairs.load(Ordering::SeqCst), 5);
        assert_eq!(turns.imports(f.record.task_id).len(), 1);
        let saved = state.load(f.record.task_id).unwrap().unwrap();
        assert!(
            saved
                .phase_retries
                .iter()
                .any(|retry| retry.phase == IntegrationPhase::Repair && retry.retries == 3)
        );
        assert!(
            saved
                .phase_retries
                .iter()
                .any(|retry| retry.phase == IntegrationPhase::Drive && retry.retries == 0)
        );
    }
}
