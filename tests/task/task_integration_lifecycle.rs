use mac_worker::test_support::integration::*;

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
            let mut candidate = record.candidates.last().cloned().unwrap_or_else(|| {
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
                return Ok(HostIntegrationResponse::Integrated {
                    identity,
                    receipt: IntegrationReceipt {
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
                    },
                });
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
        pub runtime: ManualIntegrationRuntime,
        pub observer: FakeIntegrationObserver,
    }
    impl Rig {
        pub fn new(mode: Mode, close: ClosePolicy) -> Self {
            let rig = Self {
                state: MemoryIntegrationState::default(),
                host: Host {
                    mode: Mutex::new(mode),
                    calls: Mutex::new(vec![]),
                },
                turns: FakeIntegrationTurns::default(),
                runtime: ManualIntegrationRuntime::default(),
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
                &self.runtime,
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
                step: IntegrationStep::Fetch | IntegrationStep::Repair,
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
