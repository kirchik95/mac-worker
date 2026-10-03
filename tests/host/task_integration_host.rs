use mac_worker::test_support::integration::*;
use mac_worker::test_support::{
    host::{
        process::SystemProcessRunner,
        store::{HostGc, TASK_RETENTION_MILLIS},
    },
    task::{
        model::{TaskState, TaskStatus},
        store::TaskStore,
    },
};

fn request(f: &GitIntegrationFixture, action: HostIntegrationAction) -> HostIntegrationRequest {
    HostIntegrationRequest {
        protocol_version: 7,
        task_id: f.record.task_id,
        integration_id: Some(f.record.snapshot.integration_id),
        epoch: f.record.snapshot.epoch,
        revision: f.record.snapshot.revision,
        action,
    }
}
fn execute(
    f: &GitIntegrationFixture,
    request: &HostIntegrationRequest,
) -> Result<HostIntegrationResponse, mac_worker::test_support::core::error::WorkerError> {
    HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime).execute(request)
}
fn legacy_close(f: &GitIntegrationFixture) {
    use mac_worker::test_support::host::{gc::apply_baseline_retention_close, lease::LeaseService};
    let project = &f.record.policy.project_id;
    let task = f.record.task_id;
    let before = f.store.task_status(project, task).unwrap();
    assert_eq!(before.state(), TaskState::Open);
    assert!(
        !LeaseService::new(&f.store)
            .task_scope_is_live(project, task)
            .unwrap()
    );
    let mirror = f.store.mirror_if_present(project).unwrap().unwrap();
    let refs = integration_git(
        mirror.path(),
        &[
            "for-each-ref",
            "--format=%(refname) %(objectname)",
            "refs/heads/task",
            "refs/mac-worker/bases",
        ],
    );
    let expiry = before.updated_at_millis() + TASK_RETENTION_MILLIS;
    assert!(
        !apply_baseline_retention_close(&f.store, &SystemProcessRunner, project, task, expiry - 1)
            .unwrap()
    );
    assert!(f.workspace().exists());
    assert!(
        apply_baseline_retention_close(&f.store, &SystemProcessRunner, project, task, expiry + 1)
            .unwrap()
    );
    let after = f.store.task_status(project, task).unwrap();
    assert_eq!(after.state(), TaskState::Closed);
    assert_eq!(after.head_oid(), before.head_oid());
    assert_eq!(after.turns(), before.turns());
    assert_eq!(after.updated_at_millis(), expiry + 1);
    assert!(!f.workspace().exists());
    assert_eq!(
        integration_git(
            mirror.path(),
            &[
                "for-each-ref",
                "--format=%(refname) %(objectname)",
                "refs/heads/task",
                "refs/mac-worker/bases"
            ]
        ),
        refs
    );
}

fn integration_git(path: &std::path::Path, args: &[&str]) -> String {
    let result = std::process::Command::new("/usr/bin/git")
        .arg("-C")
        .arg(path)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap()
}

fn host_record(f: &GitIntegrationFixture) -> IntegrationRecord {
    HostIntegrationStore::new(&f.store)
        .load(&f.record.policy.project_id, f.record.task_id)
        .unwrap()
        .unwrap()
}

#[test]
fn source_retirement_stop_is_durable_before_intent_staging_and_recovery() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::ProjectRegistry,
        core::{config::Config, error::WorkerError, paths::PathLayout},
        host::process::{ProcessRequest, ProcessResult, ProcessRunner},
        task::{client::TaskClient, model::LocalTaskRecord, turn_runner::InlineRunnerExecutor},
    };
    use std::{os::unix::process::ExitStatusExt, process::ExitStatus, sync::Arc};
    struct CloseTransport<'a>(&'a TaskStore<'a>);
    impl ProcessRunner for CloseTransport<'_> {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.program == "/usr/bin/git" {
                return SystemProcessRunner.run(request);
            }
            assert_eq!(request.program, "/usr/bin/ssh");
            assert_eq!(
                request.args.last().unwrap(),
                "~/.local/bin/worker host task-close"
            );
            let close = serde_json::from_slice(request.stdin.as_ref().unwrap()).unwrap();
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: serde_json::to_vec(&self.0.close(&close)?).unwrap(),
                stderr: vec![],
            })
        }
    }
    for mutation in ["cancel", "close", "discard"] {
        let mut f = GitIntegrationFixture::new();
        let original = f.commit_base();
        let head = f.commit_task();
        let mut arm = request(
            &f,
            HostIntegrationAction::Arm {
                policy: f.record.policy.clone(),
            },
        );
        arm.integration_id = None;
        arm.revision = IntegrationRevision(0);
        execute(&f, &arm).unwrap();
        let host_tasks = TaskStore::new(&f.store, &SystemProcessRunner);
        let ordinary = LocalTaskRecord::new(
            host_tasks
                .load_meta(&f.record.policy.project_id, f.record.task_id)
                .unwrap(),
            host_tasks
                .load_status(&f.record.policy.project_id, f.record.task_id)
                .unwrap(),
            Some(1001),
            None,
            Some(head),
            "c".repeat(64),
            Some("fixture-worker".into()),
            true,
            None,
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let paths = PathLayout {
            config: root.join("config.toml"),
            state: root.join("state"),
            cache: root.join("cache"),
            data: root.join("data"),
        };
        let client = ClientStateStore::open(&paths.state).unwrap();
        client.create_task(ordinary.clone()).unwrap();
        let checkout = root.join("checkout");
        assert!(
            std::process::Command::new("/usr/bin/git")
                .args(["clone", &f.record.policy.origin, checkout.to_str().unwrap()])
                .output()
                .unwrap()
                .status
                .success()
        );
        ProjectRegistry::open(&paths.controller_state_root())
            .unwrap()
            .register(
                ordinary.meta().project_id(),
                ordinary.meta().worktree_id(),
                &checkout,
            )
            .unwrap();
        client
            .write_task_project_path(&ordinary, &checkout)
            .unwrap();
        let state =
            RootedIntegrationState::open(&paths, Arc::new(ManualIntegrationRuntime::default()))
                .unwrap();
        state
            .publish_policy(f.record.task_id, &f.record.policy)
            .unwrap();
        let turns = FakeIntegrationTurns::default();
        let observer = FakeIntegrationObserver::default();
        // Deliberately retain the old finalizer observation even after close.
        // Staging must be fenced by durable source identity, not a fresh read.
        observer.insert(IntegrationTaskFacts {
            ordinary: ordinary.clone(),
            cycle_base: f.record.cycle_base.clone(),
            result_imported: true,
            session_import_complete: true,
            continuation_pending: false,
            runner_present: false,
            stop_requested: false,
            close_pending: false,
            submission_pending: false,
            auxiliary_purpose: None,
        });
        let host = HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime);
        let owner = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
        let config = Config::parse(
            "version = 1\n[[workers]]\nname = 'fixture-worker'\nssh = 'never-connect'\nslots = 1\n",
        )
        .unwrap();
        let transport = CloseTransport(&host_tasks);
        let task_client =
            TaskClient::new(&transport, &config, &paths, &client, &InlineRunnerExecutor)
                .with_integration(&owner);
        assert!(state.load(f.record.task_id).unwrap().is_none());
        if mutation == "cancel" {
            task_client.cancel(f.record.task_id).unwrap();
            assert_eq!(client.load_task(f.record.task_id).unwrap(), ordinary);
        } else {
            task_client
                .close(f.record.task_id, mutation == "discard")
                .unwrap();
        }
        // Reopen the owner before the delayed ordinary finalizer wake.
        let reopened =
            RootedIntegrationState::open(&paths, Arc::new(ManualIntegrationRuntime::default()))
                .unwrap();
        let owner = IntegrationCoordinator::new(&reopened, &host, &turns, &f.runtime, &observer);
        owner
            .on_terminal(f.record.task_id, fixture_source())
            .unwrap();
        let after = IntegrationRunner::new(owner).run(f.record.task_id).unwrap();
        assert_eq!(
            f.origin_tip(),
            original,
            "{mutation} acknowledged before a late cycle pushed"
        );
        assert_eq!(after.state, IntegrationStatus::Revoked);
        let saved = reopened.load(f.record.task_id).unwrap().unwrap();
        assert!(saved.tombstone.unwrap().acknowledged);
        assert_eq!(saved.snapshot.source_turn_id, fixture_source());
    }
}

#[test]
fn owner_can_revoke_an_armed_cycle_before_the_first_host_phase_and_fence_late_fetch() {
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    let source = f.commit_task();
    let mut arm = request(
        &f,
        HostIntegrationAction::Arm {
            policy: f.record.policy.clone(),
        },
    );
    arm.integration_id = None;
    arm.revision = IntegrationRevision(0);
    execute(&f, &arm).unwrap();
    assert!(
        HostIntegrationStore::new(&f.store)
            .load(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .is_none()
    );
    let state = MemoryIntegrationState::default();
    state
        .publish_policy(f.record.task_id, &f.record.policy)
        .unwrap();
    state
        .replace(f.record.task_id, IntegrationRevision(0), &f.record)
        .unwrap();
    let turns = FakeIntegrationTurns::default();
    let observer = FakeIntegrationObserver::default();
    observer.insert(observed(&f));
    let host = HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime);
    let owner = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
    assert_eq!(
        owner
            .revoke(f.record.task_id, f.record.snapshot.revision)
            .unwrap()
            .state,
        IntegrationStatus::Revoked
    );
    assert!(
        state
            .load(f.record.task_id)
            .unwrap()
            .unwrap()
            .tombstone
            .unwrap()
            .acknowledged
    );
    assert_eq!(
        f.execute(IntegrationStep::Fetch).unwrap_err().public_code(),
        IntegrationCode::IntegrationStopUnconfirmed.as_str()
    );
    assert_eq!(f.origin_tip(), target);
    assert_eq!(f.git(&["rev-parse", "HEAD"]), source.as_str());
    assert!(f.git(&["status", "--porcelain=v1"]).is_empty());
}

#[test]
fn stale_prephase_revoke_preserves_the_newer_epoch_fence_across_crash_replay() {
    for crash in [
        None,
        Some(IntegrationHook::AfterRevoke),
        Some(IntegrationHook::BeforeRevokeAck),
        Some(IntegrationHook::AfterRevokeAck),
    ] {
        let mut f = GitIntegrationFixture::new();
        let target = f.commit_base();
        f.commit_task();
        let mut arm = request(
            &f,
            HostIntegrationAction::Arm {
                policy: f.record.policy.clone(),
            },
        );
        arm.integration_id = None;
        arm.revision = IntegrationRevision(0);
        execute(&f, &arm).unwrap();
        let revoke = |epoch| HostIntegrationRequest {
            epoch,
            ..request(
                &f,
                HostIntegrationAction::Revoke {
                    tombstone: IntegrationTombstone {
                        epoch,
                        revision: f.record.snapshot.revision,
                        requested_at_millis: 1002,
                        acknowledged: false,
                    },
                },
            )
        };
        let old = revoke(0);
        let newer = revoke(1);
        assert!(matches!(
            execute(&f, &old).unwrap(),
            HostIntegrationResponse::Revoked { .. }
        ));
        if let Some(hook) = crash {
            f.runtime.crash_at(hook);
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute(&f, &newer)))
                    .is_err()
            );
            f.runtime.restart();
        }
        assert!(matches!(
            execute(&f, &newer).unwrap(),
            HostIntegrationResponse::Revoked { .. }
        ));
        let proof_path = f
            .store
            .task_dir(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .join("integration/revoke.json");
        let proof = std::fs::read(&proof_path).unwrap();
        assert!(matches!(
            execute(&f, &newer).unwrap(),
            HostIntegrationResponse::Revoked { .. }
        ));
        assert_eq!(
            std::fs::read(&proof_path).unwrap(),
            proof,
            "identical replay changed its proof"
        );
        let stale = execute(&f, &old);
        assert_eq!(
            std::fs::read(&proof_path).unwrap(),
            proof,
            "stale revoke erased the acknowledged newer fence: {stale:?}"
        );
        f.record.snapshot.epoch = 1;
        assert_eq!(
            f.execute(IntegrationStep::Prepare)
                .unwrap_err()
                .public_code(),
            IntegrationCode::IntegrationStopUnconfirmed.as_str()
        );
        assert_eq!(f.origin_tip(), target);
    }
}

#[test]
fn stale_revoke_cannot_acknowledge_or_rewrite_a_newer_ordinary_turn() {
    use mac_worker::test_support::{
        host::job::JobId,
        task::model::{TaskOutcome, TurnSummary, TurnTerminal},
    };
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.prepare();
    let revoke = request(
        &f,
        HostIntegrationAction::Revoke {
            tombstone: IntegrationTombstone {
                epoch: f.record.snapshot.epoch,
                revision: f.record.snapshot.revision,
                requested_at_millis: 1002,
                acknowledged: false,
            },
        },
    );
    execute(&f, &revoke).unwrap();
    let task = f
        .store
        .task_dir(&f.record.policy.project_id, f.record.task_id)
        .unwrap();
    let mut status = serde_json::to_value(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap(),
    )
    .unwrap();
    status["turns"].as_array_mut().unwrap().push(
        serde_json::to_value(TurnSummary::new(
            2,
            JobId::new(uuid::Uuid::from_u128(101)),
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            Some(false),
            false,
            Some(1003),
            Some(1004),
        ))
        .unwrap(),
    );
    let status: TaskStatus = serde_json::from_value(status).unwrap();
    std::fs::write(
        task.join("status.json"),
        serde_json::to_vec(&status).unwrap(),
    )
    .unwrap();
    let before = host_record(&f);
    let mut stale = revoke;
    stale.revision = stale.revision.next().unwrap();
    if let HostIntegrationAction::Revoke { tombstone } = &mut stale.action {
        tombstone.revision = stale.revision;
    }
    assert_eq!(
        execute(&f, &stale).unwrap_err().public_code(),
        "INTEGRATION_STATE_INVALID"
    );
    assert_eq!(host_record(&f), before);
    assert_eq!(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap(),
        status
    );
}

#[test]
fn acknowledged_stop_before_push_fences_real_close_discard_and_ordinary_resume() {
    use mac_worker::test_support::{
        client_state::RunnerLivenessVerdict,
        core::{error::WorkerError, paths::PathLayout},
        host::job::ProcessIdentity,
        task::store::TaskCloseRequest,
    };
    use std::sync::{Arc, Mutex, mpsc};
    struct BeforePush {
        base: ManualIntegrationRuntime,
        entered: Mutex<Option<mpsc::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl IntegrationRuntime for BeforePush {
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
            self.base.begin_phase(key)
        }
        fn reach(&self, hook: IntegrationHook) {
            self.base.reach(hook);
            if hook == IntegrationHook::BeforePush
                && let Some(entered) = self.entered.lock().unwrap().take()
            {
                entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
        }
    }
    for operation in ["cancel", "close", "discard", "say"] {
        let mut f = GitIntegrationFixture::new();
        let target = f.commit_base();
        let source = f.commit_task();
        f.prepare();
        f.record.snapshot.state = IntegrationStatus::CommitReady;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let paths = PathLayout {
            config: root.join("config"),
            state: root.join("state"),
            cache: root.join("cache"),
            data: root.join("data"),
        };
        let (enter, entered) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let runtime = Arc::new(BeforePush {
            base: ManualIntegrationRuntime::default(),
            entered: Mutex::new(Some(enter)),
            release: Mutex::new(resume),
        });
        let state = RootedIntegrationState::open(&paths, runtime.clone()).unwrap();
        state
            .publish_policy(f.record.task_id, &f.record.policy)
            .unwrap();
        state
            .replace(f.record.task_id, IntegrationRevision(0), &f.record)
            .unwrap();
        let observer = FakeIntegrationObserver::default();
        observer.insert(observed(&f));
        let turns = FakeIntegrationTurns::default();
        let host = HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime);
        let owner = IntegrationCoordinator::new(&state, &host, &turns, runtime.as_ref(), &observer);
        std::thread::scope(|scope| {
            let driver = scope.spawn(|| owner.drive_once(f.record.task_id));
            if entered
                .recv_timeout(std::time::Duration::from_secs(10))
                .is_err()
            {
                let _ = release.send(());
                panic!("BeforePush was not reached: {:?}", driver.join().unwrap());
            }
            let _release_on_panic = crate::support::on_drop(|| {
                if std::thread::panicking() {
                    let _ = release.send(());
                }
            });
            let pushing = state.load(f.record.task_id).unwrap().unwrap();
            assert!(pushing.push_intent.as_ref().unwrap().uncertain);
            let stopped = owner
                .revoke(f.record.task_id, pushing.snapshot.revision)
                .unwrap();
            assert_eq!(stopped.state, IntegrationStatus::Revoked);
            assert!(
                state
                    .load(f.record.task_id)
                    .unwrap()
                    .unwrap()
                    .tombstone
                    .unwrap()
                    .acknowledged
            );
            let tasks = TaskStore::new(&f.store, &SystemProcessRunner);
            match operation {
                "close" | "discard" => {
                    tasks
                        .close(&TaskCloseRequest::new(
                            &f.record.policy.project_id,
                            f.record.task_id,
                            operation == "discard",
                        ))
                        .unwrap();
                }
                "say" => {
                    let prepared = f.prepared(IntegrationTurnPurpose::Verify);
                    acquire_auxiliary(&f, &prepared);
                    tasks
                        .prepare_resume(
                            &f.record.policy.project_id,
                            f.record.task_id,
                            prepared.followup.turn_id(),
                            2,
                            "fixture-worker",
                            &source,
                        )
                        .unwrap();
                }
                "cancel" => {}
                _ => unreachable!(),
            }
            release.send(()).unwrap();
            assert_eq!(
                driver.join().unwrap().unwrap().state,
                IntegrationStatus::Revoked
            );
        });
        assert_eq!(f.origin_tip(), target, "{operation} allowed a late push");
        assert!(turns.imports(f.record.task_id).is_empty());
        assert!(
            state
                .load(f.record.task_id)
                .unwrap()
                .unwrap()
                .actor
                .is_none()
        );
        assert!(f.execute(IntegrationStep::Push).is_err());
    }
}

fn observed(f: &GitIntegrationFixture) -> IntegrationTaskFacts {
    use mac_worker::test_support::task::model::LocalTaskRecord;
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

#[test]
fn lost_push_reply_keeps_the_merged_receipt_through_real_owner_fetch_repair_and_import() {
    use mac_worker::test_support::{
        core::error::WorkerError,
        host::process::{ProcessRequest, ProcessResult, ProcessRunner},
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct CountPush(AtomicUsize);
    impl ProcessRunner for CountPush {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.args.iter().any(|arg| arg == "push") {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            SystemProcessRunner.run(request)
        }
    }
    struct LoseReply<'a> {
        service: HostIntegrationService<'a>,
        lose: AtomicBool,
        steps: Mutex<Vec<IntegrationStep>>,
    }
    impl IntegrationHost for LoseReply<'_> {
        fn execute(
            &self,
            request: &HostIntegrationRequest,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            let response = self.service.execute(request)?;
            if let HostIntegrationAction::Step { step, .. } = request.action {
                self.steps.lock().unwrap().push(step);
                if step == IntegrationStep::Push && self.lose.swap(false, Ordering::SeqCst) {
                    return Err(IntegrationCode::IntegrationNetwork.error());
                }
            }
            Ok(response)
        }
    }
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let merge = f.prepare();
    f.record.snapshot.state = IntegrationStatus::CommitReady;
    let state = MemoryIntegrationState::default();
    state
        .publish_policy(f.record.task_id, &f.record.policy)
        .unwrap();
    assert!(
        state
            .replace(f.record.task_id, IntegrationRevision(0), &f.record)
            .unwrap()
    );
    let observer = FakeIntegrationObserver::default();
    observer.insert(observed(&f));
    let turns = FakeIntegrationTurns::default();
    let runner = CountPush(AtomicUsize::new(0));
    let host = LoseReply {
        service: HostIntegrationService::new(&f.store, &runner, &f.runtime),
        lose: AtomicBool::new(true),
        steps: Mutex::new(vec![]),
    };
    let owner = IntegrationCoordinator::new(&state, &host, &turns, &f.runtime, &observer);
    assert_eq!(
        owner.drive_once(f.record.task_id).unwrap().state,
        IntegrationStatus::RetryWait
    );
    let durable = host_record(&f).receipt.unwrap();
    assert_eq!(durable.disposition, IntegrationDisposition::Merged);
    assert_eq!(durable.merge_oid, Some(merge.clone()));
    assert_eq!(f.origin_tip(), merge);
    f.runtime.advance(std::time::Duration::from_secs(2));
    let fetched = owner.drive_once(f.record.task_id).unwrap();
    assert_eq!(fetched.state, IntegrationStatus::Published);
    assert_eq!(fetched.disposition, Some(IntegrationDisposition::Merged));
    assert_eq!(fetched.merge_oid, Some(merge.clone()));
    assert_eq!(
        state.load(f.record.task_id).unwrap().unwrap().receipt,
        Some(durable.clone())
    );
    let repaired = owner.drive_once(f.record.task_id).unwrap();
    assert_eq!(repaired.state, IntegrationStatus::Integrated);
    assert_eq!(repaired.disposition, Some(IntegrationDisposition::Merged));
    assert_eq!(repaired.merge_oid, Some(merge.clone()));
    let mut imported = durable;
    imported.imported = true;
    assert_eq!(turns.imports(f.record.task_id), vec![imported.clone()]);
    assert_eq!(
        state.load(f.record.task_id).unwrap().unwrap().receipt,
        Some(imported)
    );
    assert_eq!(
        *host.steps.lock().unwrap(),
        vec![
            IntegrationStep::Push,
            IntegrationStep::Fetch,
            IntegrationStep::Repair
        ]
    );
    assert_eq!(runner.0.load(Ordering::SeqCst), 1);
    assert_eq!(f.git(&["rev-parse", "HEAD"]), merge.as_str());
}

#[test]
fn uncertain_observations_settle_the_retained_merge_before_the_source_head() {
    for step in [
        Some(IntegrationStep::Fetch),
        None,
        Some(IntegrationStep::Repair),
    ] {
        let mut f = GitIntegrationFixture::new();
        f.commit_base();
        f.commit_task();
        let merge = f.prepare();
        f.runtime.crash_at(IntegrationHook::AfterPushBeforeReceipt);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.push())).is_err());
        f.runtime.restart();
        assert!(host_record(&f).receipt.is_none());
        let action = step.map_or(HostIntegrationAction::Read, |step| {
            HostIntegrationAction::Step {
                step,
                record: Box::new(f.record.clone()),
            }
        });
        let HostIntegrationResponse::Integrated { receipt, .. } =
            execute(&f, &request(&f, action)).unwrap()
        else {
            panic!("uncertain observation must report its settled receipt")
        };
        assert_eq!(receipt.disposition, IntegrationDisposition::Merged);
        assert_eq!(receipt.merge_oid, Some(merge.clone()));
        let record = host_record(&f);
        assert_eq!(record.receipt, Some(receipt));
        assert_eq!(record.snapshot.merge_oid, Some(merge.clone()));
        assert_eq!(
            record.snapshot.disposition,
            Some(IntegrationDisposition::Merged)
        );
        assert!(!record.push_intent.unwrap().uncertain);
        assert_eq!(f.origin_tip(), merge);
    }
}

#[derive(Default)]
struct RecoveryRunner {
    fail_observation: bool,
    requests: std::sync::Mutex<Vec<mac_worker::test_support::host::process::ProcessRequest>>,
}
impl mac_worker::test_support::host::process::ProcessRunner for RecoveryRunner {
    fn run(
        &self,
        request: &mac_worker::test_support::host::process::ProcessRequest,
    ) -> Result<
        mac_worker::test_support::host::process::ProcessResult,
        mac_worker::test_support::core::error::WorkerError,
    > {
        self.requests.lock().unwrap().push(request.clone());
        if self.fail_observation && request.args.iter().any(|arg| arg == "ls-remote") {
            return Err(IntegrationCode::IntegrationNetwork.error());
        }
        SystemProcessRunner.run(request)
    }
}
fn closed_uncertain_fixture(
    target: &str,
) -> (
    GitIntegrationFixture,
    mac_worker::test_support::task::model::BaseOid,
) {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let merge = f.prepare();
    let oid = match target {
        "merge" => Some(merge.as_str()),
        "source" => Some(f.record.snapshot.source_head.as_str()),
        "neither" | "missing" => None,
        _ => panic!("unknown fixture target"),
    };
    if let Some(oid) = oid {
        let mirror = f
            .store
            .mirror_if_present(&f.record.policy.project_id)
            .unwrap()
            .unwrap();
        f.git(&[
            "-C",
            mirror.path().to_str().unwrap(),
            "push",
            &f.record.policy.origin,
            &format!("{oid}:refs/heads/main"),
        ]);
    } else if target == "missing" {
        let mirror = f
            .store
            .mirror_if_present(&f.record.policy.project_id)
            .unwrap()
            .unwrap();
        f.git(&[
            "-C",
            mirror.path().to_str().unwrap(),
            "push",
            &f.record.policy.origin,
            ":refs/heads/main",
        ]);
    }
    f.record.snapshot.state = IntegrationStatus::Pushing;
    f.record.snapshot.merge_oid = Some(merge.clone());
    f.record.push_intent = Some(IntegrationPushIntent {
        candidate: f.record.candidates.last().unwrap().id,
        expected_target: f.record.candidates.last().unwrap().target_head.clone(),
        merge_oid: merge.clone(),
        started_at_millis: 1000,
        uncertain: true,
    });
    let task = f
        .store
        .task_dir(&f.record.policy.project_id, f.record.task_id)
        .unwrap();
    std::fs::write(
        task.join("integration/record.json"),
        encode_bounded(&f.record, MAX_PRIVATE_RECORD_BYTES).unwrap(),
    )
    .unwrap();
    legacy_close(&f);
    (f, merge)
}
fn closed_repair(
    f: &GitIntegrationFixture,
    runner: &RecoveryRunner,
) -> Result<HostIntegrationResponse, mac_worker::test_support::core::error::WorkerError> {
    let result = HostIntegrationService::new(&f.store, runner, &f.runtime).execute(&request(
        f,
        HostIntegrationAction::Step {
            step: IntegrationStep::Repair,
            record: Box::new(f.record.clone()),
        },
    ));
    assert_eq!(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .state(),
        TaskState::Closed
    );
    assert!(!f.workspace().exists());
    assert_eq!(host_record(f).candidates, f.record.candidates);
    assert!(
        runner
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| !request.args.iter().any(|arg| matches!(
                arg.to_str(),
                Some("push" | "merge" | "merge-tree" | "commit-tree" | "add" | "reset")
            )))
    );
    result
}

#[test]
fn closed_repair_settles_a_retained_merge_without_workspace_or_push_authority() {
    let (f, merge) = closed_uncertain_fixture("merge");
    let runner = RecoveryRunner::default();
    let HostIntegrationResponse::Integrated { receipt, .. } = closed_repair(&f, &runner).unwrap()
    else {
        panic!("expected merged receipt")
    };
    assert_eq!(receipt.disposition, IntegrationDisposition::Merged);
    assert_eq!(receipt.merge_oid, Some(merge.clone()));
    assert_eq!(host_record(&f).snapshot.merge_oid, Some(merge.clone()));
    assert!(!host_record(&f).push_intent.unwrap().uncertain);
    assert_eq!(f.origin_tip(), merge);
    assert_eq!(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .head_oid(),
        Some(&merge)
    );
    assert_eq!(
        f.execute(IntegrationStep::Fetch).unwrap_err().public_code(),
        IntegrationCode::IntegrationWorkspaceMissing.as_str()
    );
}

#[test]
fn closed_repair_settles_only_the_source_with_an_already_integrated_receipt() {
    let (f, _) = closed_uncertain_fixture("source");
    let runner = RecoveryRunner::default();
    let HostIntegrationResponse::Integrated { receipt, .. } = closed_repair(&f, &runner).unwrap()
    else {
        panic!("expected source receipt")
    };
    assert_eq!(
        receipt.disposition,
        IntegrationDisposition::AlreadyIntegrated
    );
    assert!(receipt.merge_oid.is_none());
    let record = host_record(&f);
    assert!(record.snapshot.merge_oid.is_none());
    assert_eq!(
        record.snapshot.disposition,
        Some(IntegrationDisposition::AlreadyIntegrated)
    );
    assert!(!record.push_intent.unwrap().uncertain);
    assert_eq!(receipt.target_head, f.origin_tip());
}

#[test]
fn closed_repair_blocks_when_neither_retained_merge_nor_source_is_on_origin() {
    for target in ["neither", "missing"] {
        let (f, _) = closed_uncertain_fixture(target);
        let before = (target == "neither").then(|| f.origin_tip());
        assert!(matches!(
            closed_repair(&f, &RecoveryRunner::default()).unwrap(),
            HostIntegrationResponse::Blocked {
                code: IntegrationCode::IntegrationWorkspaceMissing,
                ..
            }
        ));
        assert!(host_record(&f).receipt.is_none());
        if let Some(before) = before {
            assert_eq!(f.origin_tip(), before);
        }
    }
}

#[test]
fn closed_repair_failed_observation_retains_uncertainty_and_retries_safely() {
    let (f, merge) = closed_uncertain_fixture("merge");
    let runner = RecoveryRunner {
        fail_observation: true,
        ..RecoveryRunner::default()
    };
    assert_eq!(
        closed_repair(&f, &runner).unwrap_err().public_code(),
        IntegrationCode::IntegrationNetwork.as_str()
    );
    let record = host_record(&f);
    assert!(record.receipt.is_none());
    assert!(record.push_intent.unwrap().uncertain);
    assert!(
        matches!(closed_repair(&f, &RecoveryRunner::default()).unwrap(), HostIntegrationResponse::Integrated { receipt, .. } if receipt.merge_oid == Some(merge))
    );
}

#[test]
fn host_arm_refuses_the_effective_default_push_branch_before_persisting_policy() {
    use mac_worker::test_support::task::model::{BranchName, PushTarget, TaskMeta};
    for push in [true, false] {
        let mut f = GitIntegrationFixture::new();
        f.commit_base();
        f.commit_task();
        let task = f
            .store
            .task_dir(&f.record.policy.project_id, f.record.task_id)
            .unwrap();
        let meta = TaskStore::new(&f.store, &SystemProcessRunner)
            .load_meta(&f.record.policy.project_id, f.record.task_id)
            .unwrap();
        let mut wire = serde_json::to_value(meta).unwrap();
        wire["publish"] = if push {
            serde_json::json!(["fetch", "push"])
        } else {
            serde_json::json!(["fetch"])
        };
        wire["publish_branch"] = serde_json::Value::Null;
        if push {
            wire["source"]["push_target"] =
                serde_json::to_value(PushTarget::new(f.record.policy.origin.clone()).unwrap())
                    .unwrap();
        }
        let meta: TaskMeta = serde_json::from_value(wire).unwrap();
        std::fs::write(task.join("meta.json"), serde_json::to_vec(&meta).unwrap()).unwrap();
        f.record.policy.target = BranchName::for_task(f.record.task_id);
        let runner = RecoveryRunner::default();
        let result = HostIntegrationService::new(&f.store, &runner, &f.runtime).execute(
            &HostIntegrationRequest {
                protocol_version: 7,
                task_id: f.record.task_id,
                integration_id: None,
                epoch: 0,
                revision: IntegrationRevision(0),
                action: HostIntegrationAction::Arm {
                    policy: f.record.policy.clone(),
                },
            },
        );
        if push {
            assert_eq!(
                result.unwrap_err().public_code(),
                IntegrationCode::IntegrationPublishTargetCollision.as_str()
            );
            assert!(!task.join("integration/policy.json").exists());
            assert!(host_record_optional(&f).is_none());
        } else {
            assert!(matches!(
                result.unwrap(),
                HostIntegrationResponse::Progress { snapshot: None, .. }
            ));
            assert!(task.join("integration/policy.json").exists());
        }
        assert!(runner.requests.lock().unwrap().is_empty());
    }
}

fn host_record_optional(f: &GitIntegrationFixture) -> Option<IntegrationRecord> {
    HostIntegrationStore::new(&f.store)
        .load(&f.record.policy.project_id, f.record.task_id)
        .unwrap()
}

#[test]
fn auxiliary_checks_follow_the_frozen_source_requirement() {
    use mac_worker::test_support::agents::agent::{ReportedCheck, ReportedCheckStatus::*};
    let check = |state| ReportedCheck::new("fixture", "true", state, "claim");
    for (source, auxiliary, code) in [
        (vec![], vec![], None),
        (
            vec![check(Pass)],
            vec![],
            Some(IntegrationCode::IntegrationChecksNotRun),
        ),
        (
            vec![check(NotRun)],
            vec![check(NotRun)],
            Some(IntegrationCode::IntegrationChecksNotRun),
        ),
        (
            vec![],
            vec![check(Fail)],
            Some(IntegrationCode::IntegrationChecksFailed),
        ),
        (
            vec![],
            vec![check(Error)],
            Some(IntegrationCode::IntegrationChecksFailed),
        ),
        (vec![check(Pass)], vec![check(Pass)], None),
        (vec![], vec![check(NotRun)], None),
    ] {
        let mut f = GitIntegrationFixture::new();
        f.write("payload.txt", b"base\n");
        f.commit_base();
        f.write("payload.txt", b"ours\n");
        f.commit_task();
        f.record.source_checks = source;
        f.advance_target_with("payload.txt", b"theirs\n");
        f.execute(IntegrationStep::Prepare).unwrap();
        f.write("payload.txt", b"resolved\n");
        f.complete_auxiliary(IntegrationTurnPurpose::Resolve);
        let status = f
            .store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .with_reported_checks(auxiliary)
            .unwrap();
        let path = f
            .store
            .task_dir(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .join("status.json");
        std::fs::write(path, serde_json::to_vec(&status).unwrap()).unwrap();
        let result = f.execute(IntegrationStep::AcceptTurn);
        if let Some(code) = code {
            assert_eq!(result.unwrap_err().public_code(), code.as_str());
        } else {
            assert!(matches!(
                result.unwrap(),
                HostIntegrationResponse::CandidateReady { .. }
            ));
        }
    }
}

#[test]
fn parked_blocked_and_uncertain_records_retain_workspaces_without_a_lease() {
    for state in [
        IntegrationStatus::Parked,
        IntegrationStatus::Blocked,
        IntegrationStatus::Pushing,
    ] {
        let mut f = GitIntegrationFixture::new();
        f.commit_base();
        f.commit_task();
        f.prepare();
        f.record.snapshot.state = state;
        if state == IntegrationStatus::Parked {
            f.record.snapshot.resume_state = Some(IntegrationStatus::CommitReady);
            f.record.snapshot.pause_reason = Some(IntegrationPauseReason::ControllerDrained);
            f.record.pause = Some(IntegrationPauseEvidence {
                reason: IntegrationPauseReason::ControllerDrained,
                effective_at_millis: 1001,
            });
        } else if state == IntegrationStatus::Blocked {
            f.record.snapshot.blocked_code = Some(IntegrationCode::IntegrationNetwork);
        } else {
            let candidate = f.record.candidates.last().unwrap();
            f.record.push_intent = Some(IntegrationPushIntent {
                candidate: candidate.id,
                expected_target: candidate.target_head.clone(),
                merge_oid: candidate.merge_oid.clone().unwrap(),
                started_at_millis: 1001,
                uncertain: true,
            });
        }
        let path = f
            .store
            .task_dir(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .join("integration/record.json");
        std::fs::write(
            path,
            encode_bounded(&f.record, MAX_PRIVATE_RECORD_BYTES).unwrap(),
        )
        .unwrap();
        HostGc::new(&f.store, &SystemProcessRunner)
            .apply_at(1001 + TASK_RETENTION_MILLIS + 1)
            .unwrap();
        assert!(f.workspace().exists());
        assert_eq!(
            f.store
                .task_status(&f.record.policy.project_id, f.record.task_id)
                .unwrap()
                .state(),
            TaskState::Open
        );
        assert_eq!(host_record(&f).snapshot.state, state);
    }
}

#[test]
fn native_clean_prepare_refuses_a_rebound_workspace_branch_or_head() {
    for change in ["branch", "head", "tracked"] {
        let mut f = GitIntegrationFixture::new();
        let base = f.commit_base();
        f.commit_task();
        if change == "branch" {
            f.git(&["branch", "-m", "rebound"]);
        } else if change == "head" {
            f.git(&["reset", "--hard", base.as_str()]);
        } else {
            f.write("base.txt", b"unreported local change\n");
        }
        assert!(f.execute(IntegrationStep::Prepare).is_err());
        assert_eq!(f.origin_tip(), base);
    }
}

#[test]
fn clean_revoke_preserves_files_without_an_auxiliary_workspace_effect() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.prepare();
    f.write("manual.txt", b"manual untracked file\n");
    f.write("base.txt", b"manual tracked edit\n");
    execute(
        &f,
        &request(
            &f,
            HostIntegrationAction::Revoke {
                tombstone: IntegrationTombstone {
                    epoch: f.record.snapshot.epoch,
                    revision: f.record.snapshot.revision,
                    requested_at_millis: 1005,
                    acknowledged: false,
                },
            },
        ),
    )
    .unwrap();
    assert_eq!(
        std::fs::read(f.workspace().join("manual.txt")).unwrap(),
        b"manual untracked file\n"
    );
    assert_eq!(
        std::fs::read(f.workspace().join("base.txt")).unwrap(),
        b"manual tracked edit\n"
    );
}

#[test]
fn clean_mirror_candidate_cannot_overwrite_preexisting_untracked_files_on_repair() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.write("target.txt", b"preexisting untracked file\n");
    let target = f.advance_target();
    let result = f.execute(IntegrationStep::Prepare);
    assert!(result.is_err());
    assert_eq!(f.origin_tip(), target);
    assert_eq!(
        std::fs::read(f.workspace().join("target.txt")).unwrap(),
        b"preexisting untracked file\n"
    );
}

#[test]
fn integration_resume_keeps_branch_head_session_lease_and_sequence_strict() {
    use mac_worker::test_support::{agents::agent::AgentKind, task::store::SessionBinding};
    for change in ["branch", "head", "session", "lease", "sequence"] {
        let mut f = GitIntegrationFixture::new();
        f.write("payload.txt", b"base\n");
        let base = f.commit_base();
        f.write("payload.txt", b"ours\n");
        f.commit_task();
        f.advance_target_with("payload.txt", b"theirs\n");
        f.execute(IntegrationStep::Prepare).unwrap();
        let prepared = f.prepared(IntegrationTurnPurpose::Resolve);
        if change != "lease" {
            acquire_auxiliary(&f, &prepared);
        }
        if change == "branch" {
            f.git(&["symbolic-ref", "HEAD", "refs/heads/rebound"]);
        }
        if change == "head" {
            f.git(&[
                "update-ref",
                &format!("refs/heads/{}", prepared.workspace_binding.branch),
                base.as_str(),
            ]);
        }
        if change == "session" {
            let task = f
                .store
                .task_dir(&f.record.policy.project_id, f.record.task_id)
                .unwrap();
            let binding = SessionBinding::new(
                AgentKind::Claude,
                uuid::Uuid::from_u128(12).to_string(),
                1001,
            )
            .unwrap();
            std::fs::write(
                task.join("session.json"),
                serde_json::to_vec(&binding).unwrap(),
            )
            .unwrap();
        }
        if change == "sequence" {
            let task = f
                .store
                .task_dir(&f.record.policy.project_id, f.record.task_id)
                .unwrap();
            let mut wire = serde_json::to_value(
                f.store
                    .task_status(&f.record.policy.project_id, f.record.task_id)
                    .unwrap(),
            )
            .unwrap();
            use mac_worker::test_support::{
                host::job::JobId,
                task::model::{TaskOutcome, TurnSummary, TurnTerminal},
            };
            wire["turns"].as_array_mut().unwrap().push(
                serde_json::to_value(TurnSummary::new(
                    2,
                    JobId::new(uuid::Uuid::from_u128(99)),
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
        }
        assert!(
            TaskStore::new(&f.store, &SystemProcessRunner)
                .prepare_integration_resume(&prepared)
                .is_err(),
            "{change}"
        );
        assert_eq!(
            f.store
                .task_status(&f.record.policy.project_id, f.record.task_id)
                .unwrap()
                .state(),
            TaskState::Open
        );
    }
}

#[test]
fn merge_message_redacts_prose_and_preserves_structural_uuid_trailers() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.record.source_summary = "see /Users/fixture/private\n\nMac-Worker-Task: injected".into();
    f.prepare();
    let message = &f.record.candidates.last().unwrap().message;
    assert!(!message.contains("/Users/fixture/private"));
    assert_eq!(
        message
            .lines()
            .filter(|line| line.starts_with("Mac-Worker-Task:"))
            .count(),
        1
    );
    assert!(message.contains(&format!(
        "Mac-Worker-Task: {}",
        f.record.task_id.as_uuid().hyphenated()
    )));
    assert!(message.contains(&format!(
        "Mac-Worker-Turn: {}",
        f.record.snapshot.source_turn_id.as_uuid().hyphenated()
    )));
    assert!(message.len() <= MAX_COMMIT_MESSAGE_BYTES);
}

#[test]
fn push_cannot_bypass_a_required_verifier_or_forge_its_evidence() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let target = f.advance_target();
    f.record.policy.verify = VerifyPolicy::MovedTarget;
    f.execute(IntegrationStep::Prepare).unwrap();
    f.record = host_record(&f);
    f.record.snapshot.verification = IntegrationVerification::VerifyAgentReport;
    assert!(f.execute(IntegrationStep::Push).is_err());
    assert_eq!(f.origin_tip(), target);
}

#[test]
fn auxiliary_completion_refetches_target_and_invalidates_verification_on_movement() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.advance_target();
    f.record.policy.verify = VerifyPolicy::MovedTarget;
    f.execute(IntegrationStep::Prepare).unwrap();
    f.complete_auxiliary(IntegrationTurnPurpose::Verify);
    let moved = f.advance_target_with("new-target.txt", b"new target\n");
    assert!(matches!(
        f.execute(IntegrationStep::AcceptTurn).unwrap(),
        HostIntegrationResponse::TargetMoved { observed_target, .. } if observed_target == moved
    ));
    assert!(matches!(
        f.execute(IntegrationStep::Prepare).unwrap(),
        HostIntegrationResponse::NeedTurn { purpose: IntegrationTurnPurpose::Verify, candidate, .. }
            if candidate.id.attempt == 2 && candidate.target_head == moved
    ));
}

#[test]
fn public_sender_refuses_legacy_closed_work() {
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    f.commit_task();
    f.prepare();
    legacy_close(&f);
    let candidate = f.record.candidates.last().unwrap();
    assert!(
        IntegrationGit::new(&f.store, &SystemProcessRunner, &f.runtime)
            .push_candidate(&f.record.policy, candidate)
            .is_err()
    );
    assert_eq!(f.origin_tip(), target);
}

#[test]
fn ordinary_resume_waits_for_confirmed_stop_even_for_a_clean_mirror_candidate() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.prepare();
    let prepared = f.prepared(IntegrationTurnPurpose::Verify);
    acquire_auxiliary(&f, &prepared);
    assert_eq!(
        TaskStore::new(&f.store, &SystemProcessRunner)
            .prepare_resume(
                &f.record.policy.project_id,
                f.record.task_id,
                prepared.followup.turn_id(),
                prepared.followup.turn_number(),
                prepared.followup.worker(),
                prepared.followup.base_oid()
            )
            .unwrap_err()
            .public_code(),
        IntegrationCode::IntegrationStopUnconfirmed.as_str()
    );
}

#[test]
fn confirmed_revoke_allows_a_new_epoch_but_never_the_old_push() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.prepare();
    execute(
        &f,
        &request(
            &f,
            HostIntegrationAction::Revoke {
                tombstone: IntegrationTombstone {
                    epoch: f.record.snapshot.epoch,
                    revision: f.record.snapshot.revision,
                    requested_at_millis: 1005,
                    acknowledged: false,
                },
            },
        ),
    )
    .unwrap();
    assert!(f.execute(IntegrationStep::Push).is_err());
    f.record = host_record(&f);
    f.record.snapshot.epoch += 1;
    f.record.snapshot.revision = f.record.snapshot.revision.next().unwrap();
    f.record.snapshot.state = IntegrationStatus::Pending;
    f.record.snapshot.attempts = 0;
    f.record.snapshot.merge_oid = None;
    f.record.snapshot.observed_target_oid = None;
    f.record.tombstone = None;
    f.record.push_intent = None;
    f.record.candidates.clear();
    let summary = f.record.source_summary.clone();
    f.record.source_summary = "rebound summary in a new epoch".into();
    assert!(f.execute(IntegrationStep::Prepare).is_err());
    f.record.source_summary = summary;
    assert!(matches!(
        f.execute(IntegrationStep::Prepare).unwrap(),
        HostIntegrationResponse::CandidateReady { .. }
    ));
}

#[test]
fn host_budget_starts_before_the_durable_intent_boundary() {
    struct ExpireAtIntent<'a>(&'a ManualIntegrationRuntime);
    impl IntegrationRuntime for ExpireAtIntent<'_> {
        fn now_millis(&self) -> u64 {
            self.0.now_millis()
        }
        fn actor(&self) -> ProcessIdentity {
            self.0.actor()
        }
        fn actor_verdict(&self, actor: ProcessIdentity) -> RunnerLivenessVerdict {
            self.0.actor_verdict(actor)
        }
        fn begin_phase(
            &self,
            key: &IntegrationPhaseKey,
        ) -> Result<IntegrationDriveAdmission, mac_worker::test_support::core::error::WorkerError>
        {
            self.0.begin_phase(key)
        }
        fn reach(&self, point: IntegrationHook) {
            if point == IntegrationHook::AfterIntent {
                self.0.advance(HOST_DEADLINE);
            }
            self.0.reach(point);
        }
    }
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    f.commit_task();
    let request = request(
        &f,
        HostIntegrationAction::Step {
            step: IntegrationStep::Prepare,
            record: Box::new(f.record.clone()),
        },
    );
    assert_eq!(
        HostIntegrationService::new(&f.store, &SystemProcessRunner, &ExpireAtIntent(&f.runtime))
            .execute(&request)
            .unwrap_err()
            .public_code(),
        IntegrationCode::IntegrationNetwork.as_str()
    );
    assert_eq!(f.origin_tip(), target);
    assert!(host_record(&f).candidates.is_empty());
}

#[test]
fn owner_import_ack_is_retained_after_repair() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.prepare();
    f.push();
    f.execute(IntegrationStep::Repair).unwrap();
    f.record = host_record(&f);
    let receipt = f.record.receipt.as_mut().unwrap();
    receipt.imported = true;
    f.record.snapshot.state = IntegrationStatus::Integrated;
    f.record.snapshot.disposition = Some(receipt.disposition);
    f.record.snapshot.observed_target_oid = Some(receipt.target_head.clone());
    f.execute(IntegrationStep::Repair).unwrap();
    let retained = host_record(&f);
    assert!(retained.receipt.unwrap().imported);
    assert_eq!(retained.snapshot.state, IntegrationStatus::Integrated);
    HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(1001 + TASK_RETENTION_MILLIS + 1)
        .unwrap();
    assert!(!f.workspace().exists());
    assert_eq!(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .state(),
        TaskState::Closed
    );
}

#[test]
fn imported_cycle_allows_a_later_source_and_archives_the_previous_receipt() {
    use mac_worker::test_support::{
        host::job::JobId,
        task::model::{TaskOutcome, TurnSummary, TurnTerminal},
    };
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.prepare();
    f.push();
    f.execute(IntegrationStep::Repair).unwrap();
    f.record = host_record(&f);
    f.record.receipt.as_mut().unwrap().imported = true;
    f.record.snapshot.revision = IntegrationRevision(20);
    f.execute(IntegrationStep::Repair).unwrap();
    let accepted = f.origin_tip();
    f.write("followup.txt", b"later source\n");
    f.git(&["add", "-A"]);
    f.git(&["commit", "-m", "later source"]);
    let head: mac_worker::test_support::task::model::BaseOid =
        f.git(&["rev-parse", "HEAD"]).parse().unwrap();
    let mirror = f.store.mirror(&f.record.policy.project_id).unwrap();
    f.git(&[
        "--git-dir",
        mirror.path().to_str().unwrap(),
        "fetch",
        "--no-tags",
        f.workspace().to_str().unwrap(),
        head.as_str(),
    ]);
    f.git(&[
        "--git-dir",
        mirror.path().to_str().unwrap(),
        "update-ref",
        &format!("refs/heads/task/{}", f.record.task_id),
        head.as_str(),
    ]);
    let source = JobId::new(uuid::Uuid::from_u128(100));
    let mut wire = serde_json::to_value(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap(),
    )
    .unwrap();
    wire["head_oid"] = serde_json::to_value(&head).unwrap();
    wire["turns"].as_array_mut().unwrap().push(
        serde_json::to_value(TurnSummary::new(
            2,
            source,
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            Some(true),
            false,
            Some(1002),
            Some(1003),
        ))
        .unwrap(),
    );
    let status: TaskStatus = serde_json::from_value(wire).unwrap();
    std::fs::write(
        f.store
            .task_dir(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .join("status.json"),
        serde_json::to_vec(&status).unwrap(),
    )
    .unwrap();
    f.record = host_record(&f);
    f.record.snapshot.source_head = head;
    f.record.snapshot.source_turn_id = source;
    f.record.snapshot.integration_id = IntegrationId::derive(
        f.record.task_id,
        source,
        &f.record.snapshot.source_head,
        &f.record.target_key,
    )
    .unwrap();
    f.record.snapshot.epoch = 0;
    f.record.snapshot.revision = IntegrationRevision(1);
    f.record.snapshot.state = IntegrationStatus::Pending;
    f.record.snapshot.attempts = 0;
    f.record.snapshot.merge_oid = None;
    f.record.snapshot.observed_target_oid = None;
    f.record.snapshot.disposition = None;
    f.record.cycle_base = accepted;
    f.record.candidates.clear();
    f.record.auxiliaries.clear();
    f.record.push_intent = None;
    f.record.receipt = None;
    assert!(matches!(
        f.execute(IntegrationStep::Prepare).unwrap(),
        HostIntegrationResponse::CandidateReady { .. }
    ));
    assert_eq!(host_record(&f).archived_receipts.len(), 1);
}

#[test]
fn a_wip_task_is_refused_before_policy_or_git_effects() {
    use mac_worker::test_support::task::model::TaskMeta;
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let meta_path = f
        .store
        .task_dir(&f.record.policy.project_id, f.record.task_id)
        .unwrap()
        .join("meta.json");
    let mut wire: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    wire["source"]["wip"] = serde_json::json!(true);
    let meta: TaskMeta = serde_json::from_value(wire).unwrap();
    std::fs::write(meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    assert_eq!(
        f.execute(IntegrationStep::Prepare)
            .unwrap_err()
            .public_code(),
        IntegrationCode::IntegrationWipBase.as_str()
    );
    assert!(
        HostIntegrationStore::new(&f.store)
            .load(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn source_check_failures_block_before_native_merge() {
    for state in ["fail", "error"] {
        let mut f = GitIntegrationFixture::new();
        let target = f.commit_base();
        f.commit_task();
        f.record.source_checks = serde_json::from_value(serde_json::json!([
            { "name": "fixture", "command": "true", "status": state, "detail": "claim" }
        ]))
        .unwrap();
        assert_eq!(
            f.execute(IntegrationStep::Prepare)
                .unwrap_err()
                .public_code(),
            IntegrationCode::IntegrationChecksFailed.as_str()
        );
        assert_eq!(f.origin_tip(), target);
    }
}

#[test]
fn frozen_source_identity_cannot_change_after_intent() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.prepare();
    f.record.source_summary = "rebound summary".into();
    assert!(f.execute(IntegrationStep::Prepare).is_err());
}

fn acquire_auxiliary(
    f: &GitIntegrationFixture,
    prepared: &PreparedIntegrationTurn,
) -> mac_worker::test_support::task::turn::TaskTurnRequest {
    use mac_worker::test_support::{
        core::protocol::MemoryPressure,
        host::{
            job::*,
            lease::{AdmissionFacts, LeaseService},
        },
        task::store::SessionBinding,
    };
    use uuid::Uuid;
    let meta = prepared.followup.expected().meta();
    use mac_worker::test_support::task::turn::{TaskTurnRequest, TurnMaterial};
    let turn = TurnMaterial::from_prompt(
        meta.task_id(),
        prepared.followup.turn_number(),
        meta.agent(),
        meta.model().map(str::to_owned),
        meta.effort().map(str::to_owned),
        meta.policy(),
        prepared.approved_turn_limits.clone(),
        prepared.followup.base_oid().clone(),
        prepared.followup.composed_prompt(),
        meta.env_profile().map(str::to_owned),
        prepared.followup.turn_id().as_uuid(),
        true,
    )
    .unwrap();
    TaskStore::new(&f.store, &SystemProcessRunner)
        .bind_session(
            meta.project_id(),
            meta.task_id(),
            SessionBinding::new(meta.agent(), Uuid::from_u128(12).to_string(), 1001).unwrap(),
        )
        .unwrap();
    let material = RequestFingerprintMaterial::new(
        prepared.followup.turn_id(),
        ClientId::new(Uuid::from_u128(20)),
        LeaseToken::new(Uuid::from_u128(21)),
        1002,
        prepared.followup.worker().into(),
        meta.project_id().into(),
        meta.worktree_id().into(),
        turn.digest(),
        String::new(),
        prepared.approved_turn_limits.timeout_millis,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap();
    let request = TaskTurnRequest::new(
        SubmitRequest::new(material.clone())
            .with_execution_scope(ExecutionScope::task(meta.task_id())),
        turn,
        prepared.followup.composed_prompt(),
    );
    let acquire = LeaseAcquireRequest::new(material)
        .with_execution_scope(ExecutionScope::task(meta.task_id()));
    let healthy = AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 200 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    };
    assert!(matches!(
        LeaseService::new(&f.store)
            .acquire(&acquire, &healthy, 1002)
            .unwrap(),
        LeaseAcquireResponse::Acquired { .. }
    ));
    request
}

#[test]
fn auxiliary_publication_records_done_and_leaves_git_for_host_staging() {
    use mac_worker::test_support::{
        host::rooted_fs::RootedDir,
        task::turn::{PreparedTask, TurnPublisher},
    };
    let mut f = GitIntegrationFixture::new();
    f.write("payload.txt", b"base\n");
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    let head = f.commit_task();
    let target = f.advance_target_with("payload.txt", b"theirs\n");
    f.execute(IntegrationStep::Prepare).unwrap();
    let prepared = f.prepared(IntegrationTurnPurpose::Resolve);
    acquire_auxiliary(&f, &prepared);
    let (meta, status) = TaskStore::new(&f.store, &SystemProcessRunner)
        .prepare_integration_resume(&prepared)
        .unwrap();
    f.write("payload.txt", b"resolved\n");
    let dir = RootedDir::create(&f.workspace().parent().unwrap().join("aux-output")).unwrap();
    std::fs::write(dir.path().join("last.md"), br#"{"status":"done","summary":"Resolved","questions":[],"files_changed":["payload.txt"],"checks":[]}"#).unwrap();
    let result = TurnPublisher::new(&f.store, &SystemProcessRunner)
        .publish(&PreparedTask::new(meta, status), &dir, Some(0))
        .unwrap();
    assert_eq!(result.head_oid(), Some(&head));
    assert_eq!(f.git(&["rev-parse", "HEAD"]), head.as_str());
    assert_eq!(f.git(&["rev-parse", "MERGE_HEAD"]), target.as_str());
    assert_eq!(f.origin_tip(), target);
    assert_eq!(
        std::fs::read(f.workspace().join("payload.txt")).unwrap(),
        b"resolved\n"
    );
}

#[test]
fn auxiliary_admission_persists_purpose_before_accepting_reduced_limits() {
    use mac_worker::test_support::{
        core::error::WorkerError,
        host::{
            job::JobId,
            job_service::{JobService, LaunchCandidate, SupervisorLauncher},
            store::SupervisorGuard,
        },
    };
    struct DoNotLaunch;
    impl SupervisorLauncher for DoNotLaunch {
        fn launch(
            &self,
            _job: JobId,
            _guard: SupervisorGuard,
        ) -> Result<LaunchCandidate, WorkerError> {
            Err(WorkerError::Unavailable(
                "fixture does not execute an agent".into(),
            ))
        }
    }
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.advance_target();
    f.record.policy.verify = VerifyPolicy::MovedTarget;
    f.execute(IntegrationStep::Prepare).unwrap();
    let prepared = f.prepared(IntegrationTurnPurpose::Verify);
    let turn = acquire_auxiliary(&f, &prepared);
    let jobs = JobService::new(&f.store, &DoNotLaunch);
    assert!(jobs.submit_turn(turn.clone()).is_err());
    let _ = jobs.submit_integration_turn(&prepared, turn);
    assert!(
        f.store
            .job(
                prepared.followup.expected().meta().project_id(),
                prepared.followup.expected().meta().worktree_id(),
                prepared.followup.turn_id()
            )
            .unwrap()
            .join("meta.json")
            .is_file()
    );
    let task = f
        .store
        .task_dir(&f.record.policy.project_id, f.record.task_id)
        .unwrap();
    assert!(
        task.join("integration")
            .join(format!("turn-{}.json", prepared.followup.turn_id()))
            .is_file()
    );
}

#[test]
fn candidate_bound_resume_accepts_conflicts_but_ordinary_resume_stays_strict() {
    let mut f = GitIntegrationFixture::new();
    f.write("payload.txt", b"base\n");
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    let head = f.commit_task();
    f.advance_target_with("payload.txt", b"theirs\n");
    f.execute(IntegrationStep::Prepare).unwrap();
    let prepared = f.prepared(IntegrationTurnPurpose::Resolve);
    acquire_auxiliary(&f, &prepared);
    let tasks = TaskStore::new(&f.store, &SystemProcessRunner);
    assert!(
        tasks
            .prepare_resume(
                &f.record.policy.project_id,
                f.record.task_id,
                prepared.followup.turn_id(),
                prepared.followup.turn_number(),
                prepared.followup.worker(),
                &head
            )
            .is_err()
    );
    let (_, status) = tasks.prepare_integration_resume(&prepared).unwrap();
    assert_eq!(status.state(), TaskState::Active);
    assert_eq!(status.head_oid(), Some(&head));
    assert_eq!(
        status.turns().last().unwrap().turn_id(),
        prepared.followup.turn_id()
    );
    assert_eq!(f.git(&["rev-parse", "HEAD"]), head.as_str());
}

#[test]
fn verifier_launch_refuses_an_index_tree_outside_the_pinned_candidate() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.advance_target();
    f.record.policy.verify = VerifyPolicy::MovedTarget;
    f.execute(IntegrationStep::Prepare).unwrap();
    let prepared = f.prepared(IntegrationTurnPurpose::Verify);
    acquire_auxiliary(&f, &prepared);
    f.write("tampered.txt", b"tampered\n");
    f.git(&["add", "tampered.txt"]);
    let error = TaskStore::new(&f.store, &SystemProcessRunner)
        .prepare_integration_resume(&prepared)
        .unwrap_err();
    assert_eq!(
        error.public_code(),
        IntegrationCode::IntegrationVerifyTreeMismatch.as_str()
    );
    assert_eq!(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .state(),
        TaskState::Open
    );
}

#[test]
fn armed_policy_reads_without_an_intent_and_survives_idle_gc() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let arm = HostIntegrationRequest {
        protocol_version: 7,
        task_id: f.record.task_id,
        integration_id: None,
        epoch: 0,
        revision: IntegrationRevision(0),
        action: HostIntegrationAction::Arm {
            policy: f.record.policy.clone(),
        },
    };
    execute(&f, &arm).unwrap();
    let read = HostIntegrationRequest {
        action: HostIntegrationAction::Read,
        ..arm
    };
    assert!(matches!(
        execute(&f, &read).unwrap(),
        HostIntegrationResponse::Progress { snapshot: None, .. }
    ));
    let gc = HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(1001 + TASK_RETENTION_MILLIS + 1)
        .unwrap();
    assert!(gc.applied().iter().all(|candidate| candidate.identifier()
        != format!("{}/{}", f.record.policy.project_id, f.record.task_id)));
    assert_eq!(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .state(),
        TaskState::Open
    );
    assert!(f.workspace().exists());
}

#[test]
fn native_prepare_and_build_reply_with_the_same_full_candidate() {
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    let head = f.commit_task();
    let prepared = f.execute(IntegrationStep::Prepare).unwrap();
    let HostIntegrationResponse::CandidateReady { candidate, .. } = prepared else {
        panic!("missing full candidate");
    };
    assert!(candidate.tree_oid.is_some());
    assert!(candidate.merge_oid.is_some());
    assert_eq!(
        f.parents(candidate.merge_oid.as_ref().unwrap()),
        vec![target.clone(), head.clone()]
    );
    let stored = HostIntegrationStore::new(&f.store)
        .load(&f.record.policy.project_id, f.record.task_id)
        .unwrap()
        .unwrap();
    assert_eq!(stored.candidates.last(), Some(candidate.as_ref()));
    f.record = stored;
    let built = f.execute(IntegrationStep::Build).unwrap();
    assert!(
        matches!(built, HostIntegrationResponse::CandidateReady { candidate: replay, .. } if replay == candidate)
    );
    assert_eq!(f.origin_tip(), target);
    assert_eq!(f.git(&["rev-parse", "HEAD"]), head.as_str());
}

#[test]
fn published_receipt_repairs_open_ref_workspace_and_status_idempotently() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    let source = f.commit_task();
    let merge = f.prepare();
    f.push();
    assert_eq!(f.git(&["rev-parse", "HEAD"]), source.as_str());
    for _ in 0..2 {
        assert!(matches!(
            f.execute(IntegrationStep::Repair).unwrap(),
            HostIntegrationResponse::Integrated { .. }
        ));
    }
    assert_eq!(f.git(&["rev-parse", "HEAD"]), merge.as_str());
    assert!(f.git(&["status", "--porcelain=v1"]).is_empty());
    assert_eq!(
        TaskStore::new(&f.store, &SystemProcessRunner)
            .load_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .head_oid(),
        Some(&merge)
    );
}

#[test]
fn revoke_restores_conflicts_and_fences_later_pushes() {
    let mut f = GitIntegrationFixture::new();
    f.write("payload.txt", b"base\n");
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    let head = f.commit_task();
    let target = f.advance_target_with("payload.txt", b"theirs\n");
    f.execute(IntegrationStep::Prepare).unwrap();
    let revoke = request(
        &f,
        HostIntegrationAction::Revoke {
            tombstone: IntegrationTombstone {
                epoch: 0,
                revision: f.record.snapshot.revision,
                requested_at_millis: 1005,
                acknowledged: false,
            },
        },
    );
    assert!(matches!(
        execute(&f, &revoke).unwrap(),
        HostIntegrationResponse::Revoked { .. }
    ));
    assert_eq!(f.git(&["rev-parse", "HEAD"]), head.as_str());
    assert!(f.git(&["status", "--porcelain=v1"]).is_empty());
    assert!(f.execute(IntegrationStep::Push).is_err());
    assert_eq!(f.origin_tip(), target);
}

#[test]
fn lost_push_reply_settles_after_legacy_close_without_workspace_resurrection() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let merge = f.prepare();
    f.runtime.crash_at(IntegrationHook::AfterPushBeforeReceipt);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.push())).is_err());
    assert_eq!(f.origin_tip(), merge);
    f.runtime.restart();
    legacy_close(&f);
    assert!(matches!(
        f.execute(IntegrationStep::Repair).unwrap(),
        HostIntegrationResponse::Integrated { .. }
    ));
    let status = f
        .store
        .task_status(&f.record.policy.project_id, f.record.task_id)
        .unwrap();
    assert_eq!(status.state(), TaskState::Closed);
    assert_eq!(status.head_oid(), Some(&merge));
    assert!(!f.workspace().exists());
    assert_eq!(f.origin_tip(), merge);
    assert!(f.execute(IntegrationStep::Push).is_err());
}

#[test]
fn native_receipt_repair_replays_after_ref_head_and_index_effects() {
    use mac_worker::test_support::{
        core::error::WorkerError,
        host::process::{ProcessRequest, ProcessResult, ProcessRunner},
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    struct CrashAfterEffect {
        mirror: std::path::PathBuf,
        workspace: std::path::PathBuf,
        boundary: &'static str,
        fired: AtomicBool,
    }
    impl ProcessRunner for CrashAfterEffect {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let result = SystemProcessRunner.run(request)?;
            let in_repo =
                |path: &std::path::Path| request.args.iter().any(|arg| arg == path.as_os_str());
            let update = request.args.iter().any(|arg| arg == "update-ref")
                && request
                    .args
                    .iter()
                    .any(|arg| arg.to_string_lossy().starts_with("refs/heads/task/"));
            let reached = match self.boundary {
                "ref" => update && in_repo(&self.mirror),
                "head" => update && in_repo(&self.workspace),
                _ => {
                    request.args.iter().any(|arg| arg == "reset")
                        && request.args.iter().any(|arg| arg == "--hard")
                }
            };
            if reached && !self.fired.swap(true, Ordering::SeqCst) {
                panic!("crash after native repair effect");
            }
            Ok(result)
        }
    }
    for boundary in ["ref", "head", "index"] {
        let mut f = GitIntegrationFixture::new();
        f.commit_base();
        f.commit_task();
        let merge = f.prepare();
        f.push();
        let runner = CrashAfterEffect {
            mirror: f
                .store
                .mirror(&f.record.policy.project_id)
                .unwrap()
                .path()
                .to_path_buf(),
            workspace: f.workspace().to_path_buf(),
            boundary,
            fired: AtomicBool::new(false),
        };
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || f.execute_with(IntegrationStep::Repair, &runner)
            ))
            .is_err()
        );
        assert!(runner.fired.load(Ordering::SeqCst));
        f.execute(IntegrationStep::Repair).unwrap();
        f.execute(IntegrationStep::Repair).unwrap();
        assert_eq!(f.git(&["rev-parse", "HEAD"]), merge.as_str());
        assert_eq!(
            f.store
                .task_status(&f.record.policy.project_id, f.record.task_id)
                .unwrap()
                .head_oid(),
            Some(&merge)
        );
        assert_eq!(f.origin_tip(), merge);
    }
}

#[test]
fn revoke_journal_and_ack_crashes_never_restore_push_authority() {
    for hook in [
        IntegrationHook::AfterRevoke,
        IntegrationHook::BeforeRevokeAck,
        IntegrationHook::AfterRevokeAck,
    ] {
        let mut f = GitIntegrationFixture::new();
        f.write("payload.txt", b"base\n");
        f.commit_base();
        f.write("payload.txt", b"ours\n");
        let head = f.commit_task();
        let target = f.advance_target_with("payload.txt", b"theirs\n");
        f.execute(IntegrationStep::Prepare).unwrap();
        let revoke = request(
            &f,
            HostIntegrationAction::Revoke {
                tombstone: IntegrationTombstone {
                    epoch: f.record.snapshot.epoch,
                    revision: f.record.snapshot.revision,
                    requested_at_millis: 1005,
                    acknowledged: false,
                },
            },
        );
        f.runtime.crash_at(hook);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute(&f, &revoke)))
                .is_err()
        );
        f.runtime.restart();
        assert!(f.execute(IntegrationStep::Push).is_err());
        assert!(matches!(
            execute(&f, &revoke).unwrap(),
            HostIntegrationResponse::Revoked { .. }
        ));
        assert!(host_record(&f).tombstone.unwrap().acknowledged);
        assert_eq!(f.git(&["rev-parse", "HEAD"]), head.as_str());
        assert_eq!(f.origin_tip(), target);
    }
}

#[test]
fn ordinary_close_waits_for_confirmed_integration_revoke() {
    use mac_worker::test_support::task::store::TaskCloseRequest;
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.prepare();
    let tasks = TaskStore::new(&f.store, &SystemProcessRunner);
    let close = TaskCloseRequest::new(&f.record.policy.project_id, f.record.task_id, false);
    assert_eq!(
        tasks.close(&close).unwrap_err().public_code(),
        IntegrationCode::IntegrationStopUnconfirmed.as_str()
    );
    assert!(f.workspace().exists());
    let revoke = request(
        &f,
        HostIntegrationAction::Revoke {
            tombstone: IntegrationTombstone {
                epoch: f.record.snapshot.epoch,
                revision: f.record.snapshot.revision,
                requested_at_millis: 1005,
                acknowledged: false,
            },
        },
    );
    execute(&f, &revoke).unwrap();
    tasks.close(&close).unwrap();
    assert_eq!(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .state(),
        TaskState::Closed
    );
}

#[test]
fn legacy_closed_clean_candidate_cannot_push_or_recreate_workspace() {
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    f.commit_task();
    f.prepare();
    legacy_close(&f);
    for step in [
        IntegrationStep::Fetch,
        IntegrationStep::Prepare,
        IntegrationStep::Build,
        IntegrationStep::Push,
    ] {
        assert_eq!(
            f.execute(step).unwrap_err().public_code(),
            IntegrationCode::IntegrationWorkspaceMissing.as_str()
        );
    }
    assert!(matches!(
        f.execute(IntegrationStep::Repair).unwrap(),
        HostIntegrationResponse::Blocked {
            code: IntegrationCode::IntegrationWorkspaceMissing,
            ..
        }
    ));
    assert!(!f.workspace().exists());
    assert_eq!(f.origin_tip(), target);
    assert_eq!(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .state(),
        TaskState::Closed
    );
}

fn step_request(step: IntegrationStep, record: IntegrationRecord) -> HostIntegrationRequest {
    HostIntegrationRequest {
        protocol_version: 7,
        task_id: record.task_id,
        integration_id: Some(record.snapshot.integration_id),
        epoch: record.snapshot.epoch,
        revision: record.snapshot.revision,
        action: HostIntegrationAction::Step {
            step,
            record: Box::new(record),
        },
    }
}

#[test]
fn clean_candidate_reply_roundtrips_and_is_scripted_through_the_host_port() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let candidate = sample_candidate(&record);
    let request = step_request(IntegrationStep::Fetch, record.clone());
    let response = HostIntegrationResponse::CandidateReady {
        identity: IntegrationResponseIdentity::for_request(&request),
        candidate: Box::new(candidate.clone()),
    };
    response.validate_for(&request).unwrap();
    assert_eq!(
        decode_host_response(&encode_host_response(&response).unwrap()).unwrap(),
        response
    );
    let mut wire = serde_json::to_value(&response).unwrap();
    assert_eq!(wire["response"], "candidate_ready");
    wire["extra"] = serde_json::json!(true);
    assert!(serde_json::from_value::<HostIntegrationResponse>(wire).is_err());
    let f = IntegrationFixture::new();
    f.host().push_response(response.clone());
    assert_eq!(f.host().execute(&request).unwrap(), response);
    let mut replay_record = record;
    replay_record.candidates.push(candidate);
    let replay = step_request(IntegrationStep::Build, replay_record);
    f.set_host_response(response.clone());
    assert_eq!(f.host().execute(&replay).unwrap(), response);
    assert_eq!(f.host_calls(), vec![request, replay]);
}

#[test]
fn clean_candidate_reply_rejects_nonstep_rebound_source_and_spent_attempts() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let request = step_request(IntegrationStep::Fetch, record.clone());
    let response = HostIntegrationResponse::CandidateReady {
        identity: IntegrationResponseIdentity::for_request(&request),
        candidate: Box::new(sample_candidate(&record)),
    };
    let mut read = request.clone();
    read.action = HostIntegrationAction::Read;
    assert!(response.validate_for(&read).is_err());
    let arm = HostIntegrationRequest {
        protocol_version: 7,
        task_id: record.task_id,
        integration_id: None,
        epoch: 0,
        revision: IntegrationRevision(0),
        action: HostIntegrationAction::Arm {
            policy: record.policy.clone(),
        },
    };
    let mut arm_response = response.clone();
    if let HostIntegrationResponse::CandidateReady { identity, .. } = &mut arm_response {
        *identity = IntegrationResponseIdentity::for_request(&arm);
    }
    assert!(arm_response.validate_for(&arm).is_err());
    let mut wrong = response.clone();
    if let HostIntegrationResponse::CandidateReady { candidate, .. } = &mut wrong {
        let head = "f".repeat(40).parse().unwrap();
        candidate.source_head = head;
        candidate.attribute_source = candidate.source_head.clone();
        candidate.ours = candidate.source_head.clone();
        candidate.clean_h.head = candidate.source_head.clone();
        candidate.validate().unwrap();
    }
    assert!(wrong.validate_for(&request).is_err());
    let mut full = record.clone();
    for attempt in 1..=MAX_CANDIDATES as u8 {
        let mut candidate = sample_candidate(&record);
        candidate.id.attempt = attempt;
        full.candidates.push(candidate);
    }
    let mut reused = response;
    if let HostIntegrationResponse::CandidateReady { candidate, .. } = &mut reused {
        candidate.message = "Rebound frozen message".into();
    }
    assert!(
        reused
            .validate_for(&step_request(IntegrationStep::Build, full))
            .is_err()
    );
}

#[test]
fn policy_arm_roundtrip_preserves_protocol_seven_and_preintent_reply_identity() {
    let request = HostIntegrationRequest {
        protocol_version: 7,
        task_id: fixture_task(),
        integration_id: None,
        epoch: 0,
        revision: IntegrationRevision(0),
        action: HostIntegrationAction::Arm {
            policy: sample_policy("main"),
        },
    };
    assert_eq!(
        decode_host_request(&encode_host_request(&request).unwrap()).unwrap(),
        request
    );
    let response = HostIntegrationResponse::Progress {
        identity: IntegrationResponseIdentity::for_request(&request),
        snapshot: None,
    };
    response.validate_for(&request).unwrap();
    let mut wrong = request.clone();
    wrong.task_id = mac_worker::test_support::task::model::TaskId::new(uuid::Uuid::from_u128(9));
    assert!(response.validate_for(&wrong).is_err());
    wrong = request;
    wrong.protocol_version = 6;
    assert!(encode_host_request(&wrong).is_err());
}

#[test]
fn all_rpc_codecs_enforce_the_exact_raw_frame_limit() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let response = HostIntegrationResponse::Blocked {
        identity: IntegrationResponseIdentity {
            protocol_version: 7,
            task_id: record.task_id,
            integration_id: Some(record.snapshot.integration_id),
            epoch: 0,
            revision: IntegrationRevision(1),
        },
        code: IntegrationCode::IntegrationNetwork,
        retry_exhausted: false,
    };
    let mut bytes = encode_host_response(&response).unwrap();
    bytes.resize(MAX_INTEGRATION_RPC_BYTES, b' ');
    assert_eq!(decode_host_response(&bytes).unwrap(), response);
    bytes.push(b' ');
    assert!(decode_host_response(&bytes).is_err());
    assert!(decode_host_request(&bytes).is_err());
}

#[test]
fn prephase_revoke_rejects_other_ids_and_sources_and_survives_gc_replay() {
    use mac_worker::test_support::{
        host::job::JobId,
        task::model::{TaskOutcome, TurnSummary, TurnTerminal},
    };
    let mut f = GitIntegrationFixture::new();
    let origin = f.commit_base();
    f.commit_task();
    let mut arm = request(
        &f,
        HostIntegrationAction::Arm {
            policy: f.record.policy.clone(),
        },
    );
    arm.integration_id = None;
    arm.revision = IntegrationRevision(0);
    execute(&f, &arm).unwrap();
    let stop = request(
        &f,
        HostIntegrationAction::Revoke {
            tombstone: IntegrationTombstone {
                epoch: 0,
                revision: f.record.snapshot.revision,
                requested_at_millis: 1002,
                acknowledged: false,
            },
        },
    );
    execute(&f, &stop).unwrap();
    let task_dir = f
        .store
        .task_dir(&f.record.policy.project_id, f.record.task_id)
        .unwrap();
    let proof = task_dir.join("integration/revoke.json");
    let before = std::fs::read(&proof).unwrap();
    let mut wrong_id = stop.clone();
    wrong_id.integration_id = Some(
        IntegrationId::derive(
            f.record.task_id,
            fixture_source(),
            &fixture_head(),
            &f.record.target_key,
        )
        .unwrap(),
    );
    assert_ne!(wrong_id.integration_id, stop.integration_id);
    assert_eq!(
        execute(&f, &wrong_id).unwrap_err().public_code(),
        "INTEGRATION_STATE_INVALID"
    );
    assert_eq!(std::fs::read(&proof).unwrap(), before);
    let mut status = serde_json::to_value(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap(),
    )
    .unwrap();
    let next_source = JobId::new(uuid::Uuid::from_u128(222));
    status["turns"].as_array_mut().unwrap().push(
        serde_json::to_value(TurnSummary::new(
            2,
            next_source,
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            Some(false),
            false,
            Some(1003),
            Some(1004),
        ))
        .unwrap(),
    );
    let status: TaskStatus = serde_json::from_value(status).unwrap();
    std::fs::write(
        task_dir.join("status.json"),
        serde_json::to_vec(&status).unwrap(),
    )
    .unwrap();
    let mut next_stop = stop.clone();
    next_stop.integration_id = Some(
        IntegrationId::derive(
            f.record.task_id,
            next_source,
            status.head_oid().unwrap(),
            &f.record.target_key,
        )
        .unwrap(),
    );
    execute(&f, &next_stop).unwrap();
    let next_proof = std::fs::read(&proof).unwrap();
    assert_eq!(
        execute(&f, &stop).unwrap_err().public_code(),
        "INTEGRATION_STATE_INVALID"
    );
    assert_eq!(std::fs::read(&proof).unwrap(), next_proof);
    HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(1001 + TASK_RETENTION_MILLIS + 1)
        .unwrap();
    execute(&f, &next_stop).unwrap();
    assert_eq!(std::fs::read(&proof).unwrap(), next_proof);
    let reopened = mac_worker::test_support::host::store::HostStore::open(f.store.root()).unwrap();
    let late =
        HostIntegrationService::new(&reopened, &SystemProcessRunner, &f.runtime).execute(&request(
            &f,
            HostIntegrationAction::Step {
                step: IntegrationStep::Prepare,
                record: Box::new(f.record.clone()),
            },
        ));
    eprintln!("delayed revoked source Prepare after replacement proof and GC: {late:?}");
    if late.is_ok() {
        f.record = HostIntegrationStore::new(&f.store)
            .load(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .unwrap();
        f.push();
    }
    eprintln!(
        "origin before stopped-cycle replay: {origin}; origin afterward: {}",
        f.origin_tip()
    );
    assert_eq!(
        f.origin_tip(),
        origin,
        "a later source's stop proof erased the earlier acknowledged fence and allowed its push"
    );
    assert_eq!(late.unwrap_err().public_code(), "INTEGRATION_STATE_INVALID");
    legacy_close(&f);
    HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(1001 + TASK_RETENTION_MILLIS * 2 + 10000)
        .unwrap();
    assert!(
        !task_dir.exists(),
        "closed expired task metadata survived GC"
    );
    assert!(execute(&f, &stop).is_err());
    assert!(execute(&f, &next_stop).is_err());
    assert!(f.execute(IntegrationStep::Prepare).is_err());
    assert!(!f.workspace().exists());
    assert_eq!(f.origin_tip(), origin);
}
