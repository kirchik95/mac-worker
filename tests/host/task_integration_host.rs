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
    wire["state"] = serde_json::json!("closed");
    let status: TaskStatus = serde_json::from_value(wire).unwrap();
    std::fs::write(
        task.join("status.json"),
        serde_json::to_vec(&status).unwrap(),
    )
    .unwrap();
    std::fs::remove_dir_all(f.workspace()).unwrap();
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
