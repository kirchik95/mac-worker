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
