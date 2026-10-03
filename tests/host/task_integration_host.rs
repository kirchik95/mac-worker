use mac_worker::test_support::integration::*;

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
