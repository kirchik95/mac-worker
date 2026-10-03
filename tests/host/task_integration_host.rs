use mac_worker::test_support::integration::*;

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
