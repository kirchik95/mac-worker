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
fn candidate_ready_requires_a_tree_and_a_merge_pin_from_build_onward() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    for step in [
        IntegrationStep::Fetch,
        IntegrationStep::Prepare,
        IntegrationStep::AcceptTurn,
        IntegrationStep::Build,
        IntegrationStep::Push,
        IntegrationStep::Repair,
    ] {
        let request = step_request(step, record.clone());
        let mut candidate = sample_candidate(&record);
        candidate.merge_oid = None;
        let ready = HostIntegrationResponse::CandidateReady {
            identity: IntegrationResponseIdentity::for_request(&request),
            candidate: Box::new(candidate.clone()),
        };
        assert_eq!(
            ready.validate_for(&request).is_ok(),
            matches!(
                step,
                IntegrationStep::Fetch | IntegrationStep::Prepare | IntegrationStep::AcceptTurn
            )
        );
        candidate.tree_oid = None;
        let missing_tree = HostIntegrationResponse::CandidateReady {
            identity: IntegrationResponseIdentity::for_request(&request),
            candidate: Box::new(candidate),
        };
        assert!(encode_host_response(&missing_tree).is_err());
    }
}

#[test]
fn candidate_ready_completes_absent_pins_once_and_keeps_all_frozen_inputs() {
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    let candidate = sample_candidate(&record);
    let mut partial = candidate.clone();
    partial.tree_oid = None;
    partial.merge_oid = None;
    record.candidates.push(partial);
    let request = step_request(IntegrationStep::Build, record.clone());
    let ready = HostIntegrationResponse::CandidateReady {
        identity: IntegrationResponseIdentity::for_request(&request),
        candidate: Box::new(candidate.clone()),
    };
    ready.validate_for(&request).unwrap();
    for field in [
        "tree_oid",
        "merge_oid",
        "target_head",
        "message",
        "identity",
        "timestamp_millis",
        "conflict_paths",
        "clean_h",
    ] {
        let mut frozen = record.clone();
        frozen.candidates[0] = candidate.clone();
        let mut wire = serde_json::to_value(&candidate).unwrap();
        wire[field] = match field {
            "target_head" => {
                wire["theirs"] = serde_json::json!("f".repeat(40));
                serde_json::json!("f".repeat(40))
            }
            "tree_oid" | "merge_oid" => serde_json::json!("f".repeat(40)),
            "message" => serde_json::json!("Different commit message"),
            "identity" => serde_json::json!({"name":"Other", "email":"other@example.test"}),
            "timestamp_millis" => serde_json::json!(1005),
            "conflict_paths" => serde_json::json!(["different.txt"]),
            "clean_h" => {
                let mut value = wire[field].clone();
                value["untracked_files"] = serde_json::json!(["different.txt"]);
                value
            }
            _ => unreachable!(),
        };
        let changed: IntegrationCandidate = serde_json::from_value(wire).unwrap();
        let response = HostIntegrationResponse::CandidateReady {
            identity: IntegrationResponseIdentity::for_request(&request),
            candidate: Box::new(changed),
        };
        assert!(
            response
                .validate_for(&step_request(IntegrationStep::Build, frozen))
                .is_err(),
            "{field}"
        );
        if !matches!(field, "tree_oid" | "merge_oid") {
            assert!(response.validate_for(&request).is_err(), "partial {field}");
        }
    }
    record.candidates[0].tree_oid = candidate.tree_oid.clone();
    ready
        .validate_for(&step_request(IntegrationStep::Build, record.clone()))
        .unwrap();
    record.candidates[0] = candidate;
    ready
        .validate_for(&step_request(IntegrationStep::Build, record))
        .unwrap();
}

#[test]
fn candidate_freezes_h_attributes_and_h_t_merge_orientation() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let candidate = sample_candidate(&record);
    candidate.validate().unwrap();
    assert_eq!(candidate.attribute_source, candidate.source_head);
    assert_eq!(candidate.ours, candidate.source_head);
    assert_eq!(candidate.theirs, candidate.target_head);
    for field in ["attribute_source", "ours", "theirs"] {
        let mut wire = serde_json::to_value(&candidate).unwrap();
        wire[field] = serde_json::json!("f".repeat(40));
        assert!(
            serde_json::from_value::<IntegrationCandidate>(wire).is_err(),
            "{field}"
        );
    }
}

#[test]
fn conflicts_and_prompts_are_bounded_without_dropping_paths() {
    let path = "a".repeat(MAX_CONFLICT_PATH_BYTES);
    assert!(validate_conflict_paths(std::slice::from_ref(&path)).is_ok());
    assert!(validate_conflict_paths(&[format!("{path}a")]).is_err());
    assert!(validate_conflict_paths(&vec!["a".into(); MAX_CONFLICT_PATHS + 1]).is_err());
    assert!(validate_conflict_paths(&vec![path; MAX_CONFLICT_PATHS]).is_err());
    for path in ["../escape", "/absolute", "a\0b", "a/../../b"] {
        assert!(validate_conflict_paths(&[path.into()]).is_err(), "{path}");
    }
    assert!(validate_prompt(&"p".repeat(MAX_PROMPT_BYTES)).is_ok());
    assert!(validate_prompt(&"p".repeat(MAX_PROMPT_BYTES + 1)).is_err());
    assert_eq!(MAX_MESSAGE_TITLE_BYTES, 120);
    assert_eq!(MAX_MESSAGE_SUMMARY_BYTES, 1024);
    assert_eq!(MAX_COMMIT_MESSAGE_BYTES, 2048);
    assert_eq!(GIT_OUTPUT_BYTES, 65536);
    assert_eq!(GIT_DEADLINE.as_secs(), 60);
    assert_eq!(HOST_DEADLINE.as_secs(), 90);
    assert_eq!(HELPER_LOOKUP_DEADLINE.as_secs(), 5);
}

#[test]
fn whole_conflict_list_counts_array_brackets_at_the_exact_prompt_bound() {
    let mut at = vec!["a".repeat(1024); 15];
    at.push("b".repeat(975));
    assert_eq!(serde_json::to_vec(&at).unwrap().len(), MAX_PROMPT_BYTES);
    validate_conflict_paths(&at).unwrap();
    at.last_mut().unwrap().push('b');
    assert_eq!(serde_json::to_vec(&at).unwrap().len(), MAX_PROMPT_BYTES + 1);
    assert!(validate_conflict_paths(&at).is_err());
}
