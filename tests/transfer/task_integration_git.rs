use mac_worker::test_support::integration::*;

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
