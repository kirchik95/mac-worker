use mac_worker::test_support::integration::*;

#[test]
fn fast_forwardable_task_still_gets_one_merge_with_exact_lease() {
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    let task = f.commit_task();
    let merge = f.prepare();
    f.push();
    assert_eq!(f.parents(&merge), vec![target, task]);
    assert_eq!(f.origin_tip(), merge);
}

#[test]
fn built_in_minus_merge_conflicts_in_bare_and_workspace() {
    let mut f = GitIntegrationFixture::new();
    f.write(".gitattributes", b"payload.txt -merge\n");
    f.write("payload.txt", b"base\n");
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    let head = f.commit_task();
    let target = f.advance_target_with("payload.txt", b"theirs\n");
    let response = f.execute(IntegrationStep::Prepare).unwrap();
    let HostIntegrationResponse::NeedTurn {
        candidate, purpose, ..
    } = response
    else {
        panic!("expected resolver, got {response:?}")
    };
    assert_eq!(purpose, IntegrationTurnPurpose::Resolve);
    assert_eq!(candidate.conflict_paths, vec!["payload.txt"]);
    assert_eq!(f.git(&["rev-parse", "HEAD"]), head.as_str());
    assert_eq!(f.git(&["rev-parse", "MERGE_HEAD"]), target.as_str());
    assert!(!f.git(&["ls-files", "--unmerged"]).is_empty());
}

#[test]
fn union_uses_h_as_ours_in_mirror_and_pinned_verify_workspace() {
    let mut f = GitIntegrationFixture::new();
    f.write(".gitattributes", b"payload.txt merge=union\n");
    f.write("payload.txt", b"base\n");
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    f.commit_task();
    f.advance_target_with("payload.txt", b"theirs\n");
    f.record.policy.verify = VerifyPolicy::MovedTarget;
    let response = f.execute(IntegrationStep::Prepare).unwrap();
    let HostIntegrationResponse::NeedTurn {
        candidate, purpose, ..
    } = response
    else {
        panic!("expected verifier, got {response:?}")
    };
    assert_eq!(purpose, IntegrationTurnPurpose::Verify);
    assert_eq!(f.git(&["write-tree"]), candidate.tree_oid.unwrap().as_str());
    assert_eq!(
        std::fs::read(f.workspace().join("payload.txt")).unwrap(),
        b"ours\ntheirs\n"
    );
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
