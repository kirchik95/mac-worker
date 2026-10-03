use mac_worker::test_support::integration::{
    IntegrationOverride, VerifyPolicy, validate_integration_target,
};

#[test]
fn authoritative_target_accepts_255_bytes_and_rejects_256() {
    assert!(validate_integration_target(&"a".repeat(255)).is_ok());
    assert!(validate_integration_target(&"a".repeat(256)).is_err());
    assert!(validate_integration_target(&format!("{}a", "é".repeat(127))).is_ok());
    assert!(validate_integration_target(&"é".repeat(128)).is_err());
}

#[test]
fn target_requires_a_short_valid_branch() {
    for invalid in ["", "refs/heads/main", "-main", "a..b", "a/b.lock", "a b"] {
        assert!(validate_integration_target(invalid).is_err(), "{invalid:?}");
    }
    assert_eq!(
        validate_integration_target("release/é").unwrap().as_str(),
        "release/é"
    );
}

#[test]
fn override_accepts_only_branch_or_false() {
    assert_eq!(
        serde_json::from_str::<IntegrationOverride>("false").unwrap(),
        IntegrationOverride::Disabled
    );
    let target: IntegrationOverride = serde_json::from_str("\"main\"").unwrap();
    assert!(matches!(target, IntegrationOverride::Target(_)));
    for invalid in ["true", "null", "12", "\"refs/heads/main\""] {
        assert!(serde_json::from_str::<IntegrationOverride>(invalid).is_err());
    }
}

#[test]
fn verify_policy_defaults_to_never_and_has_exact_wire_names() {
    assert_eq!(VerifyPolicy::default(), VerifyPolicy::Never);
    assert_eq!(
        serde_json::to_string(&VerifyPolicy::MovedTarget).unwrap(),
        "\"moved-target\""
    );
    assert!(serde_json::from_str::<VerifyPolicy>("\"always\"").is_err());
}

#[test]
fn batch_accepts_integration_inputs_at_flat_table_and_task_levels() {
    use mac_worker::test_support::integration::BatchFile;
    for input in [
        "integrate = 'main'\nverify_merge = 'moved-target'\n[[tasks]]\nprompt = 'work'",
        "[defaults]\nintegrate = false\nverify_merge = 'never'\n[[tasks]]\nprompt = 'work'",
        "[[tasks]]\nprompt = 'work'\nintegrate = 'release/é'\nverify_merge = 'never'",
    ] {
        let parsed = toml::from_str::<BatchFile>(input).unwrap();
        if input.starts_with("integrate") {
            assert!(
                matches!(&parsed.defaults.integrate,IntegrationOverride::Target(branch) if branch.as_str()=="main")
            );
            assert_eq!(
                parsed.defaults.verify_merge,
                Some(VerifyPolicy::MovedTarget)
            );
        } else if input.starts_with("[defaults]") {
            assert_eq!(parsed.defaults.integrate, IntegrationOverride::Disabled);
            assert_eq!(parsed.defaults.verify_merge, Some(VerifyPolicy::Never));
        } else {
            assert!(
                matches!(&parsed.tasks[0].integrate,IntegrationOverride::Target(branch) if branch.as_str()=="release/é")
            );
            assert_eq!(parsed.tasks[0].verify_merge, Some(VerifyPolicy::Never));
            assert_eq!(parsed.defaults.integrate, IntegrationOverride::Inherit);
        }
    }
}

#[test]
fn batch_new_flat_defaults_still_exclude_a_defaults_table() {
    use mac_worker::test_support::integration::BatchFile;
    for input in [
        "integrate = false\n[defaults]\nagent = 'codex'\n[[tasks]]\nprompt = 'work'",
        "verify_merge = 'never'\n[defaults]\nagent = 'codex'\n[[tasks]]\nprompt = 'work'",
    ] {
        let error = toml::from_str::<BatchFile>(input).unwrap_err().to_string();
        assert!(
            error.contains("either a [defaults] table or top-level keys"),
            "{error}"
        );
    }
}
