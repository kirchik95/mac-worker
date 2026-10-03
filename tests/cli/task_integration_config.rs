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
