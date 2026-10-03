use mac_worker::test_support::integration::*;

#[test]
fn controller_companion_read_contract_is_separate_strict_and_bounded() {
    let result = IntegrationReadResult {
        schema_version: 1,
        integrations: std::collections::BTreeMap::from([(fixture_task(), None)]),
    };
    result.validate().unwrap();
    let bytes = encode_bounded(&result, MAX_INTEGRATION_RPC_BYTES).unwrap();
    assert_eq!(
        decode_bounded::<IntegrationReadResult>(&bytes, MAX_INTEGRATION_RPC_BYTES).unwrap(),
        result
    );
    let mut invalid = serde_json::to_value(&result).unwrap();
    invalid["ordinary_status"] = serde_json::json!({});
    assert!(serde_json::from_value::<IntegrationReadResult>(invalid).is_err());
    let mut overflow = result;
    for seed in 3..=18 {
        overflow.integrations.insert(
            mac_worker::test_support::task::model::TaskId::new(uuid::Uuid::from_u128(seed)),
            None,
        );
    }
    assert!(overflow.validate().is_err());
    assert_eq!(HOST_FEATURE_INTEGRATION, "task.integration");
    assert_eq!(CONTROLLER_FEATURE_INTEGRATION, "controller.integration");
}
