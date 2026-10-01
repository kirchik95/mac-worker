//! T1 contract gate; operator CLI wiring and help are implemented by T7b.
use mac_worker::controller::channel::{pin::Pin, testing::identity_fixture};

#[test]
fn gate_repin_input_has_a_strict_schema() {
    let mut value = serde_json::to_value(Pin::from_identity(&identity_fixture())).unwrap();
    value["force"] = serde_json::json!(true);
    assert!(serde_json::from_value::<Pin>(value).is_err());
}
