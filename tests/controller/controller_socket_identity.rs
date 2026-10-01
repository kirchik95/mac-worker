//! T1 dependency gate, not rooted filesystem/image coverage (owned by T4).
use mac_worker::controller::channel::{pin::Pin, testing::identity_fixture};

#[test]
fn gate_pin_excludes_generation_and_journal_hints() {
    let identity = identity_fixture();
    let pin = Pin::from_identity(&identity);
    let bytes = serde_json::to_string(&pin).unwrap();
    assert!(!bytes.contains(identity.service.service_generation.as_str()));
    assert!(!bytes.contains(identity.service.journal_id.as_ref().unwrap().as_str()));
    assert!(bytes.contains(&identity.service.controller_client_id.to_string()));
    assert_eq!(serde_json::from_str::<Pin>(&bytes).unwrap(), pin);
}
