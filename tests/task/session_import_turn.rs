#[test]
fn session_interface_is_available() {
    assert_eq!(mac_worker::test_support::session::MANIFEST_SCHEMA, 1);
}
