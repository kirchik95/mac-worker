use std::process::Command;

#[test]
fn spawned_worker_has_test_support_feature() {
    assert!(std::hint::black_box(cfg!(feature = "test-support")));
    assert_eq!(
        mac_worker::test_support::runtime::FEATURE_MARKER,
        "mac-worker:test-support=enabled"
    );
    let output = Command::new(env!("CARGO_BIN_EXE_worker"))
        .env_clear()
        .env("MAC_WORKER_TEST_SUPPORT_PROBE", "1")
        .arg("--version")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"mac-worker:test-support=enabled\n");
    assert!(output.stderr.is_empty());
}
