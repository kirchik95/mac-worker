//! Build identity shown by `worker --version`.

use std::process::Command;

use mac_worker::build_id::BUILD_ID;

#[test]
fn worker_version_prints_the_baked_build_id() {
    let binary = env!("CARGO_BIN_EXE_worker");
    let output = Command::new(binary)
        .arg("--version")
        .output()
        .expect("worker --version");
    assert!(output.status.success(), "stderr: {:?}", output.stderr);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout, format!("worker {BUILD_ID}\n"));
}
