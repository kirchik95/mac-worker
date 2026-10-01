use base64::Engine;
use clap::Parser;
use mac_worker::test_support::{
    cli::Cli,
    core::error::WorkerError,
    host::process::{ProcessRequest, ProcessResult, ProcessRunner},
    runtime::{RuntimeContext, run_with_stdio_in_context},
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::Cursor,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    process::ExitStatus,
};
struct NoExternal;
impl ProcessRunner for NoExternal {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, "/bin/launchctl");
        assert_eq!(request.args[0], "print");
        Ok(ProcessResult {
            status: ExitStatus::from_raw(113 << 8),
            stdout: vec![],
            stderr: vec![],
        })
    }
}
fn key() -> String {
    let mut b = Vec::new();
    b.extend(11u32.to_be_bytes());
    b.extend(b"ssh-ed25519");
    b.extend(32u32.to_be_bytes());
    b.extend([9u8; 32]);
    format!(
        "ssh-ed25519 {}",
        base64::engine::general_purpose::STANDARD.encode(b)
    )
}
fn call(home: &std::path::Path, args: &[&str], body: serde_json::Value) -> (u8, serde_json::Value) {
    let runtime = RuntimeContext::isolated(
        BTreeMap::from([(OsString::from("HOME"), home.as_os_str().into())]),
        home.into(),
        home.into(),
    );
    let mut out = vec![];
    let mut err = vec![];
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from(args).unwrap(),
        &NoExternal,
        &runtime,
        &mut Cursor::new(serde_json::to_vec(&body).unwrap()),
        &mut out,
        &mut err,
    );
    (
        exit,
        serde_json::from_slice(&out).unwrap_or_else(|_| {
            panic!(
                "{} {}",
                String::from_utf8_lossy(&out),
                String::from_utf8_lossy(&err)
            )
        }),
    )
}
#[test]
fn hidden_authorization_preserves_keys_and_rejects_injection() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let args = ["worker", "host", "authorize-controller-key"];
    let (exit, response) = call(&home, &args, json!({"public_key":key()}));
    assert_eq!(exit, 0);
    assert_eq!(response["changed"], true);
    assert_eq!(
        call(&home, &args, json!({"public_key":key()})).1["changed"],
        false
    );
    assert_ne!(
        call(
            &home,
            &args,
            json!({"public_key":format!("{}\nevil",key())})
        )
        .0,
        0
    );
}
#[test]
fn hidden_existing_key_returns_identity_for_reviewable_boot_commands() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    fs::create_dir(home.join(".ssh")).unwrap();
    fs::set_permissions(home.join(".ssh"), fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        home.join(".ssh/mac-worker-controller_ed25519"),
        b"fixture-private",
    )
    .unwrap();
    fs::write(home.join(".ssh/mac-worker-controller_ed25519.pub"), key()).unwrap();
    let (exit, response) = call(&home, &["worker", "host", "controller-key"], json!({}));
    assert_eq!(exit, 0);
    assert_eq!(response["identity"]["home"], home.to_str().unwrap());
    assert!(
        !response["identity"]["username"]
            .as_str()
            .unwrap()
            .is_empty()
    );
}
#[test]
fn local_status_includes_service_and_drain_without_creating_controller_store() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let (exit, response) = call(
        &home,
        &["worker", "--json", "controller", "status"],
        json!({}),
    );
    assert_eq!(exit, 0);
    assert_eq!(response["service"]["loaded"], false);
    assert_eq!(response["drained"], false);
    assert!(!home.join(".local/state/mac-worker-controller").exists());
}
