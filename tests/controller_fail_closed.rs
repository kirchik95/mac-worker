//! Fail-closed controller acceptance boundaries.
//!
//! Protocol-only proofs run against the actual `worker host controller-rpc`
//! child on this base. Full enabled-CLI routing waits for the combined
//! runtime (`MAC_WORKER_TEST_SSH` + `run_enabled_controller_task`); tests
//! stay un-ignored.

#[path = "controller_fail_closed_harness.rs"]
mod harness;

use harness::{
    IsolatedHomes, REQUEST_ID, REQUEST_ID_OTHER, TASK_ID, controller_request_files, frame_json,
    framed_host_error, frozen_submit_request, host_controller_rpc, init_git_project,
    laptop_task_authority, oversize_length_prefix, run_worker, ssh_log_entries,
    ssh_logged_commands, ssh_saw_controller_rpc,
};
use mac_worker::{controller::encode_frame, protocol::PROTOCOL_VERSION};
use serde_json::json;

fn assert_no_laptop_task_authority(homes: &IsolatedHomes) {
    let paths = homes.paths();
    assert!(
        !laptop_task_authority(&paths),
        "laptop ClientStateStore task/queue/runner files must not be created; state={:?} files={:?}",
        paths.state,
        authority_files(&paths)
    );
}

fn authority_files(paths: &mac_worker::paths::PathLayout) -> Vec<String> {
    let mut found = Vec::new();
    for name in ["tasks", "queue", "runners", "turns", "runs", "dags"] {
        let root = paths.state.join(name);
        collect_regular_files(&root, &root, &mut found);
    }
    found
}

fn collect_regular_files(root: &std::path::Path, base: &std::path::Path, found: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_regular_files(&path, base, found);
        } else if path.is_file() {
            found.push(path.strip_prefix(base).unwrap_or(&path).display().to_string());
        }
    }
}

fn assert_rpc_child_typed_error(output: &harness::ChildOutput, code: &str) {
    assert!(
        !output.status.success(),
        "rpc child must fail closed; stdout={} stderr={}",
        output.stdout_lossy(),
        output.stderr_lossy()
    );
    let error = framed_host_error(&output.stdout);
    assert_eq!(error.protocol_version(), PROTOCOL_VERSION);
    assert_eq!(
        error.error().code(),
        code,
        "stderr={}",
        output.stderr_lossy()
    );
}

#[test]
fn rpc_child_rejects_incompatible_protocol_version_before_side_effects() {
    let homes = IsolatedHomes::enabled_controller();
    for version in [6_u64, 8, 0] {
        let mut value = frozen_submit_request(REQUEST_ID, "version");
        value["protocol_version"] = json!(version);
        let output = host_controller_rpc(&homes, &frame_json(&value));
        assert_rpc_child_typed_error(&output, "INCOMPATIBLE_PROTOCOL");
        let paths = homes.controller_paths();
        assert!(
            controller_request_files(&paths).is_empty(),
            "incompatible protocol wrote {:?}",
            controller_request_files(&paths)
        );
        assert_no_laptop_task_authority(&homes);
    }
}

#[test]
fn rpc_child_rejects_oversized_frame_before_side_effects() {
    let homes = IsolatedHomes::enabled_controller();
    let output = host_controller_rpc(&homes, &oversize_length_prefix());
    assert_rpc_child_typed_error(&output, "CONTROLLER_TRANSPORT");
    let error = framed_host_error(&output.stdout);
    assert!(
        error.error().message().contains("1 MiB") || error.error().message().contains("frame"),
        "oversize message={}",
        error.error().message()
    );
    let paths = homes.controller_paths();
    assert!(controller_request_files(&paths).is_empty());
    assert_no_laptop_task_authority(&homes);
}

#[test]
fn rpc_child_rejects_malformed_and_duplicate_key_requests_before_side_effects() {
    let homes = IsolatedHomes::enabled_controller();
    let malformed = encode_frame(b"{not-json").unwrap();
    let output = host_controller_rpc(&homes, &malformed);
    assert_rpc_child_typed_error(&output, "CONTROLLER_TRANSPORT");
    assert_no_laptop_task_authority(&homes);
    assert!(controller_request_files(&homes.controller_paths()).is_empty());

    let duplicate = encode_frame(
        format!(
            r#"{{"protocol_version":{version},"protocol_version":{version},"request_id":"{REQUEST_ID}","command":"task.submit","body":{{}}}}"#,
            version = PROTOCOL_VERSION
        )
        .as_bytes(),
    )
    .unwrap();
    let output = host_controller_rpc(&homes, &duplicate);
    assert_rpc_child_typed_error(&output, "INVALID_REQUEST");
    assert_no_laptop_task_authority(&homes);
    assert!(controller_request_files(&homes.controller_paths()).is_empty());
}

#[test]
fn rpc_child_conflicts_same_request_id_with_a_different_frozen_body() {
    let homes = IsolatedHomes::enabled_controller();
    let first = host_controller_rpc(
        &homes,
        &frame_json(&frozen_submit_request(REQUEST_ID, "one")),
    );
    let second = host_controller_rpc(
        &homes,
        &frame_json(&frozen_submit_request(REQUEST_ID, "two")),
    );
    assert_rpc_child_typed_error(&second, "CONTROLLER_REQUEST_CONFLICT");
    assert_no_laptop_task_authority(&homes);
    let _ = first;
}

#[test]
fn rpc_child_rejects_frozen_submit_without_source_receipt() {
    let homes = IsolatedHomes::enabled_controller();
    let output = host_controller_rpc(
        &homes,
        &frame_json(&frozen_submit_request(REQUEST_ID_OTHER, "missing-source")),
    );
    assert!(
        !output.status.success(),
        "missing source must not fake-ACK; stdout={} stderr={}",
        output.stdout_lossy(),
        output.stderr_lossy()
    );
    let error = framed_host_error(&output.stdout);
    let code = error.error().code();
    assert!(
        matches!(code, "INVALID_COMPONENT" | "BASE_UNAVAILABLE"),
        "typed missing-source code, got {code} message={}",
        error.error().message()
    );
    assert_ne!(code, "CONTROLLER_UNAVAILABLE");
    let payload = mac_worker::controller::decode_frame(&output.stdout).unwrap();
    let value: serde_json::Value = serde_json::from_slice(payload).unwrap();
    assert_ne!(
        value.get("status").and_then(|status| status.as_str()),
        Some("acked")
    );
    assert_no_laptop_task_authority(&homes);
}

fn assert_enabled_cli_fail_closed(output: &harness::ChildOutput, homes: &IsolatedHomes) {
    assert!(
        !output.status.success(),
        "enabled controller must fail closed; stdout={} stderr={}",
        output.stdout_lossy(),
        output.stderr_lossy()
    );
    let diagnostic = format!("{}{}", output.stdout_lossy(), output.stderr_lossy());
    let code = output.json_code();
    let typed = code.as_deref().unwrap_or("");
    assert!(
        diagnostic.contains("CONTROLLER_UNAVAILABLE")
            || diagnostic.contains("CONTROLLER_TRANSPORT")
            || typed == "CONTROLLER_UNAVAILABLE"
            || typed == "CONTROLLER_TRANSPORT",
        "expected typed controller failure, stdout={} stderr={}",
        output.stdout_lossy(),
        output.stderr_lossy()
    );
    assert!(
        !diagnostic.contains("not routed to the controller yet"),
        "placeholder UNAVAILABLE is not finished routing; stdout={} stderr={}",
        output.stdout_lossy(),
        output.stderr_lossy()
    );
    assert_no_laptop_task_authority(homes);
}

#[test]
fn enabled_cli_submit_wip_outage_does_not_create_laptop_task_authority() {
    let homes = IsolatedHomes::enabled_controller();
    let project = init_git_project();
    let output = run_worker(
        &homes,
        &[
            "--json",
            "task",
            "submit",
            "--prompt",
            "fail closed wip",
            "--wip",
            "--no-wait",
            "--project",
            project.path().to_str().unwrap(),
        ],
        true,
        Some(project.path()),
        None,
    );
    assert_enabled_cli_fail_closed(&output, &homes);
    assert!(
        ssh_saw_controller_rpc(&homes),
        "submit must hop host controller-rpc; log={:?}",
        ssh_log_entries(&homes)
    );
    let commands = ssh_logged_commands(&homes);
    assert!(
        commands
            .iter()
            .any(|command| command == "task.submit"
                || command == "controller.transfer.source.prepare"),
        "expected submit/source RPC, got {commands:?}"
    );
}

#[test]
fn enabled_cli_read_wait_reconcile_route_to_rpc_and_do_not_fallback() {
    let homes = IsolatedHomes::enabled_controller();
    let routes: &[(&[&str], &str)] = &[
        (&["--json", "task", "list"], "task.list"),
        (&["--json", "task", "status", TASK_ID], "task.status"),
        (&["--json", "task", "logs", TASK_ID], "task.logs"),
        (
            &[
                "--json",
                "task",
                "wait",
                "--task-id",
                TASK_ID,
                "--timeout",
                "5s",
            ],
            "task.wait.poll",
        ),
        (&["--json", "task", "reconcile"], "task.reconcile"),
    ];
    for (args, command) in routes {
        let before = ssh_logged_commands(&homes);
        let output = run_worker(&homes, args, true, None, None);
        assert_enabled_cli_fail_closed(&output, &homes);
        assert!(
            ssh_saw_controller_rpc(&homes),
            "{args:?} must use host controller-rpc; log={:?}",
            ssh_log_entries(&homes)
        );
        let after = ssh_logged_commands(&homes);
        assert!(
            after.iter().any(|logged| logged == *command)
                || after
                    .iter()
                    .skip(before.len())
                    .any(|logged| logged == *command),
            "{args:?} must record {command}, got {after:?}"
        );
    }
}

#[test]
fn enabled_cli_mutations_route_to_rpc_not_placeholder_unavailable() {
    let homes = IsolatedHomes::enabled_controller();
    let routes: &[(&[&str], &str)] = &[
        (
            &["--json", "task", "say", TASK_ID, "--message", "follow-up"],
            "task.say",
        ),
        (&["--json", "task", "cancel", TASK_ID], "task.cancel"),
        (&["--json", "task", "close", TASK_ID], "task.close"),
    ];
    for (args, command) in routes {
        let output = run_worker(&homes, args, true, None, None);
        assert_enabled_cli_fail_closed(&output, &homes);
        assert!(
            ssh_saw_controller_rpc(&homes),
            "{args:?} placeholder UNAVAILABLE is not routing; log={:?}",
            ssh_log_entries(&homes)
        );
        assert!(
            ssh_logged_commands(&homes)
                .iter()
                .any(|logged| logged == *command),
            "{args:?} must record {command}, got {:?}",
            ssh_logged_commands(&homes)
        );
    }
}

#[test]
fn default_disabled_task_list_stays_local_and_does_not_invoke_fake_ssh() {
    let homes = IsolatedHomes::default_disabled();
    let output = run_worker(&homes, &["--json", "task", "list"], true, None, None);
    assert!(
        output.status.success(),
        "default-disabled list must remain local; stdout={} stderr={}",
        output.stdout_lossy(),
        output.stderr_lossy()
    );
    assert!(ssh_log_entries(&homes).is_empty());
}
