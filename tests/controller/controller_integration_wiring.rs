use mac_worker::test_support::integration::*;
use mac_worker::test_support::{
    controller::{
        acknowledge_controller_submit_pins, parse_request, reconcile_controller_submit_pins,
        record_controller_submit_pins, record_session_source_finished,
        require_session_source_finished,
    },
    core::paths::PathLayout,
    host::process::SystemProcessRunner,
    session::{SessionAgent, SessionImportMeta},
    transfer::repo::TransferRepo,
};
use serde_json::json;

fn isolated_paths(root: &std::path::Path) -> PathLayout {
    PathLayout {
        config: root.join("config.toml"),
        state: root.join("state/mac-worker"),
        cache: root.join("cache/mac-worker"),
        data: root.join("data/mac-worker"),
    }
}

fn wrapped_request(
    base: &str,
    package: &str,
) -> mac_worker::test_support::controller::ControllerRequest {
    let import =
        SessionImportMeta::new(SessionAgent::Codex, package.to_owned(), "0.160.0").unwrap();
    let submit: FrozenSubmitBody = serde_json::from_value(json!({
        "task_id": fixture_task(), "turn_id": fixture_source(), "created_at_millis": 1000,
        "prompt": "continue frozen session", "agent": "codex", "source": "local",
        "origin_url": "https://example.test/repo.git", "publish": ["fetch"],
        "close_on": "never", "wip": false, "project_id": "a".repeat(64),
        "worktree_id": "b".repeat(64), "base_oid": base, "timeout_millis": 2700000,
        "max_followups": 10, "permissions": "workspace", "requires": [],
        "include_untracked": [], "include_empty_dirs": [], "allow_sensitive": [],
        "cli_includes": [], "session_import": import
    }))
    .unwrap();
    parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": 7, "request_id": "00000000000000000000000000000041",
            "command": "task.submit-integrating", "body": {
                "submit": submit,
                // Cleanup must still see paired pins when admission rejects policy.
                "integration": {"malformed": true}
            }
        }))
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn nested_import_retry_requires_the_original_wrapper_source_finish() {
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let request = wrapped_request(&"b".repeat(40), &"d".repeat(40));
    assert_eq!(
        require_session_source_finished(&paths, &request)
            .unwrap_err()
            .public_code(),
        "CONTROLLER_SOURCE_CONFLICT"
    );
    record_session_source_finished(&paths, &request).unwrap();
    require_session_source_finished(&paths, &request).unwrap();
    let mut wire = json!({ "protocol_version": 7, "request_id": request.request_id(),
        "command": request.command(), "body": request.body() });
    wire["body"]["submit"]["prompt"] = json!("rebound source");
    let changed = parse_request(&serde_json::to_vec(&wire).unwrap()).unwrap();
    assert_eq!(
        require_session_source_finished(&paths, &changed)
            .unwrap_err()
            .public_code(),
        "CONTROLLER_SOURCE_CONFLICT"
    );
}

#[test]
fn acknowledged_nested_wrapper_releases_both_pins_and_replays_without_recapture() {
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let repo = crate::support::GitRepo::init();
    repo.write("README", b"base\n");
    repo.commit_all("base");
    let base = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    repo.write("README", b"package stand-in\n");
    repo.commit_all("package");
    let package = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    let transfer = TransferRepo::open_or_create(&paths.cache, &repo.root().join(".git")).unwrap();
    let base_ref = format!("refs/mac-worker/bases/{}", fixture_task());
    let session_ref = format!("refs/mac-worker/sessions/{}", fixture_task());
    repo.git(&[
        "--git-dir",
        transfer.path().to_str().unwrap(),
        "update-ref",
        &base_ref,
        &base,
    ]);
    transfer
        .pin_object(
            &SystemProcessRunner,
            &session_ref,
            &package.parse().unwrap(),
        )
        .unwrap();
    let request = wrapped_request(&base, &package);
    record_controller_submit_pins(&paths, &request, &transfer).unwrap();
    record_session_source_finished(&paths, &request).unwrap();
    acknowledge_controller_submit_pins(&paths.controller_cache_root(), &request).unwrap();
    reconcile_controller_submit_pins(&paths, &SystemProcessRunner, &mut std::io::sink());
    reconcile_controller_submit_pins(&paths, &SystemProcessRunner, &mut std::io::sink());
    for reference in [&base_ref, &session_ref] {
        let output = std::process::Command::new("/usr/bin/git")
            .arg("--git-dir")
            .arg(transfer.path())
            .args(["show-ref", "--verify", reference])
            .output()
            .unwrap();
        assert!(!output.status.success(), "leaked paired pin {reference}");
    }
    assert!(
        !paths
            .controller_cache_root()
            .join("submit-pin-retirements")
            .join(format!("{}.json", request.request_id()))
            .exists()
    );
}

#[test]
fn fetch_only_integrating_batch_freezes_own_origin_before_any_task_effect() {
    use mac_worker::test_support::{
        controller::freeze_laptop_batch, core::config::Config, task::model::ClosePolicy,
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let paths = isolated_paths(&root);
    let repo = crate::support::GitRepo::init();
    repo.write("README", b"base\n");
    repo.write(".worker.toml", b"[task]\nintegrate = 'main'\n");
    repo.commit_all("base");
    let origin = root.join("origin.git");
    repo.git(&["clone", "--bare", ".", origin.to_str().unwrap()]);
    let origin_url = format!("file://{}", origin.display());
    repo.git(&["remote", "add", "origin", &origin_url]);
    let batch = root.join("batch.toml");
    std::fs::write(&batch, "[[tasks]]\nid = 'enabled'\nprompt = 'work'\n[[tasks]]\nid = 'disabled'\nprompt = 'ordinary'\nintegrate = false\n").unwrap();
    let before = repo
        .git(&["for-each-ref", "--format=%(refname) %(objectname)"])
        .stdout;
    let frozen = freeze_laptop_batch(
        &SystemProcessRunner,
        &Config::parse("version = 1\nworkers = []\n").unwrap(),
        &paths,
        repo.root(),
        &batch,
        None,
        Some(1),
    )
    .unwrap();
    let enabled = &frozen.body().nodes["enabled"].frozen;
    assert_eq!(enabled.origin_url.as_deref(), Some(origin_url.as_str()));
    assert_eq!(enabled.close_on, ClosePolicy::Never);
    assert!(
        enabled
            .requires
            .contains(&"feature:task.integration".to_owned())
    );
    assert!(enabled.requires.contains(&"origin:file".to_owned()));
    assert_eq!(
        frozen.body().nodes["disabled"].frozen.close_on,
        ClosePolicy::Done
    );
    assert_eq!(
        repo.git(&["for-each-ref", "--format=%(refname) %(objectname)"])
            .stdout,
        before
    );
    assert!(
        !paths.state.exists(),
        "freeze created an ordinary task store"
    );
    assert!(frozen.body().nodes.values().all(|node| {
        serde_json::to_value(&node.frozen)
            .unwrap()
            .get("session_import")
            .is_none()
    }));
}

#[test]
fn host_command_arms_the_prepared_task_without_running_a_merge() {
    use mac_worker::test_support::{
        cli::{Command, HostCommand, from_parts},
        runtime::{RuntimeContext, run_with_stdio_in_context},
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let mut f = GitIntegrationFixture::at(root.join("data/mac-worker"));
    f.commit_base();
    f.commit_task();
    let origin_before = f.origin_tip();
    let runtime = RuntimeContext::isolated(
        [("XDG_DATA_HOME".into(), root.join("data").into_os_string())].into(),
        root.join("home"),
        root.clone(),
    );
    let request = HostIntegrationRequest {
        protocol_version: 7,
        task_id: f.record.task_id,
        integration_id: None,
        epoch: 0,
        revision: IntegrationRevision(0),
        action: HostIntegrationAction::Arm {
            policy: f.record.policy.clone(),
        },
    };
    let mut out = Vec::new();
    let mut err = Vec::new();
    let code = run_with_stdio_in_context(
        from_parts(
            None,
            true,
            Command::Host {
                command: HostCommand::TaskIntegration,
            },
        ),
        &SystemProcessRunner,
        &runtime,
        &mut std::io::Cursor::new(encode_host_request(&request).unwrap()),
        &mut out,
        &mut err,
    );
    assert_eq!(
        code,
        0,
        "out={} err={}",
        String::from_utf8_lossy(&out),
        String::from_utf8_lossy(&err)
    );
    let response: HostIntegrationResponse = serde_json::from_slice(&out).unwrap();
    response.validate_for(&request).unwrap();
    assert!(matches!(
        response,
        HostIntegrationResponse::Progress { snapshot: None, .. }
    ));
    let path = f
        .store
        .task_dir(&f.record.policy.project_id, f.record.task_id)
        .unwrap()
        .join("integration/policy.json");
    let policy: FrozenIntegrationPolicy =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(policy, f.record.policy);
    assert_eq!(f.origin_tip(), origin_before);
    assert!(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .state()
            == mac_worker::test_support::task::model::TaskState::Open
    );
}

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
