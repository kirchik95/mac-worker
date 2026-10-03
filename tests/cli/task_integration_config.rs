use mac_worker::test_support::integration::*;
use mac_worker::test_support::task::model::ClosePolicy;

fn target(name: &str) -> IntegrationOverride {
    IntegrationOverride::Target(validate_integration_target(name).unwrap())
}

fn project_policy() -> IntegrationProjectPolicy {
    let policy = sample_policy("main");
    IntegrationProjectPolicy {
        settings: IntegrationPolicySettings::default(),
        project_id: policy.project_id,
        base_oid: policy.base_oid,
        base_task: None,
    }
}

#[test]
fn policy_precedence_freezes_requested_close_and_defaults() {
    let mut project = project_policy();
    let resolve = |project: &IntegrationProjectPolicy,
                   batch: Option<&IntegrationPolicySettings>,
                   task: &IntegrationOverride,
                   verify| {
        resolve_integration_policy(
            project,
            batch,
            task,
            verify,
            ClosePolicy::Done,
            "https://example.test/repo.git",
            IntegrationBaseKind::Committed,
        )
    };
    assert!(
        resolve(&project, None, &IntegrationOverride::Inherit, None)
            .unwrap()
            .is_none()
    );
    project.settings.integrate = target("project");
    project.settings.verify_merge = Some(VerifyPolicy::MovedTarget);
    let batch = IntegrationPolicySettings {
        integrate: target("batch"),
        verify_merge: Some(VerifyPolicy::Never),
    };
    for (defaults, override_, verify, branch, expected_verify) in [
        (
            None,
            IntegrationOverride::Inherit,
            None,
            "project",
            VerifyPolicy::MovedTarget,
        ),
        (
            Some(&batch),
            IntegrationOverride::Inherit,
            None,
            "batch",
            VerifyPolicy::Never,
        ),
        (
            Some(&batch),
            target("task"),
            Some(VerifyPolicy::MovedTarget),
            "task",
            VerifyPolicy::MovedTarget,
        ),
    ] {
        let frozen = resolve(&project, defaults, &override_, verify)
            .unwrap()
            .unwrap();
        assert_eq!(frozen.target.as_str(), branch);
        assert_eq!(frozen.verify, expected_verify);
        assert_eq!(frozen.requested_close, ClosePolicy::Done);
        assert_eq!(frozen.base_oid, project.base_oid);
        assert_eq!(frozen.base_preflight, IntegrationBasePreflight::Unknown);
    }
    assert!(
        resolve(&project, Some(&batch), &IntegrationOverride::Disabled, None)
            .unwrap()
            .is_none()
    );
    let disabled_batch = IntegrationPolicySettings {
        integrate: IntegrationOverride::Disabled,
        verify_merge: None,
    };
    assert!(
        resolve(
            &project,
            Some(&disabled_batch),
            &IntegrationOverride::Inherit,
            None
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(
        resolve(
            &project_policy(),
            None,
            &IntegrationOverride::Inherit,
            Some(VerifyPolicy::Never)
        )
        .unwrap_err()
        .public_code(),
        "TASK_CONFIG_INVALID"
    );
    assert!(
        resolve(
            &project,
            None,
            &IntegrationOverride::Disabled,
            Some(VerifyPolicy::MovedTarget)
        )
        .is_err()
    );
}

#[test]
fn resolution_requires_origin_and_valid_base_provenance() {
    let mut project = project_policy();
    project.settings.integrate = target("main");
    let mut overlong = project.clone();
    // General BranchName remains uncapped; only integration freeze has a bound.
    overlong.settings.integrate = IntegrationOverride::Target("a".repeat(256).parse().unwrap());
    assert_eq!(
        resolve_integration_policy(
            &overlong,
            None,
            &IntegrationOverride::Inherit,
            None,
            ClosePolicy::Never,
            "https://example.test/repo.git",
            IntegrationBaseKind::Committed
        )
        .unwrap_err()
        .public_code(),
        "TASK_CONFIG_INVALID"
    );
    let resolve = |project: &IntegrationProjectPolicy, origin, kind| {
        resolve_integration_policy(
            project,
            None,
            &IntegrationOverride::Inherit,
            None,
            ClosePolicy::Never,
            origin,
            kind,
        )
    };
    assert_eq!(
        resolve(&project, "", IntegrationBaseKind::Committed)
            .unwrap_err()
            .public_code(),
        "TASK_CONFIG_INVALID"
    );
    project.base_oid = None;
    assert!(
        resolve(
            &project,
            "https://example.test/repo.git",
            IntegrationBaseKind::Committed
        )
        .is_err()
    );
    assert!(
        resolve(
            &project,
            "https://example.test/repo.git",
            IntegrationBaseKind::FromTask
        )
        .is_err()
    );
    project.base_task = Some(fixture_task());
    let frozen = resolve(
        &project,
        "https://example.test/repo.git",
        IntegrationBaseKind::FromTask,
    )
    .unwrap()
    .unwrap();
    assert_eq!(frozen.base_task, Some(fixture_task()));
    assert_eq!(frozen.base_preflight, IntegrationBasePreflight::Unknown);
}

#[test]
fn cli_parses_opt_in_disable_redrive_and_hidden_forms() {
    use clap::Parser;
    use mac_worker::test_support::cli::{Cli, Command, TaskCommand, into_command};
    for args in [
        vec!["--integrate", "main", "--verify-merge", "moved-target"],
        vec!["--no-integrate"],
    ] {
        let mut argv = vec!["worker", "task", "submit", "--prompt", "work"];
        argv.extend(args);
        let parsed = Cli::try_parse_from(argv).unwrap();
        assert!(matches!(
            into_command(parsed),
            Command::Task {
                command: TaskCommand::Submit { .. }
            }
        ));
    }
    for args in [
        vec!["--integrate", "main", "--no-integrate"],
        vec!["--integrate", "refs/heads/main"],
        vec!["--verify-merge", "always"],
    ] {
        let mut argv = vec!["worker", "task", "submit", "--prompt", "work"];
        argv.extend(args);
        assert!(Cli::try_parse_from(argv).is_err());
    }
    for argv in [
        vec![
            "worker",
            "task",
            "integrate",
            "00000000000000000000000000000002",
        ],
        vec!["worker", "host", "task-integration"],
        vec!["worker", "host", "task-integration-turn"],
        vec![
            "worker",
            "integration-runner",
            "00000000000000000000000000000002",
        ],
    ] {
        assert!(Cli::try_parse_from(argv).is_ok());
    }
}

#[test]
fn project_settings_parse_optional_integration_without_global_default() {
    use mac_worker::test_support::task::project_config::ProjectSettings;
    let root = tempfile::tempdir().unwrap();
    assert!(ProjectSettings::load(root.path(), &[]).is_ok());
    for input in [
        "[task]\nintegrate = 'main'\nverify_merge = 'moved-target'",
        "[task]\nintegrate = false",
    ] {
        std::fs::write(root.path().join(".worker.toml"), input).unwrap();
        assert!(ProjectSettings::load(root.path(), &[]).is_ok());
    }
    for input in [
        "[task]\nintegrate = true",
        "[task]\nintegrate = 'refs/heads/main'",
        "[task]\nverify_merge = 'never'",
    ] {
        std::fs::write(root.path().join(".worker.toml"), input).unwrap();
        assert!(ProjectSettings::load(root.path(), &[]).is_err());
    }
}

struct PreflightRunner {
    replies: std::sync::Mutex<std::collections::VecDeque<(i32, Vec<u8>)>>,
    requests: std::sync::Mutex<Vec<mac_worker::test_support::host::process::ProcessRequest>>,
}
impl PreflightRunner {
    fn new(replies: Vec<(i32, Vec<u8>)>) -> Self {
        Self {
            replies: std::sync::Mutex::new(replies.into()),
            requests: Default::default(),
        }
    }
}
impl mac_worker::test_support::host::process::ProcessRunner for PreflightRunner {
    fn run(
        &self,
        request: &mac_worker::test_support::host::process::ProcessRequest,
    ) -> Result<
        mac_worker::test_support::host::process::ProcessResult,
        mac_worker::test_support::core::error::WorkerError,
    > {
        use std::os::unix::process::ExitStatusExt;
        self.requests.lock().unwrap().push(request.clone());
        let (code, stdout) = if request.args.iter().any(|arg| arg == "config") {
            (0, b"filter.fixture.clean\0filter.fixture.smudge\0filter.fixture.process\0filter.fixture.required\0merge.fixture.driver\0merge.fixture.recursive\0merge.union.driver\0".to_vec())
        } else {
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected command")
        };
        Ok(mac_worker::test_support::host::process::ProcessResult {
            status: std::process::ExitStatus::from_raw(code << 8),
            stdout,
            stderr: vec![],
        })
    }
}

#[test]
fn submit_preflight_is_one_bounded_exact_branch_read_and_local_ancestry_only() {
    use mac_worker::test_support::host::rooted_fs::RootedDir;
    let root = tempfile::tempdir().unwrap();
    let repo = RootedDir::open(root.path()).unwrap();
    let branch = validate_integration_target("main").unwrap();
    let advertised = format!("{}\trefs/heads/main\n", "c".repeat(40)).into_bytes();
    for (replies, expected) in [
        (
            vec![
                (0, advertised.clone()),
                (0, b"false\n".to_vec()),
                (0, b"3\n".to_vec()),
                (0, vec![]),
            ],
            Ok(IntegrationBasePreflight::Pass),
        ),
        (
            vec![
                (0, advertised.clone()),
                (0, b"false\n".to_vec()),
                (0, b"3\n".to_vec()),
                (1, vec![]),
            ],
            Err("INTEGRATION_BASE_NOT_ON_TARGET"),
        ),
        (
            vec![(0, advertised.clone()), (0, b"true\n".to_vec())],
            Ok(IntegrationBasePreflight::Unknown),
        ),
        (
            vec![
                (0, advertised.clone()),
                (0, b"false\n".to_vec()),
                (128, vec![]),
            ],
            Ok(IntegrationBasePreflight::Unknown),
        ),
        (vec![(128, vec![])], Ok(IntegrationBasePreflight::Unknown)),
        (vec![(0, vec![])], Err("INTEGRATION_TARGET_MISSING")),
        (
            vec![(0, b"malformed\n".to_vec())],
            Ok(IntegrationBasePreflight::Unknown),
        ),
    ] {
        let runner = PreflightRunner::new(replies);
        let result = preflight_integration_base(
            &runner,
            "https://example.test/repo.git",
            &branch,
            Some(&fixture_head()),
            &repo,
        )
        .map_err(|error| error.public_code());
        assert_eq!(result, expected.map_err(str::to_owned));
        let requests = runner.requests.lock().unwrap();
        let remote = requests
            .iter()
            .filter(|request| request.args.iter().any(|arg| arg == "ls-remote"))
            .collect::<Vec<_>>();
        assert_eq!(remote.len(), 1);
        assert_eq!(remote[0].args.last().unwrap(), "refs/heads/main");
        assert_eq!(
            remote[0].policy.deadline,
            std::time::Duration::from_secs(30)
        );
        assert_eq!(remote[0].policy.stdout_limit, 8 * 1024 * 1024);
        assert_eq!(remote[0].policy.stderr_limit, 64 * 1024);
        for request in requests.iter() {
            for option in [
                "gc.auto=0",
                "core.hooksPath=/dev/null",
                "core.fsmonitor=false",
                "commit.gpgSign=false",
                "submodule.recurse=false",
                "merge.autoStash=false",
                "merge.verifySignatures=false",
                "fetch.recurseSubmodules=false",
                "push.followTags=false",
                "push.recurseSubmodules=no",
                "core.attributesFile=/dev/null",
                "core.logAllRefUpdates=false",
                "core.fsync=objects,derived-metadata,reference",
                "core.fsyncMethod=fsync",
            ] {
                assert!(
                    request
                        .args
                        .windows(2)
                        .any(|pair| pair[0] == "-c" && pair[1] == option),
                    "missing integration hardening override {option}"
                );
            }
            if !request.args.iter().any(|arg| arg == "config") {
                for option in [
                    "filter.fixture.clean=",
                    "filter.fixture.smudge=",
                    "filter.fixture.process=",
                    "filter.fixture.required=false",
                    "merge.fixture.driver=/usr/bin/git merge-file %A %O %B",
                    "merge.fixture.recursive=text",
                    "merge.union.driver=/usr/bin/git merge-file --union %A %O %B",
                    "merge.union.recursive=union",
                ] {
                    assert!(
                        request
                            .args
                            .windows(2)
                            .any(|pair| pair[0] == "-c" && pair[1] == option),
                        "missing integration driver override {option}"
                    );
                }
            }
            for (key, value) in [
                ("GIT_ATTR_NOSYSTEM", "1"),
                ("GIT_CONFIG_GLOBAL", "/dev/null"),
                ("GIT_CONFIG_NOSYSTEM", "1"),
                ("GIT_TERMINAL_PROMPT", "0"),
                ("GIT_NO_LAZY_FETCH", "1"),
                ("GIT_NO_REPLACE_OBJECTS", "1"),
                ("GIT_GRAFT_FILE", "/dev/null"),
            ] {
                assert!(
                    request
                        .environment
                        .iter()
                        .any(|(name, configured)| name == key && configured == value),
                    "missing integration environment {key}"
                );
            }
            assert!(
                request
                    .environment_remove
                    .iter()
                    .any(|name| name == "GIT_NAMESPACE")
            );
            assert!(
                request
                    .environment_remove
                    .iter()
                    .any(|name| name == "GIT_SHALLOW_FILE")
            );
            assert!(!request.args.iter().any(|arg| arg == "fetch"
                || arg == "update-ref"
                || arg.to_string_lossy().contains("refs/mac-worker/")));
        }
    }
    let runner = PreflightRunner::new(vec![(0, advertised)]);
    assert_eq!(
        preflight_integration_base(
            &runner,
            "https://example.test/repo.git",
            &branch,
            None,
            &repo
        )
        .unwrap(),
        IntegrationBasePreflight::Unknown
    );
    assert_eq!(runner.requests.lock().unwrap().len(), 2); // One local driver read and one advertisement.
}

#[test]
fn real_git_preflight_proves_ancestry_not_tracking_ref_or_equality() {
    use mac_worker::test_support::host::{process::SystemProcessRunner, rooted_fs::RootedDir};
    let repo = crate::support::GitRepo::init();
    repo.write("base", b"base");
    repo.commit_all("base");
    let base = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    repo.write("target", b"target");
    repo.commit_all("target descendant");
    let path = std::fs::canonicalize(repo.root()).unwrap();
    let origin = url::Url::from_file_path(&path).unwrap().to_string();
    let root = RootedDir::open(&path).unwrap();
    let branch = validate_integration_target("main").unwrap();
    assert_eq!(
        preflight_integration_base(&SystemProcessRunner, &origin, &branch, Some(&base), &root)
            .unwrap(),
        IntegrationBasePreflight::Pass
    );
    assert_eq!(
        preflight_integration_base(
            &SystemProcessRunner,
            &origin,
            &validate_integration_target("absent").unwrap(),
            Some(&base),
            &root
        )
        .unwrap_err()
        .public_code(),
        "INTEGRATION_TARGET_MISSING"
    );
    assert!(
        repo.git(&["checkout", "--orphan", "unpublished"])
            .status
            .success()
    );
    assert!(repo.git(&["rm", "-rf", "."]).status.success());
    repo.write("private", b"private");
    repo.commit_all("unpublished input");
    let private = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(
        preflight_integration_base(
            &SystemProcessRunner,
            &origin,
            &branch,
            Some(&private),
            &root
        )
        .unwrap_err()
        .public_code(),
        "INTEGRATION_BASE_NOT_ON_TARGET"
    );
}

#[test]
fn preview_prints_effective_per_task_target_verify_and_disable() {
    use mac_worker::test_support::{
        core::config::Config, host::process::SystemProcessRunner, task::client::preview_batch_plan,
    };
    let repo = crate::support::GitRepo::init();
    repo.write("base", b"base");
    repo.commit_all("base");
    assert!(
        repo.git(&["remote", "add", "origin", "https://example.test/repo.git"])
            .status
            .success()
    );
    repo.write(
        ".worker.toml",
        b"[task]\nintegrate = 'project'\nverify_merge = 'moved-target'\n",
    );
    repo.write("batch.toml", b"integrate = 'batch'\n[[tasks]]\nprompt = 'inherit'\n[[tasks]]\nprompt = 'override'\nintegrate = 'task'\nverify_merge = 'never'\n[[tasks]]\nprompt = 'disabled'\nintegrate = false\n");
    let config: Config = toml::from_str("version = 1").unwrap();
    let preview = preview_batch_plan(
        &SystemProcessRunner,
        &config,
        &repo.root().join("batch.toml"),
        repo.root(),
    )
    .unwrap();
    let wire = serde_json::to_value(preview).unwrap();
    assert_eq!(
        wire["tasks"][0]["integration"],
        serde_json::json!({"target":"batch","verify":"moved-target"})
    );
    assert_eq!(
        wire["tasks"][1]["integration"],
        serde_json::json!({"target":"task","verify":"never"})
    );
    assert!(wire["tasks"][2].get("integration").is_none());
}

#[test]
fn explicit_controller_redrive_refuses_unwired_support_without_rpc_or_durable_rows() {
    use clap::Parser;
    use mac_worker::test_support::{
        cli::Cli,
        runtime::{RuntimeContext, run_with_io_in_context},
    };
    let root = tempfile::tempdir().unwrap();
    let home = std::fs::canonicalize(root.path()).unwrap();
    let config = home.join("config.toml");
    std::fs::write(
        &config,
        "version = 1\n[controller]\nenabled = true\nssh = 'fixture.invalid'\n",
    )
    .unwrap();
    let cli = Cli::try_parse_from([
        "worker",
        "--config",
        config.to_str().unwrap(),
        "task",
        "integrate",
        "00000000000000000000000000000002",
    ])
    .unwrap();
    let runtime = RuntimeContext::isolated(Default::default(), home.clone(), home.clone());
    let runner = PreflightRunner::new(vec![]);
    let mut stderr = Vec::new();
    let exit = run_with_io_in_context(cli, &runner, &runtime, &mut Vec::new(), &mut stderr);
    assert_eq!(exit, 69);
    assert!(
        String::from_utf8(stderr)
            .unwrap()
            .contains("INTEGRATION_UNAVAILABLE")
    );
    assert!(runner.requests.lock().unwrap().is_empty());
    assert!(!home.join(".local/state/mac-worker-controller").exists());
}

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

#[test]
fn batch_accepts_integration_inputs_at_flat_table_and_task_levels() {
    use mac_worker::test_support::integration::BatchFile;
    for input in [
        "integrate = 'main'\nverify_merge = 'moved-target'\n[[tasks]]\nprompt = 'work'",
        "[defaults]\nintegrate = false\nverify_merge = 'never'\n[[tasks]]\nprompt = 'work'",
        "[[tasks]]\nprompt = 'work'\nintegrate = 'release/é'\nverify_merge = 'never'",
    ] {
        let parsed = toml::from_str::<BatchFile>(input).unwrap();
        if input.starts_with("integrate") {
            assert!(
                matches!(&parsed.defaults.integrate,IntegrationOverride::Target(branch) if branch.as_str()=="main")
            );
            assert_eq!(
                parsed.defaults.verify_merge,
                Some(VerifyPolicy::MovedTarget)
            );
        } else if input.starts_with("[defaults]") {
            assert_eq!(parsed.defaults.integrate, IntegrationOverride::Disabled);
            assert_eq!(parsed.defaults.verify_merge, Some(VerifyPolicy::Never));
        } else {
            assert!(
                matches!(&parsed.tasks[0].integrate,IntegrationOverride::Target(branch) if branch.as_str()=="release/é")
            );
            assert_eq!(parsed.tasks[0].verify_merge, Some(VerifyPolicy::Never));
            assert_eq!(parsed.defaults.integrate, IntegrationOverride::Inherit);
        }
    }
}

#[test]
fn batch_new_flat_defaults_still_exclude_a_defaults_table() {
    use mac_worker::test_support::integration::BatchFile;
    for input in [
        "integrate = false\n[defaults]\nagent = 'codex'\n[[tasks]]\nprompt = 'work'",
        "verify_merge = 'never'\n[defaults]\nagent = 'codex'\n[[tasks]]\nprompt = 'work'",
    ] {
        let error = toml::from_str::<BatchFile>(input).unwrap_err().to_string();
        assert!(
            error.contains("either a [defaults] table or top-level keys"),
            "{error}"
        );
    }
}
