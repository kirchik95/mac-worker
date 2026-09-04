#[allow(dead_code)]
mod support;

use std::{
    ffi::CString,
    fs,
    os::unix::{
        ffi::OsStrExt,
        fs::{PermissionsExt, symlink},
    },
    sync::mpsc,
    thread,
    time::Duration,
};

use mac_worker::{
    agent::AgentKind,
    client_state::ClientStateStore,
    config::Config,
    error::{ExitKind, WorkerError},
    paths::PathLayout,
    process::SystemProcessRunner,
    project_config::{ProjectSettings, ResourceClass},
    requirements::RequirementDetector,
    task_client::TaskClient,
    turn_runner::InlineRunnerExecutor,
};
use tempfile::tempdir;

use support::GitRepo;

#[test]
fn absent_project_settings_file_uses_v1_defaults() {
    // This catches treating optional project configuration as mandatory, or
    // changing a default that controls unconfigured projects.
    let repo = GitRepo::init();

    let settings = ProjectSettings::load(repo.root(), &[]).unwrap();

    assert_eq!(settings.requires, Vec::<String>::new());
    assert_eq!(settings.resource_class, ResourceClass::Heavy);
    assert_eq!(settings.timeout, Duration::from_secs(1800));
    assert_eq!(settings.snapshot.include_untracked, Vec::<String>::new());
    assert_eq!(settings.snapshot.include_empty_dirs, Vec::<String>::new());
    assert_eq!(settings.snapshot.allow_sensitive, Vec::<String>::new());
    assert_eq!(settings.artifacts.include, Vec::<String>::new());
    assert_eq!(settings.artifacts.max_total_bytes, None);
    assert_eq!(settings.task.default_agent, "codex");
    assert_eq!(settings.task.timeout, Duration::from_secs(45 * 60));
    assert_eq!(settings.task.max_followups, 10);
}

#[test]
fn project_task_settings_parse_defaults_and_reject_deferred_capabilities() {
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        br#"version = 1
[task]
source = "local"
publish = ["fetch"]
default_agent = "codex"
timeout = "45m"
max_followups = 7
"#,
    );
    let settings = ProjectSettings::load(repo.root(), &[]).unwrap();
    assert_eq!(settings.task.max_followups, 7);

    for task in [
        "[task]\nsource = \"origin\"\n",
        "[task]\npublish = [\"push\"]\n",
        "[task]\nunknown = true\n",
    ] {
        let repo = GitRepo::init();
        repo.write(".worker.toml", task.as_bytes());
        let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();
        assert_eq!(error.public_code(), "TASK_CONFIG_INVALID");
    }
}

#[test]
fn task_client_uses_the_project_default_agent_when_cli_omits_one() {
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\n[task]\ndefault_agent = \"claude\"\n",
    );
    let state_root = tempdir().unwrap();
    let state_root_path = state_root.path().canonicalize().unwrap();
    let paths = PathLayout {
        config: repo.root().join("config.toml"),
        state: state_root_path.join("state"),
        cache: state_root_path.join("cache"),
        data: state_root_path.join("data"),
    };
    let state = ClientStateStore::open(&paths.state).unwrap();
    let config = Config::parse(include_str!("../config.example.toml")).unwrap();
    let runner = SystemProcessRunner;
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&runner, &config, &paths, &state, &executor);

    assert_eq!(
        client.default_task_agent(repo.root()).unwrap(),
        AgentKind::Claude
    );
}

#[test]
fn project_settings_reject_non_regular_config_as_usage() {
    // This catches treating an unsafe final component as a generic local I/O
    // failure instead of failing closed as invalid project policy.
    let repo = GitRepo::init();
    fs::create_dir(repo.root().join(".worker.toml")).unwrap();

    let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();

    assert!(matches!(error, WorkerError::Config(_)), "{error}");
    assert_eq!(error.exit_kind(), ExitKind::Usage);
}

#[test]
fn project_settings_preserve_regular_file_open_failures_as_io() {
    // This catches the no-follow hardening flattening a genuine regular-file
    // access failure into a usage-class policy error.
    // SAFETY: geteuid has no arguments and no memory-safety contract.
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let repo = GitRepo::init();
    let config = repo.root().join(".worker.toml");
    fs::write(&config, b"version = 1\n").unwrap();
    fs::set_permissions(&config, fs::Permissions::from_mode(0o000)).unwrap();

    let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();

    assert!(matches!(error, WorkerError::Io(_)), "{error}");
    assert_eq!(error.exit_kind(), ExitKind::Io);
}

#[test]
fn project_settings_reject_fifo_config_without_waiting_for_a_writer() {
    // This catches opening a special file without O_NONBLOCK and hanging the
    // command before its file type can be rejected.
    let repo = GitRepo::init();
    let config = repo.root().join(".worker.toml");
    let config_name = CString::new(config.as_os_str().as_bytes()).unwrap();
    // SAFETY: the CString is NUL-terminated and live for this call.
    assert_eq!(unsafe { libc::mkfifo(config_name.as_ptr(), 0o600) }, 0);
    let root = repo.root().to_path_buf();
    let (sender, receiver) = mpsc::channel();

    let handle = thread::spawn(move || {
        let _ = sender.send(ProjectSettings::load(&root, &[]));
    });
    let result = receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("project settings load blocked on a FIFO");
    handle.join().unwrap();
    let error = result.unwrap_err();

    assert!(matches!(error, WorkerError::Config(_)), "{error}");
    assert_eq!(error.exit_kind(), ExitKind::Usage);
}

#[test]
fn project_settings_reject_final_symlinks_without_reading_external_policy() {
    // This catches a final symlink authorizing sensitive project input from a
    // policy file that would not be present in the captured worktree.
    let repo = GitRepo::init();
    let external = tempdir().unwrap();
    let secret = "EXTERNAL_POLICY_SECRET_7f1e2f90";
    let project_secret = "PROJECT_ENV_SECRET_a74f083e";
    let external_policy = external.path().join("outside-policy.toml");
    fs::write(
        &external_policy,
        format!("version = 1\n# {secret}\n[snapshot]\nallow_sensitive = [\".env\"]\n"),
    )
    .unwrap();
    symlink(&external_policy, repo.root().join(".worker.toml")).unwrap();
    repo.write(".env", format!("TOKEN={project_secret}\n").as_bytes());

    let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();
    let display = error.to_string();
    let debug = format!("{error:?}");

    assert!(matches!(error, WorkerError::Config(_)), "{display}");
    assert_eq!(error.exit_kind(), ExitKind::Usage);
    for forbidden in [
        secret,
        project_secret,
        external_policy.to_string_lossy().as_ref(),
        repo.root().to_string_lossy().as_ref(),
        "allow_sensitive",
    ] {
        assert!(!display.contains(forbidden), "display leaked {forbidden:?}");
        assert!(!debug.contains(forbidden), "debug leaked {forbidden:?}");
    }
}

#[test]
fn project_settings_reject_dangling_final_symlinks_as_usage() {
    // This catches treating a dangling policy symlink as an absent optional
    // config and silently falling back to permissive-independent defaults.
    let repo = GitRepo::init();
    let missing_target = repo.root().join("outside-missing-policy.toml");
    symlink(&missing_target, repo.root().join(".worker.toml")).unwrap();

    let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();
    let diagnostics = format!("{error}\n{error:?}");

    assert!(matches!(error, WorkerError::Config(_)), "{diagnostics}");
    assert_eq!(error.exit_kind(), ExitKind::Usage);
    assert!(!diagnostics.contains(&missing_target.to_string_lossy().into_owned()));
    assert!(!diagnostics.contains(&repo.root().to_string_lossy().into_owned()));
}

#[test]
fn malformed_project_config_diagnostics_do_not_echo_source_or_secrets() {
    // This catches TOML parser snippets and semantic validation values reaching
    // Display/Debug/CLI error surfaces.
    let cases = [
        (
            "requires = [\"node\"\n# MALFORMED_CONFIG_SECRET_8af36e72\n",
            "MALFORMED_CONFIG_SECRET_8af36e72",
        ),
        (
            "requires = [\"SEMANTIC_CONFIG_SECRET_2e09d4b1\"]\n",
            "SEMANTIC_CONFIG_SECRET_2e09d4b1",
        ),
    ];

    for (contents, secret) in cases {
        let repo = GitRepo::init();
        repo.write(".worker.toml", contents.as_bytes());

        let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();
        let diagnostics = format!("{error}\n{error:?}");

        assert!(matches!(error, WorkerError::Config(_)), "{diagnostics}");
        assert_eq!(error.exit_kind(), ExitKind::Usage);
        assert!(!diagnostics.contains(secret), "diagnostics leaked {secret}");
        assert!(
            !diagnostics.contains(contents),
            "diagnostics echoed TOML source"
        );
        assert!(!diagnostics.contains(&repo.root().to_string_lossy().into_owned()));
    }
}

#[test]
fn project_settings_merge_explicit_and_cli_snapshot_inputs() {
    // This catches losing the file include or putting a CLI include before it.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        br#"version = 1
requires = ["darwin-arm64", "node"]
resource_class = "heavy"
timeout = "30m"

[snapshot]
include_untracked = ["fixtures/generated/**", "fixtures/generated/**"]
include_empty_dirs = ["fixtures/empty"]
allow_sensitive = ["fixtures/test.env"]

[artifacts]
include = ["coverage/**"]
max_total_bytes = 536870912
"#,
    );

    let settings = ProjectSettings::load(
        repo.root(),
        &[
            "tmp/contract.json".to_owned(),
            "fixtures/generated/**".to_owned(),
        ],
    )
    .unwrap();

    assert_eq!(settings.requires, vec!["darwin-arm64", "node"]);
    assert_eq!(settings.resource_class, ResourceClass::Heavy);
    assert_eq!(settings.timeout, Duration::from_secs(1800));
    assert_eq!(
        settings.snapshot.include_untracked,
        vec!["fixtures/generated/**", "tmp/contract.json"]
    );
    assert_eq!(
        settings.snapshot.include_empty_dirs,
        vec!["fixtures/empty".to_owned()]
    );
    assert_eq!(
        settings.snapshot.allow_sensitive,
        vec!["fixtures/test.env".to_owned()]
    );
    assert_eq!(settings.artifacts.include, vec!["coverage/**"]);
    assert_eq!(settings.artifacts.max_total_bytes, Some(536_870_912));
}

#[test]
fn project_settings_reject_invalid_values_and_unknown_fields_at_each_level() {
    // This catches permissive deserialization and accepting inputs that cannot
    // safely describe a project boundary.
    let cases = [
        ("requires = [\"node\", \"node\"]", "duplicate requirements"),
        ("requires = [\"Node\"]", "uppercase requirement"),
        ("timeout = \"half an hour\"", "invalid duration"),
        ("resource_class = \"small\"", "invalid resource class"),
        ("unexpected = true", "unknown root field"),
        ("[snapshot]\nunexpected = true", "unknown snapshot field"),
        ("[artifacts]\nunexpected = true", "unknown artifacts field"),
        (
            "[snapshot]\ninclude_untracked = [\"**/generated\"]",
            "glob first component",
        ),
        (
            "[snapshot]\ninclude_untracked = [\"fixtures/../generated/**\"]",
            "parent path component",
        ),
        (
            "[snapshot]\ninclude_untracked = [\"/fixtures/generated/**\"]",
            "absolute include path",
        ),
        (
            "[snapshot]\ninclude_empty_dirs = [\"fixtures/*\"]",
            "glob empty directory path",
        ),
        (
            "[snapshot]\nallow_sensitive = [\"fixtures/*.env\"]",
            "glob sensitive allowlist path",
        ),
    ];

    for (contents, name) in cases {
        let repo = GitRepo::init();
        repo.write(".worker.toml", contents.as_bytes());

        let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();

        assert!(matches!(error, WorkerError::Config(_)), "{name}: {error}");
    }
}

#[test]
fn requirement_detector_reports_root_level_regular_file_indicators_once_in_lexical_order() {
    // This catches missing mappings, recursive source scanning, and unstable
    // capability output when several files imply one capability.
    let root = tempdir().unwrap();
    for name in [
        "package.json",
        "package-lock.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "Gemfile",
        ".ruby-version",
        "pyproject.toml",
        "requirements.txt",
        ".python-version",
        "go.mod",
        "Package.swift",
        "global.json",
        "solution.sln",
        "app.csproj",
        "Dockerfile",
        "compose.yaml",
        "compose.yml",
        "playwright.config.js",
        "playwright.config.ts",
    ] {
        fs::write(root.path().join(name), b"indicator").unwrap();
    }
    fs::create_dir(root.path().join("nested")).unwrap();
    fs::write(root.path().join("nested/package.json"), b"ignored").unwrap();

    let requirements = RequirementDetector::detect(root.path()).unwrap();

    assert_eq!(
        requirements,
        vec![
            "browser", "docker", "dotnet", "go", "node", "python", "ruby", "swift",
        ]
    );
}

#[test]
fn requirement_detector_ignores_a_symlinked_indicator_file() {
    // This catches following a path controlled outside the project root during
    // requirement detection.
    let root = tempdir().unwrap();
    let target = root.path().join("outside-package.json");
    fs::write(&target, b"indicator").unwrap();
    symlink(&target, root.path().join("package.json")).unwrap();

    let requirements = RequirementDetector::detect(root.path()).unwrap();

    assert_eq!(requirements, Vec::<String>::new());
}
