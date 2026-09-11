#[allow(dead_code)]
mod support;

use std::{
    ffi::CString,
    fs,
    os::unix::{
        ffi::OsStrExt,
        fs::{PermissionsExt, symlink},
    },
    sync::{Mutex, mpsc},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use mac_worker::{
    agent::AgentKind,
    client_state::ClientStateStore,
    config::Config,
    dag::DagNodeState,
    error::{ExitKind, WorkerError},
    job::AdmissionObservation,
    paths::PathLayout,
    process::SystemProcessRunner,
    project_config::{ProjectSettings, ResourceClass},
    requirements::RequirementDetector,
    scheduler::CandidateSlot,
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
    assert_eq!(settings.setup, None);
}

#[test]
fn project_task_settings_parse_defaults_and_accept_origin_push_capabilities() {
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

    let repo = GitRepo::init();
    repo.write(".worker.toml", b"[task]\nunknown = true\n");
    let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();
    assert_eq!(error.public_code(), "TASK_CONFIG_INVALID");

    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\n[task]\nsource = \"origin\"\npublish = [\"fetch\", \"push\"]\n",
    );
    let settings = ProjectSettings::load(repo.root(), &[]).unwrap();
    assert_eq!(settings.task.source, "origin");
    assert_eq!(settings.task.publish, vec!["fetch", "push"]);
}

#[test]
fn project_task_settings_carry_model_and_effort_and_reject_unsafe_effort() {
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        br#"version = 1
[task]
default_agent = "codex"
model = "gpt-5.6-luna"
effort = "max"
"#,
    );
    let settings = ProjectSettings::load(repo.root(), &[]).unwrap();
    assert_eq!(settings.task.model.as_deref(), Some("gpt-5.6-luna"));
    assert_eq!(settings.task.effort.as_deref(), Some("max"));

    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\n[task]\neffort = \"max\\\" -c sandbox_mode=\\\"danger-full-access\"\n",
    );
    let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();
    assert_eq!(error.public_code(), "TASK_CONFIG_INVALID");
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
        ("[setup]\nunexpected = true", "unknown setup field"),
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

#[test]
fn herdr_keys_parse_with_their_defaults_and_reject_unknown_neighbours() {
    use mac_worker::config::Config;

    let minimal =
        Config::parse("version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n")
            .unwrap();
    assert!(
        minimal.notifications.herdr,
        "laptop notifications default on"
    );
    assert!(!minimal.workers[0].herdr, "worker reporting defaults off");

    let explicit = Config::parse(
        "version = 1\n\n[notifications]\nherdr = false\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\nherdr = true\n",
    )
    .unwrap();
    assert!(!explicit.notifications.herdr);
    assert!(explicit.workers[0].herdr);

    assert!(
        Config::parse(
            "version = 1\n\n[notifications]\nherdr = true\nsound = true\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
        )
        .is_err(),
        "unknown notification keys are still rejected"
    );
    assert!(
        Config::parse(
            "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\nherdr = \"yes\"\n",
        )
        .is_err(),
        "the worker flag is a boolean"
    );

    let example = Config::parse(include_str!("../config.example.toml")).unwrap();
    assert!(example.notifications.herdr);
    assert!(!example.workers[0].herdr);
}

fn preview_config() -> Config {
    Config::parse("version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n")
        .unwrap()
}

#[test]
fn batch_preview_resolves_local_head_and_reports_dag_enforced() {
    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"fn main() {}\n");
    repo.commit_all("base");
    let oid = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    repo.write(
        "tasks.toml",
        br#"
version = 1
agent = "codex"

[[tasks]]
prompt = "fix login"
files = ["src/lib.rs"]
acceptance = ["cargo test"]
"#,
    );
    let report = mac_worker::task_client::preview_batch_plan(
        &SystemProcessRunner,
        &preview_config(),
        &repo.root().join("tasks.toml"),
        repo.root(),
    )
    .unwrap();
    assert!(!report.has_config_errors(), "{:?}", report.issues);
    assert!(report.dag.enforced);
    assert_eq!(report.dag.status, "enforced");
    assert_eq!(
        report.dag.message,
        "Dependencies execute when parents are Closed and Done."
    );
    assert_eq!(report.tasks[0].base, oid);
    assert_eq!(
        report.tasks[0].acceptance_role,
        "declared_agent_instruction"
    );
    assert_eq!(oid.len(), 40);
}

#[test]
fn batch_preview_reports_unknown_agent_missing_prompt_and_trailing_slash() {
    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"fn main() {}\n");
    repo.commit_all("base");
    repo.write(
        "tasks.toml",
        br#"
version = 1

[[tasks]]
agent = "unknown-agent"
prompt = "x"

[[tasks]]
files = ["src/"]

[[tasks]]
prompt = "ok"
files = ["src/lib.rs/"]
"#,
    );
    let report = mac_worker::task_client::preview_batch_plan(
        &SystemProcessRunner,
        &preview_config(),
        &repo.root().join("tasks.toml"),
        repo.root(),
    )
    .unwrap();
    assert!(report.has_config_errors());
    let kinds: Vec<_> = report.issues.iter().map(|issue| issue.kind).collect();
    assert!(kinds.contains(&"AGENT_UNSUPPORTED"), "{kinds:?}");
    assert!(kinds.contains(&"TASK_CONFIG_INVALID"), "{kinds:?}");
}

#[test]
fn batch_preview_cli_does_not_open_client_state() {
    use std::collections::BTreeMap;

    use clap::Parser;
    use mac_worker::{
        RuntimeContext,
        cli::{Cli, Command, TaskCommand},
    };

    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"fn main() {}\n");
    repo.commit_all("base");
    repo.write(
        "tasks.toml",
        br#"
version = 1
agent = "codex"
[[tasks]]
prompt = "fix login"
depends_on = ["missing"]
"#,
    );
    let home = tempdir().unwrap();
    let config_path = home.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::new(),
        home.path().to_path_buf(),
        repo.root().to_path_buf(),
    );
    let cli = Cli::try_parse_from([
        "worker",
        "--config",
        config_path.to_str().unwrap(),
        "--json",
        "task",
        "batch",
        repo.root().join("tasks.toml").to_str().unwrap(),
        "--preview",
    ])
    .unwrap();
    assert!(matches!(
        cli.command,
        Command::Task {
            command: TaskCommand::Batch { preview: true, .. }
        }
    ));
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = mac_worker::run_with_io_in_context(
        cli,
        &SystemProcessRunner,
        &runtime,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 1, "{}", String::from_utf8_lossy(&stderr));
    let report: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(report["dag"]["status"], "enforced");
    assert!(!home.path().join(".local/state/mac-worker").exists());
}

#[test]
fn batch_submit_executes_depends_on() {
    static CWD_LOCK: Mutex<()> = Mutex::new(());
    let _cwd_lock = CWD_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"fn main() {}\n");
    repo.commit_all("base");
    repo.write(
        "tasks.toml",
        br#"
version = 1
agent = "codex"
[[tasks]]
id = "a"
prompt = "a"
depends_on = ["b"]
[[tasks]]
id = "b"
prompt = "b"
"#,
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .publish_admission_observation(
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Idle,
                vec!["darwin-arm64".into(), "agent:codex".into()],
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
            .unwrap()
            .with_local_binding(
                "mac1".into(),
                "~/.local/bin/worker".into(),
                vec!["darwin-arm64".into()],
                1,
                Some(0),
                now,
            ),
        )
        .unwrap();
    let previous = std::env::current_dir().unwrap();
    std::env::set_current_dir(repo.root()).unwrap();
    struct Restore(std::path::PathBuf);
    impl Drop for Restore {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.0);
        }
    }
    let _cwd = Restore(previous);
    let runner = SystemProcessRunner;
    let executor = InlineRunnerExecutor;
    let config = preview_config();
    let client = TaskClient::new(&runner, &config, &paths, &state, &executor);
    let report = client
        .batch(
            &repo.root().join("tasks.toml"),
            None,
            None,
            &mut std::io::sink(),
        )
        .unwrap();
    let dag = state
        .load_run_dag(report.run_id())
        .unwrap()
        .expect("dependent batch writes a DAG");
    assert_eq!(dag.nodes["b"].state, DagNodeState::Submitted);
    assert_eq!(dag.nodes["a"].state, DagNodeState::Waiting);
    assert!(
        state
            .load_task_optional(dag.nodes["a"].task_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(report.task_ids(), &[dag.nodes["b"].task_id]);
}

#[test]
fn project_setup_recipe_is_opt_in_and_requires_lockfiles_to_exist() {
    let repo = GitRepo::init();
    repo.write("Cargo.lock", b"lock\n");
    repo.write(
        ".worker.toml",
        br#"version = 1
[setup]
timeout = "10m"
commands = ["cargo fetch --locked"]
check = "cargo fetch --locked --offline"
lockfiles = ["Cargo.lock"]
"#,
    );
    let settings = ProjectSettings::load(repo.root(), &[]).unwrap();
    let setup = settings.setup.expect("setup table");
    assert_eq!(setup.timeout, Duration::from_secs(600));
    assert_eq!(setup.commands, vec!["cargo fetch --locked"]);
    assert_eq!(
        setup.check.as_deref(),
        Some("cargo fetch --locked --offline")
    );
    assert_eq!(setup.lockfiles, vec!["Cargo.lock"]);

    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        br#"version = 1
[setup]
lockfiles = ["Cargo.lock"]
"#,
    );
    let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();
    assert!(matches!(error, WorkerError::Config(_)), "{error}");
}

// ---------------------------------------------------------------------------
// F1/F2: controller-only preview. `--preview` stays local, but with
// `[controller] enabled = true` the controller host owns the worker list, so
// preview must not reject pinned worker names against an empty laptop
// inventory. The human-mode line must also match the enforced DAG status.
// ---------------------------------------------------------------------------

fn controller_only_config() -> Config {
    Config::parse(
        "version = 1\n[controller]\nenabled = true\nssh = \"mac1\"\nremote_binary = \"~/.local/bin/worker\"\n",
    )
    .unwrap()
}

fn pinned_dag_batch(repo: &GitRepo) {
    repo.write(
        "tasks.toml",
        br#"
version = 1
agent = "opencode"
source = "local"
publish = ["fetch"]

[[tasks]]
id = "parent"
worker = "mini-3"
prompt = "parent work"

[[tasks]]
id = "child"
worker = "mini-2"
depends_on = ["parent"]
base = "from:parent"
prompt = "child work"
"#,
    );
}

/// F1: the controller host owns the dispatch inventory, so a controller-only
/// laptop config must preview a pinned batch cleanly instead of reporting
/// WORKER_NOT_FOUND for every pin.
#[test]
fn batch_preview_under_enabled_controller_keeps_pinned_workers() {
    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"fn main() {}\n");
    repo.commit_all("base");
    pinned_dag_batch(&repo);
    let report = mac_worker::task_client::preview_batch_plan(
        &SystemProcessRunner,
        &controller_only_config(),
        &repo.root().join("tasks.toml"),
        repo.root(),
    )
    .unwrap();
    let kinds: Vec<_> = report.issues.iter().map(|issue| issue.kind).collect();
    assert!(
        !kinds.contains(&"WORKER_NOT_FOUND"),
        "controller-owned pins must not be validated locally: {kinds:?}"
    );
    assert!(!report.has_config_errors(), "{:?}", report.issues);
    assert_eq!(report.tasks[0].worker.as_deref(), Some("mini-3"));
    assert_eq!(report.tasks[1].worker.as_deref(), Some("mini-2"));
    assert_eq!(report.dag.status, "enforced");
}

/// Over-fix guard: without a controller the laptop inventory IS the
/// authority, so an unknown pin must still be an error. Green before and
/// after the change.
#[test]
fn batch_preview_without_controller_still_rejects_unknown_worker() {
    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"fn main() {}\n");
    repo.commit_all("base");
    pinned_dag_batch(&repo);
    let report = mac_worker::task_client::preview_batch_plan(
        &SystemProcessRunner,
        &preview_config(),
        &repo.root().join("tasks.toml"),
        repo.root(),
    )
    .unwrap();
    assert!(report.has_config_errors());
    let kinds: Vec<_> = report.issues.iter().map(|issue| issue.kind).collect();
    assert!(kinds.contains(&"WORKER_NOT_FOUND"), "{kinds:?}");
}

/// F1 at the CLI seam: exit 0 for a controller-only pinned preview, and the
/// documented locality guarantee (no client state opened) still holds.
#[test]
fn batch_preview_cli_under_enabled_controller_exits_zero_and_stays_local() {
    use std::collections::BTreeMap;

    use clap::Parser;
    use mac_worker::{RuntimeContext, cli::Cli};

    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"fn main() {}\n");
    repo.commit_all("base");
    pinned_dag_batch(&repo);
    let home = tempdir().unwrap();
    let config_path = home.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[controller]\nenabled = true\nssh = \"mac1\"\nremote_binary = \"~/.local/bin/worker\"\n",
    )
    .unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::new(),
        home.path().to_path_buf(),
        repo.root().to_path_buf(),
    );
    let cli = Cli::try_parse_from([
        "worker",
        "--config",
        config_path.to_str().unwrap(),
        "--json",
        "task",
        "batch",
        repo.root().join("tasks.toml").to_str().unwrap(),
        "--preview",
    ])
    .unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = mac_worker::run_with_io_in_context(
        cli,
        &SystemProcessRunner,
        &runtime,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(
        exit,
        0,
        "stdout={} stderr={}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(report["issues"].as_array().unwrap().len(), 0);
    assert_eq!(report["dag"]["status"], "enforced");
    assert!(!home.path().join(".local/state/mac-worker").exists());
}

/// F2: the human-readable preview line must not claim this build cannot
/// execute dependencies while the same report says `dag.status = enforced`.
#[test]
fn batch_preview_human_output_matches_enforced_dag() {
    use std::collections::BTreeMap;

    use clap::Parser;
    use mac_worker::{RuntimeContext, cli::Cli};

    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"fn main() {}\n");
    repo.commit_all("base");
    repo.write(
        "tasks.toml",
        br#"
version = 1
agent = "codex"

[[tasks]]
prompt = "fix login"
"#,
    );
    let home = tempdir().unwrap();
    let config_path = home.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::new(),
        home.path().to_path_buf(),
        repo.root().to_path_buf(),
    );
    let cli = Cli::try_parse_from([
        "worker",
        "--config",
        config_path.to_str().unwrap(),
        "task",
        "batch",
        repo.root().join("tasks.toml").to_str().unwrap(),
        "--preview",
    ])
    .unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = mac_worker::run_with_io_in_context(
        cli,
        &SystemProcessRunner,
        &runtime,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    let text = String::from_utf8(stdout).unwrap();
    assert!(
        !text.contains("cannot execute them"),
        "stale pre-enforcement wording survived: {text}"
    );
    assert!(
        text.contains("Dependencies execute when parents are Closed and Done."),
        "human output must state the enforced rule: {text}"
    );
}
