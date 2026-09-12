use clap::Parser;
use mac_worker::{
    cli::{Cli, Command, TaskCommand},
    protocol::PROTOCOL_VERSION,
    task::RunProgress,
    task_client::BatchFile,
    task_view::{TaskListJson, TaskListProjection},
};

#[test]
fn task_commands_parse_the_documented_forms() {
    for arguments in [
        vec![
            "worker", "task", "submit", "--agent", "codex", "--prompt", "x",
        ],
        vec![
            "worker", "task", "submit", "--agent", "cursor", "--prompt", "x",
        ],
        vec![
            "worker", "task", "submit", "--agent", "opencode", "--prompt", "x",
        ],
        vec![
            "worker",
            "task",
            "submit",
            "--agent",
            "codex",
            "--prompt-file",
            "prompt.md",
            "--title",
            "repair",
            "--no-wait",
        ],
        vec!["worker", "task", "list"],
        vec![
            "worker",
            "task",
            "status",
            "018f0f4a6b5c7d8e9f00112233445566",
        ],
        vec!["worker", "task", "logs", "018f0f4a6b5c7d8e9f00112233445566"],
        vec!["worker", "task", "diff", "018f0f4a6b5c7d8e9f00112233445566"],
        vec![
            "worker",
            "task",
            "result",
            "018f0f4a6b5c7d8e9f00112233445566",
        ],
        vec![
            "worker",
            "task",
            "fetch",
            "018f0f4a6b5c7d8e9f00112233445566",
        ],
        vec![
            "worker",
            "task",
            "close",
            "018f0f4a6b5c7d8e9f00112233445566",
        ],
        vec![
            "worker",
            "task",
            "publish-retry",
            "018f0f4a6b5c7d8e9f00112233445566",
        ],
        vec!["worker", "task", "reconcile"],
        vec![
            "worker",
            "task",
            "say",
            "018f0f4a6b5c7d8e9f00112233445566",
            "--message",
            "more",
        ],
        vec![
            "worker",
            "task",
            "cancel",
            "018f0f4a6b5c7d8e9f00112233445566",
        ],
        vec!["worker", "task", "wait", "--timeout", "1s"],
        vec!["worker", "task", "batch", "tasks.toml"],
        vec!["worker", "task", "batch", "tasks.toml", "--preview"],
    ] {
        Cli::try_parse_from(arguments).expect("documented task form must parse");
    }
}

#[test]
fn task_submit_requires_exactly_one_prompt_source() {
    assert!(Cli::try_parse_from(["worker", "task", "submit", "--agent", "codex",]).is_err());
    assert!(
        Cli::try_parse_from([
            "worker",
            "task",
            "submit",
            "--agent",
            "codex",
            "--prompt",
            "x",
            "--prompt-file",
            "prompt.md",
        ])
        .is_err()
    );
}

#[test]
fn task_submit_accepts_repeatable_publication_modes() {
    let parsed = Cli::try_parse_from([
        "worker",
        "task",
        "submit",
        "--agent",
        "codex",
        "--prompt",
        "publish",
        "--source",
        "origin",
        "--publish",
        "fetch",
        "--publish",
        "push",
        "--publish-branch",
        "release-candidate",
    ]);
    assert!(parsed.is_ok(), "origin push form must parse: {parsed:?}");
}

#[test]
fn task_submit_allows_attached_no_wait_to_fail_after_the_probe() {
    let parsed = Cli::try_parse_from([
        "worker",
        "task",
        "submit",
        "--agent",
        "codex",
        "--prompt",
        "x",
        "--no-wait",
        "--wait",
    ])
    .expect("--no-wait and --wait are independent lifecycle controls");
    let Command::Task {
        command:
            TaskCommand::Submit {
                no_wait: true,
                wait: true,
                ..
            },
    } = parsed.command
    else {
        panic!("expected task submit command");
    };
}

#[test]
fn batch_file_accepts_documented_top_level_defaults_and_prompt_files() {
    let parsed: BatchFile = toml::from_str(
        r#"
version = 1
agent = "codex"
base = "main"
source = "local"
publish = ["fetch"]
timeout = "45m"

[[tasks]]
title = "Flaky login spec"
prompt_file = "tasks/fix-flaky-login.md"

[[tasks]]
title = "Extract billing client"
prompt = "Move the billing client"
agent = "claude"
"#,
    )
    .expect("documented batch syntax must parse");

    assert_eq!(parsed.version, 1);
    assert_eq!(parsed.defaults.agent, "codex");
    assert_eq!(parsed.defaults.base, "main");
    assert_eq!(parsed.defaults.source, "local");
    assert_eq!(parsed.defaults.publish, vec!["fetch"]);
    assert_eq!(parsed.tasks.len(), 2);
    assert_eq!(
        parsed.tasks[0]
            .prompt_file
            .as_deref()
            .and_then(|path| path.to_str()),
        Some("tasks/fix-flaky-login.md")
    );
    assert_eq!(
        parsed.tasks[1].prompt.as_deref(),
        Some("Move the billing client")
    );
}

#[test]
fn batch_file_carries_model_and_effort_as_defaults_and_per_task_overrides() {
    let parsed: BatchFile = toml::from_str(
        r#"
version = 1
agent = "codex"
model = "gpt-5.6-luna"
effort = "max"

[[tasks]]
prompt = "inherit the defaults"

[[tasks]]
prompt = "override the effort"
effort = "medium"
"#,
    )
    .expect("model and effort must be accepted as batch defaults");

    assert_eq!(parsed.defaults.model.as_deref(), Some("gpt-5.6-luna"));
    assert_eq!(parsed.defaults.effort.as_deref(), Some("max"));
    assert_eq!(parsed.tasks[0].effort, None);
    assert_eq!(parsed.tasks[1].effort.as_deref(), Some("medium"));
}

#[test]
fn batch_file_defaults_to_version_one_and_local_fetch() {
    let parsed: BatchFile = toml::from_str(
        r#"
[[tasks]]
prompt = "x"
"#,
    )
    .expect("minimal batch syntax must parse");

    assert_eq!(parsed.version, 1);
    assert_eq!(parsed.defaults.source, "local");
    assert_eq!(parsed.defaults.publish, vec!["fetch"]);
}

#[test]
fn batch_file_rejects_unknown_keys_and_conflicting_defaults() {
    let unknown = toml::from_str::<BatchFile>(
        r#"
version = 1
unexpected = true

[[tasks]]
prompt = "x"
"#,
    );
    assert!(unknown.is_err(), "unknown batch keys must be rejected");

    let conflicting = toml::from_str::<BatchFile>(
        r#"
agent = "codex"
[defaults]
base = "main"

[[tasks]]
prompt = "x"
"#,
    );
    assert!(
        conflicting.is_err(),
        "mixed top-level and [defaults] keys must be rejected"
    );
}

#[test]
fn batch_preview_conflicts_with_wait() {
    assert!(
        Cli::try_parse_from([
            "worker",
            "task",
            "batch",
            "tasks.toml",
            "--preview",
            "--wait"
        ])
        .is_err()
    );
    let parsed =
        Cli::try_parse_from(["worker", "task", "batch", "tasks.toml", "--preview"]).unwrap();
    assert!(matches!(
        parsed.command,
        Command::Task {
            command: TaskCommand::Batch {
                preview: true,
                wait: false,
                ..
            }
        }
    ));
}

#[test]
fn batch_file_carries_declared_files_acceptance_and_depends_on() {
    let parsed: BatchFile = toml::from_str(
        r#"
version = 1
agent = "codex"

[[tasks]]
id = "login"
prompt = "fix login"
files = ["src/login.rs"]
acceptance = ["cargo test -p login"]

[[tasks]]
id = "billing"
prompt = "extract billing"
files = ["src/login.rs", "src/billing.rs"]
depends_on = ["login"]
"#,
    )
    .expect("batch metadata fields must parse");
    assert_eq!(parsed.tasks[0].id.as_deref(), Some("login"));
    assert_eq!(parsed.tasks[0].files, vec!["src/login.rs"]);
    assert_eq!(parsed.tasks[0].acceptance, vec!["cargo test -p login"]);
    assert_eq!(parsed.tasks[1].depends_on, vec!["login"]);
}

#[test]
fn task_list_json_envelope_flattens_the_shared_projection() {
    let projection = TaskListProjection {
        tasks: Vec::new(),
        runs: Vec::new(),
        progress: RunProgress::from_states(std::iter::empty()),
        dag_nodes: Vec::new(),
    };
    let value = serde_json::to_value(TaskListJson::new(PROTOCOL_VERSION, projection)).unwrap();

    assert_eq!(value["protocol_version"], PROTOCOL_VERSION);
    assert!(value.get("tasks").is_some());
    assert!(value.get("runs").is_some());
    assert!(value.get("progress").is_some());
    assert!(value.get("projection").is_none());
}
