use clap::Parser;
use mac_worker::{
    cli::{Cli, Command, ControllerCommand, HostCommand, TaskCommand},
    protocol::PROTOCOL_VERSION,
    task::RunProgress,
    task_client::BatchFile,
    task_view::{TaskListJson, TaskListProjection},
};

#[test]
fn questions_submit_accepts_only_ask_or_decide() {
    for policy in ["ask", "decide"] {
        Cli::try_parse_from([
            "worker",
            "task",
            "submit",
            "--prompt",
            "x",
            "--questions",
            policy,
        ])
        .expect("questions override");
    }
    assert!(
        Cli::try_parse_from([
            "worker",
            "task",
            "submit",
            "--prompt",
            "x",
            "--questions",
            "sometimes"
        ])
        .is_err()
    );
}

#[test]
fn questions_batch_accepts_defaults_and_node_overrides() {
    for defaults in ["questions = \"ask\"", "[defaults]\nquestions = \"ask\""] {
        let parsed: BatchFile = toml::from_str(&format!(
            "{defaults}\n[[tasks]]\nprompt = \"work\"\nquestions = \"decide\"\n"
        ))
        .expect("batch questions policy");
        assert_eq!(parsed.tasks.len(), 1);
    }
}

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
fn task_say_accepts_interrupt_only_with_the_say_message_forms() {
    let id = "018f0f4a6b5c7d8e9f00112233445566";
    assert!(
        Cli::try_parse_from([
            "worker",
            "task",
            "say",
            id,
            "--interrupt",
            "--message",
            "steer",
            "--wait",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "worker",
            "task",
            "say",
            id,
            "--interrupt",
            "--message-file",
            "note.md",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["worker", "task", "say", id, "--interrupt"]).is_err());
    assert!(Cli::try_parse_from(["worker", "task", "cancel", id, "--interrupt"]).is_err());
    assert!(
        Cli::try_parse_from([
            "worker",
            "task",
            "submit",
            "--agent",
            "codex",
            "--prompt",
            "x",
            "--interrupt",
        ])
        .is_err()
    );
    assert!(Cli::try_parse_from(["worker", "task", "status", id, "--interrupt"]).is_err());
}

#[test]
fn task_say_interrupt_records_the_flag_with_message_and_wait() {
    let id = "018f0f4a6b5c7d8e9f00112233445566";
    let parsed = Cli::try_parse_from([
        "worker",
        "task",
        "say",
        id,
        "--interrupt",
        "--message",
        "steer",
        "--wait",
    ])
    .unwrap();
    match parsed.command {
        Command::Task {
            command:
                TaskCommand::Say {
                    task_id,
                    interrupt: true,
                    wait: true,
                    message: Some(message),
                    message_file: None,
                },
        } => {
            assert_eq!(task_id.to_string(), id);
            assert_eq!(message, "steer");
        }
        other => panic!("unexpected command: {other:?}"),
    }
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

#[test]
fn controller_era_task_forms_parse_with_their_documented_fields() {
    // Relocated from the controller process harnesses: the public grammar the
    // runtime tests drive must parse without spawning a controller.
    const TASK_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
    const TURN_ID: &str = "118f0f4a6b5c7d8e9f00112233445566";
    const RUN_ID: &str = "0193f0f4a6b5c7d8e9f00112233445566";
    const TOKEN: &str = "018f0f4a6b5c7d8e9f00112233445566";
    const FINGERPRINT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const PROJECT_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const WORKTREE_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const OID: &str = "dddddddddddddddddddddddddddddddddddddddd";

    let submit = Cli::try_parse_from([
        "worker",
        "task",
        "submit",
        "--prompt",
        "first turn stays open",
        "--wip",
        "--no-wait",
        "--close-on",
        "never",
    ])
    .unwrap();
    match submit.command {
        Command::Task {
            command:
                TaskCommand::Submit {
                    wip: true,
                    no_wait: true,
                    close_on: Some(ref policy),
                    ..
                },
        } if policy == "never" => {}
        other => panic!("expected submit --wip --no-wait --close-on never, got {other:?}"),
    }

    let say = Cli::try_parse_from([
        "worker",
        "task",
        "say",
        TASK_ID,
        "--message",
        "second turn",
        "--wait",
    ])
    .unwrap();
    match say.command {
        Command::Task {
            command:
                TaskCommand::Say {
                    wait: true,
                    message: Some(ref text),
                    ..
                },
        } if text == "second turn" => {}
        other => panic!("expected say --message --wait, got {other:?}"),
    }

    let list = Cli::try_parse_from(["worker", "task", "list"]).unwrap();
    assert!(matches!(
        list.command,
        Command::Task {
            command: TaskCommand::List {
                run: None,
                state: None,
                outcome: None,
                full: false,
            }
        }
    ));
    let filtered = Cli::try_parse_from([
        "worker",
        "task",
        "list",
        "--run",
        RUN_ID,
        "--state",
        "open",
        "--outcome",
        "done",
        "--full",
    ])
    .unwrap();
    assert!(matches!(
        filtered.command,
        Command::Task {
            command: TaskCommand::List {
                run: Some(_),
                state: Some(_),
                outcome: Some(_),
                full: true,
            }
        }
    ));

    let reconcile = Cli::try_parse_from(["worker", "task", "reconcile"]).unwrap();
    assert!(matches!(
        reconcile.command,
        Command::Task {
            command: TaskCommand::Reconcile,
        }
    ));

    let wait_run = Cli::try_parse_from(["worker", "task", "wait", "--run", RUN_ID]).unwrap();
    assert!(matches!(
        wait_run.command,
        Command::Task {
            command: TaskCommand::Wait {
                task_id: None,
                run: Some(_),
                timeout: None,
            }
        }
    ));
    let wait_task = Cli::try_parse_from([
        "worker",
        "task",
        "wait",
        "--task-id",
        TASK_ID,
        "--timeout",
        "45s",
    ])
    .unwrap();
    assert!(matches!(
        wait_task.command,
        Command::Task {
            command: TaskCommand::Wait {
                task_id: Some(_),
                run: None,
                timeout: Some(_),
            }
        }
    ));

    let batch = Cli::try_parse_from(["worker", "task", "batch", "tasks.toml"]).unwrap();
    assert!(matches!(
        batch.command,
        Command::Task {
            command: TaskCommand::Batch {
                max_parallel: None,
                wait: false,
                preview: false,
                ..
            }
        }
    ));
    let omitted = Cli::try_parse_from(["worker", "--json", "task", "batch", "tasks.toml"]).unwrap();
    assert!(matches!(
        omitted.command,
        Command::Task {
            command: TaskCommand::Batch {
                max_parallel: None,
                ..
            }
        }
    ));

    for (args, describe) in [
        (
            vec!["worker", "task", "logs", TASK_ID, "--turn", "1", "--raw"],
            "task logs --turn/--raw",
        ),
        (
            vec!["worker", "task", "logs", TASK_ID, "--follow"],
            "task logs --follow",
        ),
        (
            vec!["worker", "task", "diff", TASK_ID, "--stat"],
            "task diff --stat",
        ),
        (vec!["worker", "task", "status", TASK_ID], "task status"),
        (vec!["worker", "task", "result", TASK_ID], "task result"),
        (vec!["worker", "task", "cancel", TASK_ID], "task cancel"),
        (vec!["worker", "task", "close", TASK_ID], "task close"),
        (vec!["worker", "task", "fetch", TASK_ID], "task fetch"),
    ] {
        Cli::try_parse_from(args).unwrap_or_else(|error| panic!("{describe}: {error}"));
    }

    let run = Cli::try_parse_from(["worker", "controller", "run"]).unwrap();
    assert!(matches!(
        run.command,
        Command::Controller {
            command: ControllerCommand::Run { supervised: false }
        }
    ));
    let rpc = Cli::try_parse_from(["worker", "host", "controller-rpc"]).unwrap();
    assert!(matches!(
        rpc.command,
        Command::Host {
            command: HostCommand::ControllerRpc
        }
    ));
    let receive = Cli::try_parse_from([
        "worker",
        "host",
        "controller-receive-pack",
        TOKEN,
        TOKEN,
        FINGERPRINT,
        PROJECT_ID,
        WORKTREE_ID,
        OID,
    ])
    .unwrap();
    assert!(matches!(
        receive.command,
        Command::Host {
            command: HostCommand::ControllerReceivePack { .. }
        }
    ));
    let upload = Cli::try_parse_from([
        "worker",
        "host",
        "controller-upload-pack",
        TOKEN,
        TOKEN,
        FINGERPRINT,
        TASK_ID,
        TURN_ID,
        OID,
    ])
    .unwrap();
    assert!(matches!(
        upload.command,
        Command::Host {
            command: HostCommand::ControllerUploadPack { .. }
        }
    ));
}

#[test]
fn batch_file_parses_per_task_bases_and_rejects_a_project_key() {
    // Relocated from the batch process harness: per-task committed bases,
    // `from:<parent>` bases beside `depends_on` and `close_on`, and the
    // deny_unknown_fields guard against a TOML `project` key.
    let two_roots: BatchFile = toml::from_str(
        "version = 1\n\n[[tasks]]\nid = \"alpha\"\nprompt = \"independent root alpha\"\nbase = \"alpha-base\"\n\n[[tasks]]\nid = \"beta\"\nprompt = \"independent root beta\"\nbase = \"beta-base\"\n",
    )
    .expect("public two-root batch must parse");
    assert_eq!(two_roots.tasks.len(), 2);
    assert_eq!(two_roots.tasks[0].id.as_deref(), Some("alpha"));
    assert_eq!(two_roots.tasks[0].base.as_deref(), Some("alpha-base"));
    assert_eq!(two_roots.tasks[1].id.as_deref(), Some("beta"));
    assert_eq!(two_roots.tasks[1].base.as_deref(), Some("beta-base"));
    assert!(two_roots.tasks.iter().all(|task| task.wip.is_none()));

    let dag: BatchFile = toml::from_str(
        "version = 1\n\n[[tasks]]\nid = \"parent\"\nprompt = \"parent root stays open after Done\"\nclose_on = \"never\"\n\n[[tasks]]\nid = \"child\"\ndepends_on = [\"parent\"]\nbase = \"from:parent\"\nprompt = \"child from accepted parent\"\n",
    )
    .expect("from:parent batch must parse");
    assert_eq!(dag.tasks[0].id.as_deref(), Some("parent"));
    assert_eq!(dag.tasks[0].close_on.as_deref(), Some("never"));
    assert_eq!(dag.tasks[1].base.as_deref(), Some("from:parent"));

    let error = toml::from_str::<BatchFile>(
        "version = 1\n[[tasks]]\nid = \"alpha\"\nprompt = \"unknown project key must not parse\"\nproject = \"/tmp/not-public-grammar\"\n",
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("unknown field") && error.contains("project"),
        "BatchTask deny_unknown_fields must reject project=; got {error}"
    );
}
