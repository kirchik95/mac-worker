use clap::Parser;
use mac_worker::cli::{Cli, Command, TaskCommand};

#[test]
fn task_commands_parse_the_documented_forms() {
    for arguments in [
        vec![
            "worker", "task", "submit", "--agent", "codex", "--prompt", "x",
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
