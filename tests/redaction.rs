use mac_worker::{
    redaction::{
        MAX_CHANGED_FILE_BYTES, MAX_CHANGED_FILE_COUNT, MAX_FAILURE_REASON_BYTES,
        MAX_QUESTION_BYTES, MAX_QUESTION_COUNT, MAX_SUMMARY_BYTES, RedactionBoundary,
    },
    task::{TaskOutcome, TaskState, TaskStatus, TurnSummary, TurnTerminal},
};
use proptest::prelude::*;

fn boundary() -> RedactionBoundary {
    RedactionBoundary::new("/Users/alice")
}

fn turn_id() -> mac_worker::task::TurnId {
    "018f0f4a6b5c7d8e9f00112233445566"
        .parse()
        .expect("fixture turn id")
}

#[test]
fn summary_redacts_home_paths_tilde_paths_and_tokens() {
    let text = boundary().summary(
        "read /Users/alice/.ssh/id_ed25519 and ~/.aws/credentials with sk-abc12345deadbeef and Bearer supersecret and 0123456789abcdef0123456789abcdef",
    );
    assert!(!text.contains("/Users/alice"));
    assert!(!text.contains("~/.aws"));
    assert!(!text.contains("sk-abc12345deadbeef"));
    assert!(!text.contains("supersecret"));
    assert!(!text.contains("0123456789abcdef0123456789abcdef"));
    assert!(text.contains("[path]"));
    assert!(text.contains("[token]"));
    assert!(text.contains("Bearer [token]"));
}

#[test]
fn questions_redact_and_bound_each_item_and_the_list() {
    let questions = boundary().questions([
        "open /Users/alice/.env please".to_owned(),
        "paste sk-live-abcdefghijklmnopqrstuvwxyz".to_owned(),
        "x".repeat(MAX_QUESTION_BYTES + 40),
    ]);
    assert_eq!(questions.len(), 3);
    assert!(!questions[0].contains("/Users/alice"));
    assert!(questions[0].contains("[path]"));
    assert!(!questions[1].contains("sk-live-"));
    assert!(questions[1].contains("[token]"));
    assert!(questions[2].len() <= MAX_QUESTION_BYTES);

    let many = (0..MAX_QUESTION_COUNT + 5).map(|index| format!("q{index}"));
    assert_eq!(boundary().questions(many).len(), MAX_QUESTION_COUNT);
}

#[test]
fn changed_file_names_redact_paths_and_bound_the_list() {
    let files =
        boundary().changed_files(["/Users/alice/secret.env", "~/Downloads/id_rsa", "src/ok.rs"]);
    assert_eq!(files[0], "[path]");
    assert_eq!(files[1], "[path]");
    assert_eq!(files[2], "src/ok.rs");
    let many = (0..MAX_CHANGED_FILE_COUNT + 3).map(|index| format!("f{index}.rs"));
    assert_eq!(boundary().changed_files(many).len(), MAX_CHANGED_FILE_COUNT);
    assert!(
        boundary()
            .changed_file(&"n".repeat(MAX_CHANGED_FILE_BYTES + 8))
            .len()
            <= MAX_CHANGED_FILE_BYTES
    );
}

#[test]
fn failure_reasons_escape_controls_and_redact_secrets() {
    let reason = boundary().failure_reason("agent failed\twith Bearer tok_abc and ~/token done\n");
    assert!(!reason.contains('\t'));
    assert!(!reason.contains('\n'));
    assert!(reason.contains("\\t"));
    assert!(reason.contains("\\n"));
    assert!(reason.contains("Bearer [token]"));
    assert!(reason.contains("[path]"));
    assert!(
        boundary()
            .failure_reason(&"r".repeat(MAX_FAILURE_REASON_BYTES + 20))
            .len()
            <= MAX_FAILURE_REASON_BYTES
    );
}

#[test]
fn task_status_and_turn_summary_apply_the_same_boundary() {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/Users/alice".into());
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Failed {
            reason: format!("crash at {home}/.codex/auth.json with sk-proj-abcdefghijklmnopqrstuv"),
        }),
        None,
        false,
        None,
        Some(format!("see {home}/.netrc")),
        vec!["what is in ~/.ssh/config?".into()],
        vec![format!("{home}/.env")],
        Some(format!("diff of {home}/secret")),
        vec![TurnSummary::new(
            1,
            turn_id(),
            Some(TurnTerminal::Failed),
            Some(TaskOutcome::Failed {
                reason: "Bearer leaked-token-value".into(),
            }),
            None,
            false,
            None,
            None,
        )],
        1,
    )
    .expect("status accepts redacted fields");
    let json = serde_json::to_value(&status).unwrap();
    let rendered = json.to_string();
    assert!(!rendered.contains(&format!("{home}/.codex")));
    assert!(!rendered.contains("sk-proj-"));
    assert!(!rendered.contains("leaked-token-value"));
    assert!(!rendered.contains("~/.ssh"));
    assert!(rendered.contains("[path]"));
    assert!(rendered.contains("[token]"));
}

#[test]
fn base64_looking_blobs_are_redacted() {
    let blob = "QWxhZGRpbjpvcGVuIHNlc2FtZUFsYWRkaW46b3Blbg== extra";
    let text = boundary().summary(blob);
    assert!(!text.contains("QWxhZGRpbjpvcGVuIHNlc2FtZUFsYWRkaW46b3Blbg=="));
    assert!(text.contains("[token]"));
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn boundary_never_panics_and_always_fits_the_bound(input in any::<String>()) {
        let boundary = RedactionBoundary::new("/Users/tester");
        let summary = boundary.summary(&input);
        prop_assert!(summary.len() <= MAX_SUMMARY_BYTES);
        let question = boundary.question(&input);
        prop_assert!(question.len() <= MAX_QUESTION_BYTES);
        let file = boundary.changed_file(&input);
        prop_assert!(file.len() <= MAX_CHANGED_FILE_BYTES);
        let reason = boundary.failure_reason(&input);
        prop_assert!(reason.len() <= MAX_FAILURE_REASON_BYTES);
        let title = boundary.title(&input);
        prop_assert!(title.len() <= mac_worker::redaction::MAX_TITLE_BYTES);
        let questions = boundary.questions([&input, &input, &input]);
        prop_assert!(questions.len() <= MAX_QUESTION_COUNT);
        let files = boundary.changed_files([&input, &input]);
        prop_assert!(files.len() <= MAX_CHANGED_FILE_COUNT);
    }
}
