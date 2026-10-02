use mac_worker::test_support::{
    agents::agent::Question,
    core::redaction::{
        MAX_CHANGED_FILE_BYTES, MAX_CHANGED_FILE_COUNT, MAX_FAILURE_REASON_BYTES,
        MAX_QUESTION_BYTES, MAX_QUESTION_COUNT, MAX_QUESTION_OPTION_BYTES,
        MAX_QUESTION_OPTION_COUNT, MAX_SUMMARY_BYTES, MAX_TITLE_BYTES, RedactionBoundary,
    },
    task::model::{TaskOutcome, TaskState, TaskStatus, TurnSummary, TurnTerminal},
};
use proptest::prelude::*;

fn boundary() -> RedactionBoundary {
    RedactionBoundary::new("/Users/alice")
}

fn turn_id() -> mac_worker::test_support::task::model::TurnId {
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
fn summary_redacts_other_users_home_and_temp_paths() {
    let paths = [
        "/Users/bob/project/.git/index.lock",
        "/home/bob/project/.git/index.lock",
        "/private/var/folders/ab/cd1234/T/mac-worker.log",
        "/var/folders/ab/cd1234/T/mac-worker.log",
        "/tmp/mac-worker/turn.log",
        "/private/tmp/mac-worker/turn.log",
    ];
    let input = paths.join(" ");
    let text = boundary().summary(&input);

    for path in paths {
        assert!(!text.contains(path), "path was retained: {path}");
    }
    assert_eq!(text.matches("[path]").count(), paths.len());
}

#[test]
fn questions_redact_and_bound_each_item_and_the_list() {
    let questions = boundary().questions([
        Question::open("open /Users/alice/.env please"),
        Question::open("paste sk-live-abcdefghijklmnopqrstuvwxyz"),
        Question::open("x".repeat(MAX_QUESTION_BYTES + 40)),
    ]);
    assert_eq!(questions.len(), 3);
    assert!(!questions[0].text().contains("/Users/alice"));
    assert!(questions[0].text().contains("[path]"));
    assert!(!questions[1].text().contains("sk-live-"));
    assert!(questions[1].text().contains("[token]"));
    assert!(questions[2].text().len() <= MAX_QUESTION_BYTES);

    let many = (0..MAX_QUESTION_COUNT + 5).map(|index| Question::open(format!("q{index}")));
    assert_eq!(boundary().questions(many).len(), MAX_QUESTION_COUNT);
}

#[test]
fn question_options_are_redacted_bounded_and_capped() {
    let questions = boundary().questions([Question::new(
        "which base?",
        (0..MAX_QUESTION_OPTION_COUNT + 4)
            .map(|index| match index {
                0 => "open /Users/alice/.env".to_owned(),
                1 => "y".repeat(MAX_QUESTION_OPTION_BYTES + 40),
                other => format!("option-{other}"),
            })
            .collect(),
    )]);

    let options = questions[0].options();
    assert_eq!(options.len(), MAX_QUESTION_OPTION_COUNT);
    assert!(!options[0].contains("/Users/alice"));
    assert!(options[0].contains("[path]"));
    assert!(options[1].len() <= MAX_QUESTION_OPTION_BYTES);
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
fn bearer_redaction_is_a_fixed_point() {
    // Break caught: a Codex summary said "without the Bearer prefix". Each
    // pass turned `Bearer [token]` into `Bearer [token]]`, so the host wrote a
    // status.json that never re-encoded canonically, and every status read
    // failed as INVALID_REQUEST after the turn had finished.
    let boundary = boundary();
    for input in [
        "rejects a valid JWT without the Bearer prefix\u{201d}.",
        "Bearer [token]",
        "Bearer [token]]\u{201d} pinned",
        "Bearer [path]",
        "Bearer ~/.netrc",
        "Bearer ,",
        "Bearer ",
        "bearer\t[token]x",
    ] {
        let once = boundary.summary(input);
        assert_eq!(boundary.summary(&once), once, "input {input:?}");
    }
    assert_eq!(
        boundary.summary("the Bearer prefix\u{201d}"),
        "the Bearer [token]\u{201d}"
    );
    // Records written by the old redactor keep their text unchanged.
    let stored = "without the Bearer [token]]\u{201d}.";
    assert_eq!(boundary.summary(stored), stored);
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
        prop_assert!(title.len() <= MAX_TITLE_BYTES);
        let questions = boundary.questions([
            Question::new(input.clone(), vec![input.clone()]),
            Question::open(input.clone()),
            Question::open(input.clone()),
        ]);
        prop_assert!(questions.len() <= MAX_QUESTION_COUNT);
        for question in &questions {
            prop_assert!(question.text().len() <= MAX_QUESTION_BYTES);
            for option in question.options() {
                prop_assert!(option.len() <= MAX_QUESTION_OPTION_BYTES);
            }
        }
        let files = boundary.changed_files([&input, &input]);
        prop_assert!(files.len() <= MAX_CHANGED_FILE_COUNT);
    }
}

type BoundedField = fn(&RedactionBoundary, &str) -> String;

fn redaction_fragment() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("Bearer ".to_owned()),
        Just("bearer\t".to_owned()),
        Just("Bearer".to_owned()),
        Just("[token]".to_owned()),
        Just("[path]".to_owned()),
        Just("[".to_owned()),
        Just("]".to_owned()),
        Just("sk-".to_owned()),
        Just("0123456789abcdef".to_owned()),
        Just("QWxhZGRpbjpvcGVu".to_owned()),
        Just("+/".to_owned()),
        Just("=".to_owned()),
        Just("~/".to_owned()),
        Just("~".to_owned()),
        Just("/Users/tester/".to_owned()),
        Just("/tmp/".to_owned()),
        Just("\"".to_owned()),
        Just("'".to_owned()),
        Just(",".to_owned()),
        Just(")".to_owned()),
        Just(";".to_owned()),
        Just(" ".to_owned()),
        Just("\n".to_owned()),
        Just("\\n".to_owned()),
        Just("\u{201d}".to_owned()),
        Just("\u{e9}".to_owned()),
        "[a-zA-Z0-9_-]{0,12}",
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 2048,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// Stored task records are decoded through the same boundary, and the
    /// host requires the re-encoded bytes to match the file. Any field whose
    /// second redaction differs from its first makes the record unreadable.
    #[test]
    fn redaction_is_idempotent(
        fragments in prop::collection::vec(redaction_fragment(), 0..48),
    ) {
        let input = fragments.concat();
        let boundary = RedactionBoundary::new("/Users/tester");
        let limited: [(&str, BoundedField); 6] = [
            ("summary", |boundary, text| boundary.summary(text)),
            ("question", |boundary, text| boundary.question(text)),
            ("question option", |boundary, text| boundary.question_option(text)),
            ("changed file", |boundary, text| boundary.changed_file(text)),
            ("failure reason", |boundary, text| boundary.failure_reason(text)),
            ("title", |boundary, text| boundary.title(text)),
        ];
        for (field, redact) in limited {
            let once = redact(&boundary, &input);
            prop_assert_eq!(redact(&boundary, &once), once, "{} of {:?}", field, input);
        }
    }
}
