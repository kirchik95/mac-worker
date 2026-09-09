//! The turn log renderer must print every recorded agent transcript identically
//! to the reference under `tests/fixtures/turn_log/`. The worker's herdr pane
//! relies on the same output as `worker task logs`.

use std::{
    fs,
    path::{Path, PathBuf},
};

use mac_worker::{agent::AgentKind, turn_log::render_agent_log};

const FIXTURE_ROOT: &str = "tests/fixtures";
const REFERENCE_ROOT: &str = "tests/fixtures/turn_log";

/// Recorded transcript directories and how the agent of each file is known.
/// Under `agents/` the file name starts with the agent; the other two hold a
/// single agent each.
const RECORDED_DIRS: [(&str, Option<AgentKind>); 3] = [
    ("agents", None),
    ("cursor", Some(AgentKind::Cursor)),
    ("opencode", Some(AgentKind::Opencode)),
];

fn agent_from_stem(stem: &str) -> AgentKind {
    match stem.split('-').next().unwrap_or_default() {
        "codex" => AgentKind::Codex,
        "claude" => AgentKind::Claude,
        "cursor" => AgentKind::Cursor,
        "opencode" => AgentKind::Opencode,
        other => panic!("fixture {stem} names an unknown agent {other}"),
    }
}

fn sorted_files(dir: &Path, extension: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("{} must be readable: {error}", dir.display()))
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == extension))
        .collect();
    files.sort();
    files
}

fn stem(path: &Path) -> &str {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_else(|| panic!("{} must have a UTF-8 stem", path.display()))
}

/// Every recorded transcript, the agent that produced it, and where its
/// reference rendering lives.
fn recorded_fixtures() -> Vec<(PathBuf, AgentKind, PathBuf)> {
    let mut fixtures = Vec::new();
    for (dir, fixed_agent) in RECORDED_DIRS {
        for path in sorted_files(&Path::new(FIXTURE_ROOT).join(dir), "jsonl") {
            let agent = fixed_agent.unwrap_or_else(|| agent_from_stem(stem(&path)));
            let reference = Path::new(REFERENCE_ROOT)
                .join(dir)
                .join(format!("{}.txt", stem(&path)));
            fixtures.push((path, agent, reference));
        }
    }
    fixtures
}

#[test]
fn every_recorded_transcript_renders_byte_identically_to_its_reference() {
    let fixtures = recorded_fixtures();
    assert!(!fixtures.is_empty(), "no recorded transcripts were found");
    for (path, agent, reference) in fixtures {
        let bytes = fs::read(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));
        let expected = fs::read(&reference).unwrap_or_else(|error| {
            panic!(
                "{} has no reference rendering at {}: {error}",
                path.display(),
                reference.display()
            )
        });
        let mut rendered = Vec::new();
        render_agent_log(&bytes, agent, &mut rendered).unwrap();
        assert!(
            rendered == expected,
            "{} rendered differently from {}\n--- rendered ---\n{}\n--- expected ---\n{}",
            path.display(),
            reference.display(),
            String::from_utf8_lossy(&rendered),
            String::from_utf8_lossy(&expected),
        );
    }
}

#[test]
fn every_reference_rendering_belongs_to_a_recorded_transcript() {
    let recorded: Vec<PathBuf> = recorded_fixtures()
        .into_iter()
        .map(|(_, _, reference)| reference)
        .collect();
    for (dir, _) in RECORDED_DIRS {
        for reference in sorted_files(&Path::new(REFERENCE_ROOT).join(dir), "txt") {
            assert!(
                recorded.contains(&reference),
                "{} is a stale reference: no transcript produces it",
                reference.display()
            );
        }
    }
}

fn render(agent: AgentKind, log: &str) -> String {
    let mut rendered = Vec::new();
    render_agent_log(log.as_bytes(), agent, &mut rendered).unwrap();
    String::from_utf8(rendered).expect("renderer writes UTF-8")
}

#[test]
fn plain_text_passes_through_and_unrecognised_json_is_summarised() {
    // Runner logs mix agent events with stderr and pre-launch failures. The
    // reader needs the diagnostics; unknown structured events are summarised
    // so a new error type is visible without dumping its payload.
    let log = "codex: command not found\n{\"type\":\"unknown_event\"}\n{not json\n";
    assert_eq!(
        render(AgentKind::Codex, log),
        "codex: command not found\nevent: unknown_event\n{not json\n"
    );
}

#[test]
fn consecutive_unrecognised_events_with_the_same_key_fold_into_one_line() {
    let log = concat!(
        "{\"type\":\"rate_limit\"}\n",
        "{\"type\":\"rate_limit\"}\n",
        "{\"type\":\"rate_limit\"}\n",
        "{\"type\":\"permission_denied\"}\n",
        "{\"type\":\"tool_call\",\"subtype\":\"completed\"}\n",
        "{\"type\":\"tool_call\",\"subtype\":\"completed\"}\n",
    );
    assert_eq!(
        render(AgentKind::Codex, log),
        "event: rate_limit ×3\nevent: permission_denied\nevent: tool_call/completed ×2\n"
    );
}

#[test]
fn an_unrecognised_run_flushes_when_a_recognised_event_or_plain_text_is_rendered() {
    let log = concat!(
        "{\"type\":\"rate_limit\"}\n",
        "{\"type\":\"thread.started\",\"thread_id\":\"abc\"}\n",
        "{\"type\":\"rate_limit\"}\n",
        "stderr from the runner\n",
        "{\"type\":\"rate_limit\"}\n",
    );
    assert_eq!(
        render(AgentKind::Codex, log),
        "event: rate_limit\nsession abc\nevent: rate_limit\nstderr from the runner\nevent: rate_limit\n"
    );
}

#[test]
fn declared_quiet_keys_are_omitted_rather_than_summarised() {
    let cursor = concat!(
        "{\"type\":\"thinking\",\"subtype\":\"delta\",\"text\":\"secret reasoning\"}\n",
        "{\"type\":\"thinking\",\"subtype\":\"delta\",\"text\":\"more tokens\"}\n",
        "{\"type\":\"thinking\",\"subtype\":\"completed\"}\n",
        "{\"type\":\"user\",\"message\":{\"content\":\"the prompt\"}}\n",
    );
    let rendered = render(AgentKind::Cursor, cursor);
    assert_eq!(rendered, "event: thinking/completed\nevent: user\n");
    assert!(
        !rendered.contains("secret reasoning") && !rendered.contains("more tokens"),
        "quiet thinking deltas must not leak their text: {rendered}"
    );

    let codex = "{\"type\":\"item.updated\",\"item\":{\"text\":\"partial answer\"}}\n";
    let rendered = render(AgentKind::Codex, codex);
    assert_eq!(rendered, "");
    assert!(
        !rendered.contains("partial answer"),
        "quiet item.updated patches must not leak their text: {rendered}"
    );
}

#[test]
fn a_hostile_type_is_capped_to_forty_ascii_characters() {
    let hostile = "A".repeat(80);
    let log = format!("{{\"type\":\"{hostile}\"}}\n");
    let rendered = render(AgentKind::Codex, &log);
    assert_eq!(rendered, format!("event: {}\n", "A".repeat(40)));
    assert_eq!(
        rendered.lines().count(),
        1,
        "the summary must stay one line: {rendered:?}"
    );
}

#[test]
fn an_unrecognised_event_summary_never_prints_payload_fields() {
    let log = concat!(
        "{\"type\":\"permission_denied\",\"path\":\"/secret/home/prompt.md\",",
        "\"prompt\":\"do the private thing\",\"arguments\":{\"cmd\":\"rm -rf /\"}}\n",
    );
    let rendered = render(AgentKind::Codex, log);
    assert_eq!(rendered, "event: permission_denied\n");
    assert!(
        !rendered.contains("/secret/home/prompt.md")
            && !rendered.contains("do the private thing")
            && !rendered.contains("rm -rf"),
        "payload fields leaked into the summary: {rendered}"
    );
}

#[test]
fn a_fold_does_not_carry_across_separate_render_calls() {
    // `task logs -f` hands the renderer one chunk per poll. A run split
    // across two calls prints twice; that is the documented trade-off for
    // a deterministic per-call fold.
    let first = render(
        AgentKind::Codex,
        "{\"type\":\"rate_limit\"}\n{\"type\":\"rate_limit\"}\n",
    );
    let second = render(AgentKind::Codex, "{\"type\":\"rate_limit\"}\n");
    assert_eq!(first, "event: rate_limit ×2\n");
    assert_eq!(second, "event: rate_limit\n");
}

#[test]
fn json_without_a_type_uses_the_unrecognised_key() {
    assert_eq!(
        render(AgentKind::Codex, "{\"foo\":1}\n"),
        "event: unrecognised\n"
    );
}
