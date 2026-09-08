//! The turn log renderer must print every recorded agent transcript exactly as
//! `worker task logs` printed it before the renderer moved into `turn_log`.
//! The references under `tests/fixtures/turn_log/` were dumped from that
//! pre-move renderer; the worker's herdr pane relies on the same output.

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

#[test]
fn plain_text_passes_through_and_unrecognised_json_stays_hidden() {
    // Runner logs mix agent events with stderr and pre-launch failures. The
    // reader needs the diagnostics; unknown structured events would be noise.
    let log = b"codex: command not found\n{\"type\":\"unknown_event\"}\n{not json\n";
    let mut rendered = Vec::new();
    render_agent_log(log, AgentKind::Codex, &mut rendered).unwrap();
    assert_eq!(rendered, b"codex: command not found\n{not json\n");
}
