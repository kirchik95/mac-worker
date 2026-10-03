use mac_worker::test_support::{core::error::WorkerError, session::*};
use std::{
    fs::{self, File, FileTimes},
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

const ID: &str = "abcdefab-1234-5678-90ab-abcdefabcdef";
const OTHER_ID: &str = "abcdefab-1234-5678-90ab-abcdefabcdee";

struct Fixture {
    home: FakeAgentHome,
    project: tempfile::TempDir,
    scrubber: Scrubber,
}

impl Fixture {
    fn new() -> Self {
        Self {
            home: FakeAgentHome::new(),
            project: tempfile::tempdir().unwrap(),
            scrubber: Scrubber::new(vec![]),
        }
    }

    fn root(&self) -> PathBuf {
        self.project.path().canonicalize().unwrap()
    }

    fn cx<'a>(&'a self, root: &'a Path) -> CaptureContext<'a> {
        CaptureContext {
            project_root: root,
            home: self.home.home(),
            scrubber: &self.scrubber,
            now: SystemTime::now(),
        }
    }

    fn session(&self, id: &str, cwd: &Path) -> PathBuf {
        self.home.claude(id, cwd.to_str().unwrap(), "2.1.288", 1)
    }
}

fn capture(fixture: &Fixture, root: &Path, source: &Path) -> Result<CapturedSession, WorkerError> {
    capture_for(SessionAgent::Claude).capture(source, &fixture.cx(root))
}

fn jsonl(values: &[serde_json::Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        bytes.extend(serde_json::to_vec(value).unwrap());
        bytes.push(b'\n');
    }
    bytes
}

fn main_bytes(captured: &CapturedSession) -> &[u8] {
    &captured
        .package
        .files()
        .iter()
        .find(|file| file.path == CLAUDE_MAIN_FILE)
        .unwrap()
        .bytes
}

fn modified(path: &Path, seconds: u64) {
    File::open(path)
        .unwrap()
        .set_times(
            FileTimes::new().set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)),
        )
        .unwrap();
}

fn discover(fixture: &Fixture, root: &Path, selector: &str) -> Result<PathBuf, WorkerError> {
    capture_for(SessionAgent::Claude).discover(&selector.parse().unwrap(), &fixture.cx(root))
}

fn assert_code<T>(result: Result<T, WorkerError>, expected: &str) {
    match result {
        Err(WorkerError::Task { code, .. }) => assert_eq!(code, expected),
        Err(error) => panic!("unexpected error: {error}"),
        Ok(_) => panic!("expected {expected}"),
    }
}

#[test]
fn explicit_id_searches_all_project_directories_but_not_nested_files() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let subdir = root.join("subdir");
    fs::create_dir(&subdir).unwrap();
    let source = fixture.session(ID, &subdir);
    assert_eq!(
        discover(&fixture, &root, &format!("claude:{}", ID.to_uppercase())).unwrap(),
        source
    );
    fs::remove_file(&source).unwrap();
    let nested = source.parent().unwrap().join("nested");
    fs::create_dir(&nested).unwrap();
    fs::write(nested.join(format!("{ID}.jsonl")), b"{}\n").unwrap();
    assert_code(
        discover(&fixture, &root, &format!("claude:{ID}")),
        "SESSION_NOT_FOUND",
    );
}

#[test]
fn ambiguous_explicit_id_prefers_project_then_newest_mtime() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let preferred = fixture.session(ID, &root);
    let older = fixture.session(ID, &root.join("other-one"));
    let newer = fixture.session(ID, &root.join("other-two"));
    modified(&preferred, 10);
    modified(&older, 20);
    modified(&newer, 30);
    assert_eq!(
        discover(&fixture, &root, &format!("claude:{ID}")).unwrap(),
        preferred
    );
    fs::remove_file(&preferred).unwrap();
    assert_eq!(
        discover(&fixture, &root, &format!("claude:{ID}")).unwrap(),
        newer
    );
}

#[test]
fn latest_uses_mtime_and_skips_non_uuid_uppercase_and_nested_entries() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let older = fixture.session(ID, &root);
    let latest = fixture.session(OTHER_ID, &root);
    modified(&older, 10);
    modified(&latest, 20);
    let dir = latest.parent().unwrap();
    for name in [
        "notes.jsonl".to_owned(),
        "AAAABBBB-1234-5678-90AB-AAAABBBBAAAA.jsonl".to_owned(),
    ] {
        fs::write(dir.join(name), b"{}\n").unwrap();
    }
    let nested = dir.join("nested");
    fs::create_dir(&nested).unwrap();
    fs::write(nested.join(format!("{ID}.jsonl")), b"{}\n").unwrap();
    assert_eq!(discover(&fixture, &root, "claude").unwrap(), latest);
}

#[test]
fn latest_subdirectory_session_requires_explicit_id() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root.join("subdir"));
    assert_code(discover(&fixture, &root, "claude"), "SESSION_NOT_FOUND");
    assert_eq!(
        discover(&fixture, &root, &format!("claude:{ID}")).unwrap(),
        source
    );
}

#[test]
fn missing_session_or_native_store_returns_not_found() {
    let fixture = Fixture::new();
    let root = fixture.root();
    assert_code(discover(&fixture, &root, "claude"), "SESSION_NOT_FOUND");
    fs::remove_dir_all(fixture.home.home().join(".claude")).unwrap();
    assert_code(
        discover(&fixture, &root, &format!("claude:{ID}")),
        "SESSION_NOT_FOUND",
    );
}

#[test]
fn latest_searches_given_and_canonical_symlinked_root_by_mtime() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let alias = fixture.home.home().join("project-alias");
    std::os::unix::fs::symlink(&root, &alias).unwrap();
    let canonical_source = fixture.session(ID, &root);
    assert_eq!(
        discover(&fixture, &alias, "claude").unwrap(),
        canonical_source
    );
    let given_source = fixture.session(OTHER_ID, &alias);
    modified(&canonical_source, 10);
    modified(&given_source, 20);
    assert_eq!(discover(&fixture, &alias, "claude").unwrap(), given_source);
    modified(&canonical_source, 30);
    assert_eq!(
        discover(&fixture, &alias, "claude").unwrap(),
        canonical_source
    );
}

#[test]
fn claude_capture_rejects_another_agents_selector() {
    let fixture = Fixture::new();
    let root = fixture.root();
    assert_code(discover(&fixture, &root, "codex"), "SESSION_AGENT_MISMATCH");
}

#[test]
fn capture_builds_a_manifest_and_round_trips_scrubbed_source_bytes() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    let mut bytes = claude_fixture(ID, root.to_str().unwrap(), "2.1.288", 2);
    bytes.extend(jsonl(&[serde_json::json!({
        "type": "attachment", "cwd": root, "version": "2.1.288",
        "attachment": {"filePath": root.join("src/file.rs"), "session": ID},
        "toolUseResult": {"filePath": root.join("result"), "text": "sk-ant-abcdefghijklmnopqrstuv"},
        "wireToolInputs": {"file_path": root.join("input")}
    })]));
    fs::write(&source, &bytes).unwrap();
    let captured = capture(&fixture, &root, &source).unwrap();
    assert_eq!(captured.source_path, source);
    assert_eq!(captured.package.manifest().agent, SessionAgent::Claude);
    assert_eq!(
        captured.package.manifest().format,
        SessionFormat::ClaudeJsonlV1
    );
    assert_eq!(captured.package.manifest().source_session_id, ID);
    assert_eq!(captured.package.manifest().source_agent_version, "2.1.288");
    assert_eq!(captured.package.manifest().source_cwd_relative, "");
    assert_eq!(captured.package.manifest().scrubbed, 1);
    let scrubbed: Vec<u8> = bytes
        .split_inclusive(|&b| b == b'\n')
        .flat_map(|line| {
            let mut result = fixture
                .scrubber
                .scrub_line(&line[..line.len() - 1])
                .unwrap()
                .bytes;
            result.push(b'\n');
            result
        })
        .collect();
    assert_eq!(
        materialize(main_bytes(&captured), root.to_str().unwrap(), ID).unwrap(),
        scrubbed
    );
    assert_eq!(fs::read(&source).unwrap(), bytes, "capture is read-only");
    SessionPackage::from_parts(
        &captured.package.manifest_json(),
        captured.package.files().to_vec(),
    )
    .unwrap();
}

#[test]
fn capture_records_first_cwd_relative_and_numeric_maximum_version_ignoring_suffixes() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let subdir = root.join("nested");
    fs::create_dir(&subdir).unwrap();
    let source = fixture.session(ID, &subdir);
    fs::write(
        &source,
        jsonl(&[
            serde_json::json!({"cwd": 1, "version": "2.9.999"}),
            serde_json::json!({"cwd": subdir, "version": "2.10.1-beta.999"}),
            serde_json::json!({"cwd": fixture.home.home(), "version": "2.10.0"}),
            serde_json::json!({"version": "2.9.10000"}),
        ]),
    )
    .unwrap();
    let captured = capture(&fixture, &root, &source).unwrap();
    assert_eq!(captured.package.manifest().source_cwd_relative, "nested");
    assert_eq!(
        captured.package.manifest().source_agent_version,
        "2.10.1-beta.999"
    );
}

#[test]
fn capture_refuses_outside_project_including_symlinked_cwd() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, fixture.home.home());
    assert_code(capture(&fixture, &root, &source), "SESSION_OUTSIDE_PROJECT");
    let link = root.join("escape");
    std::os::unix::fs::symlink(fixture.home.home(), &link).unwrap();
    fs::write(
        &source,
        claude_fixture(ID, link.to_str().unwrap(), "2.1.288", 1),
    )
    .unwrap();
    assert_code(capture(&fixture, &root, &source), "SESSION_OUTSIDE_PROJECT");
}

#[test]
fn capture_refuses_missing_cwd_missing_version_invalid_json_and_empty_transcript() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    for bytes in [
        jsonl(&[serde_json::json!({"version":"2.1.288"})]),
        jsonl(&[serde_json::json!({"cwd":root})]),
        jsonl(&[serde_json::json!({"cwd":null,"version":123})]),
        b"not JSON\n".to_vec(),
        b"".to_vec(),
    ] {
        fs::write(&source, bytes).unwrap();
        assert_code(capture(&fixture, &root, &source), "SESSION_UNREADABLE");
    }
}

#[test]
fn capture_drops_only_the_partial_last_line() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    let bytes = fs::read(&source).unwrap();
    let mut incomplete = bytes.clone();
    incomplete.extend(b"{not complete JSON");
    fs::write(&source, incomplete).unwrap();
    let captured = capture(&fixture, &root, &source).unwrap();
    assert_eq!(
        materialize(main_bytes(&captured), root.to_str().unwrap(), ID).unwrap(),
        bytes
    );
}

#[test]
fn capture_normalizes_given_and_canonical_roots_everywhere() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let alias = fixture.home.home().join("alias");
    std::os::unix::fs::symlink(&root, &alias).unwrap();
    let source = fixture.session(ID, &root);
    fs::write(
        &source,
        jsonl(&[serde_json::json!({
            "cwd": root, "version": "2.1.288", "sessionId": ID,
            "paths": [root.join("real"), alias.join("given")],
            "text": format!("{} {} {ID}", root.display(), alias.display())
        })]),
    )
    .unwrap();
    let captured = capture(&fixture, &alias, &source).unwrap();
    let line: serde_json::Value = serde_json::from_slice(main_bytes(&captured)).unwrap();
    assert_eq!(
        line["paths"],
        serde_json::json!([
            format!("{WORKSPACE_TOKEN}/real"),
            format!("{WORKSPACE_TOKEN}/given")
        ])
    );
    assert_eq!(line["sessionId"], SESSION_TOKEN);
    assert_eq!(
        line["text"],
        format!("{WORKSPACE_TOKEN} {WORKSPACE_TOKEN} {SESSION_TOKEN}")
    );
}

#[test]
fn capture_sidecar_is_sorted_recursive_scrubbed_normalized_and_round_trips() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    let sidecar = source.parent().unwrap().join(ID);
    fs::create_dir_all(sidecar.join("subagents/deeper")).unwrap();
    let agent_id = "a1b2c3d4";
    let mut subagent = claude_fixture(ID, root.to_str().unwrap(), "2.1.288", 1);
    subagent.extend(jsonl(&[
        serde_json::json!({"text": "Bearer abcdefghijklmnopqrstuv"}),
    ]));
    let meta = format!("{{\n  \"cwd\": \"{}\",\n  \"sessionId\": \"{ID}\",\n  \"secret\": \"sk-ant-abcdefghijklmnopqrstuv\"\n}}\n", root.display()).into_bytes();
    let text = format!("{}/file {ID} sk-ant-abcdefghijklmnopqrstuv", root.display()).into_bytes();
    let binary = [
        vec![0, 255],
        format!(" {}/asset {ID}", root.display()).into_bytes(),
    ]
    .concat();
    let entries = [
        (
            format!("subagents/agent-{agent_id}.jsonl"),
            subagent.clone(),
        ),
        (
            format!("subagents/agent-{agent_id}.meta.json"),
            meta.clone(),
        ),
        ("subagents/deeper/z.txt".to_owned(), text.clone()),
        ("a.bin".to_owned(), binary.clone()),
    ];
    for (relative, bytes) in &entries {
        fs::write(sidecar.join(relative), bytes).unwrap();
    }
    let mut partial = subagent.clone();
    partial.extend(b"{partial");
    fs::write(sidecar.join(&entries[0].0), partial).unwrap();
    let captured = capture(&fixture, &root, &source).unwrap();
    assert_eq!(captured.package.manifest().scrubbed, 2);
    let paths: Vec<_> = captured
        .package
        .files()
        .iter()
        .map(|file| file.path.as_str())
        .collect();
    assert_eq!(
        paths,
        vec![
            "main.jsonl",
            "sidecar/a.bin",
            "sidecar/subagents/agent-a1b2c3d4.jsonl",
            "sidecar/subagents/agent-a1b2c3d4.meta.json",
            "sidecar/subagents/deeper/z.txt"
        ]
    );
    for (relative, bytes) in entries {
        let file = captured
            .package
            .files()
            .iter()
            .find(|file| file.path == format!("sidecar/{relative}"))
            .unwrap();
        let expected = if relative.ends_with(".jsonl") {
            bytes
                .split_inclusive(|&b| b == b'\n')
                .flat_map(|line| {
                    let mut scrubbed = fixture
                        .scrubber
                        .scrub_line(&line[..line.len() - 1])
                        .unwrap()
                        .bytes;
                    scrubbed.push(b'\n');
                    scrubbed
                })
                .collect()
        } else if relative.ends_with(".json") {
            fixture.scrubber.scrub_line(&bytes).unwrap().bytes
        } else {
            bytes
        };
        assert_eq!(
            materialize(&file.bytes, root.to_str().unwrap(), ID).unwrap(),
            expected
        );
    }
}

#[test]
fn capture_skips_sidecar_symlinks_and_other_non_regular_entries() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    let sidecar = source.parent().unwrap().join(ID);
    fs::create_dir(&sidecar).unwrap();
    fs::write(sidecar.join("regular.txt"), b"kept").unwrap();
    std::os::unix::fs::symlink(&source, sidecar.join("linked.jsonl")).unwrap();
    std::os::unix::fs::symlink(fixture.home.home(), sidecar.join("directory-link")).unwrap();
    let fifo = std::ffi::CString::new(sidecar.join("pipe").as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let captured = capture(&fixture, &root, &source).unwrap();
    assert_eq!(captured.package.files().len(), 2);
    assert_eq!(captured.package.files()[1].path, "sidecar/regular.txt");
    fs::remove_dir_all(&sidecar).unwrap();
    std::os::unix::fs::symlink(&root, &sidecar).unwrap();
    assert_eq!(
        capture(&fixture, &root, &source)
            .unwrap()
            .package
            .files()
            .len(),
        1
    );
}

#[test]
fn capture_validates_sidecar_json_documents_and_complete_jsonl_lines() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    let sidecar = source.parent().unwrap().join(ID);
    fs::create_dir(&sidecar).unwrap();
    for (name, bytes) in [
        ("bad.json", b"{broken".as_slice()),
        ("bad.jsonl", b"{broken\n".as_slice()),
    ] {
        let path = sidecar.join(name);
        fs::write(&path, bytes).unwrap();
        assert_code(capture(&fixture, &root, &source), "SESSION_UNREADABLE");
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn capture_refuses_reserved_tokens_in_main_and_every_sidecar_file_type() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    let original = fs::read(&source).unwrap();
    for token in [WORKSPACE_TOKEN, SESSION_TOKEN] {
        let mut bytes = original.clone();
        bytes.extend(jsonl(&[serde_json::json!({"text":token})]));
        fs::write(&source, bytes).unwrap();
        assert_code(capture(&fixture, &root, &source), "SESSION_UNREADABLE");
    }
    fs::write(&source, original).unwrap();
    let sidecar = source.parent().unwrap().join(ID);
    fs::create_dir(&sidecar).unwrap();
    for name in ["token.txt", "token.json", "token.jsonl"] {
        let path = sidecar.join(name);
        fs::write(&path, jsonl(&[serde_json::json!({"text":SESSION_TOKEN})])).unwrap();
        assert_code(capture(&fixture, &root, &source), "SESSION_UNREADABLE");
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn capture_preview_is_scrubbed_collapsed_and_unicode_safe() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    let prompt = format!(
        "  hello\n\t sk-ant-abcdefghijklmnopqrstuv   {}",
        "é🦀".repeat(80)
    );
    fs::write(&source, jsonl(&[
        serde_json::json!({"cwd":root,"version":"2.1.288","type":"user","message":{"content":"  <command>skip me</command>"}}),
        serde_json::json!({"type":"assistant","message":{"content":"skip assistant"}}),
        serde_json::json!({"type":"user","message":{"content":[{"type":"tool_result","content":"skip tool text"}]}}),
        serde_json::json!({"type":"user","message":{"content":[{"type":"tool_result","content":"skip"},{"type":"text","text":prompt}]}}),
        serde_json::json!({"type":"user","message":{"content":"not the first prompt"}})
    ])).unwrap();
    let captured = capture(&fixture, &root, &source).unwrap();
    let expected: String = format!("hello [scrubbed] {}", "é🦀".repeat(80))
        .chars()
        .take(120)
        .chain(['…'])
        .collect();
    assert_eq!(
        captured.first_prompt_preview.as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(captured.package.manifest().scrubbed, 1);
}

#[test]
fn capture_preview_supports_string_content_and_absent_human_prompts() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    assert_eq!(
        capture(&fixture, &root, &source)
            .unwrap()
            .first_prompt_preview
            .as_deref(),
        Some("Synthetic prompt 0")
    );
    fs::write(&source, jsonl(&[
        serde_json::json!({"cwd":root,"version":"2.1.288","type":"user","message":{"content":[{"type":"text","text":"<system>skip</system>"},{"type":"text","text":"also skip"}]}}),
        serde_json::json!({"type":"user","message":{"content":" \n\t "}})
    ])).unwrap();
    assert!(
        capture(&fixture, &root, &source)
            .unwrap()
            .first_prompt_preview
            .is_none()
    );
}

#[test]
fn capture_recently_modified_uses_frozen_clock_and_strict_ten_second_threshold() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    modified(&source, 100);
    for (now, recent) in [(100, true), (109, true), (110, false), (111, false)] {
        let mut cx = fixture.cx(&root);
        cx.now = SystemTime::UNIX_EPOCH + Duration::from_secs(now);
        let captured = capture_for(SessionAgent::Claude)
            .capture(&source, &cx)
            .unwrap();
        assert_eq!(captured.recently_modified, recent);
    }
}

#[test]
fn capture_enforces_main_and_sidecar_per_file_caps() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    File::options()
        .write(true)
        .open(&source)
        .unwrap()
        .set_len(MAX_FILE_BYTES + 1)
        .unwrap();
    assert_code(capture(&fixture, &root, &source), "SESSION_TOO_LARGE");
    fs::write(
        &source,
        claude_fixture(ID, root.to_str().unwrap(), "2.1.288", 1),
    )
    .unwrap();
    let sidecar = source.parent().unwrap().join(ID);
    fs::create_dir(&sidecar).unwrap();
    File::create(sidecar.join("large.bin"))
        .unwrap()
        .set_len(MAX_FILE_BYTES + 1)
        .unwrap();
    assert_code(capture(&fixture, &root, &source), "SESSION_TOO_LARGE");
}

#[test]
fn capture_enforces_total_raw_byte_cap_before_normalization() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    let sidecar = source.parent().unwrap().join(ID);
    fs::create_dir(&sidecar).unwrap();
    for name in ["one.bin", "two.bin"] {
        File::create(sidecar.join(name))
            .unwrap()
            .set_len(MAX_PACKAGE_BYTES / 2)
            .unwrap();
    }
    assert_code(capture(&fixture, &root, &source), "SESSION_TOO_LARGE");
}

#[test]
fn capture_enforces_file_count_including_main_transcript() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    let sidecar = source.parent().unwrap().join(ID);
    fs::create_dir(&sidecar).unwrap();
    for i in 0..MAX_PACKAGE_FILES - 1 {
        fs::write(sidecar.join(format!("{i:04}.txt")), b"small").unwrap();
    }
    assert_eq!(
        capture(&fixture, &root, &source)
            .unwrap()
            .package
            .files()
            .len(),
        MAX_PACKAGE_FILES
    );
    fs::write(sidecar.join("one-too-many.txt"), b"small").unwrap();
    assert_code(capture(&fixture, &root, &source), "SESSION_TOO_LARGE");
}

#[test]
fn capture_refuses_reserved_tokens_before_main_scrubbing_can_hide_them() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    for token in [WORKSPACE_TOKEN, SESSION_TOKEN] {
        let line = serde_json::json!({
            "cwd": root, "version": "2.1.288",
            "text": format!("-----BEGIN PRIVATE KEY-----\n{token}\n-----END PRIVATE KEY-----")
        });
        assert_eq!(
            fixture
                .scrubber
                .scrub_line(&serde_json::to_vec(&line).unwrap())
                .unwrap()
                .replacements,
            1
        );
        fs::write(&source, jsonl(&[line])).unwrap();
        assert_code(capture(&fixture, &root, &source), "SESSION_UNREADABLE");
    }
}

#[test]
fn capture_refuses_reserved_tokens_before_sidecar_scrubbing_can_hide_them() {
    let fixture = Fixture::new();
    let root = fixture.root();
    let source = fixture.session(ID, &root);
    let sidecar = source.parent().unwrap().join(ID);
    fs::create_dir(&sidecar).unwrap();
    for name in ["token.json", "token.jsonl"] {
        let path = sidecar.join(name);
        fs::write(&path, jsonl(&[serde_json::json!({
            "text": format!("-----BEGIN PRIVATE KEY-----\n{SESSION_TOKEN}\n-----END PRIVATE KEY-----")
        })])).unwrap();
        assert_code(capture(&fixture, &root, &source), "SESSION_UNREADABLE");
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn capture_rejects_json_unsafe_roots_and_preserves_non_ascii_roots() {
    let fixture = Fixture::new();
    for name in ["quote-\"", "backslash-\\", "control-\n"] {
        let root = fixture.root().join(name);
        fs::create_dir(&root).unwrap();
        let source = fixture.session(ID, &root);
        assert_code(capture(&fixture, &root, &source), "SESSION_UNREADABLE");
    }
    let root = fixture.root().join("проект-é");
    fs::create_dir(&root).unwrap();
    let source = fixture.session(ID, &root);
    let captured = capture(&fixture, &root, &source).unwrap();
    assert_eq!(
        materialize(main_bytes(&captured), root.to_str().unwrap(), ID).unwrap(),
        fs::read(&source).unwrap()
    );
}
