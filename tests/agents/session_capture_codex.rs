use mac_worker::test_support::session::{
    CODEX_ROLLOUT_FILE, CaptureContext, CapturedSession, FakeAgentHome, MAX_FILE_BYTES, SCRUBBED,
    SESSION_TOKEN, Scrubber, SessionAgent, SessionSelector, WORKSPACE_TOKEN, capture_for,
    codex_fixture, materialize,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

const ID: &str = "01234567-89ab-4cde-8012-3456789abcde";
const OTHER_ID: &str = "11234567-89ab-4cde-8012-3456789abcde";
const SECRET: &str = "sk-ant-api03-SYNTHETIC0123456789abcdefghij";

struct Fixture {
    home: FakeAgentHome,
    project: PathBuf,
    scrubber: Scrubber,
}

impl Fixture {
    fn new() -> Self {
        let home = FakeAgentHome::new();
        let project = home.home().join("project");
        fs::create_dir(&project).unwrap();
        Self {
            home,
            project,
            scrubber: Scrubber::new(Vec::new()),
        }
    }

    fn cx(&self) -> CaptureContext<'_> {
        CaptureContext {
            project_root: &self.project,
            home: self.home.home(),
            scrubber: &self.scrubber,
            now: SystemTime::now(),
        }
    }

    fn source(&self) -> PathBuf {
        self.home
            .codex(ID, self.project.to_str().unwrap(), "0.160.0", 1)
    }

    fn rollout(&self, date: &str, timestamp: &str, id: &str, cwd: &Path) -> PathBuf {
        let dir = self.home.home().join(".codex/sessions").join(date);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("rollout-{timestamp}-{id}.jsonl"));
        fs::write(
            &path,
            codex_fixture(id, cwd.to_str().unwrap(), "0.160.0", 1),
        )
        .unwrap();
        path
    }

    fn discover(&self, selector: &str) -> Result<PathBuf, String> {
        let selector: SessionSelector = selector.parse().unwrap();
        capture_for(SessionAgent::Codex)
            .discover(&selector, &self.cx())
            .map_err(|error| error.to_string())
    }

    fn capture(&self, source: &Path) -> Result<CapturedSession, String> {
        capture_for(SessionAgent::Codex)
            .capture(source, &self.cx())
            .map_err(|error| error.to_string())
    }
}

fn append(path: &Path, line: Value) {
    let mut bytes = fs::read(path).unwrap();
    bytes.extend(serde_json::to_vec(&line).unwrap());
    bytes.push(b'\n');
    fs::write(path, bytes).unwrap();
}

fn rewrite_meta(path: &Path, change: impl FnOnce(&mut Value)) {
    let bytes = fs::read(path).unwrap();
    let end = bytes.iter().position(|&byte| byte == b'\n').unwrap();
    let mut meta: Value = serde_json::from_slice(&bytes[..end]).unwrap();
    change(&mut meta);
    let mut changed = serde_json::to_vec(&meta).unwrap();
    changed.extend_from_slice(&bytes[end..]);
    fs::write(path, changed).unwrap();
}

fn package_text(captured: &CapturedSession) -> &str {
    assert_eq!(captured.package.files().len(), 1);
    assert_eq!(captured.package.files()[0].path, CODEX_ROLLOUT_FILE);
    std::str::from_utf8(&captured.package.files()[0].bytes).unwrap()
}

fn capture_error(fixture: &Fixture, path: &Path) -> String {
    fixture.capture(path).err().expect("capture should fail")
}

#[test]
fn explicit_id_searches_all_dates_and_prefers_newest_directory() {
    let fixture = Fixture::new();
    fixture.source();
    let newest = fixture.rollout("2027/02/03", "2000-01-01T00-00-00", ID, &fixture.project);
    fixture.rollout(
        "2028/01/01",
        "2028-01-01T00-00-00",
        OTHER_ID,
        &fixture.project,
    );
    assert_eq!(
        fixture
            .discover(&format!("codex:{}", ID.to_uppercase()))
            .unwrap(),
        newest
    );
    let captured = fixture.capture(&newest).unwrap();
    assert_eq!(captured.package.manifest().agent, SessionAgent::Codex);
    assert_eq!(captured.package.manifest().source_session_id, ID);
    assert_eq!(captured.package.manifest().source_agent_version, "0.160.0");
    assert_eq!(captured.source_path, newest);
}

#[test]
fn compressed_only_is_unreadable_but_plain_copy_wins() {
    let fixture = Fixture::new();
    let source = fixture.rollout("2027/01/01", "2027-01-01T00-00-00", ID, &fixture.project);
    let compressed = source.with_extension("jsonl.zst");
    fs::rename(source, &compressed).unwrap();
    let error = fixture.discover(&format!("codex:{ID}")).unwrap_err();
    assert!(error.contains("SESSION_UNREADABLE"), "{error}");
    assert!(error.contains("compressed Codex sessions are not supported"));
    let plain = fixture.source();
    assert_eq!(fixture.discover(&format!("codex:{ID}")).unwrap(), plain);
}

#[test]
fn missing_sessions_return_not_found() {
    let fixture = Fixture::new();
    for selector in ["codex".to_owned(), format!("codex:{ID}")] {
        assert!(
            fixture
                .discover(&selector)
                .unwrap_err()
                .contains("SESSION_NOT_FOUND")
        );
    }
    fs::remove_dir_all(fixture.home.home().join(".codex/sessions")).unwrap();
    assert!(
        fixture
            .discover("codex")
            .unwrap_err()
            .contains("SESSION_NOT_FOUND")
    );
}

#[test]
fn latest_filters_cwd_and_orders_by_date_then_filename_not_mtime() {
    let fixture = Fixture::new();
    fixture.source();
    let subdir = fixture.project.join("subdir");
    fs::create_dir(&subdir).unwrap();
    fixture.rollout("2027/02/03", "2027-02-03T01-00-00", ID, &subdir);
    let chosen = fixture.rollout("2027/02/03", "2027-02-03T02-00-00", OTHER_ID, &subdir);
    // Written last, and with a later filename, but in an older date directory.
    fixture.rollout("2026/12/31", "2029-01-01T00-00-00", ID, &fixture.project);
    let sibling = fixture.home.home().join("project-other");
    fs::create_dir(&sibling).unwrap();
    fixture.rollout("2028/01/01", "2028-01-01T00-00-00", ID, &sibling);
    assert_eq!(fixture.discover("codex").unwrap(), chosen);
    assert_eq!(
        fixture
            .capture(&chosen)
            .unwrap()
            .package
            .manifest()
            .source_cwd_relative,
        "subdir"
    );
}

#[test]
fn latest_checks_at_most_200_rollouts_deterministically() {
    let fixture = Fixture::new();
    let outside = fixture.home.home();
    let mut boundary = PathBuf::new();
    for index in 0..=200 {
        let cwd = if index == 0 {
            &fixture.project
        } else {
            outside
        };
        let path = fixture.rollout(
            "2026/01/01",
            &format!("2026-01-01T00-00-{index:03}"),
            ID,
            cwd,
        );
        if index == 1 {
            boundary = path;
        }
    }
    for _ in 0..2 {
        assert!(
            fixture
                .discover("codex")
                .unwrap_err()
                .contains("SESSION_NOT_FOUND")
        );
    }
    fs::write(
        &boundary,
        codex_fixture(ID, fixture.project.to_str().unwrap(), "0.160.0", 1),
    )
    .unwrap();
    for _ in 0..2 {
        assert_eq!(fixture.discover("codex").unwrap(), boundary);
    }
}

#[test]
fn latest_reads_only_bounded_first_lines_and_skips_unusable_metadata() {
    let fixture = Fixture::new();
    let chosen = fixture.source();
    let mut bytes = fs::read(&chosen).unwrap();
    bytes.extend_from_slice(b"not valid JSON in a later line\n");
    fs::write(&chosen, bytes).unwrap();
    let too_long = fixture.rollout("2027/01/01", "2027-01-01T00-00-00", ID, &fixture.project);
    let mut bytes = codex_fixture(ID, fixture.project.to_str().unwrap(), "0.160.0", 0);
    bytes.pop();
    bytes.extend(vec![b' '; 1 << 20]);
    bytes.push(b'\n');
    fs::write(too_long, bytes).unwrap();
    let malformed = fixture.rollout("2028/01/01", "2028-01-01T00-00-00", ID, &fixture.project);
    fs::write(malformed, b"not JSON\n").unwrap();
    assert_eq!(fixture.discover("codex").unwrap(), chosen);
    assert!(capture_error(&fixture, &chosen).contains("SESSION_UNREADABLE"));
}

#[test]
fn capture_rejects_outside_cwd_and_symlink_escape() {
    let fixture = Fixture::new();
    let source = fixture
        .home
        .codex(ID, fixture.home.home().to_str().unwrap(), "0.160.0", 1);
    assert!(capture_error(&fixture, &source).contains("SESSION_OUTSIDE_PROJECT"));
    let escape = fixture.project.join("escape");
    std::os::unix::fs::symlink(fixture.home.home(), &escape).unwrap();
    rewrite_meta(&source, |meta| meta["payload"]["cwd"] = json!(escape));
    assert!(capture_error(&fixture, &source).contains("SESSION_OUTSIDE_PROJECT"));
}

#[test]
fn capture_requires_matching_lowercase_filename_and_metadata_id() {
    let fixture = Fixture::new();
    let source = fixture.source();
    rewrite_meta(&source, |meta| meta["payload"]["id"] = json!(OTHER_ID));
    assert!(capture_error(&fixture, &source).contains("SESSION_UNREADABLE"));
    let source = fixture.source();
    let uppercase = source.with_file_name(format!(
        "rollout-2026-01-01T00-00-00-{}.jsonl",
        ID.to_uppercase()
    ));
    fs::rename(source, &uppercase).unwrap();
    assert!(capture_error(&fixture, &uppercase).contains("SESSION_UNREADABLE"));
}

#[test]
fn capture_requires_session_meta_cwd_and_cli_version() {
    let fixture = Fixture::new();
    for key in ["id", "cwd", "cli_version"] {
        let source = fixture.source();
        rewrite_meta(&source, |meta| {
            meta["payload"].as_object_mut().unwrap().remove(key);
        });
        assert!(capture_error(&fixture, &source).contains("SESSION_UNREADABLE"));
    }
    for version in [json!(null), json!(3), json!(""), json!("bad version")] {
        let source = fixture.source();
        rewrite_meta(&source, |meta| meta["payload"]["cli_version"] = version);
        assert!(capture_error(&fixture, &source).contains("SESSION_UNREADABLE"));
    }
    let source = fixture.source();
    rewrite_meta(&source, |meta| meta["type"] = json!("response_item"));
    assert!(capture_error(&fixture, &source).contains("SESSION_UNREADABLE"));
    fs::write(&source, b"").unwrap();
    assert!(capture_error(&fixture, &source).contains("SESSION_UNREADABLE"));
}

#[test]
fn partial_last_line_is_ignored_without_modifying_source() {
    let fixture = Fixture::new();
    let source = fixture.source();
    let complete = fs::read(&source).unwrap();
    let mut original = complete.clone();
    original.extend_from_slice(b"{\"type\":\"event_msg\",\"payload\":");
    fs::write(&source, &original).unwrap();
    let captured = fixture.capture(&source).unwrap();
    let restored = materialize(
        &captured.package.files()[0].bytes,
        fixture.project.to_str().unwrap(),
        ID,
    )
    .unwrap();
    assert_eq!(restored, complete);
    assert_eq!(fs::read(source).unwrap(), original);
}

#[test]
fn invalid_complete_line_and_oversized_file_are_rejected() {
    let fixture = Fixture::new();
    let source = fixture.source();
    let mut bytes = fs::read(&source).unwrap();
    bytes.extend_from_slice(b"{invalid}\n");
    fs::write(&source, bytes).unwrap();
    assert!(capture_error(&fixture, &source).contains("SESSION_UNREADABLE"));
    fs::OpenOptions::new()
        .write(true)
        .open(&source)
        .unwrap()
        .set_len(MAX_FILE_BYTES + 1)
        .unwrap();
    assert!(capture_error(&fixture, &source).contains("SESSION_TOO_LARGE"));
}

#[test]
fn every_payload_field_is_scrubbed_and_token_rewritten_with_path_boundaries() {
    let fixture = Fixture::new();
    let source = fixture.source();
    let root = fixture.project.to_str().unwrap();
    append(
        &source,
        json!({"type":"event_msg", "payload":{
            "type":"exec_command_end", "item":{"aggregated_output":format!("{root}/notes.md {SECRET}")},
            "runtime_workspace_roots":[root],
            "state":{"environments":{"local":{"cwd":root}}},
            "unrelated":format!("{root}-other/notes.md"), "thread_id":ID,
            "output":SECRET
        }}),
    );
    let captured = fixture.capture(&source).unwrap();
    assert_eq!(captured.package.manifest().scrubbed, 2);
    assert_eq!(captured.package.manifest().source_cwd_relative, "");
    let text = package_text(&captured);
    assert!(!text.contains(SECRET));
    assert!(!text.contains(ID));
    let last: Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    assert_eq!(
        last["payload"]["item"]["aggregated_output"],
        format!("{WORKSPACE_TOKEN}/notes.md {SCRUBBED}")
    );
    assert_eq!(
        last["payload"]["runtime_workspace_roots"][0],
        WORKSPACE_TOKEN
    );
    assert_eq!(
        last["payload"]["state"]["environments"]["local"]["cwd"],
        WORKSPACE_TOKEN
    );
    assert_eq!(last["payload"]["thread_id"], SESSION_TOKEN);
    assert_eq!(
        last["payload"]["unrelated"],
        format!("{root}-other/notes.md")
    );
}

#[test]
fn canonical_and_given_roots_both_match_and_normalize() {
    let fixture = Fixture::new();
    let alias = fixture.home.home().join("project-alias");
    std::os::unix::fs::symlink(&fixture.project, &alias).unwrap();
    let canonical = fixture.project.canonicalize().unwrap();
    let source = fixture
        .home
        .codex(ID, canonical.to_str().unwrap(), "0.160.0", 0);
    append(
        &source,
        json!({"type":"event_msg", "payload":{
            "runtime_workspace_roots":[alias, canonical],
            "item":{"aggregated_output":format!("{}/file.txt", alias.display())}
        }}),
    );
    let mut cx = fixture.cx();
    cx.project_root = &alias;
    let capture = capture_for(SessionAgent::Codex);
    assert_eq!(
        capture.discover(&"codex".parse().unwrap(), &cx).unwrap(),
        source
    );
    let captured = capture.capture(&source, &cx).unwrap();
    let text = package_text(&captured);
    assert!(!text.contains(alias.to_str().unwrap()));
    assert!(!text.contains(canonical.to_str().unwrap()));
    assert!(text.contains(&format!("{WORKSPACE_TOKEN}/file.txt")));
}

#[test]
fn reserved_tokens_and_json_unsafe_roots_are_refused() {
    let fixture = Fixture::new();
    for token in [WORKSPACE_TOKEN, SESSION_TOKEN] {
        let source = fixture.source();
        append(
            &source,
            json!({"type":"event_msg", "payload":{"message":token}}),
        );
        assert!(capture_error(&fixture, &source).contains("SESSION_UNREADABLE"));
    }
    let unsafe_root = fixture.home.home().join("project-\"quoted");
    fs::create_dir(&unsafe_root).unwrap();
    let source = fixture
        .home
        .codex(ID, unsafe_root.to_str().unwrap(), "0.160.0", 0);
    let mut cx = fixture.cx();
    cx.project_root = &unsafe_root;
    let error = capture_for(SessionAgent::Codex)
        .capture(&source, &cx)
        .err()
        .unwrap();
    assert!(error.to_string().contains("SESSION_UNREADABLE"));
}

#[test]
fn preview_prefers_first_user_event_and_never_exposes_secrets() {
    let fixture = Fixture::new();
    let source = fixture.source();
    append(
        &source,
        json!({"type":"event_msg", "payload":{"type":"user_message", "message":format!("  Event\n prompt\t{SECRET}  ")}}),
    );
    append(
        &source,
        json!({"type":"event_msg", "payload":{"type":"user_message", "message":"second event"}}),
    );
    assert_eq!(
        fixture
            .capture(&source)
            .unwrap()
            .first_prompt_preview
            .as_deref(),
        Some("Event prompt [scrubbed]")
    );
}

#[test]
fn preview_falls_back_to_user_input_skips_wrappers_and_truncates_unicode() {
    let fixture = Fixture::new();
    let source = fixture
        .home
        .codex(ID, fixture.project.to_str().unwrap(), "0.160.0", 0);
    append(
        &source,
        json!({"type":"response_item", "payload":{"type":"message", "role":"user", "content":[{"type":"input_text", "text":"<environment_context> wrapper"}, {"type":"input_text", "text":"not the first input"}]}}),
    );
    append(
        &source,
        json!({"type":"response_item", "payload":{"type":"message", "role":"assistant", "content":[{"type":"input_text", "text":"not a user prompt"}]}}),
    );
    append(
        &source,
        json!({"type":"response_item", "payload":{"type":"message", "role":"user", "content":[{"type":"image"}, {"type":"input_text", "text":format!("  {}\n done", "é".repeat(121))}]}}),
    );
    assert_eq!(
        fixture.capture(&source).unwrap().first_prompt_preview,
        Some(format!("{}…", "é".repeat(120)))
    );
    let source = fixture
        .home
        .codex(ID, fixture.project.to_str().unwrap(), "0.160.0", 0);
    assert!(
        fixture
            .capture(&source)
            .unwrap()
            .first_prompt_preview
            .is_none()
    );
}

#[test]
fn materialize_round_trip_reproduces_the_scrubbed_complete_source() {
    let fixture = Fixture::new();
    let source = fixture.source();
    append(
        &source,
        json!({"type":"event_msg", "payload":{"type":"user_message", "message":format!("Remember {SECRET}, cwd {} and thread {ID}", fixture.project.display())}}),
    );
    let original = fs::read_to_string(&source).unwrap();
    let captured = fixture.capture(&source).unwrap();
    let restored = materialize(
        &captured.package.files()[0].bytes,
        fixture.project.to_str().unwrap(),
        ID,
    )
    .unwrap();
    assert_eq!(restored, original.replace(SECRET, SCRUBBED).as_bytes());
    assert_eq!(fs::read_to_string(source).unwrap(), original);
}

#[test]
fn recently_modified_uses_injected_time_and_a_strict_ten_second_boundary() {
    let fixture = Fixture::new();
    let source = fixture.source();
    let modified = fs::metadata(&source).unwrap().modified().unwrap();
    for (age, expected) in [(9, true), (10, false), (60, false)] {
        let mut cx = fixture.cx();
        cx.now = modified + Duration::from_secs(age);
        assert_eq!(
            capture_for(SessionAgent::Codex)
                .capture(&source, &cx)
                .unwrap()
                .recently_modified,
            expected
        );
    }
}
