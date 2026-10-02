use mac_worker::test_support::{core::error::WorkerError, session::*};
use std::{
    fs::{self, File, FileTimes},
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
