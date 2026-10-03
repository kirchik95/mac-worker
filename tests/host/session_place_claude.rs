use mac_worker::test_support::{core::error::WorkerError, session::*};
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    fs,
    os::unix::{ffi::OsStringExt, fs::symlink},
    path::{Path, PathBuf},
};

const SOURCE_ID: &str = "abcdefab-1234-4678-90ab-abcdefabcdef";
const IMPORTED_ID: &str = "12345678-abcd-4321-9876-0123456789ab";
const SOURCE_ROOT: &str = "/synthetic/source";
const WORKSPACE: &str = "/host/tasks/synthetic/workspace";
const VERSION: &str = "2.1.288";

struct Fixture {
    root: tempfile::TempDir,
    store: StoreWriter,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let store = StoreWriter::open(root.path()).unwrap();
        Self { root, store }
    }

    fn place(
        &self,
        package: &SessionPackage,
        workspace: &Path,
    ) -> Result<PlacedSession, WorkerError> {
        place_for(SessionAgent::Claude).place(
            package,
            &PlaceContext {
                workspace,
                store: &self.store,
                session_id: IMPORTED_ID,
                placed_at_millis: 1_791_014_400_000,
            },
        )
    }

    fn read(&self, relative: &str) -> Option<Vec<u8>> {
        self.store.read_file(relative, MAX_FILE_BYTES).unwrap()
    }
}

fn package(agent: SessionAgent, files: Vec<(&str, Vec<u8>)>) -> SessionPackage {
    SessionPackage::build(
        PackageSource {
            agent,
            source_session_id: SOURCE_ID.into(),
            source_agent_version: VERSION.into(),
            source_cwd_relative: String::new(),
            scrubbed: 0,
        },
        files
            .into_iter()
            .map(|(path, bytes)| PackageFile {
                path: path.into(),
                bytes: normalize(&bytes, &[SOURCE_ROOT], SOURCE_ID).unwrap(),
            })
            .collect(),
    )
    .unwrap()
}

fn main_bytes() -> Vec<u8> {
    claude_fixture(SOURCE_ID, SOURCE_ROOT, VERSION, 2)
}

fn main_package() -> SessionPackage {
    package(SessionAgent::Claude, vec![(CLAUDE_MAIN_FILE, main_bytes())])
}

fn project(workspace: &str) -> String {
    format!("projects/{}", claude_project_dir(workspace))
}

fn primary(workspace: &str) -> String {
    format!("{}/{IMPORTED_ID}.jsonl", project(workspace))
}

fn sidecar(workspace: &str, relative: &str) -> String {
    format!("{}/{IMPORTED_ID}/{relative}", project(workspace))
}

fn assert_placement_failed(result: Result<PlacedSession, WorkerError>) {
    match result {
        Err(WorkerError::Task { code, .. }) => assert_eq!(code, "SESSION_PLACEMENT_FAILED"),
        Err(error) => panic!("unexpected placement error: {error}"),
        Ok(_) => panic!("placement unexpectedly succeeded"),
    }
}

#[test]
fn materializes_main_subagent_json_and_non_json_sidecars() {
    let fixture = Fixture::new();
    let metadata = json!({
        "sessionId": SOURCE_ID,
        "attachment": {"snapshot": {"path": format!("{SOURCE_ROOT}/src/main.rs")}},
        "text": format!("Read {SOURCE_ROOT}/notes for session {SOURCE_ID}")
    });
    let mut main = main_bytes();
    main.extend(serde_json::to_vec(&metadata).unwrap());
    main.push(b'\n');
    let subagent = claude_fixture(SOURCE_ID, SOURCE_ROOT, VERSION, 1);
    let mut opaque = format!("{SOURCE_ROOT}/tool-output {SOURCE_ID}").into_bytes();
    opaque.push(0xff); // Non-JSON files do not require UTF-8 or JSON validation.
    let package = package(
        SessionAgent::Claude,
        vec![
            ("sidecar/tool-results/output.dat", opaque.clone()),
            (
                "sidecar/subagents/agent-synthetic.meta.json",
                serde_json::to_vec(&metadata).unwrap(),
            ),
            (CLAUDE_MAIN_FILE, main.clone()),
            ("sidecar/subagents/agent-synthetic.jsonl", subagent.clone()),
        ],
    );

    let placed = fixture.place(&package, Path::new(WORKSPACE)).unwrap();
    assert_eq!(
        place_for(SessionAgent::Claude).agent(),
        SessionAgent::Claude
    );
    assert_eq!(placed.primary_relative, primary(WORKSPACE));
    let mut expected_files = vec![
        primary(WORKSPACE),
        sidecar(WORKSPACE, "subagents/agent-synthetic.jsonl"),
        sidecar(WORKSPACE, "subagents/agent-synthetic.meta.json"),
        sidecar(WORKSPACE, "tool-results/output.dat"),
    ];
    expected_files.sort();
    assert_eq!(placed.files, expected_files);

    for (relative, original) in [
        (primary(WORKSPACE), main),
        (
            sidecar(WORKSPACE, "subagents/agent-synthetic.jsonl"),
            subagent,
        ),
        (
            sidecar(WORKSPACE, "subagents/agent-synthetic.meta.json"),
            serde_json::to_vec(&metadata).unwrap(),
        ),
        (sidecar(WORKSPACE, "tool-results/output.dat"), opaque),
    ] {
        let (text, suffix) = if relative.ends_with(".dat") {
            (
                &original[..original.len() - 1],
                &original[original.len() - 1..],
            )
        } else {
            (original.as_slice(), &[][..])
        };
        let mut expected = std::str::from_utf8(text)
            .unwrap()
            .replace(SOURCE_ROOT, WORKSPACE)
            .replace(SOURCE_ID, IMPORTED_ID)
            .into_bytes();
        expected.extend_from_slice(suffix);
        let actual = fixture.read(&relative).unwrap();
        assert_eq!(actual, expected, "{relative}");
        assert!(
            !actual
                .windows(WORKSPACE_TOKEN.len())
                .any(|w| w == WORKSPACE_TOKEN.as_bytes())
        );
        assert!(
            !actual
                .windows(SESSION_TOKEN.len())
                .any(|w| w == SESSION_TOKEN.as_bytes())
        );
        if relative.ends_with(".jsonl") {
            for line in actual.split_inclusive(|&b| b == b'\n') {
                let value: Value = serde_json::from_slice(line).unwrap();
                assert_eq!(value["sessionId"], IMPORTED_ID);
            }
        } else if relative.ends_with(".json") {
            let value: Value = serde_json::from_slice(&actual).unwrap();
            assert_eq!(value["sessionId"], IMPORTED_ID);
            assert_eq!(
                value["attachment"]["snapshot"]["path"],
                format!("{WORKSPACE}/src/main.rs")
            );
        }
    }
}

#[test]
fn sidecars_are_sorted_and_main_is_written_last() {
    let fixture = Fixture::new();
    let package = package(
        SessionAgent::Claude,
        vec![
            ("sidecar/c.txt", b"third".to_vec()),
            (CLAUDE_MAIN_FILE, main_bytes()),
            ("sidecar/b.txt", b"second".to_vec()),
            ("sidecar/a.txt", b"first".to_vec()),
        ],
    );
    let conflict = sidecar(WORKSPACE, "b.txt");
    fixture.store.write_file(&conflict, b"existing").unwrap();

    assert_placement_failed(fixture.place(&package, Path::new(WORKSPACE)));
    assert_eq!(
        fixture.read(&sidecar(WORKSPACE, "a.txt")),
        Some(b"first".to_vec())
    );
    assert_eq!(fixture.read(&conflict), Some(b"existing".to_vec()));
    assert_eq!(fixture.read(&sidecar(WORKSPACE, "c.txt")), None);
    assert_eq!(fixture.read(&primary(WORKSPACE)), None);
}

#[test]
fn repeated_placement_is_unchanged() {
    let fixture = Fixture::new();
    let package = package(
        SessionAgent::Claude,
        vec![
            (CLAUDE_MAIN_FILE, main_bytes()),
            (
                "sidecar/subagents/agent-synthetic.jsonl",
                claude_fixture(SOURCE_ID, SOURCE_ROOT, VERSION, 1),
            ),
        ],
    );
    let first = fixture.place(&package, Path::new(WORKSPACE)).unwrap();
    let before: Vec<_> = first
        .files
        .iter()
        .map(|relative| fixture.read(relative).unwrap())
        .collect();
    let second = fixture.place(&package, Path::new(WORKSPACE)).unwrap();
    assert_eq!(first.primary_relative, second.primary_relative);
    assert_eq!(first.files, second.files);
    for (relative, bytes) in first.files.iter().zip(before) {
        assert_eq!(fixture.read(relative), Some(bytes.clone()));
        assert_eq!(
            fixture.store.write_file(relative, &bytes).unwrap(),
            WriteOutcome::Unchanged
        );
    }
}

#[test]
fn refuses_different_existing_main_without_overwriting() {
    let fixture = Fixture::new();
    let relative = primary(WORKSPACE);
    fixture
        .store
        .write_file(&relative, b"{\"existing\":true}\n")
        .unwrap();
    assert_placement_failed(fixture.place(&main_package(), Path::new(WORKSPACE)));
    assert_eq!(
        fixture.read(&relative),
        Some(b"{\"existing\":true}\n".to_vec())
    );
}

#[test]
fn refuses_symlinked_project_directory() {
    let fixture = Fixture::new();
    let outside = tempfile::tempdir().unwrap();
    fs::create_dir(fixture.root.path().join("projects")).unwrap();
    symlink(outside.path(), fixture.root.path().join(project(WORKSPACE))).unwrap();
    assert_placement_failed(fixture.place(&main_package(), Path::new(WORKSPACE)));
    assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
}

#[test]
fn refuses_json_unsafe_workspace_before_writing() {
    for workspace in [
        "/host/quote\"/workspace",
        "/host/back\\slash",
        "/host/control\n",
        "/host/\u{7f}",
    ] {
        let fixture = Fixture::new();
        assert_placement_failed(fixture.place(&main_package(), Path::new(workspace)));
        assert!(!fixture.root.path().join("projects").exists());
    }
}

#[test]
fn refuses_non_utf8_workspace_before_writing() {
    let fixture = Fixture::new();
    let workspace = PathBuf::from(OsString::from_vec(b"/host/invalid-\xff/workspace".to_vec()));
    assert_placement_failed(fixture.place(&main_package(), &workspace));
    assert!(!fixture.root.path().join("projects").exists());
}

#[test]
fn refuses_wrong_agent_before_writing() {
    let fixture = Fixture::new();
    let package = package(
        SessionAgent::Codex,
        vec![(
            CODEX_ROLLOUT_FILE,
            codex_fixture(SOURCE_ID, SOURCE_ROOT, "0.160.0", 1),
        )],
    );
    assert_placement_failed(fixture.place(&package, Path::new(WORKSPACE)));
    assert!(!fixture.root.path().join("projects").exists());
}

#[test]
fn requires_main_transcript() {
    for files in [
        vec![],
        vec![("sidecar/subagents/agent-synthetic.jsonl", main_bytes())],
    ] {
        let fixture = Fixture::new();
        let package = package(SessionAgent::Claude, files);
        assert_placement_failed(fixture.place(&package, Path::new(WORKSPACE)));
        assert!(!fixture.root.path().join("projects").exists());
    }
}

#[test]
fn refuses_unexpected_package_paths_before_writing() {
    for unexpected in [
        "other.jsonl",
        "sidecar",
        "sidecar-other/agent.jsonl",
        CODEX_ROLLOUT_FILE,
    ] {
        let fixture = Fixture::new();
        let package = package(
            SessionAgent::Claude,
            vec![
                (CLAUDE_MAIN_FILE, main_bytes()),
                (unexpected, b"{}\n".to_vec()),
            ],
        );
        assert_placement_failed(fixture.place(&package, Path::new(WORKSPACE)));
        assert!(!fixture.root.path().join("projects").exists());
    }
}

#[test]
fn validates_every_jsonl_line_and_json_document_before_writing() {
    for (path, bytes) in [
        (CLAUDE_MAIN_FILE, b"{}\nnot-json\n".to_vec()),
        (CLAUDE_MAIN_FILE, b"{}\n\n".to_vec()),
        (CLAUDE_MAIN_FILE, b"{} {}\n".to_vec()),
        (
            "sidecar/subagents/agent-synthetic.jsonl",
            b"{}\n{\"partial\":\n".to_vec(),
        ),
        (
            "sidecar/subagents/agent-synthetic.meta.json",
            b"{}\n{}\n".to_vec(),
        ),
        (
            "sidecar/subagents/agent-synthetic.meta.json",
            b"{\"bad\":\"\xff\"}".to_vec(),
        ),
    ] {
        let fixture = Fixture::new();
        let mut files = vec![("sidecar/a.txt", b"valid non-JSON".to_vec()), (path, bytes)];
        if path != CLAUDE_MAIN_FILE {
            files.push((CLAUDE_MAIN_FILE, main_bytes()));
        }
        let package = package(SessionAgent::Claude, files);
        assert_placement_failed(fixture.place(&package, Path::new(WORKSPACE)));
        assert!(!fixture.root.path().join("projects").exists());
    }
}

#[test]
fn long_workspace_uses_hashed_project_directory() {
    let fixture = Fixture::new();
    let workspace = format!("/{}", "a".repeat(201));
    let placed = fixture
        .place(&main_package(), Path::new(&workspace))
        .unwrap();
    let directory = format!("-{}-85qkr6", "a".repeat(199));
    assert_eq!(
        placed.primary_relative,
        format!("projects/{directory}/{IMPORTED_ID}.jsonl")
    );
    assert_eq!(placed.files, vec![placed.primary_relative.clone()]);
    let bytes = fixture.read(&placed.primary_relative).unwrap();
    let first: Value =
        serde_json::from_slice(bytes.split(|&b| b == b'\n').next().unwrap()).unwrap();
    assert_eq!(first["cwd"], workspace);
}

#[test]
fn accepts_non_ascii_workspace_without_canonicalizing() {
    let fixture = Fixture::new();
    let workspace = "/synthetic/café/工作😀/workspace";
    let placed = fixture
        .place(&main_package(), Path::new(workspace))
        .unwrap();
    assert_eq!(placed.primary_relative, primary(workspace));
    let bytes = fixture.read(&placed.primary_relative).unwrap();
    let first: Value =
        serde_json::from_slice(bytes.split(|&b| b == b'\n').next().unwrap()).unwrap();
    assert_eq!(first["cwd"], workspace);
}
