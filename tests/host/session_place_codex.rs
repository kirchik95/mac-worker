use std::{
    fs,
    os::unix::{
        ffi::OsStringExt,
        fs::{MetadataExt, symlink},
    },
    path::PathBuf,
};

use mac_worker::test_support::{
    core::error::WorkerError,
    session::{
        CODEX_ROLLOUT_FILE, PackageFile, PackageSource, PlaceContext, PlacedSession, SESSION_TOKEN,
        SessionAgent, SessionPackage, StoreWriter, WORKSPACE_TOKEN, WriteOutcome, codex_fixture,
        place_for,
    },
};
use serde_json::{Value, json};

const ID: &str = "01234567-89ab-4cde-8012-3456789abcde";
const PLACED_AT: u64 = 1_709_210_096_789;

struct Fixture {
    _temp: tempfile::TempDir,
    workspace: PathBuf,
    store: StoreWriter,
}

impl Fixture {
    fn new() -> Self {
        Self::with_workspace("workspace")
    }

    fn with_workspace(name: &str) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let workspace = root.join(name);
        fs::create_dir(&workspace).unwrap();
        let store_root = root.join("codex");
        fs::create_dir(&store_root).unwrap();
        let store = StoreWriter::open(&store_root).unwrap();
        Self {
            _temp: temp,
            workspace,
            store,
        }
    }

    fn place(
        &self,
        package: &SessionPackage,
        placed_at_millis: u64,
    ) -> Result<PlacedSession, WorkerError> {
        let placer = place_for(SessionAgent::Codex);
        assert_eq!(placer.agent(), SessionAgent::Codex);
        placer.place(
            package,
            &PlaceContext {
                workspace: &self.workspace,
                store: &self.store,
                session_id: ID,
                placed_at_millis,
            },
        )
    }

    fn reject(&self, package: &SessionPackage) {
        let error = self.place(package, PLACED_AT).err().expect("must reject");
        assert_eq!(error.public_code(), "SESSION_PLACEMENT_FAILED");
        assert_eq!(fs::read_dir(self.store.root()).unwrap().count(), 0);
    }
}

fn package(agent: SessionAgent, files: Vec<PackageFile>) -> SessionPackage {
    SessionPackage::build(
        PackageSource {
            agent,
            source_session_id: "fedcba98-7654-4321-8012-3456789abcde".into(),
            source_agent_version: "0.160.0".into(),
            source_cwd_relative: String::new(),
            scrubbed: 0,
        },
        files,
    )
    .unwrap()
}

fn rollout(bytes: Vec<u8>) -> SessionPackage {
    package(
        SessionAgent::Codex,
        vec![PackageFile {
            path: CODEX_ROLLOUT_FILE.into(),
            bytes,
        }],
    )
}

fn normalized_rollout() -> Vec<u8> {
    codex_fixture(SESSION_TOKEN, WORKSPACE_TOKEN, "0.160.0", 1)
}

fn assert_path(placed_at_millis: u64, date: &str, timestamp: &str) {
    let fixture = Fixture::new();
    let placed = fixture
        .place(&rollout(normalized_rollout()), placed_at_millis)
        .unwrap();
    let expected = format!("sessions/{date}/rollout-{timestamp}-{ID}.jsonl");
    assert_eq!(placed.primary_relative, expected);
    assert_eq!(placed.files, vec![expected.clone()]);
    assert!(fixture.store.root().join(expected).is_file());
    // Placement creates no databases, index, or other native-store files.
    let entries: Vec<_> = fs::read_dir(fixture.store.root())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(entries, vec!["sessions"]);
}

#[test]
fn utc_filename_at_unix_epoch() {
    assert_path(0, "1970/01/01", "1970-01-01T00-00-00");
}

#[test]
fn utc_filename_on_leap_days() {
    assert_path(951_786_123_000, "2000/02/29", "2000-02-29T01-02-03");
    assert_path(1_709_210_096_000, "2024/02/29", "2024-02-29T12-34-56");
}

#[test]
fn utc_filename_across_year_boundary() {
    assert_path(1_735_689_599_999, "2024/12/31", "2024-12-31T23-59-59");
    assert_path(1_735_689_600_000, "2025/01/01", "2025-01-01T00-00-00");
}

#[test]
fn utc_filename_truncates_millis_to_seconds() {
    for millis in [0, 1, 499, 500, 999] {
        assert_path(millis, "1970/01/01", "1970-01-01T00-00-00");
    }
    assert_path(1_000, "1970/01/01", "1970-01-01T00-00-01");
    assert_path(PLACED_AT, "2024/02/29", "2024-02-29T12-34-56");
}

#[test]
fn utc_filename_skips_leap_day_in_non_leap_century() {
    assert_path(4_107_542_399_999, "2100/02/28", "2100-02-28T23-59-59");
    assert_path(4_107_542_400_000, "2100/03/01", "2100-03-01T00-00-00");
}

#[test]
fn materializes_tokens_in_nested_fields_and_preserves_other_bytes() {
    let fixture = Fixture::new();
    let mut bytes = normalized_rollout();
    bytes.extend(
        serde_json::to_vec(&json!({
            "type": "turn_context",
            "payload": {
                "runtime_workspace_roots": [WORKSPACE_TOKEN],
                "permission_profile": {"write": [{"path": format!("{WORKSPACE_TOKEN}/src")}]},
                "item": {
                    "aggregated_output": format!("open {WORKSPACE_TOKEN}/src/lib.rs in {SESSION_TOKEN}"),
                    "command": ["read", format!("{WORKSPACE_TOKEN}/src/lib.rs")]
                },
                "session_id": SESSION_TOKEN,
                "untouched": "Synthetic output"
            }
        }))
        .unwrap(),
    );
    bytes.push(b'\n');
    let placed = fixture.place(&rollout(bytes.clone()), PLACED_AT).unwrap();
    let actual = fixture
        .store
        .read_file(&placed.primary_relative, 64 << 20)
        .unwrap()
        .unwrap();
    let workspace = fixture.workspace.to_str().unwrap();
    let expected = String::from_utf8(bytes)
        .unwrap()
        .replace(WORKSPACE_TOKEN, workspace)
        .replace(SESSION_TOKEN, ID);
    assert_eq!(actual, expected.as_bytes());
    let lines: Vec<Value> = expected
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines[0]["payload"]["id"], ID);
    assert_eq!(lines[0]["payload"]["cwd"], workspace);
    assert_eq!(lines[2]["payload"]["runtime_workspace_roots"][0], workspace);
    assert_eq!(lines[2]["payload"]["session_id"], ID);
}

#[test]
fn accepts_meta_cwd_in_workspace_subdirectory() {
    let fixture = Fixture::new();
    fs::create_dir(fixture.workspace.join("nested")).unwrap();
    let bytes = codex_fixture(
        SESSION_TOKEN,
        &format!("{WORKSPACE_TOKEN}/nested"),
        "0.160.0",
        0,
    );
    fixture.place(&rollout(bytes), PLACED_AT).unwrap();
}

#[test]
fn accepts_cwd_in_missing_workspace_subdirectory() {
    let fixture = Fixture::new();
    let bytes = codex_fixture(
        SESSION_TOKEN,
        &format!("{WORKSPACE_TOKEN}/missing/nested"),
        "0.160.0",
        0,
    );
    fixture.place(&rollout(bytes), PLACED_AT).unwrap();
}

#[test]
fn rejects_cwd_that_escapes_workspace_through_symlink() {
    let fixture = Fixture::new();
    symlink(fixture.store.root(), fixture.workspace.join("escape")).unwrap();
    for cwd in [
        format!("{WORKSPACE_TOKEN}/escape"),
        format!("{WORKSPACE_TOKEN}/escape/missing"),
    ] {
        fixture.reject(&rollout(codex_fixture(SESSION_TOKEN, &cwd, "0.160.0", 0)));
    }
}

#[test]
fn rejects_meta_id_that_does_not_match_target() {
    let fixture = Fixture::new();
    for id in ["fedcba98-7654-4321-8012-3456789abcde", "", WORKSPACE_TOKEN] {
        fixture.reject(&rollout(codex_fixture(id, WORKSPACE_TOKEN, "0.160.0", 0)));
    }
}

#[test]
fn rejects_missing_or_wrong_first_session_meta() {
    let fixture = Fixture::new();
    for first in [
        json!({"type":"response_item", "payload":{"id":SESSION_TOKEN,"cwd":WORKSPACE_TOKEN}}),
        json!({"type":"session_meta", "payload":{"cwd":WORKSPACE_TOKEN}}),
        json!({"type":"session_meta", "payload":{"id":SESSION_TOKEN}}),
        json!({"type":"session_meta", "payload":{"id":SESSION_TOKEN,"cwd":42}}),
        json!({"type":"session_meta", "payload":null}),
    ] {
        let mut bytes = serde_json::to_vec(&first).unwrap();
        bytes.push(b'\n');
        bytes.extend(normalized_rollout());
        fixture.reject(&rollout(bytes));
    }
}

#[test]
fn rejects_cwd_outside_workspace_including_prefix_and_parent_traps() {
    let fixture = Fixture::new();
    for cwd in [
        "/outside".to_owned(),
        format!("{WORKSPACE_TOKEN}-other"),
        format!("{WORKSPACE_TOKEN}/../codex"),
        "relative/path".to_owned(),
    ] {
        fixture.reject(&rollout(codex_fixture(SESSION_TOKEN, &cwd, "0.160.0", 0)));
    }
}

#[test]
fn validates_every_jsonl_line_before_writing() {
    let fixture = Fixture::new();
    for bytes in [Vec::new(), b"\n".to_vec(), b"not JSON\n".to_vec()] {
        fixture.reject(&rollout(bytes));
    }
    for tail in [b"not JSON\n".as_slice(), b"\n", b"{\"partial\":", b"\xff\n"] {
        let mut bytes = normalized_rollout();
        bytes.extend_from_slice(tail);
        fixture.reject(&rollout(bytes));
    }
}

#[test]
fn repeat_placement_is_unchanged_and_deterministic() {
    let fixture = Fixture::new();
    let package = rollout(normalized_rollout());
    let first = fixture.place(&package, PLACED_AT).unwrap();
    let path = fixture.store.root().join(&first.primary_relative);
    let before = fs::metadata(&path).unwrap();
    let bytes = fs::read(&path).unwrap();
    let second = fixture.place(&package, PLACED_AT).unwrap();
    let after = fs::metadata(&path).unwrap();
    assert_eq!(second.primary_relative, first.primary_relative);
    assert_eq!(second.files, first.files);
    assert_eq!(fs::read(path).unwrap(), bytes);
    assert_eq!(after.ino(), before.ino());
    assert_eq!(
        (after.mtime(), after.mtime_nsec()),
        (before.mtime(), before.mtime_nsec())
    );
    assert_eq!(
        fixture
            .store
            .write_file(&first.primary_relative, &bytes)
            .unwrap(),
        WriteOutcome::Unchanged
    );
}

#[test]
fn rejects_existing_different_content_without_overwriting() {
    let fixture = Fixture::new();
    let package = rollout(normalized_rollout());
    let placed = fixture.place(&package, PLACED_AT).unwrap();
    let path = fixture.store.root().join(&placed.primary_relative);
    let different = b"Synthetic existing content\n";
    fs::write(&path, different).unwrap();
    let error = fixture
        .place(&package, PLACED_AT)
        .err()
        .expect("must reject");
    assert_eq!(error.public_code(), "SESSION_PLACEMENT_FAILED");
    assert_eq!(fs::read(path).unwrap(), different);
}

#[test]
fn rejects_wrong_agent_or_file_set_before_writing() {
    let fixture = Fixture::new();
    fixture.reject(&package(
        SessionAgent::Claude,
        vec![PackageFile {
            path: CODEX_ROLLOUT_FILE.into(),
            bytes: normalized_rollout(),
        }],
    ));
    fixture.reject(&package(SessionAgent::Codex, vec![]));
    fixture.reject(&package(
        SessionAgent::Codex,
        vec![PackageFile {
            path: "main.jsonl".into(),
            bytes: normalized_rollout(),
        }],
    ));
    fixture.reject(&package(
        SessionAgent::Codex,
        vec![
            PackageFile {
                path: CODEX_ROLLOUT_FILE.into(),
                bytes: normalized_rollout(),
            },
            PackageFile {
                path: "extra.json".into(),
                bytes: b"{}".to_vec(),
            },
        ],
    ));
}

#[test]
fn refuses_workspace_with_quote_backslash_or_control_character() {
    for name in ["work\"space", "work\\space", "work\nspace"] {
        Fixture::with_workspace(name).reject(&rollout(normalized_rollout()));
    }
}

#[test]
fn permits_non_ascii_workspace() {
    let fixture = Fixture::with_workspace("work-é-工作");
    let placed = fixture
        .place(&rollout(normalized_rollout()), PLACED_AT)
        .unwrap();
    let bytes = fs::read(fixture.store.root().join(placed.primary_relative)).unwrap();
    let meta: Value = serde_json::from_slice(bytes.split(|&b| b == b'\n').next().unwrap()).unwrap();
    assert_eq!(meta["payload"]["cwd"], fixture.workspace.to_str().unwrap());
}

#[test]
fn refuses_non_utf8_workspace() {
    let mut fixture = Fixture::new();
    fixture.workspace = fixture
        .workspace
        .parent()
        .unwrap()
        .join(std::ffi::OsString::from_vec(b"workspace-\xff".to_vec()));
    // APFS refuses non-UTF-8 names, so exercise the path directly without creating it.
    fixture.reject(&rollout(normalized_rollout()));
}
