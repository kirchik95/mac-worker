use mac_worker::test_support::session::*;
use serde_json::json;
use std::{fs, time::SystemTime};

const ID: &str = "01234567-89ab-4cde-8012-3456789abcde";
const OTHER_ID: &str = "11234567-89ab-4cde-8012-3456789abcde";

#[test]
fn review_fix_complete_lines_refuses_symlinks_and_non_regular_files() {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let temp = tempfile::tempdir().unwrap();
    let regular = temp.path().join("regular.jsonl");
    fs::write(&regular, b"{}\n").unwrap();
    let link = temp.path().join("link.jsonl");
    std::os::unix::fs::symlink(&regular, &link).unwrap();
    assert_eq!(
        read_complete_lines(&link, MAX_FILE_BYTES)
            .unwrap_err()
            .public_code(),
        "SESSION_UNREADABLE"
    );
    assert_eq!(
        read_complete_lines(temp.path(), MAX_FILE_BYTES)
            .unwrap_err()
            .public_code(),
        "SESSION_UNREADABLE"
    );
    let fifo = temp.path().join("pipe.jsonl");
    let name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: name is a NUL-terminated path within this test's temporary directory.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    assert_eq!(
        read_complete_lines(&fifo, MAX_FILE_BYTES)
            .unwrap_err()
            .public_code(),
        "SESSION_UNREADABLE"
    );
}

#[test]
fn review_fix_normalizes_paths_after_json_newline_and_tab_escapes() {
    let source = serde_json::to_vec(&json!({
        "output": "files:\n/w/feat/a.rs\n\t/w/feat/b.rs"
    }))
    .unwrap();
    let normalized = normalize(&source, &["/w/feat"], ID).unwrap();
    let placed = materialize(&normalized, "/pool/workspace", OTHER_ID).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&placed).unwrap();
    assert_eq!(
        value["output"],
        "files:\n/pool/workspace/a.rs\n\t/pool/workspace/b.rs"
    );
}

#[test]
fn review_fix_does_not_rewrite_unicode_sibling_paths() {
    let source = serde_json::to_vec(&json!({
        "paths": ["/w/featé/a.rs", "/w/feat日本/a.rs", "/w/feat/a.rs"]
    }))
    .unwrap();
    let normalized = normalize(&source, &["/w/feat"], ID).unwrap();
    let placed = materialize(&normalized, "/pool/workspace", OTHER_ID).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&placed).unwrap();
    assert_eq!(
        value["paths"],
        json!(["/w/featé/a.rs", "/w/feat日本/a.rs", "/pool/workspace/a.rs"])
    );
}

#[test]
fn review_fix_latest_claude_filters_encoded_directory_collisions() {
    let home = FakeAgentHome::new();
    let wanted = home.home().join("project_a");
    let unrelated = home.home().join("project-a");
    fs::create_dir(&wanted).unwrap();
    fs::create_dir(&unrelated).unwrap();
    assert_eq!(
        claude_project_dir(wanted.to_str().unwrap()),
        claude_project_dir(unrelated.to_str().unwrap())
    );
    let older = home.claude(ID, wanted.to_str().unwrap(), "2.1.288", 1);
    let newer = home.claude(OTHER_ID, unrelated.to_str().unwrap(), "2.1.288", 1);
    for (path, seconds) in [(&older, 100), (&newer, 200)] {
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_times(
                fs::FileTimes::new()
                    .set_modified(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(seconds)),
            )
            .unwrap();
    }
    let scrubber = Scrubber::new(vec![]);
    let cx = CaptureContext {
        project_root: &wanted,
        home: home.home(),
        scrubber: &scrubber,
        now: SystemTime::now(),
    };
    let capture = capture_for(SessionAgent::Claude);
    let selected = capture.discover(&"claude".parse().unwrap(), &cx).unwrap();
    let captured = capture.capture(&selected, &cx);
    assert!(
        captured.is_ok(),
        "selected {} instead of {}; capture returned {}",
        selected.display(),
        older.display(),
        captured.err().unwrap().public_code()
    );
    assert_eq!(selected, older);
}
