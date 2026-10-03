use mac_worker::test_support::session::*;
use serde_json::json;
use std::{fs, os::unix::fs::symlink, time::SystemTime};

const ID: &str = "01234567-89ab-4cde-8012-3456789abcde";

fn source(
    home: &FakeAgentHome,
    agent: SessionAgent,
    root: &std::path::Path,
    version: &str,
) -> std::path::PathBuf {
    match agent {
        SessionAgent::Claude => home.claude(ID, root.to_str().unwrap(), version, 1),
        SessionAgent::Codex => home.codex(ID, root.to_str().unwrap(), version, 1),
    }
}

#[test]
fn sec_fix_claude_version_must_not_reintroduce_scrubbed_token() {
    check_version(SessionAgent::Claude);
}
#[test]
fn sec_fix_codex_version_must_not_reintroduce_scrubbed_token() {
    check_version(SessionAgent::Codex);
}
fn check_version(agent: SessionAgent) {
    {
        let home = FakeAgentHome::new();
        let root = home.home().join("project");
        fs::create_dir(&root).unwrap();
        let token = format!("sk-{}", "a".repeat(20));
        let version = format!("0.160.0-{token}");
        let path = source(&home, agent, &root, &version);
        let scrubber = Scrubber::new(vec![]);
        let cx = CaptureContext {
            project_root: &root,
            home: home.home(),
            scrubber: &scrubber,
            now: SystemTime::now(),
        };
        assert!(
            scrubber
                .scrub_line(&serde_json::to_vec(&version).unwrap())
                .unwrap()
                .replacements
                > 0
        );
        let error = match capture_for(agent).capture(&path, &cx) {
            Err(error) => error,
            Ok(_) => panic!("unsafe version was accepted for {agent:?}"),
        };
        assert_eq!(error.public_code(), "SESSION_UNREADABLE");
        assert!(error.to_string().contains("unsupported agent version text"));
        assert!(!error.to_string().contains(&version));
        assert!(!error.to_string().contains(&token));
    }
}

#[test]
fn sec_fix_claude_main_parent_substitution_must_be_refused() {
    check_parent(SessionAgent::Claude);
}
#[test]
fn sec_fix_codex_main_parent_substitution_must_be_refused() {
    check_parent(SessionAgent::Codex);
}
fn check_parent(agent: SessionAgent) {
    {
        let home = FakeAgentHome::new();
        let root = home.home().join("project");
        fs::create_dir(&root).unwrap();
        source(&home, agent, &root, "0.160.0");
        let scrubber = Scrubber::new(vec![]);
        let cx = CaptureContext {
            project_root: &root,
            home: home.home(),
            scrubber: &scrubber,
            now: SystemTime::now(),
        };
        let capture = capture_for(agent);
        let selector: SessionSelector = format!("{}:{ID}", agent.as_str()).parse().unwrap();
        let selected = capture.discover(&selector, &cx).unwrap();
        let outside = home.home().join("unrelated-private");
        fs::create_dir(&outside).unwrap();
        let mut bytes = fs::read(&selected).unwrap();
        bytes.extend(
            serde_json::to_vec(&json!({"private": "SYNTHETIC_OUTSIDE_MAIN_BYTES"})).unwrap(),
        );
        bytes.push(b'\n');
        fs::write(outside.join(selected.file_name().unwrap()), bytes).unwrap();
        let parent = selected.parent().unwrap();
        fs::rename(parent, home.home().join("original-native-parent")).unwrap();
        symlink(&outside, parent).unwrap();
        let captured = capture.capture(&selected, &cx);
        if let Ok(captured) = &captured {
            assert!(
                !captured
                    .package
                    .files()
                    .iter()
                    .any(|file| String::from_utf8_lossy(&file.bytes)
                        .contains("SYNTHETIC_OUTSIDE_MAIN_BYTES")),
                "{agent:?}: main capture followed a substituted ancestor symlink and packaged outside bytes"
            );
        }
        assert!(
            captured.is_err(),
            "{agent:?}: main capture accepted the substituted ancestor"
        );
    }
}

#[test]
fn sec_fix_capture_rejects_path_and_malformed_version_text_without_echoing() {
    let invalid = [
        "2",
        "v2.1.288",
        "2..1",
        ".2.1",
        "2.1.",
        "2.1.2.3.4",
        "2.1-",
        "2.1+",
        "2.1-a_b",
        "2.1-a-b",
        "2.1-a+b",
        "2.1-é",
        "0.160.0-/Users/synthetic/private",
        "2.1 beta",
        "2.1\nprivate",
        "0.160.0+AKIAABCDEFGHIJKLMNOP",
    ]
    .into_iter()
    .map(str::to_owned)
    .chain([
        format!("2.1-{}", "a".repeat(33)),
        format!("{}.0", "1".repeat(63)),
    ]);
    for version in invalid {
        for agent in [SessionAgent::Claude, SessionAgent::Codex] {
            let home = FakeAgentHome::new();
            let root = home.home().join("project");
            fs::create_dir(&root).unwrap();
            let path = source(&home, agent, &root, &version);
            let scrubber = Scrubber::new(vec![]);
            let cx = CaptureContext {
                project_root: &root,
                home: home.home(),
                scrubber: &scrubber,
                now: SystemTime::now(),
            };
            let error = match capture_for(agent).capture(&path, &cx) {
                Err(error) => error,
                Ok(_) => panic!("unsupported version accepted for {agent:?}"),
            };
            assert_eq!(error.public_code(), "SESSION_UNREADABLE");
            assert!(error.to_string().contains("unsupported agent version text"));
            assert!(!error.to_string().contains(&version));
        }
    }
}

#[test]
fn sec_fix_capture_accepts_numeric_versions_and_bounded_suffixes() {
    for version in [
        "2.1.288".to_owned(),
        "0.160.0".to_owned(),
        "0.160.0-beta.1".to_owned(),
        "1.2".to_owned(),
        "1.2.3.4+Build.7".to_owned(),
        format!("1.2-{}", "a".repeat(32)),
        format!("{}.0", "1".repeat(62)),
    ] {
        for agent in [SessionAgent::Claude, SessionAgent::Codex] {
            let home = FakeAgentHome::new();
            let root = home.home().join("project");
            fs::create_dir(&root).unwrap();
            let path = source(&home, agent, &root, &version);
            let scrubber = Scrubber::new(vec![]);
            let cx = CaptureContext {
                project_root: &root,
                home: home.home(),
                scrubber: &scrubber,
                now: SystemTime::now(),
            };
            let captured = capture_for(agent).capture(&path, &cx).unwrap();
            assert_eq!(captured.package.manifest().source_agent_version, version);
            let import = SessionImportMeta::new(agent, "a".repeat(40), &version).unwrap();
            assert_eq!(
                serde_json::from_slice::<SessionImportMeta>(&serde_json::to_vec(&import).unwrap())
                    .unwrap(),
                import
            );
        }
    }
}

#[test]
fn sec_fix_capture_rejects_grammatical_versions_matching_exact_scrubber_values() {
    for agent in [SessionAgent::Claude, SessionAgent::Codex] {
        let home = FakeAgentHome::new();
        let root = home.home().join("project");
        fs::create_dir(&root).unwrap();
        let version = "0.160.0-beta.1234";
        let path = source(&home, agent, &root, version);
        let scrubber = Scrubber::new(vec!["beta.1234".to_owned()]);
        let cx = CaptureContext {
            project_root: &root,
            home: home.home(),
            scrubber: &scrubber,
            now: SystemTime::now(),
        };
        match capture_for(agent).capture(&path, &cx) {
            Err(error) => {
                assert_eq!(error.public_code(), "SESSION_UNREADABLE");
                assert!(!error.to_string().contains(version));
            }
            Ok(_) => panic!("version bypassed the capture scrubber for {agent:?}"),
        }
    }
}

#[test]
fn sec_fix_claude_chooses_highest_valid_version_ignoring_unsafe_lines() {
    let home = FakeAgentHome::new();
    let root = home.home().join("project");
    fs::create_dir(&root).unwrap();
    let path = source(
        &home,
        SessionAgent::Claude,
        &root,
        "99.0.0-/Users/synthetic/private",
    );
    let token = "AKIAABCDEFGHIJKLMNOP";
    let mut bytes = fs::read(&path).unwrap();
    for version in [
        "2.9.999",
        "2.10.1-beta.1",
        "2.10.0",
        &format!("99.0.0+{token}"),
        "999.0.0-beta.1234",
    ] {
        bytes.extend(serde_json::to_vec(&json!({"version": version})).unwrap());
        bytes.push(b'\n');
    }
    fs::write(&path, bytes).unwrap();
    let scrubber = Scrubber::new(vec!["beta.1234".to_owned()]);
    let cx = CaptureContext {
        project_root: &root,
        home: home.home(),
        scrubber: &scrubber,
        now: SystemTime::now(),
    };
    let captured = capture_for(SessionAgent::Claude)
        .capture(&path, &cx)
        .unwrap();
    assert_eq!(
        captured.package.manifest().source_agent_version,
        "2.10.1-beta.1"
    );
    assert!(captured.package.manifest().scrubbed > 0);
    assert!(
        captured
            .package
            .files()
            .iter()
            .all(|file| !String::from_utf8_lossy(&file.bytes).contains(token))
    );
    assert!(
        !String::from_utf8(captured.package.manifest_json())
            .unwrap()
            .contains("synthetic/private")
    );
}

#[test]
fn sec_fix_import_metadata_rejects_unsafe_version_construction_and_deserialization() {
    let valid = SessionImportMeta::new(SessionAgent::Codex, "a".repeat(40), "0.160.0").unwrap();
    for version in [
        "v1",
        "2",
        "2..1",
        "2.1.2.3.4",
        "2.1-a-b",
        "2.1+",
        "0.160.0-sk-aaaaaaaaaaaaaaaaaaaa",
        "0.160.0+AKIAABCDEFGHIJKLMNOP",
        "0.160.0-/Users/synthetic/private",
        &format!("1.2-{}", "a".repeat(33)),
        &format!("{}.0", "1".repeat(63)),
    ] {
        let error =
            SessionImportMeta::new(SessionAgent::Codex, "a".repeat(40), version).unwrap_err();
        assert_eq!(error.public_code(), "TASK_CONFIG_INVALID");
        assert!(!error.to_string().contains(version));
        let mut wire = serde_json::to_value(&valid).unwrap();
        wire["source_agent_version"] = json!(version);
        let error = serde_json::from_value::<SessionImportMeta>(wire).unwrap_err();
        assert!(!error.to_string().contains(version));
    }
}

#[test]
fn sec_fix_main_capture_refuses_sources_outside_native_store() {
    for agent in [SessionAgent::Claude, SessionAgent::Codex] {
        let home = FakeAgentHome::new();
        let root = home.home().join("project");
        fs::create_dir(&root).unwrap();
        let path = source(&home, agent, &root, "0.160.0");
        let outside = home.home().join(path.file_name().unwrap());
        fs::rename(&path, &outside).unwrap();
        let store = home.home().join(match agent {
            SessionAgent::Claude => ".claude/projects",
            SessionAgent::Codex => ".codex/sessions",
        });
        let traversal = store.join("../..").join(outside.file_name().unwrap());
        let scrubber = Scrubber::new(vec![]);
        let cx = CaptureContext {
            project_root: &root,
            home: home.home(),
            scrubber: &scrubber,
            now: SystemTime::now(),
        };
        for path in [&outside, &traversal] {
            match capture_for(agent).capture(path, &cx) {
                Err(error) => assert_eq!(error.public_code(), "SESSION_UNREADABLE"),
                Ok(_) => panic!("capture accepted an outside source for {agent:?}"),
            }
        }
    }
}
