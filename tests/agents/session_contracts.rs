use mac_worker::test_support::session::*;

const ID: &str = "abcdefab-1234-5678-90ab-abcdefabcdef";

#[test]
fn tokens_obey_boundaries_and_round_trip() {
    assert_eq!(
        rewrite_root(
            b"/w/feat /w/feature /x/w/feat /w/feat/a /w/feat.x",
            "/w/feat",
            "T"
        ),
        b"T /w/feature /x/w/feat T/a /w/feat.x"
    );
    assert_eq!(
        rewrite_root(b"x/w/feat a/w/feat-", "/w/feat", "T"),
        b"x/w/feat a/w/feat-"
    );
    let input = format!("/w/nested/a /w/b {ID}");
    let norm = normalize(input.as_bytes(), &["/w", "/w/nested"], ID).unwrap();
    assert_eq!(
        norm,
        format!("{WORKSPACE_TOKEN}/a {WORKSPACE_TOKEN}/b {SESSION_TOKEN}").as_bytes()
    );
    assert_eq!(
        materialize(&normalize(b"/w/a", &["/w"], ID).unwrap(), "/w", ID).unwrap(),
        b"/w/a"
    );
    for token in [WORKSPACE_TOKEN, SESSION_TOKEN] {
        assert!(normalize(token.as_bytes(), &["/w"], ID).is_err());
    }
}

#[test]
fn claude_directory_encoding() {
    assert_eq!(
        claude_project_dir("/Users/penso/.herdr/worktrees/herdr-gpui/worktree-calm-forest-9099"),
        "-Users-penso--herdr-worktrees-herdr-gpui-worktree-calm-forest-9099"
    );
    let host = format!(
        "/Users/test/.local/share/mac-worker/host/tasks/{}/{}/workspace",
        "a".repeat(64),
        "b".repeat(32)
    );
    assert_eq!(claude_project_dir(&host), host.replace(['/', '.'], "-"));
    assert_eq!(
        claude_project_dir(&"a".repeat(201)),
        format!("{}-rkvsv5", "a".repeat(200))
    );
    assert_eq!(claude_project_dir("/😀"), "---");
}

#[test]
fn selector_and_import_meta_validation() {
    let selector: SessionSelector = format!("claude:{}", ID.to_uppercase()).parse().unwrap();
    assert_eq!(selector.id(), Some(ID));
    assert_eq!(selector.agent(), SessionAgent::Claude);
    assert!("codex".parse::<SessionSelector>().unwrap().id().is_none());
    for bad in [
        "cursor",
        "CLAUDE",
        "claude:",
        "codex:no",
        "claude:abcdefab1234567890ababcdefabcdef",
    ] {
        assert!(bad.parse::<SessionSelector>().is_err());
    }
    let meta = SessionImportMeta::new(SessionAgent::Codex, "a".repeat(40), "0.160.0").unwrap();
    assert_eq!(meta.format(), SessionFormat::CodexRolloutV1);
    for (oid, version) in [
        ("A".repeat(40), "1"),
        ("a".repeat(39), "1"),
        ("a".repeat(40), ""),
        ("a".repeat(40), "a b"),
    ] {
        assert!(SessionImportMeta::new(SessionAgent::Claude, oid, version).is_err());
    }
}

fn source() -> PackageSource {
    PackageSource {
        agent: SessionAgent::Claude,
        source_session_id: ID.into(),
        source_agent_version: "1.0.0".into(),
        source_cwd_relative: String::new(),
        scrubbed: 0,
    }
}
#[test]
fn packages_validate_paths_hashes_and_manifest() {
    let package = SessionPackage::build(
        source(),
        vec![PackageFile {
            path: CLAUDE_MAIN_FILE.into(),
            bytes: b"{}\n".to_vec(),
        }],
    )
    .unwrap();
    assert_eq!(
        SessionPackage::from_parts(&package.manifest_json(), package.files().to_vec()).unwrap(),
        package
    );
    for path in ["", "/abs", "../escape", "a/../b", "a//b", "./a", "a\\b"] {
        assert!(
            SessionPackage::build(
                source(),
                vec![PackageFile {
                    path: path.into(),
                    bytes: vec![]
                }]
            )
            .is_err()
        );
    }
    assert!(
        SessionPackage::from_parts(
            &package.manifest_json(),
            vec![PackageFile {
                path: CLAUDE_MAIN_FILE.into(),
                bytes: b"[]\n".to_vec()
            }]
        )
        .is_err()
    );
    let mut manifest: serde_json::Value = serde_json::from_slice(&package.manifest_json()).unwrap();
    manifest["unknown"] = true.into();
    assert!(
        SessionPackage::from_parts(
            &serde_json::to_vec(&manifest).unwrap(),
            package.files().to_vec()
        )
        .is_err()
    );
    assert!(
        SessionPackage::build(
            source(),
            (0..=MAX_PACKAGE_FILES)
                .map(|i| PackageFile {
                    path: i.to_string(),
                    bytes: vec![]
                })
                .collect()
        )
        .is_err()
    );
}

#[test]
fn complete_lines_relative_paths_and_store_roots() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("input");
    std::fs::write(&path, b"{}\n[]\npartial").unwrap();
    assert_eq!(
        read_complete_lines(&path, 100).unwrap(),
        vec![b"{}".to_vec(), b"[]".to_vec()]
    );
    assert!(read_complete_lines(&path, 2).is_err());
    std::fs::write(&path, b"invalid\n").unwrap();
    assert!(read_complete_lines(&path, 100).is_err());
    assert_eq!(relative_inside(temp.path(), temp.path()).unwrap(), "");
    std::fs::create_dir(temp.path().join("child")).unwrap();
    assert_eq!(
        relative_inside(temp.path(), &temp.path().join("child")).unwrap(),
        "child"
    );
    let other = tempfile::tempdir().unwrap();
    assert!(relative_inside(temp.path(), other.path()).is_err());
    assert_eq!(
        store_root(SessionAgent::Claude, temp.path(), &[]).unwrap(),
        temp.path().join(".claude")
    );
    assert_eq!(
        store_root(
            SessionAgent::Codex,
            temp.path(),
            &[("CODEX_HOME".into(), "/custom".into())]
        )
        .unwrap(),
        std::path::PathBuf::from("/custom")
    );
}

#[test]
fn amended_json_safe_paths_and_store_overrides() {
    for path in ["/a\"b", "/a\\b", "/a\nb", "/a\u{7f}b"] {
        assert_eq!(
            normalize(b"{}", &[path], ID).unwrap_err().public_code(),
            "SESSION_UNREADABLE"
        );
        assert_eq!(
            materialize(WORKSPACE_TOKEN.as_bytes(), path, ID)
                .unwrap_err()
                .public_code(),
            "SESSION_PLACEMENT_FAILED"
        );
    }
    let path = "/проект/日本語";
    assert_eq!(
        materialize(&normalize(path.as_bytes(), &[path], ID).unwrap(), path, ID).unwrap(),
        path.as_bytes()
    );
    for path in ["", "relative", "~/store"] {
        assert!(
            store_root(
                SessionAgent::Claude,
                std::path::Path::new("/home"),
                &[("CLAUDE_CONFIG_DIR".into(), path.into())]
            )
            .is_err()
        );
    }
    let overrides = vec![
        ("CODEX_HOME".into(), "relative".into()),
        ("CODEX_HOME".into(), "/last".into()),
    ];
    assert_eq!(
        store_root(
            SessionAgent::Codex,
            std::path::Path::new("/home"),
            &overrides
        )
        .unwrap(),
        std::path::PathBuf::from("/last")
    );
}

#[test]
fn import_metadata_deserialization_validates_every_rule() {
    let meta = SessionImportMeta::new(SessionAgent::Claude, "a".repeat(40), "1.2.3").unwrap();
    let value = serde_json::to_value(&meta).unwrap();
    for (key, bad) in [
        ("format", serde_json::json!("codex-rollout-v1")),
        ("package_oid", serde_json::json!("A".repeat(40))),
        ("package_oid", serde_json::json!("a".repeat(39))),
        ("package_oid", serde_json::json!("g".repeat(64))),
        ("source_agent_version", serde_json::json!("")),
        ("source_agent_version", serde_json::json!("a".repeat(65))),
        ("source_agent_version", serde_json::json!("a b")),
        ("source_agent_version", serde_json::json!("a\nb")),
        ("source_agent_version", serde_json::json!("非ASCII")),
        ("unknown", serde_json::json!(true)),
    ] {
        let mut invalid = value.clone();
        invalid[key] = bad;
        assert!(
            serde_json::from_value::<SessionImportMeta>(invalid).is_err(),
            "{key}"
        );
    }
    assert_eq!(
        serde_json::from_value::<SessionImportMeta>(value).unwrap(),
        meta
    );
    assert!(SessionImportMeta::new(SessionAgent::Codex, "0".repeat(64), "v1").is_ok());
}

#[test]
fn imported_session_ids_are_hyphenated_task_ids() {
    let task_id: mac_worker::test_support::task::model::TaskId =
        "abcdefab1234567890ababcdefabcdef".parse().unwrap();
    assert_eq!(imported_session_id(&task_id), ID);
}

#[test]
fn minimum_version_requirements_round_trip() {
    for agent in [SessionAgent::Claude, SessionAgent::Codex] {
        let requirement = agent_min_requirement(agent, "0.160.0");
        assert_eq!(
            parse_agent_min_requirement(&requirement),
            Some((agent, "0.160.0".into()))
        );
    }
    for bad in [
        "codex@1",
        "agent-min:cursor@1",
        "agent-min:codex@",
        "agent-min:codex@1 2",
    ] {
        assert!(parse_agent_min_requirement(bad).is_none());
    }
}

#[test]
fn session_error_codes_have_spec_exits_and_hints() {
    use mac_worker::test_support::core::error::hint_for;
    for (code, exit) in [
        ("SESSION_NOT_FOUND", 64),
        ("SESSION_UNREADABLE", 64),
        ("SESSION_TOO_LARGE", 64),
        ("SESSION_OUTSIDE_PROJECT", 64),
        ("SESSION_NEEDS_WIP", 64),
        ("SESSION_REQUIRES_SNAPSHOT", 64),
        ("SESSION_AGENT_MISMATCH", 64),
        ("SESSION_IMPORT_UNSUPPORTED", 64),
        ("SESSION_AGENT_TOO_OLD", 75),
        ("SESSION_PLACEMENT_FAILED", 70),
    ] {
        assert_eq!(session_error(code, "synthetic").exit_code(), exit, "{code}");
        assert!(hint_for(code).is_some(), "{code}");
    }
}

#[test]
fn frozen_submit_is_additive_and_propagates_import_metadata() {
    use mac_worker::test_support::task::prepared_submit::FrozenSubmitBody;
    let old = serde_json::json!({
        "task_id": "00000000000000000000000000000001",
        "turn_id": "00000000000000000000000000000002",
        "created_at_millis": 1, "prompt": "Synthetic prompt", "agent": "codex",
        "source": "local", "publish": ["fetch"], "close_on": "never", "wip": false,
        "project_id": "a".repeat(64), "worktree_id": "b".repeat(64), "base_oid": "a".repeat(40),
        "timeout_millis": 1000, "max_followups": 10, "permissions": "workspace",
        "requires": [], "include_untracked": [], "include_empty_dirs": [], "allow_sensitive": [],
        "cli_includes": [], "wait_for_capacity": true
    });
    let mut body: FrozenSubmitBody = serde_json::from_value(old.clone()).unwrap();
    assert!(body.session_import.is_none());
    assert_eq!(serde_json::to_value(&body).unwrap(), old);
    body.session_import =
        Some(SessionImportMeta::new(SessionAgent::Codex, "a".repeat(40), "0.160.0").unwrap());
    let bytes = serde_json::to_vec(&body).unwrap();
    let restored: FrozenSubmitBody = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(restored, body);
    assert_eq!(
        restored.prepared().unwrap().session_import,
        body.session_import
    );
    let mut bad = old;
    bad["unknown"] = true.into();
    assert!(serde_json::from_value::<FrozenSubmitBody>(bad).is_err());
}

#[test]
fn package_byte_caps_duplicate_paths_and_file_sets() {
    let error = SessionPackage::build(
        source(),
        vec![PackageFile {
            path: "main.jsonl".into(),
            bytes: vec![0; MAX_FILE_BYTES as usize + 1],
        }],
    )
    .unwrap_err();
    assert_eq!(error.public_code(), "SESSION_TOO_LARGE");
    let error = SessionPackage::build(
        source(),
        vec![
            PackageFile {
                path: "a".into(),
                bytes: vec![0; MAX_PACKAGE_BYTES as usize / 2 + 1],
            },
            PackageFile {
                path: "b".into(),
                bytes: vec![0; MAX_PACKAGE_BYTES as usize / 2],
            },
        ],
    )
    .unwrap_err();
    assert_eq!(error.public_code(), "SESSION_TOO_LARGE");
    for paths in [["a", "a"], ["a", "a/b"]] {
        assert!(
            SessionPackage::build(
                source(),
                paths
                    .into_iter()
                    .map(|path| PackageFile {
                        path: path.into(),
                        bytes: vec![]
                    })
                    .collect()
            )
            .is_err()
        );
    }
    let package = SessionPackage::build(
        source(),
        vec![
            PackageFile {
                path: "z".into(),
                bytes: vec![1],
            },
            PackageFile {
                path: "a".into(),
                bytes: vec![2],
            },
        ],
    )
    .unwrap();
    assert_eq!(package.files()[0].path, "a");
    assert!(SessionPackage::from_parts(&package.manifest_json(), vec![]).is_err());
    let mut manifest: serde_json::Value = serde_json::from_slice(&package.manifest_json()).unwrap();
    manifest["files"][0]["bytes"] = serde_json::json!(MAX_FILE_BYTES + 1);
    assert_eq!(
        SessionPackage::from_parts(
            &serde_json::to_vec(&manifest).unwrap(),
            package.files().to_vec()
        )
        .unwrap_err()
        .public_code(),
        "SESSION_TOO_LARGE"
    );
    manifest["files"][0]["bytes"] = serde_json::json!(1);
    manifest["schema"] = serde_json::json!(2);
    assert!(
        SessionPackage::from_parts(
            &serde_json::to_vec(&manifest).unwrap(),
            package.files().to_vec()
        )
        .is_err()
    );
}

#[test]
fn synthetic_homes_and_capture_doubles_are_read_only() {
    use std::{sync::Mutex, time::SystemTime};
    let home = FakeAgentHome::new();
    let path = home.claude(ID, "/synthetic", "1.2.3", 2);
    let codex = home.codex(ID, "/synthetic", "0.160.0", 2);
    assert_eq!(read_complete_lines(&path, MAX_FILE_BYTES).unwrap().len(), 4);
    assert_eq!(
        read_complete_lines(&codex, MAX_FILE_BYTES).unwrap().len(),
        3
    );
    let before = std::fs::read(&path).unwrap();
    let package = SessionPackage::build(
        source(),
        vec![PackageFile {
            path: CLAUDE_MAIN_FILE.into(),
            bytes: before.clone(),
        }],
    )
    .unwrap();
    let fake = FakeCapture {
        agent: SessionAgent::Claude,
        source: path.clone(),
        package,
        calls: Mutex::new(Vec::new()),
    };
    let scrubber = Scrubber::new(vec![]);
    let cx = CaptureContext {
        project_root: home.home(),
        home: home.home(),
        scrubber: &scrubber,
        now: SystemTime::UNIX_EPOCH,
    };
    assert_eq!(
        fake.discover(&"claude".parse().unwrap(), &cx).unwrap(),
        path
    );
    assert_eq!(fake.capture(&path, &cx).unwrap().source_path, path);
    assert_eq!(fake.calls.lock().unwrap().len(), 2);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(
        capture_for(SessionAgent::Claude)
            .capture(&path, &cx)
            .err()
            .unwrap()
            .public_code(),
        "SESSION_IMPORT_UNSUPPORTED"
    );
}
