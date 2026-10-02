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
        materialize(&normalize(b"/w/a", &["/w"], ID).unwrap(), "/w", ID),
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
    assert_eq!(
        claude_project_dir(&host),
        host.replace('/', "-").replace('.', "-")
    );
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
        store_root(SessionAgent::Claude, temp.path(), &[]),
        temp.path().join(".claude")
    );
    assert_eq!(
        store_root(
            SessionAgent::Codex,
            temp.path(),
            &[("CODEX_HOME".into(), "/custom".into())]
        ),
        std::path::PathBuf::from("/custom")
    );
}
