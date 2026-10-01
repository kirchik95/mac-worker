use mac_worker::test_support::core::manifest::SnapshotManifest;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn valid_manifest_bytes() -> Vec<u8> {
    format!(
        concat!(
            r#"{{"version":1,"project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""head":null,"branch":null,"dirty":false,"relative_working_dir":"","#,
            r#""entries":[{{"path":"payload.txt","kind":"file","mode":420,"size":7,"#,
            r#""sha256":"239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5","#,
            r#""symlink_target":null}}],"tracked_deletions":[]}}"#,
        ),
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
    )
    .into_bytes()
}

fn insert_before_final_brace(bytes: &[u8], insertion: &[u8]) -> Vec<u8> {
    let mut changed = bytes[..bytes.len() - 1].to_vec();
    changed.extend_from_slice(insertion);
    changed.push(b'}');
    changed
}

#[test]
// Supersedes the shared local-manifest assertions in manifest_request_and_response_reject_unknown_duplicate_and_invalid_fields.
fn local_snapshot_manifest_decoding_preserves_identity_and_rejects_unknown_fields() {
    let manifest: SnapshotManifest = serde_json::from_slice(&valid_manifest_bytes()).unwrap();
    assert_eq!(manifest.project_id, PROJECT_ID);

    let unknown_manifest = insert_before_final_brace(&valid_manifest_bytes(), b",\"extra\":1");
    assert!(serde_json::from_slice::<SnapshotManifest>(&unknown_manifest).is_err());
}
