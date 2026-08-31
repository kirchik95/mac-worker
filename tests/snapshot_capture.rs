mod support;

use std::{
    collections::BTreeSet,
    ffi::OsStr,
    fs, io,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, PermissionsExt, symlink},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{Arc, Barrier, Mutex},
    time::Instant,
};

use mac_worker::{
    error::WorkerError,
    inputs::{
        InputOrigin, InputSelection, InputSelector, RelativePath, SelectedInput, SelectedInputKind,
    },
    manifest::{ManifestEntry, ManifestEntryKind, SnapshotManifest},
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project::{ProjectContext, ProjectInspector},
    project_config::SnapshotSettings,
    snapshot::{Snapshot, SnapshotBuilder, SnapshotHook},
};
use proptest::{prelude::*, test_runner::Config as ProptestConfig};

use support::{GitRepo, create_directory};

const MANIFEST_PROPERTY_CASES: u32 = 256;
const MAX_MANIFEST_PATH_BYTES: usize = 1_024;
const MUTATION_CAPTURE_COUNT: usize = 1_000;
const STAGING_SIBLING: &str = ".partial-22222222-2222-4222-8222-222222222222";

fn settings(include_untracked: &[&str], include_empty_dirs: &[&str]) -> SnapshotSettings {
    SnapshotSettings {
        include_untracked: include_untracked
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        include_empty_dirs: include_empty_dirs
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        allow_sensitive: Vec::new(),
    }
}

fn inspect(repo: &GitRepo) -> ProjectContext {
    ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .expect("inspect fixture repository")
}

fn select(context: &ProjectContext, settings: &SnapshotSettings) -> InputSelection {
    InputSelector::new(&SystemProcessRunner)
        .select(context, settings)
        .expect("select fixture inputs")
}

fn capture(repo: &GitRepo, cache_root: &Path, settings: &SnapshotSettings) -> Snapshot {
    let context = inspect(repo);
    capture_context(&context, cache_root, settings)
}

fn capture_context(
    context: &ProjectContext,
    cache_root: &Path,
    settings: &SnapshotSettings,
) -> Snapshot {
    let initial = select(context, settings);
    SnapshotBuilder::new(&SystemProcessRunner, cache_root)
        .capture(context, settings, initial)
        .expect("capture verified snapshot")
}

fn entry<'a>(snapshot: &'a Snapshot, path: &str) -> &'a ManifestEntry {
    snapshot
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == path)
        .unwrap_or_else(|| panic!("snapshot manifest omitted {path}"))
}

fn manifest_entry_mut<'a>(manifest: &'a mut SnapshotManifest, path: &str) -> &'a mut ManifestEntry {
    manifest
        .entries
        .iter_mut()
        .find(|entry| entry.path == path)
        .unwrap_or_else(|| panic!("snapshot manifest omitted {path}"))
}

fn mode(path: impl AsRef<Path>) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
}

fn base_manifest() -> SnapshotManifest {
    SnapshotManifest {
        version: 1,
        project_id: "project".into(),
        worktree_id: "worktree".into(),
        head: Some("abc".into()),
        branch: Some("main".into()),
        dirty: true,
        relative_working_dir: "nested\ncwd".into(),
        entries: vec![
            ManifestEntry {
                path: "z".into(),
                kind: ManifestEntryKind::Symlink,
                mode: 0o777,
                size: 4,
                sha256: "zz".into(),
                symlink_target: Some("../x".into()),
            },
            ManifestEntry {
                path: "a\nβ".into(),
                kind: ManifestEntryKind::File,
                mode: 0o644,
                size: 2,
                sha256: "aa".into(),
                symlink_target: None,
            },
        ],
        tracked_deletions: vec!["z-deleted".into(), "a-deleted".into()],
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: MANIFEST_PROPERTY_CASES,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn shuffled_manifest_entries_have_identical_canonical_bytes_and_digest(
        payloads in prop::array::uniform8(any::<u8>()),
        entry_order in prop::array::uniform8(any::<u16>()),
        deletion_order in prop::array::uniform4(any::<u16>()),
    ) {
        // Catches canonicalization depending on caller insertion order. The
        // exact wire-format fixture below independently protects field order.
        let entries = payloads
            .into_iter()
            .enumerate()
            .map(|(index, payload)| ManifestEntry {
                path: format!("generated/{index:02}-β {payload:03}"),
                kind: ManifestEntryKind::File,
                mode: if payload & 1 == 0 { 0o644 } else { 0o755 },
                size: u64::from(payload),
                sha256: format!("{payload:02x}").repeat(32),
                symlink_target: None,
            })
            .collect::<Vec<_>>();
        let tracked_deletions = (0..4)
            .map(|index| format!("deleted/{index:02}-line\\nname"))
            .collect::<Vec<_>>();
        prop_assert!(entries.iter().all(|entry| entry.path.len() <= MAX_MANIFEST_PATH_BYTES));
        prop_assert!(tracked_deletions.iter().all(|path| path.len() <= MAX_MANIFEST_PATH_BYTES));

        let manifest = SnapshotManifest {
            version: 1,
            project_id: "property-project".into(),
            worktree_id: "property-worktree".into(),
            head: Some("0123456789abcdef".into()),
            branch: Some("property".into()),
            dirty: true,
            relative_working_dir: "nested".into(),
            entries,
            tracked_deletions,
        };
        let mut shuffled = manifest.clone();
        let mut keyed_entries = shuffled
            .entries
            .drain(..)
            .enumerate()
            .collect::<Vec<_>>();
        keyed_entries.sort_by_key(|(index, _)| (entry_order[*index], *index));
        shuffled.entries = keyed_entries.into_iter().map(|(_, entry)| entry).collect();
        if shuffled.entries == manifest.entries {
            shuffled.entries.rotate_left(1);
        }
        let mut keyed_deletions = shuffled
            .tracked_deletions
            .drain(..)
            .enumerate()
            .collect::<Vec<_>>();
        keyed_deletions.sort_by_key(|(index, _)| (deletion_order[*index], *index));
        shuffled.tracked_deletions = keyed_deletions
            .into_iter()
            .map(|(_, deletion)| deletion)
            .collect();
        if shuffled.tracked_deletions == manifest.tracked_deletions {
            shuffled.tracked_deletions.rotate_left(1);
        }

        prop_assert_ne!(&shuffled.entries, &manifest.entries);
        prop_assert_ne!(&shuffled.tracked_deletions, &manifest.tracked_deletions);
        let canonical = manifest.canonical_bytes().unwrap();
        prop_assert!(
            canonical.starts_with(br#"{"version":1,"project_id":"property-project""#),
            "canonical manifest prefix changed"
        );
        prop_assert_eq!(shuffled.canonical_bytes().unwrap(), canonical);
        prop_assert_eq!(shuffled.digest().unwrap(), manifest.digest().unwrap());
    }
}

#[test]
fn canonical_manifest_has_exact_compact_sorted_wire_bytes_and_digest() {
    // Catches field reordering, pretty-printing, path-order drift, or hashing
    // bytes other than the exact canonical JSON sidecar.
    let manifest = base_manifest();
    let expected = concat!(
        r#"{"version":1,"project_id":"project","worktree_id":"worktree","#,
        r#""head":"abc","branch":"main","dirty":true,"#,
        r#""relative_working_dir":"nested\ncwd","entries":["#,
        r#"{"path":"a\nβ","kind":"file","mode":420,"size":2,"sha256":"aa","symlink_target":null},"#,
        r#"{"path":"z","kind":"symlink","mode":511,"size":4,"sha256":"zz","symlink_target":"../x"}],"#,
        r#""tracked_deletions":["a-deleted","z-deleted"]}"#,
    );

    assert_eq!(manifest.canonical_bytes().unwrap(), expected.as_bytes());
    assert_eq!(
        manifest.digest().unwrap(),
        "420e3a23fbd1641552f0635d7d87e0f6f30befba27185e13d8cd95374f0776fd"
    );
}

#[test]
fn every_snapshot_semantic_changes_the_manifest_digest() {
    // Catches omitting any source or worktree identity field from the digest.
    let base = base_manifest();
    let base_digest = base.digest().unwrap();
    let mut variants = Vec::new();

    let mut changed = base.clone();
    changed.entries[1].sha256 = "different-bytes".into();
    variants.push(changed);

    let mut changed = base.clone();
    changed.entries[1].mode = 0o755;
    variants.push(changed);

    let mut changed = base.clone();
    changed.entries[0].symlink_target = Some("different-target".into());
    changed.entries[0].sha256 = "different-link".into();
    variants.push(changed);

    let mut changed = base.clone();
    changed.entries.push(ManifestEntry {
        path: "declared-empty".into(),
        kind: ManifestEntryKind::Directory,
        mode: 0o755,
        size: 0,
        sha256: "directory".into(),
        symlink_target: None,
    });
    variants.push(changed);

    let mut changed = base.clone();
    changed.tracked_deletions.push("another-deletion".into());
    variants.push(changed);

    let mut changed = base.clone();
    changed.relative_working_dir = "elsewhere".into();
    variants.push(changed);

    let mut changed = base.clone();
    changed.head = Some("def".into());
    variants.push(changed);

    let mut changed = base.clone();
    changed.branch = Some("feature".into());
    variants.push(changed);

    let mut changed = base.clone();
    changed.dirty = false;
    variants.push(changed);

    let mut changed = base.clone();
    changed.project_id = "other-project".into();
    variants.push(changed);

    let mut changed = base;
    changed.worktree_id = "other-worktree".into();
    variants.push(changed);

    for variant in variants {
        assert_ne!(variant.digest().unwrap(), base_digest);
    }
}

#[test]
fn unchanged_captures_have_identical_manifests_despite_unique_capture_ids() {
    // Catches capture UUIDs, staging locations, or timestamps leaking into the
    // manifest and defeating stable snapshot identity.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"stable bytes\n");
    repo.commit_all("track stable input");
    let cache = tempfile::tempdir().unwrap();

    let first = capture(&repo, cache.path(), &settings(&[], &[]));
    let second = capture(&repo, cache.path(), &settings(&[], &[]));

    assert_ne!(first.capture_id, second.capture_id);
    assert_ne!(first.root, second.root);
    assert_eq!(
        first.manifest.canonical_bytes().unwrap(),
        second.manifest.canonical_bytes().unwrap()
    );
    assert_eq!(first.digest, second.digest);

    first.cleanup().unwrap();
    second.cleanup().unwrap();
}

#[test]
fn real_capture_hashes_same_length_file_bytes_and_entry_domains_exactly() {
    // Catches constant, size-only, text-decoded, or non-domain-separated entry
    // hashes. Both file captures stay dirty against the same HEAD so only the
    // four payload bytes differ in their manifests.
    let repo = GitRepo::init();
    repo.write("payload.bin", b"0000");
    repo.write("target.txt", b"link target contents\n");
    symlink("target.txt", repo.root().join("tracked-link")).unwrap();
    repo.commit_all("track hash fixtures");
    create_directory(repo.root().join("declared/empty"));
    repo.write("payload.bin", b"ABCD");
    let cache = tempfile::tempdir().unwrap();
    let settings = settings(&[], &["declared/empty"]);

    let first = capture(&repo, cache.path(), &settings);
    assert!(first.manifest.dirty);
    assert_eq!(
        entry(&first, "payload.bin"),
        &ManifestEntry {
            path: "payload.bin".into(),
            kind: ManifestEntryKind::File,
            mode: 0o644,
            size: 4,
            sha256: "e12e115acf4552b2568b55e93cbd39394c4ef81c82447fafc997882a02d23677".into(),
            symlink_target: None,
        }
    );
    assert_eq!(
        entry(&first, "tracked-link"),
        &ManifestEntry {
            path: "tracked-link".into(),
            kind: ManifestEntryKind::Symlink,
            mode: 0o777,
            size: 10,
            sha256: "dfd50ff65ada073aad22ed9e89ed05ec4bbb5542dc759e8605b997de20ce2d8c".into(),
            symlink_target: Some("target.txt".into()),
        }
    );
    assert_eq!(
        entry(&first, "declared/empty"),
        &ManifestEntry {
            path: "declared/empty".into(),
            kind: ManifestEntryKind::Directory,
            mode: 0o755,
            size: 0,
            sha256: "4b66bf93b3932a9539880b2421e35019af9daf84363a0246280ffcac173b2678".into(),
            symlink_target: None,
        }
    );

    repo.write("payload.bin", b"WXYZ");
    let second = capture(&repo, cache.path(), &settings);
    assert!(second.manifest.dirty);
    assert_eq!(entry(&second, "payload.bin").size, 4);
    assert_eq!(
        entry(&second, "payload.bin").sha256,
        "21e32f5321cad49ab4cf78ba5ed231e0f36d0c78d34108fda1be939f33fba149"
    );
    assert_ne!(
        entry(&first, "payload.bin").sha256,
        entry(&second, "payload.bin").sha256
    );
    let mut normalized_second = second.manifest.clone();
    manifest_entry_mut(&mut normalized_second, "payload.bin").sha256 =
        entry(&first, "payload.bin").sha256.clone();
    assert_eq!(first.manifest, normalized_second);
    assert_ne!(first.digest, second.digest);

    first.cleanup().unwrap();
    second.cleanup().unwrap();
}

#[test]
fn real_capture_digest_tracks_executable_mode() {
    let repo = GitRepo::init();
    repo.write("tool", b"committed\n");
    fs::set_permissions(repo.root().join("tool"), fs::Permissions::from_mode(0o644)).unwrap();
    repo.commit_all("track mode fixture");
    repo.write("tool", b"captured bytes\n");
    let cache = tempfile::tempdir().unwrap();

    let regular = capture(&repo, cache.path(), &settings(&[], &[]));
    fs::set_permissions(repo.root().join("tool"), fs::Permissions::from_mode(0o755)).unwrap();
    let executable = capture(&repo, cache.path(), &settings(&[], &[]));

    assert!(regular.manifest.dirty);
    assert!(executable.manifest.dirty);
    assert_eq!(entry(&regular, "tool").mode, 0o644);
    assert_eq!(entry(&executable, "tool").mode, 0o755);
    assert_eq!(
        entry(&regular, "tool").sha256,
        entry(&executable, "tool").sha256
    );
    let mut normalized_executable = executable.manifest.clone();
    manifest_entry_mut(&mut normalized_executable, "tool").mode = entry(&regular, "tool").mode;
    assert_eq!(regular.manifest, normalized_executable);
    assert_ne!(regular.digest, executable.digest);

    regular.cleanup().unwrap();
    executable.cleanup().unwrap();
}

#[test]
fn real_capture_digest_tracks_symlink_target() {
    let repo = GitRepo::init();
    symlink("base!", repo.root().join("tracked-link")).unwrap();
    repo.commit_all("track symlink fixture");
    fs::remove_file(repo.root().join("tracked-link")).unwrap();
    symlink("alpha", repo.root().join("tracked-link")).unwrap();
    let cache = tempfile::tempdir().unwrap();

    let alpha = capture(&repo, cache.path(), &settings(&[], &[]));
    fs::remove_file(repo.root().join("tracked-link")).unwrap();
    symlink("omega", repo.root().join("tracked-link")).unwrap();
    let omega = capture(&repo, cache.path(), &settings(&[], &[]));

    assert!(alpha.manifest.dirty);
    assert!(omega.manifest.dirty);
    assert_eq!(entry(&alpha, "tracked-link").size, 5);
    assert_eq!(entry(&omega, "tracked-link").size, 5);
    assert_eq!(
        entry(&alpha, "tracked-link").symlink_target.as_deref(),
        Some("alpha")
    );
    assert_eq!(
        entry(&omega, "tracked-link").symlink_target.as_deref(),
        Some("omega")
    );
    assert_ne!(
        entry(&alpha, "tracked-link").sha256,
        entry(&omega, "tracked-link").sha256
    );
    let mut normalized_omega = omega.manifest.clone();
    let normalized_link = manifest_entry_mut(&mut normalized_omega, "tracked-link");
    normalized_link.sha256 = entry(&alpha, "tracked-link").sha256.clone();
    normalized_link.symlink_target = entry(&alpha, "tracked-link").symlink_target.clone();
    assert_eq!(alpha.manifest, normalized_omega);
    assert_ne!(alpha.digest, omega.digest);

    alpha.cleanup().unwrap();
    omega.cleanup().unwrap();
}

#[test]
fn real_capture_digest_tracks_declared_empty_directory_set() {
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"stable\n");
    repo.commit_all("track empty-directory fixture");
    create_directory(repo.root().join("declared/empty"));
    let cache = tempfile::tempdir().unwrap();

    let omitted = capture(&repo, cache.path(), &settings(&[], &[]));
    let declared = capture(&repo, cache.path(), &settings(&[], &["declared/empty"]));

    assert!(
        omitted
            .manifest
            .entries
            .iter()
            .all(|entry| entry.path != "declared/empty")
    );
    assert_eq!(
        entry(&declared, "declared/empty").kind,
        ManifestEntryKind::Directory
    );
    let mut normalized_declared = declared.manifest.clone();
    normalized_declared
        .entries
        .retain(|entry| entry.path != "declared/empty");
    assert_eq!(omitted.manifest, normalized_declared);
    assert_ne!(omitted.digest, declared.digest);

    omitted.cleanup().unwrap();
    declared.cleanup().unwrap();
}

#[test]
fn real_capture_digest_tracks_deletion_set() {
    let repo = GitRepo::init();
    repo.write("stable.txt", b"stable\n");
    repo.write("delete-me.txt", b"remove this\n");
    repo.commit_all("track deletion fixture");
    fs::remove_file(repo.root().join("delete-me.txt")).unwrap();
    let cache = tempfile::tempdir().unwrap();

    let deleted_from_worktree = capture(&repo, cache.path(), &settings(&[], &[]));
    assert_eq!(
        deleted_from_worktree.manifest.tracked_deletions,
        ["delete-me.txt"]
    );
    assert!(
        repo.git(&["rm", "--cached", "--ignore-unmatch", "delete-me.txt"])
            .status
            .success()
    );
    let deleted_from_index = capture(&repo, cache.path(), &settings(&[], &[]));

    assert!(deleted_from_index.manifest.tracked_deletions.is_empty());
    assert_eq!(
        deleted_from_worktree.manifest.entries,
        deleted_from_index.manifest.entries
    );
    let mut normalized_worktree = deleted_from_worktree.manifest.clone();
    normalized_worktree.tracked_deletions.clear();
    assert_eq!(normalized_worktree, deleted_from_index.manifest);
    assert_ne!(deleted_from_worktree.digest, deleted_from_index.digest);

    deleted_from_worktree.cleanup().unwrap();
    deleted_from_index.cleanup().unwrap();
}

#[test]
fn real_capture_digest_tracks_relative_working_directory() {
    let repo = GitRepo::init();
    repo.write("nested/tracked.txt", b"stable\n");
    repo.commit_all("track relative working directory fixture");
    let cache = tempfile::tempdir().unwrap();
    let root_context = inspect(&repo);
    let nested_context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(&repo.root().join("nested"))
        .unwrap();
    let settings = settings(&[], &[]);

    let root = capture_context(&root_context, cache.path(), &settings);
    let nested = capture_context(&nested_context, cache.path(), &settings);

    assert_eq!(root.manifest.relative_working_dir, "");
    assert_eq!(nested.manifest.relative_working_dir, "nested");
    assert_eq!(entry(&nested, "nested").kind, ManifestEntryKind::Directory);
    assert!(nested.root.join("nested").is_dir());
    let mut normalized_nested = nested.manifest.clone();
    normalized_nested.relative_working_dir = root.manifest.relative_working_dir.clone();
    normalized_nested
        .entries
        .retain(|entry| entry.path != "nested");
    assert_eq!(root.manifest, normalized_nested);
    assert_ne!(root.digest, nested.digest);

    root.cleanup().unwrap();
    nested.cleanup().unwrap();
}

#[test]
fn real_capture_digest_tracks_head() {
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"stable\n");
    repo.commit_all("track HEAD fixture");
    let cache = tempfile::tempdir().unwrap();
    let before = capture(&repo, cache.path(), &settings(&[], &[]));

    assert!(
        repo.git(&["commit", "--allow-empty", "-m", "advance HEAD"])
            .status
            .success()
    );
    let after = capture(&repo, cache.path(), &settings(&[], &[]));

    assert_ne!(before.manifest.head, after.manifest.head);
    assert_eq!(before.manifest.entries, after.manifest.entries);
    assert_eq!(before.manifest.project_id, after.manifest.project_id);
    assert_eq!(before.manifest.worktree_id, after.manifest.worktree_id);
    let mut normalized_after = after.manifest.clone();
    normalized_after.head = before.manifest.head.clone();
    assert_eq!(before.manifest, normalized_after);
    assert_ne!(before.digest, after.digest);

    before.cleanup().unwrap();
    after.cleanup().unwrap();
}

#[test]
fn real_capture_digest_tracks_project_identity() {
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"stable\n");
    repo.commit_all("track project identity fixture");
    assert!(
        repo.git(&[
            "remote",
            "add",
            "origin",
            "https://example.test/project-a.git",
        ])
        .status
        .success()
    );
    let cache = tempfile::tempdir().unwrap();
    let first = capture(&repo, cache.path(), &settings(&[], &[]));

    assert!(
        repo.git(&[
            "remote",
            "set-url",
            "origin",
            "https://example.test/project-b.git",
        ])
        .status
        .success()
    );
    let second = capture(&repo, cache.path(), &settings(&[], &[]));

    assert_ne!(first.manifest.project_id, second.manifest.project_id);
    assert_eq!(first.manifest.worktree_id, second.manifest.worktree_id);
    assert_eq!(first.manifest.head, second.manifest.head);
    assert_eq!(first.manifest.entries, second.manifest.entries);
    let mut normalized_second = second.manifest.clone();
    normalized_second.project_id = first.manifest.project_id.clone();
    assert_eq!(first.manifest, normalized_second);
    assert_ne!(first.digest, second.digest);

    first.cleanup().unwrap();
    second.cleanup().unwrap();
}

#[test]
fn real_capture_digest_tracks_worktree_identity() {
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"stable\n");
    repo.commit_all("track worktree identity fixture");
    assert!(repo.git(&["checkout", "--detach", "HEAD"]).status.success());
    let linked_parent = tempfile::tempdir().unwrap();
    let linked_root = linked_parent.path().join("linked");
    assert!(
        repo.git(&[
            "worktree",
            "add",
            "--detach",
            linked_root.to_str().unwrap(),
            "HEAD",
        ])
        .status
        .success()
    );
    let cache = tempfile::tempdir().unwrap();
    let primary_context = inspect(&repo);
    let linked_context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(&linked_root)
        .unwrap();
    let settings = settings(&[], &[]);

    let primary = capture_context(&primary_context, cache.path(), &settings);
    let linked = capture_context(&linked_context, cache.path(), &settings);

    assert_eq!(primary.manifest.project_id, linked.manifest.project_id);
    assert_ne!(primary.manifest.worktree_id, linked.manifest.worktree_id);
    assert_eq!(primary.manifest.head, linked.manifest.head);
    assert_eq!(primary.manifest.branch, None);
    assert_eq!(linked.manifest.branch, None);
    assert_eq!(primary.manifest.entries, linked.manifest.entries);
    let mut normalized_linked = linked.manifest.clone();
    normalized_linked.worktree_id = primary.manifest.worktree_id.clone();
    assert_eq!(primary.manifest, normalized_linked);
    assert_ne!(primary.digest, linked.digest);

    primary.cleanup().unwrap();
    linked.cleanup().unwrap();
}

struct RecordingHook<F> {
    capture_id: Mutex<Option<String>>,
    action: F,
}

impl<F> RecordingHook<F> {
    fn new(action: F) -> Self {
        Self {
            capture_id: Mutex::new(None),
            action,
        }
    }

    fn capture_id(&self) -> String {
        self.capture_id
            .lock()
            .unwrap()
            .clone()
            .expect("hook recorded capture ID")
    }
}

impl<F> SnapshotHook for RecordingHook<F>
where
    F: Fn(&Path) -> io::Result<()> + Send + Sync,
{
    fn after_materialization(&self, tree: &Path) -> io::Result<()> {
        let partial_name = tree
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("missing partial capture path"))?;
        let capture_id = partial_name
            .strip_prefix(".partial-")
            .ok_or_else(|| io::Error::other("invalid partial capture path"))?;
        *self.capture_id.lock().unwrap() = Some(capture_id.to_owned());
        (self.action)(tree)
    }
}

type Mutation = fn(&Path) -> io::Result<()>;

fn mutate_file_bytes(root: &Path) -> io::Result<()> {
    fs::write(root.join("tracked.txt"), b"changed bytes\n")
}

fn mutate_file_mode(root: &Path) -> io::Result<()> {
    fs::set_permissions(root.join("tracked.txt"), fs::Permissions::from_mode(0o755))
}

fn mutate_file_type(root: &Path) -> io::Result<()> {
    fs::remove_file(root.join("tracked.txt"))?;
    symlink("tracked-link", root.join("tracked.txt"))
}

fn mutate_symlink_target(root: &Path) -> io::Result<()> {
    fs::remove_file(root.join("tracked-link"))?;
    symlink("different-target", root.join("tracked-link"))
}

fn mutate_symlink_target_to_non_utf8(root: &Path) -> io::Result<()> {
    fs::remove_file(root.join("tracked-link"))?;
    symlink(
        OsStr::from_bytes(b"non-utf8-\xff-target"),
        root.join("tracked-link"),
    )
}

fn mutate_tracked_deletion(root: &Path) -> io::Result<()> {
    fs::remove_file(root.join("remove-me.txt"))
}

fn mutate_new_uncovered_untracked(root: &Path) -> io::Result<()> {
    fs::write(root.join("uncovered.txt"), b"new uncovered input\n")
}

fn mutate_removed_path(root: &Path) -> io::Result<()> {
    fs::remove_file(root.join("included/keep.txt"))
}

fn mutate_included_path_set(root: &Path) -> io::Result<()> {
    fs::write(root.join("included/new.txt"), b"new included input\n")
}

fn assert_snapshot_changed(label: &str, mutation: Mutation) {
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"original bytes\n");
    repo.write("remove-me.txt", b"present at enumeration\n");
    symlink("tracked.txt", repo.root().join("tracked-link")).unwrap();
    repo.commit_all("track mutation fixtures");
    repo.write("included/keep.txt", b"included local input\n");
    let settings = settings(&["included/*.txt"], &[]);
    let context = inspect(&repo);
    let initial = select(&context, &settings);
    let cache = tempfile::tempdir().unwrap();
    let staging = cache.path().join("snapshots/staging");
    create_directory(staging.join(STAGING_SIBLING));
    fs::write(
        staging.join(STAGING_SIBLING).join("sentinel"),
        b"keep sibling\n",
    )
    .unwrap();
    let repo_root = repo.root().to_path_buf();
    let hook = RecordingHook::new(move |_tree: &Path| mutation(&repo_root));

    let error = SnapshotBuilder::with_hook(&SystemProcessRunner, cache.path(), &hook)
        .capture(&context, &settings, initial)
        .expect_err("source mutation must reject capture");

    assert!(
        matches!(
            error,
            WorkerError::Snapshot {
                code: "SNAPSHOT_CHANGED",
                ..
            }
        ),
        "{label}: {error}"
    );
    let capture_id = hook.capture_id();
    assert!(
        !cache
            .path()
            .join("snapshots/ready")
            .join(&capture_id)
            .exists(),
        "{label}: changed capture was published"
    );
    assert!(
        !staging.join(format!(".partial-{capture_id}")).exists(),
        "{label}: owned partial capture was not removed"
    );
    assert_eq!(
        fs::read(staging.join(STAGING_SIBLING).join("sentinel")).unwrap(),
        b"keep sibling\n",
        "{label}: cleanup crossed its owned boundary"
    );
}

#[test]
fn every_enumerated_source_mutation_is_rejected_without_publication() {
    // Each case catches a distinct way a second-pass check could compare only
    // file content while missing selection or metadata drift.
    let cases: [(&str, Mutation); 9] = [
        ("file bytes", mutate_file_bytes),
        ("file mode", mutate_file_mode),
        ("entry type", mutate_file_type),
        ("symlink target", mutate_symlink_target),
        (
            "symlink target becomes non-UTF-8",
            mutate_symlink_target_to_non_utf8,
        ),
        ("tracked deletion", mutate_tracked_deletion),
        ("new uncovered untracked", mutate_new_uncovered_untracked),
        ("removed selected path", mutate_removed_path),
        ("included path set", mutate_included_path_set),
    ];

    for (label, mutation) in cases {
        assert_snapshot_changed(label, mutation);
    }
}

struct SinglePathSelectionRunner {
    path: String,
}

impl ProcessRunner for SinglePathSelectionRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, OsStr::new("/usr/bin/git"));
        let (status, stdout) = if request.args.iter().any(|argument| argument == "--stage") {
            (
                ExitStatus::from_raw(0),
                format!(
                    "100644 0123456789012345678901234567890123456789 0\t{}\0",
                    self.path
                )
                .into_bytes(),
            )
        } else if request
            .args
            .iter()
            .any(|argument| argument == "--get-regexp")
        {
            (ExitStatus::from_raw(1 << 8), Vec::new())
        } else if request.args.iter().any(|argument| argument == "check-attr") {
            (
                ExitStatus::from_raw(0),
                [self.path.as_bytes(), b"\0filter\0unspecified\0"].concat(),
            )
        } else {
            (ExitStatus::from_raw(0), Vec::new())
        };
        Ok(ProcessResult {
            status,
            stdout,
            stderr: Vec::new(),
        })
    }
}

fn assert_snapshot_capture_roots_empty(cache: &Path) {
    for root in [
        cache.join("snapshots/staging"),
        cache.join("snapshots/ready"),
    ] {
        if !root.exists() {
            continue;
        }
        for entry in fs::read_dir(&root).unwrap() {
            let entry = entry.unwrap();
            assert_eq!(
                entry.file_name(),
                ".mac-worker-rooted-fs",
                "unexpected capture residue under {}",
                root.display()
            );
            assert!(entry.file_type().unwrap().is_dir());
            assert!(
                fs::read_dir(entry.path()).unwrap().next().is_none(),
                "private capture cleanup residue remained under {}",
                root.display()
            );
        }
    }
}

#[test]
fn one_thousand_deterministic_source_mutations_publish_zero_snapshots() {
    // Catches accepting even one hybrid capture at the binding 1,000-edit
    // go/no-go threshold while proving every owned partial is removed.
    let source = tempfile::tempdir().unwrap();
    create_directory(source.path().join("matrix"));
    let root = fs::canonicalize(source.path()).unwrap();
    let context = ProjectContext {
        root: root.clone(),
        relative_cwd: PathBuf::new(),
        git_dir: root.join(".git"),
        common_dir: root.join(".git"),
        head: Some("0123456789012345678901234567890123456789".into()),
        branch: Some("matrix".into()),
        project_id: "matrix-project".into(),
        worktree_id: "matrix-worktree".into(),
        dirty: true,
    };
    let settings = settings(&[], &[]);
    let cache = tempfile::tempdir().unwrap();
    let started = Instant::now();
    let mut successful_publications = 0;
    let mut mutated_paths = BTreeSet::new();

    for index in 0..MUTATION_CAPTURE_COUNT {
        let relative = format!("matrix/capture-{index:04}.bin");
        assert!(mutated_paths.insert(relative.clone()));
        let source_path = root.join(&relative);
        fs::write(&source_path, format!("before-{index:04}\n")).unwrap();
        let path = RelativePath::parse(relative.as_bytes()).unwrap();
        let initial = InputSelection {
            entries: vec![SelectedInput {
                path: path.clone(),
                origin: InputOrigin::Tracked,
                kind: SelectedInputKind::FilesystemEntry,
            }],
            tracked_deletions: Vec::new(),
            warnings: Vec::new(),
        };
        let runner = SinglePathSelectionRunner {
            path: relative.clone(),
        };
        let hook_path = source_path.clone();
        let hook = RecordingHook::new(move |_tree: &Path| {
            fs::write(&hook_path, format!("after--{index:04}\n"))
        });

        match SnapshotBuilder::with_hook(&runner, cache.path(), &hook)
            .capture(&context, &settings, initial)
        {
            Ok(snapshot) => {
                successful_publications += 1;
                snapshot.cleanup().unwrap();
                break;
            }
            Err(error) => assert!(
                matches!(
                    error,
                    WorkerError::Snapshot {
                        code: "SNAPSHOT_CHANGED",
                        ..
                    }
                ),
                "capture {index} returned {error}"
            ),
        }

        let capture_id = hook.capture_id();
        assert!(
            !cache
                .path()
                .join("snapshots/staging")
                .join(format!(".partial-{capture_id}"))
                .exists(),
            "capture {index} retained its partial tree"
        );
        assert!(
            !cache
                .path()
                .join("snapshots/ready")
                .join(capture_id)
                .exists(),
            "capture {index} reached ready publication"
        );
        assert_snapshot_capture_roots_empty(cache.path());
        fs::remove_file(source_path).unwrap();
    }

    let elapsed = started.elapsed();
    eprintln!(
        "mutation matrix: {MUTATION_CAPTURE_COUNT} captures in {:.3}s",
        elapsed.as_secs_f64()
    );
    assert_eq!(successful_publications, 0);
    assert_eq!(mutated_paths.len(), MUTATION_CAPTURE_COUNT);
    assert_snapshot_capture_roots_empty(cache.path());
}

#[test]
fn initially_non_utf8_symlink_target_is_unsupported_path_encoding() {
    // Catches treating an unsupported input present before materialization as
    // a concurrent source mutation instead of its stable encoding failure.
    let repo = GitRepo::init();
    symlink(
        OsStr::from_bytes(b"non-utf8-\xff-target"),
        repo.root().join("tracked-link"),
    )
    .unwrap();
    repo.commit_all("track unsupported symlink target");
    let cache = tempfile::tempdir().unwrap();
    let context = inspect(&repo);
    let settings = settings(&[], &[]);
    let initial = select(&context, &settings);

    let error = SnapshotBuilder::new(&SystemProcessRunner, cache.path())
        .capture(&context, &settings, initial)
        .expect_err("initial non-UTF-8 target must be rejected");

    assert!(matches!(
        error,
        WorkerError::Snapshot {
            code: "UNSUPPORTED_PATH_ENCODING",
            ..
        }
    ));
}

#[test]
fn capture_preserves_binary_and_unusual_names_without_git_or_inline_manifest() {
    // Catches text decoding, newline splitting, Git metadata inclusion, or
    // treating the manifest as another project input.
    let repo = GitRepo::init();
    let unusual = "fixtures/привет\nданные.bin";
    let binary = b"\0\xffbinary\n\0";
    repo.write(unusual, binary);
    repo.write("fixtures/zero.bin", b"");
    repo.write("bin/tool", b"#!/bin/sh\nexit 0\n");
    fs::set_permissions(
        repo.root().join("bin/tool"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    symlink("../bin/tool\n", repo.root().join("tool-link")).unwrap();
    let outside = tempfile::tempdir().unwrap();
    let outside_target = outside.path().join("must-not-dereference.txt");
    let outside_bytes = b"outside symlink target bytes must not be captured\n";
    fs::write(&outside_target, outside_bytes).unwrap();
    symlink(&outside_target, repo.root().join("absolute-link")).unwrap();
    repo.commit_all("track unusual snapshot inputs");
    let cache = tempfile::tempdir().unwrap();

    let snapshot = capture(&repo, cache.path(), &settings(&[], &[]));

    assert_eq!(fs::read(snapshot.root.join(unusual)).unwrap(), binary);
    assert_eq!(
        fs::read(snapshot.root.join("fixtures/zero.bin")).unwrap(),
        b""
    );
    assert_eq!(entry(&snapshot, "fixtures/zero.bin").size, 0);
    assert_eq!(
        entry(&snapshot, "fixtures/zero.bin").sha256,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        fs::read_link(snapshot.root.join("tool-link")).unwrap(),
        PathBuf::from("../bin/tool\n")
    );
    assert_eq!(
        fs::read_link(snapshot.root.join("absolute-link")).unwrap(),
        outside_target
    );
    assert_eq!(fs::read(&outside_target).unwrap(), outside_bytes);
    assert!(
        !snapshot
            .manifest
            .canonical_bytes()
            .unwrap()
            .windows(outside_bytes.len())
            .any(|window| window == outside_bytes)
    );
    assert!(!contains_component_named(&snapshot.root, ".git"));
    assert!(!snapshot.root.join("manifest.json").exists());
    assert_eq!(
        snapshot.manifest_path,
        snapshot.root.parent().unwrap().join("manifest.json")
    );
    assert_eq!(
        fs::read(&snapshot.manifest_path).unwrap(),
        snapshot.manifest.canonical_bytes().unwrap()
    );
    assert_eq!(snapshot.digest, snapshot.manifest.digest().unwrap());
    assert_eq!(snapshot.file_count, 5);
    assert_eq!(
        snapshot.total_bytes,
        binary.len() as u64
            + b"#!/bin/sh\nexit 0\n".len() as u64
            + b"../bin/tool\n".len() as u64
            + outside_target.as_os_str().as_bytes().len() as u64
    );

    snapshot.cleanup().unwrap();
}

#[test]
fn capture_preserves_a_255_byte_utf8_filename_when_supported() {
    // Catches character-count truncation or lossy normalization at the common
    // 255-byte component boundary. Only ENAMETOOLONG permits this one skip.
    let repo = GitRepo::init();
    let name = format!("{}a", "é".repeat(127));
    assert_eq!(name.len(), 255);
    let bytes = b"maximum UTF-8 filename bytes\n";
    match fs::write(repo.root().join(&name), bytes) {
        Ok(()) => {}
        Err(error) if error.raw_os_error() == Some(libc::ENAMETOOLONG) => {
            eprintln!("255-byte UTF-8 filename unsupported: ENAMETOOLONG");
            return;
        }
        Err(error) => panic!("255-byte UTF-8 filename creation failed: {error}"),
    }
    repo.commit_all("track maximum UTF-8 filename");
    let cache = tempfile::tempdir().unwrap();

    let snapshot = capture(&repo, cache.path(), &settings(&[], &[]));

    assert_eq!(fs::read(snapshot.root.join(&name)).unwrap(), bytes);
    assert_eq!(entry(&snapshot, &name).path.len(), 255);
    snapshot.cleanup().unwrap();
    assert_snapshot_capture_roots_empty(cache.path());
}

struct BarrierHook {
    barrier: Arc<Barrier>,
}

impl SnapshotHook for BarrierHook {
    fn after_materialization(&self, _tree: &Path) -> io::Result<()> {
        self.barrier.wait();
        Ok(())
    }
}

#[test]
fn simultaneous_linked_worktree_captures_have_isolated_exact_cleanup() {
    // Catches shared cache publication or cleanup crossing from one linked
    // worktree's capture into another live immutable snapshot.
    let repo = GitRepo::init();
    repo.write("tracked.bin", b"\0linked-worktree-bytes\xff\n");
    repo.write(
        "nested/readable.txt",
        b"still readable after sibling cleanup\n",
    );
    repo.commit_all("track concurrent capture fixture");
    let linked_parent = tempfile::tempdir().unwrap();
    let linked_root = linked_parent.path().join("linked");
    assert!(
        repo.git(&[
            "worktree",
            "add",
            "--detach",
            linked_root.to_str().unwrap(),
            "HEAD",
        ])
        .status
        .success()
    );
    let primary_context = inspect(&repo);
    let linked_context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(&linked_root)
        .unwrap();
    let settings = settings(&[], &[]);
    let primary_initial = select(&primary_context, &settings);
    let linked_initial = select(&linked_context, &settings);
    let cache = tempfile::tempdir().unwrap();
    let hook = BarrierHook {
        barrier: Arc::new(Barrier::new(2)),
    };

    let (primary, linked) = std::thread::scope(|scope| {
        let primary_handle = scope.spawn(|| {
            SnapshotBuilder::with_hook(&SystemProcessRunner, cache.path(), &hook)
                .capture(&primary_context, &settings, primary_initial)
                .unwrap()
        });
        let linked_handle = scope.spawn(|| {
            SnapshotBuilder::with_hook(&SystemProcessRunner, cache.path(), &hook)
                .capture(&linked_context, &settings, linked_initial)
                .unwrap()
        });
        (
            primary_handle.join().unwrap(),
            linked_handle.join().unwrap(),
        )
    });

    assert_ne!(primary.capture_id, linked.capture_id);
    assert_eq!(
        primary.manifest.project_id, linked.manifest.project_id,
        "linked worktrees must retain shared project identity"
    );
    assert_ne!(primary.manifest.worktree_id, linked.manifest.worktree_id);
    let primary_container = primary.root.parent().unwrap().to_path_buf();
    let linked_manifest_bytes = fs::read(&linked.manifest_path).unwrap();
    let linked_binary_bytes = fs::read(linked.root.join("tracked.bin")).unwrap();
    let linked_readable_bytes = fs::read(linked.root.join("nested/readable.txt")).unwrap();

    primary.cleanup().unwrap();

    assert!(!primary_container.exists());
    assert_eq!(
        fs::read(&linked.manifest_path).unwrap(),
        linked_manifest_bytes
    );
    assert_eq!(
        fs::read(linked.root.join("tracked.bin")).unwrap(),
        linked_binary_bytes
    );
    assert_eq!(
        fs::read(linked.root.join("nested/readable.txt")).unwrap(),
        linked_readable_bytes
    );
    assert_eq!(
        linked.manifest.canonical_bytes().unwrap(),
        linked_manifest_bytes
    );

    linked.cleanup().unwrap();
    assert_snapshot_capture_roots_empty(cache.path());
}

fn contains_component_named(root: &Path, wanted: &str) -> bool {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return false,
    };
    for entry in entries.flatten() {
        if entry.file_name() == wanted {
            return true;
        }
        if entry.file_type().is_ok_and(|kind| kind.is_dir())
            && contains_component_named(&entry.path(), wanted)
        {
            return true;
        }
    }
    false
}

#[test]
fn published_tree_files_and_manifest_are_read_only() {
    // Catches publishing a writable tree whose later mutation would invalidate
    // the already returned manifest digest.
    let repo = GitRepo::init();
    repo.write("nested/data.txt", b"data\n");
    repo.write("nested/tool", b"#!/bin/sh\n");
    fs::set_permissions(
        repo.root().join("nested/tool"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    create_directory(repo.root().join("declared/empty"));
    repo.commit_all("track permission fixtures");
    let cache = tempfile::tempdir().unwrap();

    let snapshot = capture(&repo, cache.path(), &settings(&[], &["declared/empty"]));

    assert_eq!(mode(&snapshot.root), 0o555);
    assert_eq!(mode(snapshot.root.join("nested")), 0o555);
    assert_eq!(mode(snapshot.root.join("nested/data.txt")), 0o444);
    assert_eq!(mode(snapshot.root.join("nested/tool")), 0o555);
    assert_eq!(mode(snapshot.root.join("declared/empty")), 0o555);
    assert_eq!(mode(&snapshot.manifest_path), 0o444);
    let data_entry = snapshot
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "nested/data.txt")
        .unwrap();
    let tool_entry = snapshot
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "nested/tool")
        .unwrap();
    assert_eq!(data_entry.mode, 0o644);
    assert_eq!(tool_entry.mode, 0o755);

    snapshot.cleanup().unwrap();
}

#[test]
fn cleanup_removes_only_its_read_only_snapshot() {
    // Catches broad cache cleanup or cleanup that relies on writable published
    // modes and therefore leaves immutable snapshots behind.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"snapshot bytes\n");
    repo.commit_all("track cleanup fixture");
    let cache = tempfile::tempdir().unwrap();
    let first = capture(&repo, cache.path(), &settings(&[], &[]));
    let second = capture(&repo, cache.path(), &settings(&[], &[]));
    let first_container = first.root.parent().unwrap().to_path_buf();
    let second_container = second.root.parent().unwrap().to_path_buf();

    first.cleanup().unwrap();

    assert!(!first_container.exists());
    assert!(second_container.exists());
    assert_eq!(
        fs::read(second.root.join("tracked.txt")).unwrap(),
        b"snapshot bytes\n"
    );
    second.cleanup().unwrap();
}

#[test]
fn staging_root_is_new_owner_only_and_contains_no_caller_payload() {
    // Catches reusing a caller-populated destination or creating a partial root
    // with group/world access before project bytes are materialized.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"input\n");
    repo.commit_all("track staging fixture");
    let cache = tempfile::tempdir().unwrap();
    let context = inspect(&repo);
    let settings = settings(&[], &[]);
    let initial = select(&context, &settings);
    let hook = RecordingHook::new(|tree: &Path| {
        let partial = tree.parent().unwrap();
        if mode(partial) != 0o700 {
            return Err(io::Error::other("partial root is not owner-only"));
        }
        let mut names = fs::read_dir(partial)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<io::Result<Vec<_>>>()?;
        names.sort();
        if names != ["tree"] {
            return Err(io::Error::other(format!(
                "partial root contains caller payload: {names:?}"
            )));
        }
        Ok(())
    });

    let snapshot = SnapshotBuilder::with_hook(&SystemProcessRunner, cache.path(), &hook)
        .capture(&context, &settings, initial)
        .unwrap();

    snapshot.cleanup().unwrap();
}

#[derive(Default)]
struct PostPublicationFailureHook {
    published: Mutex<Option<PathBuf>>,
}

impl SnapshotHook for PostPublicationFailureHook {
    fn after_materialization(&self, _tree: &Path) -> io::Result<()> {
        Ok(())
    }

    fn after_publication(&self, capture: &Path) -> io::Result<()> {
        *self.published.lock().unwrap() = Some(capture.to_path_buf());
        Err(io::Error::from_raw_os_error(libc::EMFILE))
    }
}

#[test]
fn post_rename_failure_removes_only_the_rebound_owned_capture() {
    // Catches returning an error after publication without retaining an
    // identity-bound cleanup handle for the directory at its new name.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"published bytes\n");
    repo.commit_all("track publication failure fixture");
    let cache = tempfile::tempdir().unwrap();
    let sibling = capture(&repo, cache.path(), &settings(&[], &[]));
    let sibling_container = sibling.root.parent().unwrap().to_path_buf();
    let context = inspect(&repo);
    let settings = settings(&[], &[]);
    let initial = select(&context, &settings);
    let hook = PostPublicationFailureHook::default();

    let error = SnapshotBuilder::with_hook(&SystemProcessRunner, cache.path(), &hook)
        .capture(&context, &settings, initial)
        .expect_err("post-publication failure must not return a snapshot");

    match error {
        WorkerError::Io(error) => assert_eq!(error.raw_os_error(), Some(libc::EMFILE)),
        other => panic!("post-publication failure had wrong classification: {other}"),
    }
    let failed_capture = hook.published.lock().unwrap().clone().unwrap();
    assert!(!failed_capture.exists());
    assert!(sibling_container.exists());
    assert_eq!(
        fs::read(sibling.root.join("tracked.txt")).unwrap(),
        b"published bytes\n"
    );

    sibling.cleanup().unwrap();
}

#[test]
fn preexisting_publication_is_preserved_and_never_reused() {
    // Catches rename-overwrite publication or treating an existing capture ID
    // as a cache hit without proving its contents.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"input\n");
    repo.commit_all("track collision fixture");
    let cache = tempfile::tempdir().unwrap();
    let context = inspect(&repo);
    let settings = settings(&[], &[]);
    let initial = select(&context, &settings);
    let marker = Mutex::new(None::<PathBuf>);
    let hook = RecordingHook::new(|tree: &Path| {
        let partial = tree.parent().unwrap();
        let capture_name = partial.file_name().unwrap().to_str().unwrap();
        let capture_id = capture_name.strip_prefix(".partial-").unwrap();
        let snapshots = partial.parent().unwrap().parent().unwrap();
        let ready_capture = snapshots.join("ready").join(capture_id);
        fs::create_dir(&ready_capture)?;
        let path = ready_capture.join("marker");
        fs::write(&path, b"preexisting\n")?;
        *marker.lock().unwrap() = Some(path);
        Ok(())
    });

    let error = SnapshotBuilder::with_hook(&SystemProcessRunner, cache.path(), &hook)
        .capture(&context, &settings, initial)
        .expect_err("publication collision must fail");

    assert!(matches!(
        error,
        WorkerError::Snapshot {
            code: "SNAPSHOT_PUBLICATION_CONFLICT",
            ..
        }
    ));
    let marker = marker.lock().unwrap().clone().unwrap();
    assert_eq!(fs::read(marker).unwrap(), b"preexisting\n");
    assert!(
        !cache
            .path()
            .join("snapshots/staging")
            .join(format!(".partial-{}", hook.capture_id()))
            .exists()
    );
}

#[test]
fn cleanup_failure_is_an_io_error_and_never_a_successful_capture() {
    // Catches swallowing exact-owned cleanup failures after a hook error and
    // incorrectly reporting a usable snapshot.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"input\n");
    repo.commit_all("track cleanup failure fixture");
    let cache = tempfile::tempdir().unwrap();
    let context = inspect(&repo);
    let settings = settings(&[], &[]);
    let initial = select(&context, &settings);
    let moved = Mutex::new(None::<(PathBuf, u64, Vec<u8>)>);
    let replaced = Mutex::new(None::<(PathBuf, u64, Vec<u8>)>);
    let hook = RecordingHook::new(|tree: &Path| {
        let partial = tree.parent().unwrap();
        let staging = partial.parent().unwrap();
        let moved_path = staging.join("moved-owned-capture");
        fs::rename(partial, &moved_path)?;
        symlink(&moved_path, partial)?;
        let moved_meta = fs::symlink_metadata(&moved_path)?;
        let moved_bytes = fs::read(moved_path.join("tree/tracked.txt"))?;
        *moved.lock().unwrap() = Some((moved_path, moved_meta.ino(), moved_bytes));
        let replaced_meta = fs::symlink_metadata(partial)?;
        let replaced_bytes = fs::read_link(partial)?.as_os_str().as_bytes().to_vec();
        *replaced.lock().unwrap() =
            Some((partial.to_path_buf(), replaced_meta.ino(), replaced_bytes));
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected hook failure",
        ))
    });

    let error = SnapshotBuilder::with_hook(&SystemProcessRunner, cache.path(), &hook)
        .capture(&context, &settings, initial)
        .expect_err("failed owned cleanup must not return a snapshot");

    match error {
        WorkerError::Io(error) => assert!(
            matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ESTALE)),
            "cleanup failure errno: {error:?}"
        ),
        other => panic!("cleanup failure had wrong classification: {other}"),
    }
    assert!(
        !cache
            .path()
            .join("snapshots/ready")
            .join(hook.capture_id())
            .exists()
    );

    let (replaced_path, replaced_ino, replaced_bytes) = replaced.into_inner().unwrap().unwrap();
    let (moved_path, moved_ino, moved_bytes) = moved.into_inner().unwrap().unwrap();
    assert_eq!(
        fs::symlink_metadata(&replaced_path).unwrap().ino(),
        replaced_ino,
        "substituted symlink inode mutated before manual cleanup"
    );
    assert_eq!(
        fs::read_link(&replaced_path)
            .unwrap()
            .as_os_str()
            .as_bytes(),
        replaced_bytes.as_slice(),
        "substituted symlink target mutated before manual cleanup"
    );
    assert_eq!(
        fs::symlink_metadata(&moved_path).unwrap().ino(),
        moved_ino,
        "relocated capture inode mutated before manual cleanup"
    );
    assert_eq!(
        fs::read(moved_path.join("tree/tracked.txt")).unwrap(),
        moved_bytes,
        "relocated capture bytes mutated before manual cleanup"
    );

    fs::remove_file(&replaced_path).unwrap();
    fs::remove_dir_all(&moved_path).unwrap();
}

#[test]
fn cache_path_components_are_created_without_following_symlinks() {
    // Catches create_dir_all following a caller-planted cache component and
    // writing snapshot state outside the configured cache root.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"input\n");
    repo.commit_all("track cache containment fixture");
    let fixture = tempfile::tempdir().unwrap();
    let outside = create_directory(fixture.path().join("outside"));
    let linked_cache = fixture.path().join("linked-cache");
    symlink(&outside, &linked_cache).unwrap();
    let context = inspect(&repo);
    let settings = settings(&[], &[]);
    let initial = select(&context, &settings);

    let error = SnapshotBuilder::new(&SystemProcessRunner, &linked_cache)
        .capture(&context, &settings, initial)
        .expect_err("cache symlink must be rejected");

    assert!(matches!(error, WorkerError::Io(_)));
    assert!(!outside.join("snapshots").exists());
}

#[test]
fn summary_counts_selected_inputs_without_exposing_capture_state() {
    // Catches losing selection-origin, deletion, or warning counts before the
    // doctor layer consumes the immutable summary.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"tracked\n");
    repo.write("deleted.txt", b"deleted later\n");
    repo.commit_all("track summary fixtures");
    fs::remove_file(repo.root().join("deleted.txt")).unwrap();
    repo.write("included/local.txt", b"local\n");
    create_directory(repo.root().join("declared/empty"));
    let cache = tempfile::tempdir().unwrap();

    let snapshot = capture(
        &repo,
        cache.path(),
        &settings(&["included/*.txt"], &["declared/empty"]),
    );
    let summary = snapshot.summary();

    assert_eq!(summary.digest, snapshot.digest);
    assert_eq!(summary.file_count, 2);
    assert_eq!(
        summary.total_bytes,
        b"tracked\n".len() as u64 + b"local\n".len() as u64
    );
    assert_eq!(summary.tracked_deletion_count, 1);
    assert_eq!(summary.included_untracked_count, 2);
    assert_eq!(summary.warning_count, 0);

    snapshot.cleanup().unwrap();
}

#[test]
fn selection_origin_changes_are_compared_exactly() {
    // Catches reducing the second-pass check to a path-only set comparison.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"tracked\n");
    repo.commit_all("track origin fixture");
    let cache = tempfile::tempdir().unwrap();
    let context = inspect(&repo);
    let settings = settings(&[], &[]);
    let mut initial = select(&context, &settings);
    initial.entries[0].origin = InputOrigin::IncludedUntracked;

    let error = SnapshotBuilder::new(&SystemProcessRunner, cache.path())
        .capture(&context, &settings, initial)
        .expect_err("origin mismatch must reject capture");

    assert!(matches!(
        error,
        WorkerError::Snapshot {
            code: "SNAPSHOT_CHANGED",
            ..
        }
    ));
}

struct CaptureCleanupFault {
    capture_id: Mutex<Option<String>>,
    repo_root: PathBuf,
}

impl SnapshotHook for CaptureCleanupFault {
    fn after_materialization(&self, tree: &Path) -> io::Result<()> {
        let partial_name = tree
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("missing partial capture path"))?;
        let capture_id = partial_name
            .strip_prefix(".partial-")
            .ok_or_else(|| io::Error::other("invalid partial capture path"))?;
        *self.capture_id.lock().unwrap() = Some(capture_id.to_owned());
        mutate_file_bytes(&self.repo_root)
    }

    fn after_owned_cleanup_commit(&self) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::EIO))
    }
}

fn assert_cleanup_decision_is_delete(path: &Path) {
    let metadata = fs::symlink_metadata(path).unwrap();
    assert!(
        metadata.file_type().is_file(),
        "cleanup decision must be a regular file: {}",
        path.display()
    );
    let value: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap())
        .expect("cleanup decision must be valid JSON");
    assert!(
        value.is_object(),
        "cleanup decision must be a JSON object: {value:?}"
    );
    assert_eq!(
        value.get("decision"),
        Some(&serde_json::json!("delete")),
        "cleanup decision must be Delete: {value}"
    );
}

fn assert_canonical_tree_delete_journal(namespace: &Path) {
    assert!(
        namespace.is_dir(),
        "expected tree journal {}",
        namespace.display()
    );
    let mut intent = 0usize;
    let mut decision = 0usize;
    let mut operation = 0usize;
    let mut quarantine = 0usize;
    let mut decision_path = None;
    for entry in fs::read_dir(namespace).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let name = entry.file_name();
        let bytes = name.as_bytes();
        if bytes.starts_with(b"cleanup-intent-v1-") {
            intent += 1;
        } else if bytes.starts_with(b"cleanup-decision-v1-") {
            decision += 1;
            decision_path = Some(path);
        } else if bytes.starts_with(b"cleanup-op-v1-") {
            operation += 1;
        } else if bytes.starts_with(b"cleanup-tree-v1-") {
            quarantine += 1;
        }
    }
    assert_eq!(
        (intent, decision, operation, quarantine),
        (1, 1, 1, 1),
        "unexpected snapshot tree journal under {}",
        namespace.display()
    );
    assert_cleanup_decision_is_delete(&decision_path.expect("decision role path"));
}

#[test]
fn capture_failure_after_delete_leaves_partial_journal_and_scan_converges() {
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"original bytes\n");
    repo.commit_all("track cleanup interrupt fixture");
    let cache = tempfile::tempdir().unwrap();
    let staging = cache.path().join("snapshots/staging");
    create_directory(staging.join(STAGING_SIBLING));
    fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        staging.join(STAGING_SIBLING).join("sentinel"),
        b"keep sibling\n",
    )
    .unwrap();
    let context = inspect(&repo);
    let settings = settings(&[], &[]);
    let initial = select(&context, &settings);
    let hook = CaptureCleanupFault {
        capture_id: Mutex::new(None),
        repo_root: repo.root().to_path_buf(),
    };

    let error = SnapshotBuilder::with_hook(&SystemProcessRunner, cache.path(), &hook)
        .capture(&context, &settings, initial)
        .expect_err("interrupted owned cleanup must not return a snapshot");
    match error {
        WorkerError::Io(error) => assert_eq!(error.raw_os_error(), Some(libc::EIO)),
        other => panic!("cleanup failure had wrong classification: {other}"),
    }
    let capture_id = hook.capture_id.lock().unwrap().clone().unwrap();
    assert!(!staging.join(format!(".partial-{capture_id}")).exists());
    assert_canonical_tree_delete_journal(&staging.join(".mac-worker-rooted-fs"));
    assert_eq!(
        fs::read(staging.join(STAGING_SIBLING).join("sentinel")).unwrap(),
        b"keep sibling\n"
    );

    let retried = SnapshotBuilder::new(&SystemProcessRunner, cache.path())
        .capture(&context, &settings, select(&context, &settings))
        .unwrap();
    retried.cleanup().unwrap();
    let namespace = staging.join(".mac-worker-rooted-fs");
    if namespace.exists() {
        assert!(
            fs::read_dir(&namespace).unwrap().next().is_none(),
            "staging cleanup journal remained"
        );
    }
    assert_eq!(
        fs::read(staging.join(STAGING_SIBLING).join("sentinel")).unwrap(),
        b"keep sibling\n"
    );
    assert!(
        fs::read_dir(&staging)
            .unwrap()
            .filter(|entry| {
                let name = entry.as_ref().unwrap().file_name();
                name != STAGING_SIBLING && name != ".mac-worker-rooted-fs"
            })
            .next()
            .is_none(),
        "owned partial capture remained after retry"
    );
}

#[test]
fn ready_cleanup_after_delete_does_not_delete_sibling_ready_uuid() {
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"snapshot bytes\n");
    repo.commit_all("track ready cleanup fixture");
    let cache = tempfile::tempdir().unwrap();
    let first = capture(&repo, cache.path(), &settings(&[], &[]));
    let second = capture(&repo, cache.path(), &settings(&[], &[]));
    let first_container = first.root.parent().unwrap().to_path_buf();
    let second_container = second.root.parent().unwrap().to_path_buf();
    let second_identity = (
        fs::symlink_metadata(&second_container).unwrap().ino(),
        fs::read(second.root.join("tracked.txt")).unwrap(),
    );

    let error = first
        .cleanup_with_hook(&|| Err(io::Error::from_raw_os_error(libc::EIO)))
        .unwrap_err();
    match error {
        WorkerError::Io(error) => assert_eq!(error.raw_os_error(), Some(libc::EIO)),
        other => panic!("ready cleanup failure had wrong classification: {other}"),
    }
    assert!(!first_container.exists());
    assert_canonical_tree_delete_journal(
        &cache.path().join("snapshots/ready/.mac-worker-rooted-fs"),
    );
    assert!(second_container.exists());
    assert_eq!(
        (
            fs::symlink_metadata(&second_container).unwrap().ino(),
            fs::read(second.root.join("tracked.txt")).unwrap(),
        ),
        second_identity
    );

    let resumed = capture(&repo, cache.path(), &settings(&[], &[]));
    assert!(!first_container.exists());
    assert!(second_container.exists());
    assert_eq!(
        (
            fs::symlink_metadata(&second_container).unwrap().ino(),
            fs::read(second.root.join("tracked.txt")).unwrap(),
        ),
        second_identity
    );
    resumed.cleanup().unwrap();
    second.cleanup().unwrap();
    assert_snapshot_capture_roots_empty(cache.path());
}

#[derive(Clone, Copy, Debug)]
enum StartupScope {
    Staging,
    Ready,
}

#[derive(Clone, Copy, Debug)]
enum StartupResidueKind {
    LegacyCleanupUuid,
    DirectRemoveUuid,
    WrongModeNamespace,
}

fn file_inode_and_bytes(path: &Path) -> (u64, Vec<u8>) {
    (
        fs::symlink_metadata(path).unwrap().ino(),
        fs::read(path).unwrap(),
    )
}

fn assert_startup_scan_fail_closed(scope: StartupScope, kind: StartupResidueKind) {
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"input\n");
    repo.commit_all("track residue fixture");
    let cache = tempfile::tempdir().unwrap();
    let staging = cache.path().join("snapshots/staging");
    let ready = cache.path().join("snapshots/ready");
    let parent = match scope {
        StartupScope::Staging => &staging,
        StartupScope::Ready => &ready,
    };
    create_directory(parent);
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).unwrap();

    let sibling_sentinel = match scope {
        StartupScope::Staging => {
            create_directory(parent.join(STAGING_SIBLING));
            let sentinel = parent.join(STAGING_SIBLING).join("sentinel");
            fs::write(&sentinel, b"keep sibling\n").unwrap();
            sentinel
        }
        StartupScope::Ready => {
            let sibling = parent.join("22222222-2222-4222-8222-222222222222");
            create_directory(&sibling);
            fs::set_permissions(&sibling, fs::Permissions::from_mode(0o700)).unwrap();
            let sentinel = sibling.join("sentinel");
            fs::write(&sentinel, b"keep ready sibling\n").unwrap();
            sentinel
        }
    };
    let sibling_identity = file_inode_and_bytes(&sibling_sentinel);

    let residue_path = match kind {
        StartupResidueKind::LegacyCleanupUuid => {
            let namespace = parent.join(".mac-worker-rooted-fs");
            fs::create_dir_all(&namespace).unwrap();
            fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700)).unwrap();
            let leftover = namespace.join("cleanup-11111111-1111-4111-8111-111111111111");
            fs::create_dir(&leftover).unwrap();
            fs::set_permissions(&leftover, fs::Permissions::from_mode(0o700)).unwrap();
            let sentinel = leftover.join("sentinel");
            fs::write(&sentinel, b"legacy-unbound").unwrap();
            sentinel
        }
        StartupResidueKind::DirectRemoveUuid => {
            let residue = parent.join("remove-303f0f4a-6b5c-4d8e-9f00-112233445566");
            fs::write(&residue, b"direct-remove-residue").unwrap();
            residue
        }
        StartupResidueKind::WrongModeNamespace => {
            let namespace = parent.join(".mac-worker-rooted-fs");
            fs::create_dir_all(&namespace).unwrap();
            fs::set_permissions(&namespace, fs::Permissions::from_mode(0o755)).unwrap();
            namespace
        }
    };
    let residue_meta = fs::symlink_metadata(&residue_path).unwrap();
    let residue_ino = residue_meta.ino();
    let residue_mode = residue_meta.permissions().mode() & 0o777;
    let residue_bytes = residue_meta
        .file_type()
        .is_file()
        .then(|| fs::read(&residue_path).unwrap());

    let context = inspect(&repo);
    let settings = settings(&[], &[]);
    let initial = select(&context, &settings);
    let error = SnapshotBuilder::new(&SystemProcessRunner, cache.path())
        .capture(&context, &settings, initial)
        .expect_err("startup residue must fail closed");
    assert!(matches!(error, WorkerError::Io(_)), "{error}");
    assert_eq!(
        file_inode_and_bytes(&sibling_sentinel),
        sibling_identity,
        "sibling inode/bytes mutated for {scope:?}/{kind:?}"
    );
    let after = fs::symlink_metadata(&residue_path).unwrap();
    assert_eq!(after.ino(), residue_ino, "residue inode mutated");
    assert_eq!(after.permissions().mode() & 0o777, residue_mode);
    if let Some(bytes) = residue_bytes {
        assert_eq!(fs::read(&residue_path).unwrap(), bytes);
    }
    if matches!(scope, StartupScope::Staging) {
        assert!(
            !ready.exists()
                || fs::read_dir(&ready)
                    .unwrap()
                    .filter(|entry| entry.as_ref().unwrap().file_name() != ".mac-worker-rooted-fs")
                    .next()
                    .is_none()
        );
    }
}

#[test]
fn startup_scan_fail_closed_on_legacy_residue() {
    for (scope, kind) in [
        (StartupScope::Staging, StartupResidueKind::LegacyCleanupUuid),
        (StartupScope::Ready, StartupResidueKind::LegacyCleanupUuid),
        (StartupScope::Staging, StartupResidueKind::DirectRemoveUuid),
        (StartupScope::Ready, StartupResidueKind::DirectRemoveUuid),
        (
            StartupScope::Staging,
            StartupResidueKind::WrongModeNamespace,
        ),
        (StartupScope::Ready, StartupResidueKind::WrongModeNamespace),
    ] {
        assert_startup_scan_fail_closed(scope, kind);
    }
}
