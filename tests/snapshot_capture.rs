mod support;

use std::{
    fs, io,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    sync::Mutex,
};

use mac_worker::{
    error::WorkerError,
    inputs::{InputOrigin, InputSelection, InputSelector},
    manifest::{ManifestEntry, ManifestEntryKind, SnapshotManifest},
    process::SystemProcessRunner,
    project::{ProjectContext, ProjectInspector},
    project_config::SnapshotSettings,
    snapshot::{SnapshotBuilder, SnapshotHook},
};

use support::{GitRepo, create_directory};

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

fn capture(
    repo: &GitRepo,
    cache_root: &Path,
    settings: &SnapshotSettings,
) -> mac_worker::snapshot::Snapshot {
    let context = inspect(repo);
    let initial = select(&context, settings);
    SnapshotBuilder::new(&SystemProcessRunner, cache_root)
        .capture(&context, settings, initial)
        .expect("capture verified snapshot")
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
    create_directory(staging.join(".partial-sibling"));
    fs::write(staging.join(".partial-sibling/sentinel"), b"keep sibling\n").unwrap();
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
        fs::read(staging.join(".partial-sibling/sentinel")).unwrap(),
        b"keep sibling\n",
        "{label}: cleanup crossed its owned boundary"
    );
}

#[test]
fn every_enumerated_source_mutation_is_rejected_without_publication() {
    // Each case catches a distinct way a second-pass check could compare only
    // file content while missing selection or metadata drift.
    let cases: [(&str, Mutation); 8] = [
        ("file bytes", mutate_file_bytes),
        ("file mode", mutate_file_mode),
        ("entry type", mutate_file_type),
        ("symlink target", mutate_symlink_target),
        ("tracked deletion", mutate_tracked_deletion),
        ("new uncovered untracked", mutate_new_uncovered_untracked),
        ("removed selected path", mutate_removed_path),
        ("included path set", mutate_included_path_set),
    ];

    for (label, mutation) in cases {
        assert_snapshot_changed(label, mutation);
    }
}

#[test]
fn capture_preserves_binary_and_unusual_names_without_git_or_inline_manifest() {
    // Catches text decoding, newline splitting, Git metadata inclusion, or
    // treating the manifest as another project input.
    let repo = GitRepo::init();
    let unusual = "fixtures/привет\nданные.bin";
    let binary = b"\0\xffbinary\n\0";
    repo.write(unusual, binary);
    repo.write("bin/tool", b"#!/bin/sh\nexit 0\n");
    fs::set_permissions(
        repo.root().join("bin/tool"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    symlink("../bin/tool\n", repo.root().join("tool-link")).unwrap();
    repo.commit_all("track unusual snapshot inputs");
    let cache = tempfile::tempdir().unwrap();

    let snapshot = capture(&repo, cache.path(), &settings(&[], &[]));

    assert_eq!(fs::read(snapshot.root.join(unusual)).unwrap(), binary);
    assert_eq!(
        fs::read_link(snapshot.root.join("tool-link")).unwrap(),
        PathBuf::from("../bin/tool\n")
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
    assert_eq!(snapshot.file_count, 3);
    assert_eq!(
        snapshot.total_bytes,
        binary.len() as u64 + b"#!/bin/sh\nexit 0\n".len() as u64 + b"../bin/tool\n".len() as u64
    );

    snapshot.cleanup().unwrap();
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
    let moved = Mutex::new(None::<PathBuf>);
    let replaced = Mutex::new(None::<PathBuf>);
    let hook = RecordingHook::new(|tree: &Path| {
        let partial = tree.parent().unwrap();
        let staging = partial.parent().unwrap();
        let moved_path = staging.join("moved-owned-capture");
        fs::rename(partial, &moved_path)?;
        symlink(&moved_path, partial)?;
        *moved.lock().unwrap() = Some(moved_path);
        *replaced.lock().unwrap() = Some(partial.to_path_buf());
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected hook failure",
        ))
    });

    let error = SnapshotBuilder::with_hook(&SystemProcessRunner, cache.path(), &hook)
        .capture(&context, &settings, initial)
        .expect_err("failed owned cleanup must not return a snapshot");

    match error {
        WorkerError::Io(error) => assert_eq!(error.raw_os_error(), Some(libc::ELOOP)),
        other => panic!("cleanup failure had wrong classification: {other}"),
    }
    assert!(
        !cache
            .path()
            .join("snapshots/ready")
            .join(hook.capture_id())
            .exists()
    );

    fs::remove_file(replaced.into_inner().unwrap().unwrap()).unwrap();
    fs::remove_dir_all(moved.into_inner().unwrap().unwrap()).unwrap();
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
