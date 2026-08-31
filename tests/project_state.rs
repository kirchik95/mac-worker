#[allow(dead_code)]
mod support;

use std::{
    ffi::OsStr,
    fs, io,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

use mac_worker::{
    error::WorkerError,
    manifest::ManifestEntryKind,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::{ProjectPreparationError, ProjectPreparationRequest, ProjectState},
};

use support::{GitRepo, create_directory};

fn request(project: &Path) -> ProjectPreparationRequest {
    ProjectPreparationRequest {
        project: project.to_path_buf(),
        cli_includes: Vec::new(),
    }
}

fn assert_no_owned_capture(cache: &Path) {
    for directory in [
        cache.join("snapshots/staging"),
        cache.join("snapshots/ready"),
    ] {
        if !directory.exists() {
            continue;
        }
        for entry in fs::read_dir(&directory).unwrap() {
            let entry = entry.unwrap();
            assert_eq!(
                entry.file_name(),
                ".mac-worker-rooted-fs",
                "owned capture leaked in {}",
                directory.display()
            );
            assert!(fs::read_dir(entry.path()).unwrap().next().is_none());
        }
    }
}

#[test]
fn stable_project_preparation_returns_the_probed_state_and_owned_publication() {
    // Catches preparation returning a newly interpreted state or exposing a
    // caller-derived cache parent instead of the exact ready capture.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\nrequires = [\"docker\"]\n[artifacts]\ninclude = [\"target/**\"]\nmax_total_bytes = 4096\n",
    );
    repo.write("README.md", b"tracked\n");
    repo.commit_all("stable preparation fixture");
    let state = tempfile::tempdir().unwrap();
    let cache = state.path().join("cache");
    let runner = SystemProcessRunner;

    let before = ProjectState::load(&runner, repo.root(), &[]).unwrap();
    let prepared = ProjectState::prepare(&runner, &cache, request(repo.root()), &before).unwrap();

    assert_eq!(prepared.state, before);
    let publication = prepared.snapshot.publication_root().to_path_buf();
    assert!(publication.is_dir());
    assert_eq!(prepared.snapshot.root, publication.join("tree"));
    assert_eq!(
        prepared.snapshot.manifest_path,
        publication.join("manifest.json")
    );
    let mut names = fs::read_dir(&publication)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["manifest.json", "tree"]);

    prepared.cleanup().unwrap();
    assert!(!publication.exists());
    assert_no_owned_capture(&cache);
}

#[test]
fn worktree_root_uses_the_publication_tree_as_its_implicit_working_directory() {
    // Catches inventing an empty-path manifest entry or trying to materialize
    // a child for the worktree-root working directory.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("root working directory fixture");
    let cache = tempfile::tempdir().unwrap();

    let before = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let prepared = ProjectState::prepare(
        &SystemProcessRunner,
        cache.path(),
        request(repo.root()),
        &before,
    )
    .unwrap();

    assert_eq!(prepared.snapshot.manifest.relative_working_dir, "");
    assert!(prepared.snapshot.root.is_dir());
    assert!(
        prepared
            .snapshot
            .manifest
            .entries
            .iter()
            .all(|entry| !entry.path.is_empty())
    );
    prepared.cleanup().unwrap();
}

#[test]
fn nested_empty_working_directory_is_manifested_and_materialized() {
    // Catches dropping a non-empty working directory when no selected input
    // descends from it, which would make a later worker unable to chdir there.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked elsewhere\n");
    repo.commit_all("nested working directory fixture");
    let nested = create_directory(repo.root().join("crates/empty-app"));
    let cache = tempfile::tempdir().unwrap();

    let before = ProjectState::load(&SystemProcessRunner, &nested, &[]).unwrap();
    let prepared = ProjectState::prepare(
        &SystemProcessRunner,
        cache.path(),
        request(&nested),
        &before,
    )
    .unwrap();

    assert_eq!(
        prepared.snapshot.manifest.relative_working_dir,
        "crates/empty-app"
    );
    let working_entry = prepared
        .snapshot
        .manifest
        .entries
        .iter()
        .find(|entry| entry.path == "crates/empty-app")
        .expect("nested working directory must be in the manifest");
    assert_eq!(working_entry.kind, ManifestEntryKind::Directory);
    assert!(prepared.snapshot.root.join("crates/empty-app").is_dir());
    prepared.cleanup().unwrap();
}

#[test]
fn artifact_settings_drift_alone_blocks_before_selection_or_cache_access() {
    // Catches omitting effective artifact settings from state equality while
    // Git context and requirements remain exactly unchanged.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\n[artifacts]\ninclude = [\"baseline/**\"]\nmax_total_bytes = 1024\n",
    );
    repo.write("README.md", b"tracked\n");
    repo.write("dirty.txt", b"committed\n");
    repo.commit_all("artifact policy A fixture");
    repo.write("dirty.txt", b"already dirty\n");
    let before = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();

    repo.write(
        ".worker.toml",
        b"version = 1\n[artifacts]\ninclude = [\"changed/**\"]\nmax_total_bytes = 2048\n",
    );
    let after = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();

    assert!(before.context.dirty);
    assert_eq!(before.context, after.context);
    assert_eq!(before.requirements, after.requirements);
    assert_eq!(before.settings.requires, after.settings.requires);
    assert_eq!(
        before.settings.resource_class,
        after.settings.resource_class
    );
    assert_eq!(before.settings.timeout, after.settings.timeout);
    assert_eq!(before.settings.snapshot, after.settings.snapshot);
    assert_eq!(before.settings.artifacts.include, ["baseline/**"]);
    assert_eq!(before.settings.artifacts.max_total_bytes, Some(1024));
    assert_eq!(after.settings.artifacts.include, ["changed/**"]);
    assert_eq!(after.settings.artifacts.max_total_bytes, Some(2048));

    let state = tempfile::tempdir().unwrap();
    let cache = state.path().join("poison-cache");
    fs::write(&cache, b"must remain an unopened regular file\n").unwrap();
    let runner = SelectionCountingRunner::default();

    let error = ProjectState::prepare(&runner, &cache, request(repo.root()), &before)
        .expect_err("artifact settings drift must block preparation");

    assert_snapshot_changed(error);
    assert_eq!(runner.selection_queries.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::read(&cache).unwrap(),
        b"must remain an unopened regular file\n"
    );
}

#[test]
fn detected_requirements_drift_alone_blocks_before_selection_or_cache_access() {
    // Catches omitting detected requirements from state equality while Git
    // context and effective settings remain exactly unchanged.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.write("dirty.txt", b"committed\n");
    repo.commit_all("requirements A fixture");
    repo.write("dirty.txt", b"already dirty\n");
    let before = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();

    repo.write("package.json", b"{}\n");
    let after = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();

    assert!(before.context.dirty);
    assert_eq!(before.context, after.context);
    assert_eq!(before.settings, after.settings);
    assert!(before.requirements.is_empty());
    assert_eq!(after.requirements, ["node"]);

    let state = tempfile::tempdir().unwrap();
    let cache = state.path().join("poison-cache");
    fs::write(&cache, b"must remain an unopened regular file\n").unwrap();
    let runner = SelectionCountingRunner::default();

    let error = ProjectState::prepare(&runner, &cache, request(repo.root()), &before)
        .expect_err("detected requirements drift must block preparation");

    assert_snapshot_changed(error);
    assert_eq!(runner.selection_queries.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::read(&cache).unwrap(),
        b"must remain an unopened regular file\n"
    );
}

#[test]
fn head_drift_blocks_before_selection_or_cache_access() {
    // Catches omitting HEAD from context equality while the already-dirty bit,
    // branch, settings, and requirements remain unchanged.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.write("dirty.txt", b"committed\n");
    repo.commit_all("HEAD A fixture");
    repo.write("dirty.txt", b"already dirty\n");
    let before = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();

    assert!(
        repo.git(&["commit", "--allow-empty", "-m", "HEAD B fixture"])
            .status
            .success()
    );
    let after = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();

    assert!(before.context.dirty);
    assert!(after.context.dirty);
    assert_ne!(before.context.head, after.context.head);
    let mut normalized_after = after.context.clone();
    normalized_after.head = before.context.head.clone();
    assert_eq!(before.context, normalized_after);
    assert_eq!(before.settings, after.settings);
    assert_eq!(before.requirements, after.requirements);

    let state = tempfile::tempdir().unwrap();
    let cache = state.path().join("poison-cache");
    fs::write(&cache, b"must remain an unopened regular file\n").unwrap();
    let runner = SelectionCountingRunner::default();

    let error = ProjectState::prepare(&runner, &cache, request(repo.root()), &before)
        .expect_err("HEAD drift must block preparation");

    assert_snapshot_changed(error);
    assert_eq!(runner.selection_queries.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::read(&cache).unwrap(),
        b"must remain an unopened regular file\n"
    );
}

fn assert_snapshot_changed(error: ProjectPreparationError) {
    assert!(matches!(
        error,
        ProjectPreparationError::Worker {
            error: WorkerError::Snapshot {
                code: "SNAPSHOT_CHANGED",
                ..
            },
            ..
        }
    ));
}

#[derive(Default)]
struct SelectionCountingRunner {
    selection_queries: AtomicUsize,
}

impl ProcessRunner for SelectionCountingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git")
            && request.args.iter().any(|argument| argument == "ls-files")
        {
            self.selection_queries.fetch_add(1, Ordering::SeqCst);
        }
        SystemProcessRunner.run(request)
    }
}

enum ReloadFailure {
    BeforeSelection,
    AfterCapture,
    AfterCaptureWithCleanupSabotage { cache: PathBuf },
}

struct PostCaptureSettingsMutationRunner {
    root: PathBuf,
    inspections: AtomicUsize,
}

enum WorkingDirectoryReplacement {
    File,
    Symlink(PathBuf),
}

struct WorkingDirectoryMutationRunner {
    working_directory: PathBuf,
    replacement: WorkingDirectoryReplacement,
    applied: AtomicUsize,
    inspections: AtomicUsize,
}

impl ProcessRunner for WorkingDirectoryMutationRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git")
            && request
                .args
                .iter()
                .any(|argument| argument == "--show-toplevel")
        {
            self.inspections.fetch_add(1, Ordering::SeqCst);
        }
        if request.program == OsStr::new("/usr/bin/git")
            && request.args.iter().any(|argument| argument == "check-attr")
            && self.applied.fetch_add(1, Ordering::SeqCst) == 0
        {
            let result = SystemProcessRunner.run(request)?;
            fs::remove_dir(&self.working_directory)?;
            match &self.replacement {
                WorkingDirectoryReplacement::File => {
                    fs::write(&self.working_directory, b"not a directory\n")?;
                }
                WorkingDirectoryReplacement::Symlink(target) => {
                    symlink(target, &self.working_directory)?;
                }
            }
            return Ok(result);
        }
        SystemProcessRunner.run(request)
    }
}

impl ProcessRunner for PostCaptureSettingsMutationRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git")
            && request
                .args
                .iter()
                .any(|argument| argument == "--show-toplevel")
            && self.inspections.fetch_add(1, Ordering::SeqCst) == 1
        {
            fs::write(
                self.root.join(".worker.toml"),
                b"version = 1\n[artifacts]\ninclude = [\"changed/**\"]\n",
            )?;
        }
        SystemProcessRunner.run(request)
    }
}

struct ReloadFailureRunner {
    mode: ReloadFailure,
    inspections: AtomicUsize,
    selection_queries: AtomicUsize,
}

impl ReloadFailureRunner {
    fn new(mode: ReloadFailure) -> Self {
        Self {
            mode,
            inspections: AtomicUsize::new(0),
            selection_queries: AtomicUsize::new(0),
        }
    }
}

impl ProcessRunner for ReloadFailureRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git")
            && request.args.iter().any(|argument| argument == "ls-files")
        {
            self.selection_queries.fetch_add(1, Ordering::SeqCst);
        }
        if request.program == OsStr::new("/usr/bin/git")
            && request
                .args
                .iter()
                .any(|argument| argument == "--show-toplevel")
        {
            let inspection = self.inspections.fetch_add(1, Ordering::SeqCst);
            let should_fail = match self.mode {
                ReloadFailure::BeforeSelection => inspection == 0,
                ReloadFailure::AfterCapture
                | ReloadFailure::AfterCaptureWithCleanupSabotage { .. } => inspection == 1,
            };
            if should_fail {
                if let ReloadFailure::AfterCaptureWithCleanupSabotage { ref cache } = self.mode {
                    let ready = cache.join("snapshots/ready");
                    let capture = fs::read_dir(&ready)
                        .unwrap()
                        .map(|entry| entry.unwrap().path())
                        .find(|path| path.file_name().unwrap() != ".mac-worker-rooted-fs")
                        .expect("capture must be published before the C reload");
                    let moved = cache.join("moved-owned-capture");
                    fs::rename(&capture, &moved).unwrap();
                    symlink(&moved, &capture).unwrap();
                }
                return Err(WorkerError::Io(io::Error::from_raw_os_error(libc::EMFILE)));
            }
        }
        SystemProcessRunner.run(request)
    }
}

#[test]
fn failed_b_reload_does_not_select_inputs_or_access_the_cache() {
    // Catches moving selection or cache mutation ahead of the authoritative B
    // reload. The double injects only that reload failure; Git stays real.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("failed B reload fixture");
    let before = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state = tempfile::tempdir().unwrap();
    let cache = state.path().join("poison-cache");
    fs::write(&cache, b"cache poison\n").unwrap();
    let runner = ReloadFailureRunner::new(ReloadFailure::BeforeSelection);

    let error = ProjectState::prepare(&runner, &cache, request(repo.root()), &before)
        .expect_err("failed B reload must abort preparation");

    assert!(matches!(
        error,
        ProjectPreparationError::Worker {
            error: WorkerError::Project {
                code: "GIT_INSPECTION_FAILED",
                ..
            },
            ..
        }
    ));
    assert_eq!(runner.selection_queries.load(Ordering::SeqCst), 0);
    assert_eq!(fs::read(&cache).unwrap(), b"cache poison\n");
}

#[test]
fn failed_c_reload_exactly_cleans_the_published_capture() {
    // Catches leaking an owned publication when C cannot be loaded after a
    // successful real capture.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("failed C reload fixture");
    let before = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let cache = tempfile::tempdir().unwrap();
    let runner = ReloadFailureRunner::new(ReloadFailure::AfterCapture);

    let error = ProjectState::prepare(&runner, cache.path(), request(repo.root()), &before)
        .expect_err("failed C reload must abort preparation");

    assert!(matches!(
        error,
        ProjectPreparationError::Worker {
            error: WorkerError::Project {
                code: "GIT_INSPECTION_FAILED",
                ..
            },
            ..
        }
    ));
    let after = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    assert_eq!(before.context, after.context);
    assert_eq!(before.settings, after.settings);
    assert_eq!(before.requirements, after.requirements);
    assert_no_owned_capture(cache.path());
}

#[test]
fn b_c_settings_mismatch_returns_snapshot_changed_after_exact_cleanup() {
    // Catches accepting a publication whose effective settings no longer
    // match the settings that selected and captured its bytes.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\n[artifacts]\ninclude = [\"baseline/**\"]\nmax_total_bytes = 1024\n",
    );
    repo.write("README.md", b"tracked\n");
    repo.write("dirty.txt", b"committed\n");
    repo.commit_all("B/C settings drift fixture");
    repo.write("dirty.txt", b"already dirty\n");
    let before = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let cache = tempfile::tempdir().unwrap();
    let runner = PostCaptureSettingsMutationRunner {
        root: repo.root().to_path_buf(),
        inspections: AtomicUsize::new(0),
    };

    let error = ProjectState::prepare(&runner, cache.path(), request(repo.root()), &before)
        .expect_err("B/C settings drift must discard the publication");

    assert!(matches!(
        error,
        ProjectPreparationError::Worker {
            error: WorkerError::Snapshot {
                code: "SNAPSHOT_CHANGED",
                ..
            },
            ..
        }
    ));
    let after = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    assert!(before.context.dirty);
    assert_eq!(before.context, after.context);
    assert_eq!(before.requirements, after.requirements);
    assert_eq!(before.settings.requires, after.settings.requires);
    assert_eq!(
        before.settings.resource_class,
        after.settings.resource_class
    );
    assert_eq!(before.settings.timeout, after.settings.timeout);
    assert_eq!(before.settings.snapshot, after.settings.snapshot);
    assert_eq!(before.settings.artifacts.include, ["baseline/**"]);
    assert_eq!(before.settings.artifacts.max_total_bytes, Some(1024));
    assert_eq!(after.settings.artifacts.include, ["changed/**"]);
    assert_eq!(after.settings.artifacts.max_total_bytes, None);
    assert_no_owned_capture(cache.path());
}

#[test]
fn working_directory_replaced_by_file_or_symlink_fails_before_c_reload() {
    // Catches materializing the requested working path from stale context
    // without first proving every source component is still a real directory.
    for replacement_kind in ["file", "symlink"] {
        let repo = GitRepo::init();
        repo.write("README.md", b"tracked\n");
        repo.commit_all("working directory replacement fixture");
        let nested = create_directory(repo.root().join("nested"));
        let sibling = create_directory(repo.root().join("sibling"));
        let before = ProjectState::load(&SystemProcessRunner, &nested, &[]).unwrap();
        let cache = tempfile::tempdir().unwrap();
        let runner = WorkingDirectoryMutationRunner {
            working_directory: nested.clone(),
            replacement: match replacement_kind {
                "file" => WorkingDirectoryReplacement::File,
                "symlink" => WorkingDirectoryReplacement::Symlink(sibling),
                _ => unreachable!(),
            },
            applied: AtomicUsize::new(0),
            inspections: AtomicUsize::new(0),
        };

        let error = ProjectState::prepare(&runner, cache.path(), request(&nested), &before)
            .expect_err("stale non-directory working path must fail closed");

        assert!(matches!(
            error,
            ProjectPreparationError::Worker {
                error: WorkerError::Snapshot {
                    code: "SNAPSHOT_CHANGED",
                    ..
                },
                ..
            }
        ));
        assert_eq!(
            runner.inspections.load(Ordering::SeqCst),
            1,
            "{replacement_kind} replacement reached the C reload"
        );
        assert_no_owned_capture(cache.path());
    }
}

#[test]
fn failed_c_reload_cleanup_io_error_is_authoritative_and_crosses_no_boundary() {
    // Catches returning the reload failure after exact cleanup itself fails,
    // or deleting through a caller-name replacement while handling it.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("failed C cleanup fixture");
    let before = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let cache = tempfile::tempdir().unwrap();
    let runner = ReloadFailureRunner::new(ReloadFailure::AfterCaptureWithCleanupSabotage {
        cache: cache.path().to_path_buf(),
    });

    let error = ProjectState::prepare(&runner, cache.path(), request(repo.root()), &before)
        .expect_err("cleanup failure must override the failed C reload");

    match error {
        ProjectPreparationError::Worker {
            error: WorkerError::Io(error),
            ..
        } => {
            assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        }
        other => panic!("cleanup failure had wrong classification: {other}"),
    }
    let after = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    assert_eq!(before.context, after.context);
    assert_eq!(before.settings, after.settings);
    assert_eq!(before.requirements, after.requirements);
    let moved = cache.path().join("moved-owned-capture");
    assert!(moved.join("tree/README.md").is_file());
    let ready = cache.path().join("snapshots/ready");
    let replacement = fs::read_dir(&ready)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.file_name().unwrap() != ".mac-worker-rooted-fs")
        .unwrap();
    assert!(
        fs::symlink_metadata(&replacement)
            .unwrap()
            .file_type()
            .is_symlink()
    );

    fs::remove_file(replacement).unwrap();
    fs::set_permissions(&moved, fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(moved.join("tree"), fs::Permissions::from_mode(0o700)).unwrap();
    fs::remove_dir_all(moved).unwrap();
}
