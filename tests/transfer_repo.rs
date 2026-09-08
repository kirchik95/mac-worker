mod support;

use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    time::Duration,
};

use mac_worker::{
    error::WorkerError,
    process::SystemProcessRunner,
    project::{ProjectContext, ProjectInspector},
    project_config::{
        ArtifactSettings, ProjectSettings, ResourceClass, SnapshotSettings, TaskSettings,
    },
    task::{BaseOid, GitIdentity, TaskId},
    transfer_repo::{BaseKind, RepositoryFingerprint, TransferRepo, repo_id_for},
};
use support::{GitRepo, create_directory, recording_runner::RecordingRunner};
use uuid::Uuid;

fn runner() -> SystemProcessRunner {
    SystemProcessRunner
}

fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(1))
}

fn identity() -> GitIdentity {
    GitIdentity::new("Ada Lovelace", "ada@example.test").expect("fixture identity")
}

fn settings() -> ProjectSettings {
    settings_including(&[])
}

fn settings_including(patterns: &[&str]) -> ProjectSettings {
    ProjectSettings {
        requires: Vec::new(),
        resource_class: ResourceClass::Heavy,
        timeout: Duration::from_secs(1_800),
        snapshot: SnapshotSettings {
            include_untracked: patterns
                .iter()
                .map(|pattern| (*pattern).to_owned())
                .collect(),
            include_empty_dirs: Vec::new(),
            allow_sensitive: Vec::new(),
        },
        artifacts: ArtifactSettings {
            include: Vec::new(),
            max_total_bytes: None,
        },
        task: TaskSettings {
            model: None,
            effort: None,
            source: "local".into(),
            publish: vec!["fetch".into()],
            env_profile: None,
            default_agent: "codex".into(),
            timeout: Duration::from_secs(45 * 60),
            max_followups: 10,
            permissions: std::collections::BTreeMap::new(),
        },
    }
}

struct Fixture {
    repo: GitRepo,
}

impl Fixture {
    fn init() -> Self {
        Self {
            repo: GitRepo::init(),
        }
    }

    fn path(&self) -> &Path {
        self.repo.root()
    }

    fn write(&self, path: &str, bytes: &[u8]) {
        self.repo.write(path, bytes);
    }

    fn git(&self, args: &[&str]) -> std::process::Output {
        self.repo.git(args)
    }

    fn commit_all(&self, message: &str) {
        self.repo.commit_all(message);
    }

    fn git_path(&self, relative: &str) -> PathBuf {
        self.path().join(".git").join(relative)
    }

    fn common_dir(&self) -> PathBuf {
        fs::canonicalize(self.path().join(".git")).expect("canonicalize common dir")
    }

    fn context(&self) -> ProjectContext {
        ProjectInspector::new(&SystemProcessRunner)
            .inspect(self.path())
            .expect("inspect fixture repository")
    }

    fn head(&self) -> BaseOid {
        self.rev_parse("HEAD")
    }

    fn rev_parse(&self, reference: &str) -> BaseOid {
        let output = self.git(&["rev-parse", "--verify", reference]);
        assert!(output.status.success(), "rev-parse {reference} failed");
        let oid = String::from_utf8(output.stdout)
            .expect("utf8 oid")
            .trim()
            .to_owned();
        oid.parse().expect("base oid")
    }

    fn worktree_bytes(&self, path: &str) -> Vec<u8> {
        fs::read(self.path().join(path)).expect("read worktree bytes")
    }

    fn has_ref(&self, name: &str) -> bool {
        self.git(&["show-ref", "--verify", "--quiet", name])
            .status
            .success()
    }
}

fn cache_root() -> tempfile::TempDir {
    tempfile::tempdir().expect("cache root")
}

fn repo_with_commits() -> Fixture {
    let repo = Fixture::init();
    repo.write("src/app.rs", b"fn main() {}\n");
    repo.write("README.md", b"hello\n");
    repo.commit_all("initial");
    repo
}

fn repo_with_dirty_worktree() -> Fixture {
    let repo = repo_with_commits();
    repo.write(".gitignore", b"target/\n");
    repo.write("deleted.rs", b"gone\n");
    repo.commit_all("ignore and deleted");
    repo.write("src/app.rs", b"fn main() { println!(\"dirty\"); }\n");
    repo.write("staged.txt", b"staged\n");
    assert!(repo.git(&["add", "staged.txt"]).status.success());
    fs::remove_file(repo.path().join("deleted.rs")).expect("delete tracked file");
    repo.write("fixtures/generated.txt", b"fixture\n");
    create_directory(repo.path().join("target"));
    repo.write("target/out.bin", b"ignored\n");
    repo
}

fn repo_with_branch(name: &str) -> Fixture {
    let repo = repo_with_commits();
    assert!(repo.git(&["checkout", "-b", name]).status.success());
    repo.write("feature.txt", b"on feature\n");
    repo.commit_all("feature commit");
    repo
}

fn repo_with_tracked(path: &str) -> Fixture {
    let repo = repo_with_commits();
    repo.write(path, b"secret=1\n");
    repo.commit_all("track secret");
    repo
}

fn repo_and_transfer_with_result(
    cache: &tempfile::TempDir,
    task: TaskId,
) -> (Fixture, TransferRepo) {
    let repo = repo_with_commits();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let output = git_in(
        transfer.path(),
        &[
            "update-ref",
            &format!("refs/mac-worker/results/{task}"),
            repo.head().as_str(),
        ],
    );
    assert!(output.status.success(), "plant result ref");
    (repo, transfer)
}

fn git_in(dir: &Path, args: &[&str]) -> std::process::Output {
    let mut command = std::process::Command::new("/usr/bin/git");
    command
        .args(["--git-dir"])
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_INDEX_FILE");
    command.output().expect("run git in transfer")
}

fn mutate_between_captures(repo: &Fixture) -> impl Fn() + '_ {
    move || repo.write("src/app.rs", b"fn main() { println!(\"mutated\"); }\n")
}

#[test]
fn committed_base_resolves_without_any_write_to_the_user_repository() {
    let repo = repo_with_commits();
    let before = RepositoryFingerprint::capture(repo.path()).unwrap();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    assert_eq!(transfer.repo_id(), repo_id_for(&repo.common_dir()).unwrap());
    let base = transfer
        .resolve_base(&runner(), &repo.context(), "HEAD")
        .unwrap();
    assert_eq!(base.kind(), BaseKind::Committed);
    assert_eq!(RepositoryFingerprint::capture(repo.path()).unwrap(), before);
    let alternates = fs::read_to_string(transfer.path().join("objects/info/alternates")).unwrap();
    assert!(alternates.trim().ends_with("objects"));
}

#[test]
fn wip_selection_honours_the_user_repository_configuration() {
    let repo = repo_with_commits();
    repo.write("scratch.log", b"x");
    let exclude = repo.git_path("info/exclude");
    if let Some(parent) = exclude.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(&exclude, "scratch.log\n").unwrap();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(
            &runner(),
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
        )
        .unwrap();
    assert!(!transfer.tree_of(base.oid()).contains("scratch.log"));
}

#[test]
fn clones_of_one_origin_get_separate_transfer_repositories_and_missing_alternates_fail_early() {
    let origin = repo_with_commits();
    let clone_a_dir = tempfile::tempdir().unwrap();
    let clone_b_dir = tempfile::tempdir().unwrap();
    let clone_a_path = clone_a_dir.path().join("repo");
    let clone_b_path = clone_b_dir.path().join("repo");
    assert!(
        origin
            .git(&[
                "clone",
                origin.path().to_str().unwrap(),
                clone_a_path.to_str().unwrap()
            ])
            .status
            .success()
    );
    assert!(
        origin
            .git(&[
                "clone",
                origin.path().to_str().unwrap(),
                clone_b_path.to_str().unwrap()
            ])
            .status
            .success()
    );
    assert!(
        std::process::Command::new("/usr/bin/git")
            .args([
                "-C",
                clone_b_path.to_str().unwrap(),
                "remote",
                "set-url",
                "origin",
                "https://example.test/repo.git",
            ])
            .status()
            .unwrap()
            .success()
    );
    let common_a = fs::canonicalize(clone_a_path.join(".git")).unwrap();
    let common_b = fs::canonicalize(clone_b_path.join(".git")).unwrap();
    let cache = cache_root();
    let a = TransferRepo::open_or_create(cache.path(), &common_a).unwrap();
    let b = TransferRepo::open_or_create(cache.path(), &common_b).unwrap();
    assert_ne!(a.path(), b.path());
    let context_b = ProjectInspector::new(&SystemProcessRunner)
        .inspect(&clone_b_path)
        .unwrap();
    assert!(b.resolve_base(&runner(), &context_b, "HEAD").is_ok());
    fs::remove_dir_all(&clone_b_path).unwrap();
    assert_eq!(
        b.verify_alternates().unwrap_err().public_code(),
        "BASE_UNAVAILABLE"
    );
}

#[test]
fn wip_base_captures_selection_into_transfer_repo_only() {
    let repo = repo_with_dirty_worktree();
    let before = RepositoryFingerprint::capture(repo.path()).unwrap();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(
            &runner(),
            &repo.context(),
            task_id(),
            &settings_including(&["fixtures/**"]),
            &identity(),
        )
        .unwrap();
    assert_eq!(base.kind(), BaseKind::Wip);
    let tree = transfer.tree_of(base.oid());
    assert_eq!(tree.blob("src/app.rs"), repo.worktree_bytes("src/app.rs"));
    assert!(
        tree.contains("fixtures/generated.txt")
            && !tree.contains("deleted.rs")
            && !tree.contains("target/out.bin")
    );
    assert_eq!(transfer.parent_of(base.oid()), repo.head());
    assert_eq!(RepositoryFingerprint::capture(repo.path()).unwrap(), before);
    assert!(transfer.has_ref(&format!("refs/mac-worker/bases/{}", task_id())));
    assert!(!repo.has_ref(&format!("refs/mac-worker/bases/{}", task_id())));
}

#[test]
fn second_capture_mismatch_is_snapshot_changed_and_leaves_no_ref() {
    let repo = repo_with_commits();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let err = transfer
        .build_wip_base_with_hook(
            &runner(),
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
            &|| {
                mutate_between_captures(&repo)();
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(err.public_code(), "SNAPSHOT_CHANGED");
    assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{}", task_id())));
}

#[test]
fn committed_base_with_tracked_secret_fails_sensitive_path() {
    let repo = repo_with_tracked(".env");
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .resolve_base(&runner(), &repo.context(), "HEAD")
        .unwrap();
    assert_eq!(
        transfer
            .check_sensitive_tree(&runner(), base.oid(), &settings())
            .unwrap_err()
            .public_code(),
        "SENSITIVE_PATH"
    );
}

#[test]
fn import_result_writes_exactly_one_remote_tracking_ref() {
    let cache = cache_root();
    let (repo, transfer) = repo_and_transfer_with_result(&cache, task_id());
    let before = RepositoryFingerprint::capture(repo.path()).unwrap();
    let receipt = transfer
        .import_result(&runner(), &repo.common_dir(), "mini-1", task_id())
        .unwrap();
    let after = RepositoryFingerprint::capture(repo.path()).unwrap();
    assert_eq!(
        after.diff(&before),
        vec![format!(
            "+refs/remotes/mac-worker/mini-1/task/{}",
            task_id()
        )]
    );
    assert_eq!(receipt.head(), &repo.head());
    assert!(!repo.git_path("FETCH_HEAD").exists());
    assert!(
        !repo
            .git_path(&format!(
                "logs/refs/remotes/mac-worker/mini-1/task/{}",
                task_id()
            ))
            .exists()
    );
}

#[test]
fn import_result_rejects_a_user_repository_other_than_its_bound_transfer_repo() {
    let cache = cache_root();
    let task = task_id();
    let (_source, transfer) = repo_and_transfer_with_result(&cache, task);
    let other = repo_with_commits();
    let before = RepositoryFingerprint::capture(other.path()).unwrap();

    let error = transfer
        .import_result(&runner(), &other.common_dir(), "mini-1", task)
        .expect_err("a transfer repository must be bound to its original user repository");

    assert_eq!(error.public_code(), "BASE_UNAVAILABLE");
    assert_eq!(
        RepositoryFingerprint::capture(other.path()).unwrap(),
        before
    );
}

#[test]
fn import_result_rejects_worker_names_that_cannot_be_a_configured_ref_component() {
    for worker in [
        "mini/other",
        "mini..other",
        "mini.",
        "mini.lock",
        "m\u{00e9}ni",
    ] {
        let cache = cache_root();
        let (repo, transfer) = repo_and_transfer_with_result(&cache, task_id());
        let before = RepositoryFingerprint::capture(repo.path()).unwrap();

        let error = transfer
            .import_result(&runner(), &repo.common_dir(), worker, task_id())
            .unwrap_err();

        assert_eq!(error.public_code(), "TASK_CONFIG_INVALID", "{worker:?}");
        assert_eq!(RepositoryFingerprint::capture(repo.path()).unwrap(), before);
    }
}

#[test]
fn base_refs_resolve_in_the_user_repository_not_the_bare_transfer_repository() {
    let repo = repo_with_branch("feature");
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .resolve_base(&runner(), &repo.context(), "feature")
        .unwrap();
    assert_eq!(base.oid(), &repo.rev_parse("feature"));
    assert!(
        !git_in(transfer.path(), &["rev-parse", "--verify", "feature"])
            .status
            .success()
    );
}

#[test]
fn selection_works_from_a_linked_worktree() {
    let repo = repo_with_commits();
    let linked = repo.path().join("linked-worktree");
    assert!(
        repo.git(&["worktree", "add", linked.to_str().unwrap(), "HEAD"])
            .status
            .success()
    );
    fs::write(linked.join("src/app.rs"), b"linked change\n").unwrap();
    let context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(&linked)
        .unwrap();
    assert_ne!(context.root, repo.path());
    assert_eq!(context.common_dir, repo.common_dir());
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &context.common_dir).unwrap();
    let base = transfer
        .build_wip_base(&runner(), &context, task_id(), &settings(), &identity())
        .unwrap();
    assert_eq!(
        transfer.tree_of(base.oid()).blob("src/app.rs"),
        b"linked change\n"
    );
}

#[test]
fn symlinks_and_executable_bits_are_preserved() {
    let repo = repo_with_commits();
    symlink("src/app.rs", repo.path().join("link-to-app")).unwrap();
    repo.write("bin/tool.sh", b"#!/bin/sh\n");
    let mut permissions = fs::metadata(repo.path().join("bin/tool.sh"))
        .unwrap()
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(repo.path().join("bin/tool.sh"), permissions).unwrap();
    assert!(
        repo.git(&["add", "link-to-app", "bin/tool.sh"])
            .status
            .success()
    );
    repo.commit_all("link and exec");
    fs::write(repo.path().join("bin/tool.sh"), b"#!/bin/sh\necho hi\n").unwrap();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(
            &runner(),
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
        )
        .unwrap();
    let tree = transfer.tree_of(base.oid());
    assert_eq!(tree.mode("link-to-app"), "120000");
    assert_eq!(tree.blob("link-to-app"), b"src/app.rs");
    assert_eq!(tree.mode("bin/tool.sh"), "100755");
}

#[test]
fn uncovered_untracked_files_are_untracked_input() {
    let repo = repo_with_commits();
    repo.write("scratch.tmp", b"nope");
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    assert_eq!(
        transfer
            .build_wip_base(
                &runner(),
                &repo.context(),
                task_id(),
                &settings(),
                &identity()
            )
            .unwrap_err()
            .public_code(),
        "UNTRACKED_INPUT"
    );
}

#[test]
fn resolve_base_rejects_non_commit_objects_and_outside_refs() {
    let repo = repo_with_commits();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let tree = String::from_utf8(repo.git(&["rev-parse", "HEAD^{tree}"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    assert!(
        transfer
            .resolve_base(&runner(), &repo.context(), &tree)
            .is_err()
    );
    assert!(
        transfer
            .resolve_base(&runner(), &repo.context(), "../outside")
            .is_err()
    );
}

#[test]
fn merge_in_progress_is_task_config_invalid() {
    let repo = repo_with_commits();
    fs::write(repo.git_path("MERGE_HEAD"), format!("{}\n", repo.head())).unwrap();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    assert_eq!(
        transfer
            .resolve_base(&runner(), &repo.context(), "HEAD")
            .unwrap_err()
            .public_code(),
        "TASK_CONFIG_INVALID"
    );
    assert_eq!(
        transfer
            .build_wip_base(
                &runner(),
                &repo.context(),
                task_id(),
                &settings(),
                &identity()
            )
            .unwrap_err()
            .public_code(),
        "TASK_CONFIG_INVALID"
    );
}

#[test]
fn dirty_report_counts_without_contents() {
    let repo = repo_with_dirty_worktree();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .resolve_base(&runner(), &repo.context(), "HEAD")
        .unwrap();
    let dirty = base.dirty();
    assert!(dirty.modified >= 1);
    assert!(dirty.added >= 1);
    assert!(dirty.deleted >= 1);
    assert!(!format!("{} {} {}", dirty.modified, dirty.added, dirty.deleted).contains("println"));
}

#[test]
fn release_base_removes_only_the_task_ref() {
    let repo = repo_with_commits();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let first = task_id();
    let second = TaskId::new(Uuid::from_u128(2));
    transfer
        .build_wip_base(&runner(), &repo.context(), first, &settings(), &identity())
        .unwrap();
    transfer
        .build_wip_base(&runner(), &repo.context(), second, &settings(), &identity())
        .unwrap();
    transfer.release_base(&runner(), first).unwrap();
    assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{first}")));
    assert!(transfer.has_ref(&format!("refs/mac-worker/bases/{second}")));
}

#[test]
fn write_commands_use_the_transfer_git_dir_and_ignore_user_index_lock() {
    let repo = repo_with_commits();
    repo.write("src/app.rs", b"changed\n");
    fs::write(repo.git_path("index.lock"), b"locked\n").unwrap();
    let cache = cache_root();
    let recorder = RecordingRunner::passthrough();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    transfer
        .build_wip_base(
            &recorder,
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
        )
        .unwrap();
    let user_git = repo.git_path("");
    for args in recorder.write_args() {
        let rendered: Vec<String> = args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert!(
            rendered
                .iter()
                .any(|arg| arg == transfer.path().to_str().unwrap()
                    || arg == &format!("--git-dir={}", transfer.path().display())),
            "write command missing transfer git-dir: {rendered:?}"
        );
        assert!(
            !rendered.iter().any(|arg| arg == user_git.to_str().unwrap()),
            "write command targeted the user git dir: {rendered:?}"
        );
    }
}
#[test]
fn open_or_create_rejects_a_symlinked_transfer_directory() {
    let repo = repo_with_commits();
    let cache = cache_root();
    let repo_id = repo_id_for(&repo.common_dir()).unwrap();
    let transfer_parent = cache.path().join("transfer");
    fs::create_dir_all(&transfer_parent).unwrap();
    let outside = cache.path().join("outside.git");
    fs::create_dir(&outside).unwrap();
    symlink(&outside, transfer_parent.join(format!("{repo_id}.git"))).unwrap();

    let error = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap_err();
    assert!(
        matches!(error, WorkerError::Io(_) | WorkerError::Git { .. }),
        "{error:?}"
    );
    assert!(
        !outside.join("HEAD").exists(),
        "a symlink transfer directory must not be initialized"
    );
    assert!(
        !outside
            .join("objects")
            .join("info")
            .join("alternates")
            .exists(),
        "alternates must not be written through a symlink"
    );
}

#[test]
fn alternates_write_does_not_follow_a_symlink() {
    let repo = repo_with_commits();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let planted = cache.path().join("planted-alternates");
    fs::write(&planted, b"keep-me\n").unwrap();
    let alternates = transfer.path().join("objects/info/alternates");
    fs::remove_file(&alternates).unwrap();
    symlink(&planted, &alternates).unwrap();
    drop(transfer);

    let error = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap_err();
    assert!(
        matches!(error, WorkerError::Io(_) | WorkerError::Git { .. }),
        "{error:?}"
    );
    assert_eq!(fs::read(&planted).unwrap(), b"keep-me\n");
}

#[test]
fn worktree_hashing_does_not_follow_a_symlink_to_outside_bytes() {
    let repo = repo_with_commits();
    let outside = cache_root();
    let secret = outside.path().join("SECRET_OUTSIDE");
    fs::write(&secret, b"SECRET_OUTSIDE\n").unwrap();
    symlink(&secret, repo.path().join("leak")).unwrap();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(
            &runner(),
            &repo.context(),
            task_id(),
            &settings_including(&["leak"]),
            &identity(),
        )
        .unwrap();
    let tree = transfer.tree_of(base.oid());
    assert_eq!(tree.mode("leak"), "120000");
    assert_eq!(tree.blob("leak"), secret.as_os_str().as_encoded_bytes());
    assert_ne!(tree.blob("leak"), b"SECRET_OUTSIDE\n");
}

#[test]
fn transfer_cache_directories_and_alternates_are_owner_only() {
    let repo = repo_with_commits();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    for path in [
        transfer.path().to_path_buf(),
        transfer.path().join("objects"),
        transfer.path().join("objects/info"),
        transfer.path().join("scratch"),
    ] {
        if !path.exists() {
            continue;
        }
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode,
            0o700,
            "{} must be owner-only, got {mode:o}",
            path.display()
        );
    }
    let alternates = transfer.path().join("objects/info/alternates");
    let mode = fs::metadata(&alternates).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "alternates must be owner-only, got {mode:o}");
}

#[test]
fn two_handles_for_one_repository_open_concurrently() {
    let cache = cache_root();
    let repo = repo_with_commits();
    let first = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let cache_path = cache.path().to_path_buf();
    let common_dir = repo.common_dir();
    let handle = std::thread::spawn(move || {
        sender
            .send(TransferRepo::open_or_create(&cache_path, &common_dir))
            .unwrap();
    });
    let second = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("a second handle must not wait for the first")
        .unwrap();
    handle.join().unwrap();
    assert_eq!(second.repo_id(), first.repo_id());
    assert_eq!(second.path(), first.path());
}

#[test]
fn concurrent_handles_build_bases_for_different_tasks() {
    let cache = cache_root();
    let repo = repo_with_commits();
    repo.write("README.md", b"hello, dirty\n");
    let held = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let tasks = [
        TaskId::new(Uuid::from_u128(11)),
        TaskId::new(Uuid::from_u128(12)),
    ];
    let (sender, receiver) = std::sync::mpsc::channel();
    let workers: Vec<_> = tasks
        .iter()
        .map(|task| {
            let sender = sender.clone();
            let cache_path = cache.path().to_path_buf();
            let common_dir = repo.common_dir();
            let context = repo.context();
            let task = *task;
            std::thread::spawn(move || {
                let result =
                    TransferRepo::open_or_create(&cache_path, &common_dir).and_then(|transfer| {
                        transfer
                            .build_wip_base(&runner(), &context, task, &settings(), &identity())
                            .map(|_| ())
                    });
                sender.send(result).unwrap();
            })
        })
        .collect();
    drop(sender);
    for _ in &tasks {
        receiver
            .recv_timeout(Duration::from_secs(30))
            .expect("wip bases must not wait for the held handle")
            .expect("concurrent wip bases succeed");
    }
    for worker in workers {
        worker.join().unwrap();
    }
    for task in tasks {
        let output = git_in(
            held.path(),
            &[
                "rev-parse",
                "--verify",
                &format!("refs/mac-worker/bases/{task}"),
            ],
        );
        assert!(output.status.success(), "base ref for {task} exists");
    }
}

#[test]
fn import_result_does_not_wait_for_another_live_handle() {
    let cache = cache_root();
    let (repo, transfer) = repo_and_transfer_with_result(&cache, task_id());
    let other = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let common_dir = repo.common_dir();
    let handle = std::thread::spawn(move || {
        sender
            .send(
                transfer
                    .import_result(&runner(), &common_dir, "mini-1", task_id())
                    .map(|_| ()),
            )
            .unwrap();
    });
    receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("import must not wait for another handle")
        .unwrap();
    handle.join().unwrap();
    drop(other);
    assert!(repo.has_ref(&format!(
        "refs/remotes/mac-worker/mini-1/task/{}",
        task_id()
    )));
}

#[test]
fn concurrent_first_creation_yields_one_initialized_repository() {
    let cache = cache_root();
    let repo = repo_with_commits();
    let barrier = std::sync::Barrier::new(4);
    let repo_ids: Vec<String> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let barrier = &barrier;
                let cache_path = cache.path().to_path_buf();
                let repo = &repo;
                scope.spawn(move || {
                    barrier.wait();
                    TransferRepo::open_or_create(&cache_path, &repo.common_dir())
                        .map(|transfer| transfer.repo_id().to_owned())
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| {
                worker
                    .join()
                    .unwrap()
                    .expect("every concurrent creator succeeds")
            })
            .collect()
    });
    assert!(repo_ids.iter().all(|repo_id| repo_id == &repo_ids[0]));
    let transfer_dir = cache.path().join("transfer");
    let repos = fs::read_dir(&transfer_dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".git"))
        .count();
    assert_eq!(repos, 1);
    assert!(
        transfer_dir
            .join(format!("{}.git/HEAD", repo_ids[0]))
            .exists()
    );
    assert!(
        transfer_dir
            .join(format!("{}.git/scratch", repo_ids[0]))
            .is_dir()
    );
}
