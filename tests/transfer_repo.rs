mod support;

use std::{
    collections::BTreeMap,
    fs,
    os::unix::{
        fs::{PermissionsExt, symlink},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};

use mac_worker::{
    error::{ProcessError, ProcessStream, WorkerError},
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
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
        setup: None,
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

const SPACE_NAME: &str = "file with space.txt";
const UNICODE_NAME: &str = "привет.bin";
const TAB_NAME: &str = "name\twith\ttab.txt";
const NEWLINE_NAME: &str = "name\nwith\nnewline.txt";
const COMMA_NAME: &str = "name,with,comma.txt";
const DASH_NAME: &str = "-leading-dash.txt";
const BINARY_BYTES: &[u8] = b"\x00\xff\xfe binary \n\x00";

struct FailOnWriteTree;

impl ProcessRunner for FailOnWriteTree {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.args.iter().any(|argument| argument == "write-tree") {
            return Ok(ProcessResult {
                status: ExitStatus::from_raw(1 << 8),
                stdout: Vec::new(),
                stderr: b"injected write-tree failure\n".to_vec(),
            });
        }
        SystemProcessRunner.run(request)
    }
}

fn update_index_requests(runner: &RecordingRunner) -> Vec<ProcessRequest> {
    runner
        .requests()
        .into_iter()
        .filter(|request| {
            request
                .args
                .iter()
                .any(|argument| argument == "update-index")
        })
        .collect()
}

fn hash_object_count(runner: &RecordingRunner) -> usize {
    runner
        .requests()
        .iter()
        .filter(|request| {
            request
                .args
                .iter()
                .any(|argument| argument == "hash-object")
        })
        .count()
}

fn fast_import_count(runner: &RecordingRunner) -> usize {
    runner
        .requests()
        .iter()
        .filter(|request| {
            request
                .args
                .iter()
                .any(|argument| argument == "fast-import")
        })
        .count()
}

fn split_tab(record: &[u8]) -> (&[u8], &[u8]) {
    let index = record
        .iter()
        .position(|byte| *byte == b'\t')
        .expect("records are meta TAB path");
    (&record[..index], &record[index + 1..])
}

fn index_info_paths(stdin: &[u8]) -> Vec<String> {
    stdin
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
        .map(|record| {
            let (_, path) = split_tab(record);
            String::from_utf8(path.to_vec()).expect("utf8 index path")
        })
        .collect()
}

fn scratch_index_residue(transfer: &TransferRepo) -> Vec<String> {
    let scratch = transfer.path().join("scratch");
    if !scratch.exists() {
        return Vec::new();
    }
    let mut names: Vec<String> = fs::read_dir(&scratch)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name != ".mac-worker-rooted-fs")
        .collect();
    names.sort();
    names
}

fn transfer_tree_nul(git_dir: &Path, oid: &str) -> BTreeMap<String, (String, Vec<u8>)> {
    let listing = git_in(git_dir, &["ls-tree", "-r", "-z", oid]);
    assert!(listing.status.success(), "ls-tree -z must succeed");
    let mut entries = BTreeMap::new();
    for record in listing.stdout.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        let (meta, path) = split_tab(record);
        let meta = std::str::from_utf8(meta).expect("utf8 ls-tree meta");
        let mut parts = meta.split_whitespace();
        let mode = parts.next().expect("mode").to_owned();
        let _kind = parts.next();
        let object = parts.next().expect("oid");
        let path = String::from_utf8(path.to_vec()).expect("utf8 ls-tree path");
        let blob = git_in(git_dir, &["cat-file", "-p", object]);
        assert!(blob.status.success(), "cat-file {object}");
        entries.insert(path, (mode, blob.stdout));
    }
    entries
}

fn make_executable(path: &Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn repo_for_batched_index() -> Fixture {
    let repo = repo_with_commits();
    repo.write(SPACE_NAME, b"space-bytes\n");
    repo.write(UNICODE_NAME, b"unicode-bytes\n");
    repo.write(TAB_NAME, b"tab-bytes\n");
    repo.write(NEWLINE_NAME, b"newline-bytes\n");
    repo.write(COMMA_NAME, b"comma-bytes\n");
    repo.write(DASH_NAME, b"dash-bytes\n");
    repo.write("payload.bin", BINARY_BYTES);
    repo.write("bin/tool.sh", b"#!/bin/sh\necho hi\n");
    make_executable(&repo.path().join("bin/tool.sh"));
    symlink("src/app.rs", repo.path().join("link-to-app")).unwrap();
    repo.write("gone.txt", b"delete-me\n");
    for index in 0..8 {
        repo.write(
            &format!("extra/{index:02}.txt"),
            format!("extra-{index:02}\n").as_bytes(),
        );
    }
    repo.commit_all("unusual and extra snapshot inputs");
    fs::remove_file(repo.path().join("gone.txt")).expect("delete tracked file");
    repo.write("fixtures/generated.txt", b"fixture\n");
    repo
}

fn expected_batched_tree(repo: &Fixture) -> BTreeMap<String, (String, Vec<u8>)> {
    let mut expected = BTreeMap::new();
    let root = repo.path();
    let mut insert_file = |path: &str, mode: &str| {
        expected.insert(
            path.to_owned(),
            (mode.to_owned(), fs::read(root.join(path)).unwrap()),
        );
    };
    insert_file("README.md", "100644");
    insert_file("src/app.rs", "100644");
    insert_file(SPACE_NAME, "100644");
    insert_file(UNICODE_NAME, "100644");
    insert_file(TAB_NAME, "100644");
    insert_file(NEWLINE_NAME, "100644");
    insert_file(COMMA_NAME, "100644");
    insert_file(DASH_NAME, "100644");
    insert_file("payload.bin", "100644");
    insert_file("bin/tool.sh", "100755");
    insert_file("fixtures/generated.txt", "100644");
    for index in 0..8 {
        insert_file(&format!("extra/{index:02}.txt"), "100644");
    }
    expected.insert(
        "link-to-app".into(),
        (
            "120000".into(),
            fs::read_link(root.join("link-to-app"))
                .unwrap()
                .as_os_str()
                .as_encoded_bytes()
                .to_vec(),
        ),
    );
    expected
}

#[test]
fn wip_capture_writes_distinct_blobs_in_one_fast_import() {
    // Break caught: one hash-object per distinct content (baseline D=H=20).
    // Approved contract: one blob-only fast-import for D<=64 small payloads,
    // zero leftover hash-object, second capture is memo-only.
    let repo = repo_for_batched_index();
    let distinct = expected_batched_tree(&repo).len();
    assert_eq!(distinct, 20, "fixture D is the independent want");
    let cache = cache_root();
    let recorder = RecordingRunner::passthrough();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    transfer
        .build_wip_base(
            &recorder,
            &repo.context(),
            task_id(),
            &settings_including(&["fixtures/**"]),
            &identity(),
        )
        .unwrap();
    assert_eq!(fast_import_count(&recorder), 1);
    assert_eq!(hash_object_count(&recorder), 0);
    let imports: Vec<_> = recorder
        .requests()
        .into_iter()
        .filter(|request| {
            request
                .args
                .iter()
                .any(|argument| argument == "fast-import")
        })
        .collect();
    let args: Vec<&str> = imports[0]
        .args
        .iter()
        .filter_map(|argument| argument.to_str())
        .collect();
    assert!(
        args.contains(&"--quiet") && args.contains(&"--done"),
        "blob batch must be quiet done fast-import: {args:?}"
    );
}

#[test]
fn wip_capture_batches_index_updates_and_preserves_tree_bytes() {
    // Break caught: per-file update-index --cacheinfo, or 2H hash-object on a
    // stable worktree after the digest memo, or a later build reusing that memo.
    let repo = repo_for_batched_index();
    let expected = expected_batched_tree(&repo);
    let hashed_entries = expected.len();
    assert_eq!(hashed_entries, 20, "fixture H is the independent want");
    let cache = cache_root();
    let recorder = RecordingRunner::passthrough();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(
            &recorder,
            &repo.context(),
            task_id(),
            &settings_including(&["fixtures/**"]),
            &identity(),
        )
        .unwrap();

    let updates = update_index_requests(&recorder);
    assert_eq!(
        updates.len(),
        2,
        "at most one update-index per nonempty capture, not one per file; got {:?}",
        updates
            .iter()
            .map(|request| request.args.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(fast_import_count(&recorder), 1);
    assert_eq!(hash_object_count(&recorder), 0);
    let second_task = TaskId::new(Uuid::from_u128(2));
    let second_recorder = RecordingRunner::passthrough();
    transfer
        .build_wip_base(
            &second_recorder,
            &repo.context(),
            second_task,
            &settings_including(&["fixtures/**"]),
            &identity(),
        )
        .unwrap();
    assert_eq!(
        fast_import_count(&second_recorder),
        1,
        "a later build_wip_base must not reuse the previous memo"
    );
    assert_eq!(hash_object_count(&second_recorder), 0);
    for request in &updates {
        let args: Vec<&str> = request
            .args
            .iter()
            .filter_map(|argument| argument.to_str())
            .collect();
        assert!(
            args.contains(&"--index-info") && args.contains(&"-z") && args.contains(&"--add"),
            "index batch must be NUL-framed --index-info: {args:?}"
        );
        assert!(
            !args.iter().any(|argument| *argument == "--cacheinfo"
                || argument.contains("--cacheinfo")
                || argument.contains("100644,")),
            "per-file --cacheinfo must not remain: {args:?}"
        );
        let stdin = request.stdin.as_deref().expect("index-info stdin");
        assert!(
            stdin.ends_with(&[0]) || stdin.is_empty(),
            "index-info payload must be NUL-framed"
        );
        let paths = index_info_paths(stdin);
        for name in [
            SPACE_NAME,
            UNICODE_NAME,
            TAB_NAME,
            NEWLINE_NAME,
            COMMA_NAME,
            DASH_NAME,
            "payload.bin",
            "bin/tool.sh",
            "link-to-app",
            "fixtures/generated.txt",
        ] {
            assert!(
                paths.iter().any(|path| path == name),
                "index-info stdin missing {name:?} in {paths:?}"
            );
        }
        assert!(!paths.iter().any(|path| path == "gone.txt"));
        assert_eq!(paths.len(), hashed_entries);
    }

    let tree = transfer_tree_nul(transfer.path(), base.oid().as_str());
    assert_eq!(tree, expected);
    assert!(!tree.contains_key("gone.txt"));
    assert_eq!(scratch_index_residue(&transfer), [] as [String; 0]);
}

#[test]
fn wip_empty_selection_does_not_call_update_index() {
    // Break caught: adding a vacuous update-index batch when read-tree --empty
    // already produced the empty tree. H=0 must stay at zero index processes.
    let repo = repo_with_commits();
    fs::remove_file(repo.path().join("README.md")).unwrap();
    fs::remove_file(repo.path().join("src/app.rs")).unwrap();
    let cache = cache_root();
    let recorder = RecordingRunner::passthrough();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(
            &recorder,
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
        )
        .unwrap();
    assert_eq!(update_index_requests(&recorder).len(), 0);
    assert_eq!(hash_object_count(&recorder), 0);
    assert_eq!(fast_import_count(&recorder), 0);
    assert!(transfer_tree_nul(transfer.path(), base.oid().as_str()).is_empty());
    assert_eq!(scratch_index_residue(&transfer), [] as [String; 0]);
}

#[test]
fn wip_capture_failure_removes_owned_scratch_index() {
    // Break caught: leaving GIT_INDEX_FILE scratch bytes after a transfer Git error.
    let repo = repo_with_commits();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let error = transfer
        .build_wip_base(
            &FailOnWriteTree,
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "BASE_UNAVAILABLE");
    assert_eq!(scratch_index_residue(&transfer), [] as [String; 0]);
    assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{}", task_id())));
}

#[test]
fn unusual_name_mutation_between_captures_is_still_snapshot_changed() {
    // Break caught: batched index-info reuse skipping the second full capture.
    let repo = repo_for_batched_index();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let error = transfer
        .build_wip_base_with_hook(
            &runner(),
            &repo.context(),
            task_id(),
            &settings_including(&["fixtures/**"]),
            &identity(),
            &|| {
                repo.write(COMMA_NAME, b"comma-bytes-mutated\n");
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "SNAPSHOT_CHANGED");
    assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{}", task_id())));
    assert_eq!(scratch_index_residue(&transfer), [] as [String; 0]);
}

#[test]
fn wip_capture_hashes_identical_blob_bytes_once_per_build() {
    // Break caught: hashing every path twice, or refusing to share a blob OID
    // between a regular file and a symlink whose target bytes match.
    let repo = repo_with_commits();
    repo.write("twin.txt", b"src/app.rs");
    symlink("src/app.rs", repo.path().join("twin-link")).unwrap();
    repo.write("other.txt", b"unique\n");
    repo.commit_all("shared blob bytes");
    let cache = cache_root();
    let recorder = RecordingRunner::passthrough();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(
            &recorder,
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
        )
        .unwrap();
    // README, src/app.rs, twin.txt, twin-link, other.txt → H=5, D=4
    // (twin.txt bytes equal the symlink target "src/app.rs").
    assert_eq!(fast_import_count(&recorder), 1);
    assert_eq!(hash_object_count(&recorder), 0);
    assert_eq!(update_index_requests(&recorder).len(), 2);
    let tree = transfer_tree_nul(transfer.path(), base.oid().as_str());
    assert_eq!(
        tree.get("twin.txt").map(|(mode, _)| mode.as_str()),
        Some("100644")
    );
    assert_eq!(
        tree.get("twin-link").map(|(mode, _)| mode.as_str()),
        Some("120000")
    );
    assert_eq!(tree["twin.txt"].1, tree["twin-link"].1);
}

#[test]
fn executable_mode_change_between_captures_is_snapshot_changed() {
    // Break caught: memoizing mode with content so a chmod is invisible.
    let repo = repo_with_commits();
    repo.write("tool.sh", b"#!/bin/sh\n");
    repo.commit_all("non-executable tool");
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let error = transfer
        .build_wip_base_with_hook(
            &runner(),
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
            &|| {
                make_executable(&repo.path().join("tool.sh"));
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "SNAPSHOT_CHANGED");
}

#[test]
fn symlink_target_change_between_captures_is_snapshot_changed() {
    // Break caught: skipping the second rooted read because the path already
    // has a blob OID in the memo.
    let repo = repo_with_commits();
    symlink("src/app.rs", repo.path().join("link")).unwrap();
    repo.commit_all("symlink");
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let error = transfer
        .build_wip_base_with_hook(
            &runner(),
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
            &|| {
                fs::remove_file(repo.path().join("link")).unwrap();
                symlink("README.md", repo.path().join("link")).unwrap();
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "SNAPSHOT_CHANGED");
}

const BLOB_BATCH_MAX_BYTES: usize = 1024 * 1024;
const BLOB_BATCH_MAX_OBJECTS: usize = 64;
const GET_MARK_LINE_BYTES: usize = 41;
const PROTOCOL_BYTES: &[u8] = b"blob\nmark :99\ndata 3\nNO\nget-mark :1\ndone\n";
const NO_FINAL_LF: &[u8] = b"no-final-lf";
const NUL_PAYLOAD: &[u8] = b"x\x00y\xffz";

#[derive(Clone, Default)]
struct BlobWriteStats {
    events: Arc<Mutex<Vec<(&'static str, usize)>>>,
    fast_import_stdout: Arc<Mutex<Vec<usize>>>,
}

impl BlobWriteStats {
    fn events(&self) -> Vec<(&'static str, usize)> {
        self.events.lock().expect("stats").clone()
    }

    fn fast_import_stdin(&self) -> Vec<usize> {
        self.events()
            .into_iter()
            .filter_map(|(verb, len)| (verb == "fast-import").then_some(len))
            .collect()
    }

    fn fast_import_stdout(&self) -> Vec<usize> {
        self.fast_import_stdout.lock().expect("stats").clone()
    }

    fn hash_object_stdin(&self) -> Vec<usize> {
        self.events()
            .into_iter()
            .filter_map(|(verb, len)| (verb == "hash-object").then_some(len))
            .collect()
    }
}

impl ProcessRunner for BlobWriteStats {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let stdin_len = request.stdin.as_ref().map(Vec::len).unwrap_or(0);
        let fast_import = request
            .args
            .iter()
            .any(|argument| argument == "fast-import");
        let hash_object = request
            .args
            .iter()
            .any(|argument| argument == "hash-object");
        let result = SystemProcessRunner.run(request)?;
        if fast_import {
            self.events
                .lock()
                .expect("stats")
                .push(("fast-import", stdin_len));
            self.fast_import_stdout
                .lock()
                .expect("stats")
                .push(result.stdout.len());
        }
        if hash_object {
            self.events
                .lock()
                .expect("stats")
                .push(("hash-object", stdin_len));
        }
        Ok(result)
    }
}

struct InjectFastImport {
    response: Mutex<Option<Result<ProcessResult, WorkerError>>>,
    downstream_writes: Mutex<Vec<String>>,
}

impl InjectFastImport {
    fn once(result: Result<ProcessResult, WorkerError>) -> Self {
        Self {
            response: Mutex::new(Some(result)),
            downstream_writes: Mutex::new(Vec::new()),
        }
    }

    fn downstream_writes(&self) -> Vec<String> {
        self.downstream_writes.lock().expect("inject").clone()
    }
}

impl ProcessRunner for InjectFastImport {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let is_import = request
            .args
            .iter()
            .any(|argument| argument == "fast-import");
        if is_import && let Some(injected) = self.response.lock().expect("inject").take() {
            return injected;
        }
        if self.response.lock().expect("inject").is_none() {
            for verb in ["update-index", "write-tree", "commit-tree", "update-ref"] {
                if request.args.iter().any(|argument| argument == verb) {
                    self.downstream_writes
                        .lock()
                        .expect("inject")
                        .push(verb.to_owned());
                }
            }
        }
        SystemProcessRunner.run(request)
    }
}

fn framed_fast_import_len(payload_lens: &[usize]) -> usize {
    let mut total = 0;
    for (index, len) in payload_lens.iter().enumerate() {
        let mark = index + 1;
        total += format!("blob\nmark :{mark}\ndata {len}\n").len() + len + 1;
    }
    for mark in 1..=payload_lens.len() {
        total += format!("get-mark :{mark}\n").len();
    }
    total + b"done\n".len()
}

/// Conservative recorded stdin-length bound for one flushed batch: unique
/// payload bytes stay <= 1 MiB, plus framing for at most 64 blob/mark/data/
/// get-mark commands and `done`. This is stdin length, not Vec capacity or RSS.
fn max_framed_batch_stdin_len() -> usize {
    const PER_OBJECT: usize = b"blob\nmark :64\ndata 1048576\n\nget-mark :64\n".len();
    BLOB_BATCH_MAX_BYTES + BLOB_BATCH_MAX_OBJECTS * PER_OBJECT + b"done\n".len()
}

fn fake_oid_hex() -> &'static [u8] {
    b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
}

fn fake_oid_lines(count: usize) -> Vec<u8> {
    let mut stdout = Vec::with_capacity(count * GET_MARK_LINE_BYTES);
    for _ in 0..count {
        stdout.extend_from_slice(fake_oid_hex());
        stdout.push(b'\n');
    }
    stdout
}

fn assert_no_published_base(transfer: &TransferRepo, error: &WorkerError, code: &str) {
    assert_eq!(error.public_code(), code);
    assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{}", task_id())));
    assert_eq!(scratch_index_residue(transfer), [] as [String; 0]);
}

#[test]
fn wip_capture_flushes_after_sixty_four_unique_blobs() {
    let repo = repo_with_commits();
    for index in 0..63 {
        repo.write(
            &format!("batch/{index:03}.txt"),
            format!("payload-{index:03}\n").as_bytes(),
        );
    }
    repo.commit_all("sixty-five unique blobs");
    let stats = BlobWriteStats::default();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(&stats, &repo.context(), task_id(), &settings(), &identity())
        .unwrap();
    let stdin = stats.fast_import_stdin();
    assert_eq!(
        stdin.len(),
        1,
        "D=65 flushes 64 unique objects as one fast-import"
    );
    assert_eq!(
        stats.hash_object_stdin(),
        vec![b"fn main() {}\n".len()],
        "the 65th unique blob is a singleton hash-object tail"
    );
    assert_eq!(
        stats.fast_import_stdout(),
        vec![BLOB_BATCH_MAX_OBJECTS * GET_MARK_LINE_BYTES]
    );
    assert!(
        stdin.iter().all(|len| *len <= max_framed_batch_stdin_len()),
        "recorded stdin length exceeded 1MiB+64-object framing: {stdin:?}"
    );
    assert_eq!(scratch_index_residue(&transfer), [] as [String; 0]);
    assert_eq!(
        transfer_tree_nul(transfer.path(), base.oid().as_str()).len(),
        65
    );
}

#[test]
fn wip_capture_flushes_when_unique_payload_bytes_would_exceed_one_mib() {
    let repo = Fixture::init();
    repo.write("a.bin", &vec![b'A'; 600_000]);
    repo.write("b.bin", &vec![b'B'; 600_000]);
    repo.commit_all("two payloads over one MiB together");
    let stats = BlobWriteStats::default();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(&stats, &repo.context(), task_id(), &settings(), &identity())
        .unwrap();
    assert_eq!(
        stats.events(),
        vec![("hash-object", 600_000), ("hash-object", 600_000),],
        "each 600KiB unique payload is a singleton hash-object flush"
    );
    assert_eq!(stats.fast_import_stdin().len(), 0);
    let tree = transfer_tree_nul(transfer.path(), base.oid().as_str());
    assert_eq!(tree["a.bin"].1, vec![b'A'; 600_000]);
    assert_eq!(tree["b.bin"].1, vec![b'B'; 600_000]);
}

#[test]
fn wip_capture_falls_back_to_hash_object_for_payloads_over_one_mib() {
    let repo = Fixture::init();
    repo.write("a.txt", b"small-a\n");
    repo.write("b.txt", b"small-b\n");
    repo.write("c.bin", &vec![b'C'; BLOB_BATCH_MAX_BYTES + 1]);
    repo.commit_all("oversized sibling");
    let stats = BlobWriteStats::default();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(&stats, &repo.context(), task_id(), &settings(), &identity())
        .unwrap();
    assert_eq!(
        stats.events(),
        vec![
            (
                "fast-import",
                framed_fast_import_len(&[b"small-a\n".len(), b"small-b\n".len()])
            ),
            ("hash-object", BLOB_BATCH_MAX_BYTES + 1),
        ],
        "two pending small blobs must flush as fast-import before oversized hash-object"
    );
    assert!(
        stats
            .fast_import_stdin()
            .iter()
            .all(|len| *len <= max_framed_batch_stdin_len())
    );
    let tree = transfer_tree_nul(transfer.path(), base.oid().as_str());
    assert_eq!(tree["a.txt"].1, b"small-a\n");
    assert_eq!(tree["b.txt"].1, b"small-b\n");
    assert_eq!(tree["c.bin"].1, vec![b'C'; BLOB_BATCH_MAX_BYTES + 1]);
}

#[test]
fn wip_capture_batches_exactly_one_mib_payload() {
    // Break caught: singleton (exactly 1 MiB, under the oversized bound) still
    // used fast-import. Approved contract: one unique pending object uses
    // hash-object; 1 MiB is singleton, not oversized.
    let repo = Fixture::init();
    repo.write("exact.bin", &vec![b'E'; BLOB_BATCH_MAX_BYTES]);
    repo.commit_all("exactly one MiB");
    let stats = BlobWriteStats::default();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(&stats, &repo.context(), task_id(), &settings(), &identity())
        .unwrap();
    assert_eq!(
        stats.events(),
        vec![("hash-object", BLOB_BATCH_MAX_BYTES)],
        "exactly 1 MiB alone is a singleton hash-object, not fast-import"
    );
    assert_eq!(stats.fast_import_stdin().len(), 0);
    let tree = transfer_tree_nul(transfer.path(), base.oid().as_str());
    assert_eq!(tree["exact.bin"].1, vec![b'E'; BLOB_BATCH_MAX_BYTES]);
}

#[test]
fn wip_capture_batches_one_mib_and_empty_as_one_fast_import() {
    // Break caught: treating exactly 1 MiB as oversized, or failing to
    // promote a Single payload when the second unique object is zero bytes.
    let repo = Fixture::init();
    repo.write("a.bin", &vec![b'E'; BLOB_BATCH_MAX_BYTES]);
    repo.write("empty.txt", b"");
    repo.commit_all("one MiB then empty");
    let stats = BlobWriteStats::default();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(&stats, &repo.context(), task_id(), &settings(), &identity())
        .unwrap();
    assert_eq!(
        stats.events(),
        vec![(
            "fast-import",
            framed_fast_import_len(&[BLOB_BATCH_MAX_BYTES, 0])
        )],
        "exactly 1 MiB plus a distinct empty blob must share one two-object fast-import"
    );
    assert_eq!(stats.hash_object_stdin().len(), 0);
    assert_eq!(stats.fast_import_stdout(), vec![2 * GET_MARK_LINE_BYTES]);
    let tree = transfer_tree_nul(transfer.path(), base.oid().as_str());
    assert_eq!(tree["a.bin"].1, vec![b'E'; BLOB_BATCH_MAX_BYTES]);
    assert_eq!(tree["empty.txt"].1, Vec::<u8>::new());
}

#[test]
fn wip_capture_shares_pending_duplicates_and_keeps_modes_independent() {
    let repo = repo_with_commits();
    repo.write("same.txt", b"shared-bytes\n");
    repo.write("same-exec.sh", b"shared-bytes\n");
    make_executable(&repo.path().join("same-exec.sh"));
    repo.commit_all("duplicate pending bytes");
    let stats = BlobWriteStats::default();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(&stats, &repo.context(), task_id(), &settings(), &identity())
        .unwrap();
    assert_eq!(stats.fast_import_stdin().len(), 1);
    assert_eq!(stats.hash_object_stdin().len(), 0);
    assert_eq!(
        stats.fast_import_stdout(),
        vec![3 * GET_MARK_LINE_BYTES],
        "README + app.rs + one shared digest must occupy three marks, not one per path"
    );
    let tree = transfer_tree_nul(transfer.path(), base.oid().as_str());
    assert_eq!(tree["same.txt"].0, "100644");
    assert_eq!(tree["same-exec.sh"].0, "100755");
    assert_eq!(tree["same.txt"].1, tree["same-exec.sh"].1);
    assert_eq!(tree["same.txt"].1, b"shared-bytes\n");
}

#[test]
fn wip_empty_files_use_one_hash_object() {
    let repo = repo_with_commits();
    fs::remove_file(repo.path().join("README.md")).unwrap();
    fs::remove_file(repo.path().join("src/app.rs")).unwrap();
    repo.write("empty.txt", b"");
    repo.write("empty-too.txt", b"");
    repo.write("empty-exec.sh", b"");
    make_executable(&repo.path().join("empty-exec.sh"));
    repo.commit_all("empty-only");
    let recorder = RecordingRunner::passthrough();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(
            &recorder,
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
        )
        .unwrap();
    assert_eq!(fast_import_count(&recorder), 0);
    assert_eq!(hash_object_count(&recorder), 1);
    let hashed = recorder
        .requests()
        .into_iter()
        .find(|request| {
            request
                .args
                .iter()
                .any(|argument| argument == "hash-object")
        })
        .expect("empty singleton");
    assert_eq!(hashed.stdin.as_deref(), Some(&b""[..]));
    let tree = transfer_tree_nul(transfer.path(), base.oid().as_str());
    assert_eq!(tree["empty.txt"], ("100644".into(), Vec::new()));
    assert_eq!(tree["empty-too.txt"], ("100644".into(), Vec::new()));
    assert_eq!(tree["empty-exec.sh"], ("100755".into(), Vec::new()));
}

#[test]
fn wip_capture_preserves_binary_and_protocol_like_blob_bytes() {
    let repo = repo_with_commits();
    repo.write("proto.txt", PROTOCOL_BYTES);
    repo.write("nul.bin", NUL_PAYLOAD);
    repo.write("no-lf", NO_FINAL_LF);
    repo.commit_all("protocol payloads");
    let recorder = RecordingRunner::passthrough();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let base = transfer
        .build_wip_base(
            &recorder,
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
        )
        .unwrap();
    assert_eq!(fast_import_count(&recorder), 1);
    let tree = transfer_tree_nul(transfer.path(), base.oid().as_str());
    assert_eq!(tree["proto.txt"].1, PROTOCOL_BYTES);
    assert_eq!(tree["nul.bin"].1, NUL_PAYLOAD);
    assert_eq!(tree["no-lf"].1, NO_FINAL_LF);
}

fn failed_build_retries(inject: Result<ProcessResult, WorkerError>, code: &str) {
    let repo = repo_with_commits();
    let cache = cache_root();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.common_dir()).unwrap();
    let injector = InjectFastImport::once(inject);
    let error = transfer
        .build_wip_base(
            &injector,
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
        )
        .unwrap_err();
    assert_no_published_base(&transfer, &error, code);
    assert_eq!(
        injector.downstream_writes(),
        [] as [String; 0],
        "rejected importer output must not reach update-index/write-tree"
    );
    let base = transfer
        .build_wip_base(
            &runner(),
            &repo.context(),
            task_id(),
            &settings(),
            &identity(),
        )
        .unwrap();
    assert!(!transfer_tree_nul(transfer.path(), base.oid().as_str()).is_empty());
}

#[test]
fn wip_fast_import_nonzero_does_not_publish_and_retries() {
    failed_build_retries(
        Ok(ProcessResult {
            status: ExitStatus::from_raw(1 << 8),
            stdout: fake_oid_lines(2),
            stderr: b"injected fast-import failure\n".to_vec(),
        }),
        "BASE_UNAVAILABLE",
    );
}

#[test]
fn wip_fast_import_truncated_stdout_does_not_publish_and_retries() {
    let mut stdout = fake_oid_lines(1);
    stdout.extend_from_slice(fake_oid_hex());
    stdout.push(b'\r');
    failed_build_retries(
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        }),
        "BASE_UNAVAILABLE",
    );
}

#[test]
fn wip_fast_import_too_few_oids_does_not_publish_and_retries() {
    failed_build_retries(
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: fake_oid_lines(1),
            stderr: Vec::new(),
        }),
        "BASE_UNAVAILABLE",
    );
}

#[test]
fn wip_fast_import_too_many_oids_does_not_publish_and_retries() {
    failed_build_retries(
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: fake_oid_lines(3),
            stderr: Vec::new(),
        }),
        "BASE_UNAVAILABLE",
    );
}

#[test]
fn wip_fast_import_noncanonical_stdout_does_not_publish_and_retries() {
    let mut stdout = fake_oid_lines(1);
    stdout.extend_from_slice(b"E69DE29BB2D1D6434B8B29AE775AD8C2E48C5391\n");
    failed_build_retries(
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        }),
        "BASE_UNAVAILABLE",
    );
}

#[test]
fn wip_fast_import_timeout_keeps_process_error_and_retries() {
    failed_build_retries(
        Err(ProcessError::DeadlineExceeded {
            deadline: Duration::from_secs(30),
        }
        .into()),
        "PROCESS",
    );
}

#[test]
fn wip_fast_import_output_limit_keeps_process_error_and_retries() {
    failed_build_retries(
        Err(ProcessError::OutputLimitExceeded {
            stream: ProcessStream::Stdout,
            limit: 64 * 1024 * 1024,
        }
        .into()),
        "PROCESS",
    );
}
