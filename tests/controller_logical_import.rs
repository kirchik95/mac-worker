//! Logical controller result import guard: the controller-transfer cache is
//! a logical project/worktree hash, not a physical user repository.
//!
//! `import_result` must not demand physical repo identity from a logical
//! cache (`user_alternates == false`), while ordinary user transfers keep
//! rejecting foreign repositories and keep mandatory alternates verification.
//! Real Git only; no worker binary is spawned.

mod support;

use std::process::Command;

use mac_worker::{
    process::SystemProcessRunner,
    task::{BaseOid, TaskId},
    transfer_repo::{TransferRepo, repo_id_for},
};
use support::GitRepo;
use uuid::Uuid;

const RUNNER: SystemProcessRunner = SystemProcessRunner;
const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn task_n(n: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(n))
}

fn head_oid(repo: &GitRepo) -> BaseOid {
    String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn seed_result_ref(cache: &TransferRepo, source: &GitRepo, task: TaskId, oid: &BaseOid) {
    let spec = format!("{oid}:refs/mac-worker/results/{task}");
    let output = Command::new("/usr/bin/git")
        .current_dir(cache.path())
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(["fetch", "--quiet", "--no-write-fetch-head"])
        .arg(source.root().join(".git"))
        .arg(&spec)
        .output()
        .expect("seed fetch into controller cache");
    assert!(
        output.status.success(),
        "seed fetch failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_object_type(repo: &GitRepo, oid: &str) -> String {
    String::from_utf8_lossy(&repo.git(&["cat-file", "-t", oid]).stdout)
        .trim()
        .to_owned()
}

fn git_ref_oid(repo: &GitRepo, git_ref: &str) -> String {
    String::from_utf8_lossy(&repo.git(&["rev-parse", "--verify", git_ref]).stdout)
        .trim()
        .to_owned()
}

fn open_logical_cache(root: &std::path::Path) -> TransferRepo {
    let cache_root = support::create_directory(root.join("controller-cache"));
    TransferRepo::open_or_create_controller_cache(&cache_root, PROJECT_ID, WORKTREE_ID).unwrap()
}

#[test]
fn logical_cache_import_reaches_target_ref() {
    let temp = tempfile::tempdir().unwrap();
    let cache = open_logical_cache(temp.path());

    let source = GitRepo::init();
    source.write("result.txt", b"logical controller result\n");
    source.commit_all("result");
    let oid = head_oid(&source);
    let task = task_n(0x1001);
    seed_result_ref(&cache, &source, task, &oid);

    // The importing checkout is a different physical repository by design:
    // the logical cache identity is a project/worktree hash, never a repo id.
    let user = GitRepo::init();
    user.write("work.txt", b"unrelated checkout\n");
    user.commit_all("work");
    assert_ne!(
        repo_id_for(&user.root().join(".git")).unwrap(),
        cache.repo_id(),
        "physical user repo must differ from the logical cache identity"
    );

    let receipt = cache
        .import_result(&RUNNER, &user.root().join(".git"), "mini-1", task)
        .expect("logical cache import must succeed");
    assert_eq!(receipt.head(), &oid);
    let local_ref = format!("refs/remotes/mac-worker/mini-1/task/{task}");
    assert_eq!(receipt.local_ref(), local_ref);
    assert_eq!(git_ref_oid(&user, &local_ref), oid.as_str());
    assert_eq!(git_object_type(&user, oid.as_str()), "commit");
}

#[test]
fn ordinary_transfer_rejects_wrong_user_repo() {
    let temp = tempfile::tempdir().unwrap();
    let repo_a = GitRepo::init();
    repo_a.write("a.txt", b"a\n");
    repo_a.commit_all("a");
    let repo_b = GitRepo::init();
    repo_b.write("b.txt", b"b\n");
    repo_b.commit_all("b");

    let cache_root = support::create_directory(temp.path().join("user-cache"));
    let transfer = TransferRepo::open_or_create(&cache_root, &repo_a.root().join(".git")).unwrap();
    let error = transfer
        .import_result(
            &RUNNER,
            &repo_b.root().join(".git"),
            "mini-1",
            task_n(0x2001),
        )
        .expect_err("ordinary transfer must reject a foreign user repository");
    let message = error.to_string();
    assert!(
        message.contains("BASE_UNAVAILABLE"),
        "foreign repo must stay BASE_UNAVAILABLE, got: {message}"
    );
}

#[test]
fn alternates_protection_preserved() {
    let temp = tempfile::tempdir().unwrap();
    let repo = GitRepo::init();
    repo.write("a.txt", b"a\n");
    repo.commit_all("a");

    let cache_root = support::create_directory(temp.path().join("user-cache"));
    let transfer = TransferRepo::open_or_create(&cache_root, &repo.root().join(".git")).unwrap();
    std::fs::remove_file(transfer.path().join("objects/info/alternates")).unwrap();
    let error = transfer
        .import_result(&RUNNER, &repo.root().join(".git"), "mini-1", task_n(0x3001))
        .expect_err("missing alternates must stay a hard error");
    let message = error.to_string();
    assert!(
        message.contains("BASE_UNAVAILABLE"),
        "alternates protection must stay BASE_UNAVAILABLE, got: {message}"
    );
}
