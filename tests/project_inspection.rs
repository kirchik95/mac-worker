mod support;

use std::{
    collections::VecDeque, os::unix::process::ExitStatusExt, path::PathBuf, process::ExitStatus,
    sync::Mutex,
};

use mac_worker::{
    error::WorkerError,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project::ProjectInspector,
};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

use support::{GitRepo, create_directory};

#[test]
fn inspection_uses_the_worktree_root_and_current_relative_directory() {
    // This catches snapshot requests being rooted at the caller's nested
    // directory instead of the Git worktree root.
    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"pub fn value() -> u8 { 1 }\n");
    repo.commit_all("initial");
    let nested = repo.root().join("src");

    let context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(&nested)
        .unwrap();

    assert_eq!(context.root, repo.root().canonicalize().unwrap());
    assert_eq!(context.relative_cwd, PathBuf::from("src"));
    assert_eq!(context.head.as_deref().map(str::len), Some(40));
    assert!(!context.project_id.is_empty());
    assert!(!context.worktree_id.is_empty());
}

#[test]
fn origin_credentials_never_change_or_appear_in_the_public_identity() {
    // This catches using a remote URL verbatim, which would leak credentials
    // and make the same project appear different for each authenticated URL.
    let repo = GitRepo::init();
    repo.git(&[
        "remote",
        "add",
        "origin",
        "https://alice:secret@example.com/acme/app.git",
    ]);
    let context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .unwrap();

    assert!(!context.project_id.contains("alice"));
    assert!(!context.project_id.contains("secret"));
    assert_eq!(context.project_id.len(), 64);
}

#[test]
fn inspection_and_fixture_ignore_ambient_git_config_overrides() {
    // This catches ambient GIT_CONFIG_COUNT entries overriding the local
    // origin, which would both contaminate fixture setup and change the public
    // project identity selected by inspection.
    const CHILD: &str = "PROJECT_INSPECTION_CONTAMINATION_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "inspection_and_fixture_ignore_ambient_git_config_overrides",
            ])
            .env(CHILD, "1")
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "remote.origin.url")
            .env(
                "GIT_CONFIG_VALUE_0",
                "https://evil.example/overridden/project.git",
            )
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }

    let repo = GitRepo::init();
    assert!(
        repo.git(&[
            "config",
            "remote.origin.url",
            "https://example.com/acme/app.git",
        ])
        .status
        .success()
    );

    let context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .unwrap();
    let expected = format!(
        "{:x}",
        Sha256::digest(b"origin\0https://example.com/acme/app.git")
    );

    assert_eq!(context.project_id, expected);
}

#[test]
fn inspection_reports_a_detached_head_without_inventing_a_branch() {
    // This catches treating a detached commit as a branch named HEAD.
    let repo = GitRepo::init();
    repo.write("README.md", b"initial\n");
    repo.commit_all("initial");
    assert!(repo.git(&["checkout", "--detach"]).status.success());

    let context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .unwrap();

    assert_eq!(context.head.as_deref().map(str::len), Some(40));
    assert_eq!(context.branch, None);
}

#[test]
fn inspection_handles_an_unborn_repository() {
    // This catches rejecting a valid worktree merely because its first commit
    // has not yet been created.
    let repo = GitRepo::init();

    let context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .unwrap();

    assert_eq!(context.head, None);
    assert_eq!(context.branch.as_deref(), Some("main"));
    assert!(!context.dirty);
}

#[test]
fn no_origin_identity_is_derived_from_the_canonical_common_git_directory() {
    // This catches falling back to the worktree root, which would assign a
    // different project identity to linked worktrees without an origin.
    let repo = GitRepo::init();
    let context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .unwrap();
    let expected = format!(
        "{:x}",
        Sha256::digest(
            [
                b"common-dir\0".as_slice(),
                context.common_dir.as_os_str().as_encoded_bytes(),
            ]
            .concat()
        )
    );

    assert_eq!(context.project_id, expected);
}

#[test]
fn tracked_byte_changes_mark_the_worktree_dirty() {
    // This catches inspecting only staged changes and missing modifications
    // to already tracked bytes.
    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"pub fn value() -> u8 { 1 }\n");
    repo.commit_all("initial");
    repo.write("src/lib.rs", b"pub fn value() -> u8 { 2 }\n");

    let context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .unwrap();

    assert!(context.dirty);
}

#[test]
fn linked_worktrees_share_a_project_identity_but_have_unique_worktree_identities() {
    // This catches hashing the worktree root for the project identity, which
    // would prevent sharing immutable objects across linked worktrees.
    let repo = GitRepo::init();
    repo.write("README.md", b"initial\n");
    repo.commit_all("initial");
    let holder = tempdir().unwrap();
    let linked_path = holder.path().join("linked");
    assert!(
        repo.git(&[
            "worktree",
            "add",
            "-b",
            "linked-worktree",
            linked_path.to_str().unwrap(),
            "HEAD",
        ])
        .status
        .success()
    );

    let inspector = ProjectInspector::new(&SystemProcessRunner);
    let primary = inspector.inspect(repo.root()).unwrap();
    let linked = inspector.inspect(&linked_path).unwrap();

    assert_eq!(primary.project_id, linked.project_id);
    assert_ne!(primary.worktree_id, linked.worktree_id);
}

#[test]
fn non_repository_directory_returns_a_coded_project_error() {
    // This catches surfacing a machine-specific Git diagnostic instead of the
    // stable error boundary expected by callers.
    let directory = tempdir().unwrap();

    let error = ProjectInspector::new(&SystemProcessRunner)
        .inspect(directory.path())
        .unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Project {
            code: "NOT_A_WORKTREE",
            ..
        }
    ));
}

#[test]
fn multi_line_origin_output_is_rejected_as_a_coded_project_error() {
    // This catches accepting a newline-delimited remote value and hashing an
    // ambiguous identity that could conceal a second origin.
    let repo = GitRepo::init();
    assert!(
        repo.git(&[
            "config",
            "remote.origin.url",
            "https://example.com/acme/app.git\nhttps://evil.example/acme/app.git",
        ])
        .status
        .success()
    );

    let error = ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Project {
            code: "INVALID_GIT_OUTPUT",
            ..
        }
    ));
}

#[test]
fn caller_path_outside_discovered_root_returns_a_coded_project_error() {
    // This catches accepting a runner-reported root that does not contain the
    // requested directory, which could snapshot unrelated filesystem bytes.
    let directory = tempdir().unwrap();
    let root = create_directory(directory.path().join("worktree"));
    let outside = create_directory(directory.path().join("outside"));
    let runner = ScriptedRunner::new(vec![result(0, root.as_os_str().as_encoded_bytes())]);

    let error = ProjectInspector::new(&runner)
        .inspect(&outside)
        .unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Project {
            code: "PATH_OUTSIDE_WORKTREE",
            ..
        }
    ));
}

struct ScriptedRunner {
    results: Mutex<VecDeque<ProcessResult>>,
}

impl ScriptedRunner {
    fn new(results: Vec<ProcessResult>) -> Self {
        Self {
            results: Mutex::new(results.into()),
        }
    }
}

impl ProcessRunner for ScriptedRunner {
    fn run(&self, _request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        Ok(self
            .results
            .lock()
            .unwrap()
            .pop_front()
            .expect("a process result for each inspection request"))
    }
}

fn result(code: i32, stdout: &[u8]) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(code << 8),
        stdout: [stdout, b"\n"].concat(),
        stderr: Vec::new(),
    }
}
