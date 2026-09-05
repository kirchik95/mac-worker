#[allow(dead_code)]
mod support;

use std::os::unix::process::ExitStatusExt;

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    client_state::ClientStateStore,
    git_transport::GitTransport,
    process::ProcessResult,
    project::ProjectInspector,
    rooted_fs::RootedDir,
    task::{
        BranchName, ClosePolicy, GitIdentity, PublishMode, RunId, RunRecord, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskSource,
    },
    transfer_repo::TransferRepo,
};
use support::GitRepo;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn task_meta(source: TaskSource, publish: Vec<PublishMode>) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id: TaskId::new(Uuid::from_u128(1)),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        policy: PermissionPolicy::Workspace,
        source,
        publish,
        publish_branch: Some("release-candidate".parse().unwrap()),
        base_oid: "0123456789012345678901234567890123456789".parse().unwrap(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "publish the change".into(),
        created_at_millis: 100,
    })
    .unwrap()
}

#[test]
fn origin_source_and_push_are_recorded_as_one_canonical_scope() {
    let origin = task_meta(
        TaskSource::Origin {
            url: "https://example.test/repo.git".into(),
        },
        vec![PublishMode::Fetch, PublishMode::Push],
    );
    let value = serde_json::to_value(origin).unwrap();
    assert_eq!(value["source"]["url"], "https://example.test/repo.git");
    assert_eq!(value["publish"], serde_json::json!(["fetch", "push"]));
    assert_eq!(value["publish_branch"], "release-candidate");
}

#[test]
fn an_origin_url_must_be_normalized_before_it_enters_the_task_record() {
    let result = TaskMeta::new(TaskMetaInput {
        task_id: TaskId::new(Uuid::from_u128(2)),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Origin {
            url: "https://user:secret@EXAMPLE.test/repo.git?token=secret".into(),
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: "0123456789012345678901234567890123456789".parse().unwrap(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "publish the change".into(),
        created_at_millis: 100,
    });
    assert_eq!(result.unwrap_err().public_code(), "TASK_CONFIG_INVALID");
}

#[test]
fn origin_preflight_requires_the_exact_advertised_object_and_redacts_the_url() {
    let runner = support::recording_runner::RecordingRunner::returning(ProcessResult {
        status: std::process::ExitStatus::from_raw(0),
        stdout: b"0123456789012345678901234567890123456788 refs/heads/main\n".to_vec(),
        stderr: b"remote diagnostic with secret\n".to_vec(),
    });
    let base: mac_worker::task::BaseOid =
        "0123456789012345678901234567890123456789".parse().unwrap();
    let error = GitTransport::new(&runner)
        .preflight_origin(
            "https://user:secret@EXAMPLE.test/repo.git?token=secret",
            &base,
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "BASE_NOT_ON_ORIGIN");
    let request = runner.single_request();
    assert_eq!(request.args[0], "ls-remote");
    assert_eq!(
        request.args[1], "https://example.test/repo.git",
        "only the normalized origin may reach Git"
    );
    assert!(!format!("{error:?}").contains("secret"));
}

#[test]
fn origin_base_reference_resolution_is_read_only() {
    let repo = GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let before = mac_worker::transfer_repo::RepositoryFingerprint::capture(repo.root()).unwrap();
    let context = ProjectInspector::new(&mac_worker::process::SystemProcessRunner)
        .inspect(repo.root())
        .unwrap();
    let base =
        TransferRepo::resolve_base_oid(&mac_worker::process::SystemProcessRunner, &context, "HEAD")
            .unwrap();
    let after = mac_worker::transfer_repo::RepositoryFingerprint::capture(repo.root()).unwrap();
    assert_eq!(base.as_str().len(), 40);
    assert_eq!(
        before, after,
        "origin resolution must not write the user repo"
    );
}

#[test]
fn origin_fetch_uses_only_the_normalized_url_and_exact_base_oid() {
    let mirror_root = tempfile::tempdir().unwrap();
    let mirror = RootedDir::create(&mirror_root.path().join("mirror")).unwrap();
    let runner = support::recording_runner::RecordingRunner::returning_success();
    let base: mac_worker::task::BaseOid =
        "0123456789012345678901234567890123456789".parse().unwrap();
    mac_worker::git_transport::GitTransport::new(&runner)
        .fetch_origin(
            "https://user:secret@EXAMPLE.test/repo.git?token=secret",
            &base,
            &mirror,
        )
        .unwrap();
    let request = runner.single_request();
    let args = request
        .args
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(args.windows(2).any(|pair| {
        pair == [
            "-C".to_owned(),
            mirror.path().to_string_lossy().into_owned(),
        ]
    }));
    assert!(
        args.iter()
            .any(|arg| arg == "https://example.test/repo.git")
    );
    assert!(args.iter().any(|arg| arg == base.as_str()));
    assert!(!args.iter().any(|arg| arg.contains("secret")));
}

#[test]
fn origin_push_uses_the_fixed_task_ref_and_normalized_destination() {
    let mirror_root = tempfile::tempdir().unwrap();
    let mirror = RootedDir::create(&mirror_root.path().join("mirror")).unwrap();
    let runner = support::recording_runner::RecordingRunner::returning_success();
    let task = TaskId::new(Uuid::from_u128(9));
    let branch: BranchName = "release-candidate".parse().unwrap();
    GitTransport::new(&runner)
        .push_origin(
            "https://user:secret@EXAMPLE.test/repo.git?token=secret",
            task,
            &branch,
            &mirror,
        )
        .unwrap();
    let request = runner.single_request();
    let args = request
        .args
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        args.iter()
            .any(|arg| arg == "https://example.test/repo.git")
    );
    assert!(
        args.iter()
            .any(|arg| { arg == &format!("refs/heads/task/{task}:refs/heads/release-candidate") })
    );
    assert!(!args.iter().any(|arg| arg.contains("secret")));
}

#[test]
fn origin_push_failure_has_a_stable_code_without_remote_output() {
    let mirror_root = tempfile::tempdir().unwrap();
    let mirror = RootedDir::create(&mirror_root.path().join("mirror")).unwrap();
    let runner = support::recording_runner::RecordingRunner::returning(ProcessResult {
        status: std::process::ExitStatus::from_raw(1 << 8),
        stdout: Vec::new(),
        stderr: b"remote secret diagnostic\n".to_vec(),
    });
    let task = TaskId::new(Uuid::from_u128(9));
    let branch: BranchName = "release-candidate".parse().unwrap();
    let error = GitTransport::new(&runner)
        .push_origin(
            "https://user:secret@EXAMPLE.test/repo.git?token=secret",
            task,
            &branch,
            &mirror,
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "PUBLISH_FAILED");
    assert_eq!(
        error.exit_kind(),
        mac_worker::error::ExitKind::Infrastructure
    );
    assert!(!format!("{error:?}").contains("secret"));
}

#[test]
fn client_state_atomically_reserves_a_publish_branch_within_a_run() {
    let root = tempfile::tempdir().unwrap();
    let state_root = root.path().canonicalize().unwrap().join("state");
    let state = ClientStateStore::open(&state_root).unwrap();
    let run_id = RunId::new(Uuid::from_u128(10));
    state
        .create_run(RunRecord::new(run_id, None, Vec::new(), 2, 100).unwrap())
        .unwrap();
    let branch: BranchName = "release-candidate".parse().unwrap();
    state
        .reserve_run_publish_branch(run_id, branch.clone())
        .unwrap();
    let duplicate = state
        .reserve_run_publish_branch(run_id, branch)
        .unwrap_err();
    assert_eq!(duplicate.public_code(), "TASK_CONFIG_INVALID");
    assert_eq!(state.load_run(run_id).unwrap().publish_branches().len(), 1);
}
