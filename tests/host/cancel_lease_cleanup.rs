//! Cancellation must not leave `LEASE_RELEASE_FAILED` on a job whose worker
//! slot is already free. Waiting and dispatching rows never acquire a lease;
//! a running cancel that wins the release must stay clean when a second
//! cancel retries the same identity.

use std::{sync::Mutex, time::Duration};

use mac_worker::{
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    error::WorkerError,
    job::{JobId, ProcessIdentity, QueueEntry, QueueEntryKind, QueueState},
    scheduler::WorkerPreference,
};

use crate::support;

fn config() -> Config {
    Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        controller: Default::default(),
        ssh: Default::default(),
        workers: vec![WorkerEntry {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            slots: 1,
            capabilities: Vec::new(),
            remote_binary: "~/.local/bin/worker".into(),
            herdr: false,
        }],
    }
}

fn assert_no_host_lease(temp: &tempfile::TempDir) {
    assert!(
        !temp.path().join("leases").exists(),
        "queued cancellation must not create a host lease"
    );
}

mod task_ports {
    use super::*;
    use mac_worker::{
        agent::{AgentKind, PermissionPolicy},
        job::CommandSummary,
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
        project_state::ProjectState,
        supervisor::SystemProcessInspector,
        task::{
            BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunnerIdentity,
            TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState,
            TaskStatus, TurnSummary,
        },
        task_client::TaskClient,
        turn_runner::InlineRunnerExecutor,
    };
    use std::path::PathBuf;

    struct CurrentDir(PathBuf);
    impl Drop for CurrentDir {
        fn drop(&mut self) {
            std::env::set_current_dir(&self.0).unwrap();
        }
    }

    #[derive(Default)]
    struct LocalGitOnlyRunner {
        unexpected: Mutex<Vec<ProcessRequest>>,
    }
    impl ProcessRunner for LocalGitOnlyRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.program == "/usr/bin/git" {
                return SystemProcessRunner.run(request);
            }
            self.unexpected.lock().unwrap().push(request.clone());
            Err(WorkerError::Protocol(
                "queued cancel attempted nonlocal work".into(),
            ))
        }
    }

    fn queued_task(
        store: &ClientStateStore,
        project: &ProjectState,
        seed: u128,
        owner: ProcessIdentity,
    ) -> (TaskId, JobId) {
        let task = TaskId::new(uuid::Uuid::from_u128(seed + 10_000));
        let turn = JobId::new(uuid::Uuid::from_u128(seed));
        let base: BaseOid = "a".repeat(40).parse().unwrap();
        let meta = TaskMeta::new(TaskMetaInput {
            task_id: task,
            run_id: None,
            project_id: project.context.project_id.clone(),
            worktree_id: project.context.worktree_id.clone(),
            agent: AgentKind::Codex,
            model: None,
            effort: None,
            policy: PermissionPolicy::Workspace,
            source: TaskSource::Local {
                wip: false,
                push_target: None,
            },
            publish: vec![PublishMode::Fetch],
            publish_branch: None,
            base_oid: base.clone(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Cancel Test", "cancel@example.test").unwrap(),
            title: None,
            prompt: "queued cancel".into(),
            created_at_millis: 1,
        })
        .unwrap();
        let status = TaskStatus::new(
            TaskState::Queued,
            None,
            Some("mini-1".into()),
            false,
            Some(base),
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1,
                turn,
                None,
                None,
                None,
                false,
                Some(1),
                None,
            )],
            1,
        )
        .unwrap();
        store
            .create_task(
                LocalTaskRecord::new(
                    meta,
                    status,
                    None,
                    Some(RunnerIdentity::new(owner)),
                    None,
                    project.context.project_id.clone(),
                    None,
                    true,
                    None,
                )
                .unwrap(),
            )
            .unwrap();
        store
            .write_turn_prompt(task, turn, "queued cancel")
            .unwrap();
        drop(store.open_runner_log(task, turn).unwrap());
        store
            .enqueue(
                QueueEntry::new(
                    turn,
                    store.client_id(),
                    project.context.project_id.clone(),
                    project.context.worktree_id.clone(),
                    CommandSummary::shell(),
                    Vec::new(),
                    WorkerPreference::Automatic,
                    QueueEntryKind::TaskTurn,
                    None,
                    owner,
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        (task, turn)
    }

    fn fixture() -> (
        support::GitRepo,
        tempfile::TempDir,
        CurrentDir,
        ProjectState,
        ProcessIdentity,
    ) {
        let repo = support::GitRepo::init();
        repo.write("base.txt", b"base\n");
        repo.commit_all("base");
        let cwd = CurrentDir(std::env::current_dir().unwrap());
        std::env::set_current_dir(repo.root()).unwrap();
        let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
        let owner = SystemProcessInspector
            .identity_for_pid(std::process::id())
            .unwrap();
        (repo, tempfile::tempdir().unwrap(), cwd, project, owner)
    }

    #[test]
    // Supersedes v1 test: cancel_waiting_row_leaves_no_lease_and_no_remote_work.
    fn task_cancel_waiting_row_leaves_no_lease_and_no_remote_work() {
        let (_repo, temp, _cwd, project, owner) = fixture();
        let paths = support::task_harness::paths(temp.path().canonicalize().unwrap());
        let store = ClientStateStore::open(&paths.state).unwrap();
        let (task, _turn) = queued_task(&store, &project, 91_001, owner);
        let runner = LocalGitOnlyRunner::default();
        let config = config();
        let report = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor)
            .cancel(task)
            .unwrap();
        assert_eq!(report.task_id(), task);
        assert_eq!(report.status().state(), TaskState::Abandoned);
        assert_eq!(
            report.status().last_outcome(),
            Some(&TaskOutcome::failed("CANCELLED"))
        );
        assert!(store.queue_snapshot().unwrap().entries().is_empty());
        assert!(runner.unexpected.lock().unwrap().is_empty());
        assert_no_host_lease(&temp);
        assert!(!paths.host_state_root().exists());
    }

    #[test]
    // Supersedes v1 test: cancel_dispatching_row_requests_cancel_without_acquiring_or_releasing_a_lease.
    fn task_cancel_dispatching_row_requests_cancel_without_acquiring_or_releasing_a_lease() {
        let (_repo, temp, _cwd, project, owner) = fixture();
        let paths = support::task_harness::paths(temp.path().canonicalize().unwrap());
        let store = ClientStateStore::open(&paths.state).unwrap();
        let (task, turn) = queued_task(&store, &project, 91_101, owner);
        store
            .claim_task_turn(owner, turn, &["mini-1".into()], 2)
            .unwrap()
            .expect("owner must claim its waiting row");
        let runner = LocalGitOnlyRunner::default();
        let config = config();
        let started = std::time::Instant::now();
        let error = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor)
            .cancel(task)
            .unwrap_err();
        assert_eq!(error.public_code(), "TASK_BUSY");
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(runner.unexpected.lock().unwrap().is_empty());
        let queue = store.queue_snapshot().unwrap();
        assert_eq!(queue.entries().len(), 1);
        assert!(matches!(queue.entries()[0].state(),
            QueueState::Dispatching { dispatch_owner, .. } if *dispatch_owner == owner));
        assert!(queue.entries()[0].is_cancel_requested());
        assert_no_host_lease(&temp);
        assert!(!paths.host_state_root().exists());
    }

    #[test]
    // Supersedes v1 test: cancel_waiting_then_cancel_again_stays_local_and_lease_free.
    fn task_cancel_waiting_then_cancel_again_stays_local_and_lease_free() {
        let (_repo, temp, _cwd, project, owner) = fixture();
        let paths = support::task_harness::paths(temp.path().canonicalize().unwrap());
        let store = ClientStateStore::open(&paths.state).unwrap();
        let (task, _turn) = queued_task(&store, &project, 91_201, owner);
        let runner = LocalGitOnlyRunner::default();
        let config = config();
        let client = TaskClient::new(&runner, &config, &paths, &store, &InlineRunnerExecutor);
        let first = client.cancel(task).unwrap();
        let second = client.cancel(task).unwrap();
        assert_eq!(first.task_id(), task);
        assert_eq!(second.task_id(), task);
        assert_eq!(first.status(), second.status());
        assert_eq!(second.status().state(), TaskState::Abandoned);
        assert!(store.queue_snapshot().unwrap().entries().is_empty());
        assert!(runner.unexpected.lock().unwrap().is_empty());
        assert_no_host_lease(&temp);
        assert!(!paths.host_state_root().exists());
    }
}
