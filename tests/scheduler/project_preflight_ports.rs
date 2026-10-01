use crate::{
    support::GitRepo,
    task_diagnostics_ports::runtime,
    task_ports_fixture::{self as fixture, TaskRemote},
};
use mac_worker::{
    client_state::ClientStateStore,
    error::WorkerError,
    host_store::HostStore,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::{ProjectPreparationError, ProjectPreparationRequest, ProjectState},
    scheduler::WorkerPreference,
    task_client::TaskClient,
    turn_runner::InlineRunnerExecutor,
};
use std::{
    ffi::OsStr,
    fs,
    os::unix::process::ExitStatusExt,
    path::Path,
    process::ExitStatus,
    sync::{Arc, Mutex, atomic::Ordering},
};

fn repo(settings: &[u8]) -> GitRepo {
    let repo = GitRepo::init();
    repo.write(".worker.toml", settings);
    repo.write("README.md", b"tracked\n");
    repo.commit_all("preflight fixture");
    repo
}
fn assert_no_capture(cache: &Path) {
    for directory in [
        cache.join("snapshots/staging"),
        cache.join("snapshots/ready"),
    ] {
        if !directory.exists() {
            continue;
        }
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            assert_eq!(entry.file_name(), ".mac-worker-rooted-fs");
            assert!(fs::read_dir(entry.path()).unwrap().next().is_none());
        }
    }
}
#[test]
fn task_admission_cache_is_project_neutral_across_requirement_sets() {
    // Supersedes run_command::shared_admission_cache_remains_project_neutral_across_requirement_sets.
    let incompatible = repo(b"version = 1\nrequires = ['missing-capability']\n");
    let compatible = repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let runtime = runtime(temp.path());
    let store = ClientStateStore::open(&runtime.paths.state)
        .unwrap()
        .with_admission_clock(Arc::new(|| Ok(26_000)));
    let remote = TaskRemote::new(HostStore::open(&temp.path().join("host")).unwrap());
    let config = fixture::config(1);
    let client = TaskClient::new(
        &remote,
        &config,
        &runtime.paths,
        &store,
        &InlineRunnerExecutor,
    );
    let mut first = fixture::request(&incompatible, false);
    first.preference = WorkerPreference::Automatic;
    let error = client
        .submit(first, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert!(matches!(
        error,
        WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            ..
        }
    ));
    assert!(store.list_tasks().unwrap().is_empty());
    let mut second = fixture::request(&compatible, false);
    second.preference = WorkerPreference::Automatic;
    let submitted = client
        .submit(second, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let turn = store
        .queue_entry_for_task_turn(submitted.task_id())
        .unwrap()
        .unwrap()
        .job_id();
    let completed = fixture::run(
        &remote,
        &config,
        &runtime.paths,
        &store,
        submitted.task_id(),
        turn,
    )
    .unwrap();
    assert_eq!(completed.status().worker(), Some("mini-a"));
    assert_eq!(remote.probe_count.load(Ordering::SeqCst), 1);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
}

#[derive(Default)]
struct LocalGit {
    requests: Mutex<Vec<ProcessRequest>>,
    fail_index_query: bool,
}
impl ProcessRunner for LocalGit {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        assert_eq!(
            request.program,
            OsStr::new("/usr/bin/git"),
            "preflight crossed remote boundary"
        );
        if self.fail_index_query && request.args.iter().any(|arg| arg == "ls-files") {
            return Ok(ProcessResult {
                status: ExitStatus::from_raw(2 << 8),
                stdout: Vec::new(),
                stderr: b"planted Git index failure".to_vec(),
            });
        }
        SystemProcessRunner.run(request)
    }
}
impl LocalGit {
    fn assert_no_writes(&self) {
        let requests = self.requests.lock().unwrap();
        assert!(!requests.is_empty());
        for request in requests.iter() {
            assert_eq!(request.program, OsStr::new("/usr/bin/git"));
            assert!(!request.args.iter().any(|arg| matches!(
                arg.to_str(),
                Some(
                    "fast-import"
                        | "hash-object"
                        | "update-index"
                        | "write-tree"
                        | "commit-tree"
                        | "update-ref"
                        | "fetch"
                        | "push"
                )
            )));
        }
    }
}
#[test]
fn invalid_artifact_configuration_stops_task_preflight_before_remote_or_capture_effects() {
    // Ports shared preflight ordering from run_command::artifact_configuration_stops_before_every_remote_or_capture_effect; ARTIFACTS_UNSUPPORTED is batch-only.
    let repo =
        repo(b"version = 1\n[artifacts]\ninclude = ['../target/**']\nmax_total_bytes = 10\n");
    let temp = tempfile::tempdir().unwrap();
    let runtime = runtime(temp.path());
    let store = ClientStateStore::open(&runtime.paths.state).unwrap();
    let runner = LocalGit::default();
    let config = fixture::config(1);
    let error = TaskClient::new(
        &runner,
        &config,
        &runtime.paths,
        &store,
        &InlineRunnerExecutor,
    )
    .submit(
        fixture::request(&repo, false),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert!(matches!(error, WorkerError::Config(_)));
    assert_eq!(error.exit_code(), 64);
    runner.assert_no_writes();
    assert!(store.list_jobs().unwrap().is_empty());
    assert!(store.list_tasks().unwrap().is_empty());
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert!(!runtime.paths.cache.join("snapshots").exists());
}
fn preparation_failure(fail_git: bool) -> ProjectPreparationError {
    let repo = repo(b"version = 1\n");
    if !fail_git {
        repo.write("untracked.txt", b"declare me explicitly\n");
    }
    let temp = tempfile::tempdir().unwrap();
    let runtime = runtime(temp.path());
    let store = ClientStateStore::open(&runtime.paths.state).unwrap();
    let runner = LocalGit {
        fail_index_query: fail_git,
        ..Default::default()
    };
    let state = ProjectState::load(&runner, repo.root(), &[]).unwrap();
    let failure = ProjectState::prepare(
        &runner,
        &runtime.paths.cache,
        ProjectPreparationRequest {
            project: repo.root().into(),
            cli_includes: Vec::new(),
        },
        &state,
    )
    .unwrap_err();
    runner.assert_no_writes();
    assert_no_capture(&runtime.paths.cache);
    assert!(store.list_jobs().unwrap().is_empty());
    assert!(store.list_tasks().unwrap().is_empty());
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    failure
}
#[test]
fn project_preparation_reports_actionable_untracked_input_before_capture() {
    // Ports shared selection safety from run_command::user_correctable_input_selection_failures_are_project_usage_errors; batch exit mapping stays in that test.
    match preparation_failure(false) {
        ProjectPreparationError::Selection(failure) => {
            assert_eq!(failure.code, "UNTRACKED_INPUT");
            assert_eq!(failure.total_path_count, 1);
            assert_eq!(failure.paths[0].as_str(), "untracked.txt");
        }
        other => panic!("actionable input failure lost its selection identity: {other}"),
    }
}
#[test]
fn project_preparation_preserves_operational_git_selection_failure_before_capture() {
    // Ports shared selection safety from run_command::operational_git_selection_failures_remain_snapshot_infrastructure_errors; batch exit mapping stays in that test.
    match preparation_failure(true) {
        ProjectPreparationError::Selection(failure) => {
            assert_eq!(failure.code, "GIT_INPUT_SELECTION_FAILED");
            assert!(failure.paths.is_empty());
        }
        other => panic!("operational Git failure lost its identity: {other}"),
    }
}
