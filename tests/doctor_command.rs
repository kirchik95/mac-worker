#[allow(dead_code)]
mod support;

use std::{
    collections::VecDeque,
    ffi::OsStr,
    fs,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use mac_worker::{
    config::{Config, WorkerEntry},
    doctor::{DoctorRequest, DoctorService},
    error::WorkerError,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    protocol::{HealthStatus, IssueSeverity},
};

use support::GitRepo;

#[derive(Clone)]
struct DoctorRunner {
    ssh_results: Arc<Mutex<VecDeque<Result<ProcessResult, WorkerError>>>>,
    mutation: Option<Arc<SnapshotMutation>>,
}

struct SnapshotMutation {
    root: PathBuf,
    stage_queries: AtomicUsize,
}

impl DoctorRunner {
    fn new(ssh_results: Vec<Result<ProcessResult, WorkerError>>) -> Self {
        Self {
            ssh_results: Arc::new(Mutex::new(ssh_results.into())),
            mutation: None,
        }
    }

    fn mutating_snapshot(
        root: &Path,
        ssh_results: Vec<Result<ProcessResult, WorkerError>>,
    ) -> Self {
        Self {
            ssh_results: Arc::new(Mutex::new(ssh_results.into())),
            mutation: Some(Arc::new(SnapshotMutation {
                root: root.to_path_buf(),
                stage_queries: AtomicUsize::new(0),
            })),
        }
    }
}

impl ProcessRunner for DoctorRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/ssh") {
            return self
                .ssh_results
                .lock()
                .unwrap()
                .pop_front()
                .expect("one SSH result per configured worker");
        }

        if request.program == OsStr::new("/usr/bin/git")
            && request.args.iter().any(|argument| argument == "ls-files")
            && request.args.iter().any(|argument| argument == "--stage")
            && let Some(mutation) = &self.mutation
            && mutation.stage_queries.fetch_add(1, Ordering::SeqCst) == 1
        {
            fs::write(
                mutation.root.join("tracked.txt"),
                b"changed after materialization\n",
            )?;
        }

        SystemProcessRunner.run(request)
    }
}

fn exit_status(code: i32) -> ExitStatus {
    ExitStatus::from_raw(code << 8)
}

fn ready_probe(capabilities: &[&str]) -> Result<ProcessResult, WorkerError> {
    let response = serde_json::json!({
        "protocol_version": 1,
        "hostname": "mini.local",
        "arch": "arm64",
        "os_version": "26.2",
        "free_disk_bytes": 536_870_912_u64,
        "memory_pressure": "normal",
        "swap_used_bytes": 134_217_728_u64,
        "capabilities": capabilities,
    });
    Ok(ProcessResult {
        status: exit_status(0),
        stdout: serde_json::to_vec(&response).unwrap(),
        stderr: Vec::new(),
    })
}

fn offline_probe() -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: exit_status(255),
        stdout: Vec::new(),
        stderr: b"offline".to_vec(),
    })
}

fn worker(name: &str, ssh: &str, capabilities: &[&str]) -> WorkerEntry {
    WorkerEntry {
        name: name.into(),
        ssh: ssh.into(),
        slots: 1,
        capabilities: capabilities.iter().map(|value| (*value).into()).collect(),
        remote_binary: "~/.local/bin/worker".into(),
    }
}

fn config(workers: Vec<WorkerEntry>) -> Config {
    Config {
        version: 1,
        workers,
    }
}

fn paths(root: &Path) -> PathLayout {
    PathLayout {
        config: root.join("config.toml"),
        state: root.join("state"),
        cache: root.join("cache"),
        data: root.join("data"),
    }
}

fn inspect(
    repo: &GitRepo,
    state_root: &Path,
    config: &Config,
    runner: &DoctorRunner,
) -> Result<mac_worker::protocol::DoctorReport, WorkerError> {
    let paths = paths(state_root);
    DoctorService {
        runner,
        config,
        paths: &paths,
    }
    .inspect(DoctorRequest {
        project: repo.root().to_path_buf(),
        cli_includes: Vec::new(),
    })
}

fn assert_no_doctor_snapshot(cache: &Path) {
    for directory in [
        cache.join("snapshots/staging"),
        cache.join("snapshots/ready"),
    ] {
        if directory.exists() {
            for entry in fs::read_dir(&directory).unwrap() {
                let entry = entry.unwrap();
                assert_eq!(
                    entry.file_name(),
                    ".mac-worker-rooted-fs",
                    "doctor left an owned capture in {}",
                    directory.display()
                );
                assert!(entry.file_type().unwrap().is_dir());
                assert!(fs::read_dir(entry.path()).unwrap().next().is_none());
            }
        }
    }
}

#[test]
fn clean_project_snapshot_and_eligible_worker_are_ready_and_cleanup_the_capture() {
    // Catches losing configured requirement order, publishing a doctor-owned
    // capture, or deciding readiness without both snapshot and worker proof.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\nrequires = [\"swift\", \"darwin-arm64\"]\n",
    );
    repo.write("README.md", b"tracked\n");
    repo.write("Dockerfile", b"FROM scratch\n");
    repo.write("Package.swift", b"// package\n");
    repo.write("package.json", b"{}\n");
    repo.commit_all("ready doctor fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &["darwin-arm64", "git"])]);
    let runner = DoctorRunner::new(vec![ready_probe(&[
        "darwin-arm64",
        "git",
        "swift",
        "docker",
        "node",
    ])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert_eq!(report.version, 1);
    assert!(report.ready);
    assert_eq!(
        report.requirements,
        ["swift", "darwin-arm64", "docker", "node"]
    );
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert!(report.workers[0].missing_capabilities.is_empty());
    assert!(
        report
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| { snapshot.file_count == 5 && !snapshot.digest.is_empty() })
    );
    assert!(report.issues.is_empty());
    assert_eq!(
        report.project.display_name,
        repo.root().file_name().unwrap().to_string_lossy()
    );
    assert_eq!(report.project.relative_working_dir, "");
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains(&repo.root().to_string_lossy().into_owned())
    );
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn uncovered_untracked_input_is_a_bounded_typed_blocker_but_keeps_worker_health() {
    // Catches converting selection failures into an opaque error, leaking raw
    // control characters, or skipping independent worker diagnostics.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("untracked doctor fixture");
    repo.write(
        "local\ninput.txt",
        b"untracked contents must not be reported\n",
    );
    for index in 0..105 {
        repo.write(&format!("untracked/{index:03}.txt"), b"local\n");
    }
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &[])]);
    let runner = DoctorRunner::new(vec![ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(!report.ready);
    assert!(report.snapshot.is_none());
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert_eq!(report.issues.len(), 1);
    assert_eq!(report.issues[0].severity, IssueSeverity::Blocker);
    assert_eq!(report.issues[0].code, "UNTRACKED_INPUT");
    assert_eq!(report.issues[0].paths.len(), 100);
    assert_eq!(report.issues[0].paths[0], "local\\ninput.txt");
    assert!(!report.issues[0].message.contains("untracked contents"));
    assert!(!report.issues[0].message.contains('\n'));
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn explicitly_allowed_sensitive_path_is_only_a_content_free_warning() {
    // Catches allowlisting either blocking readiness or copying secret bytes
    // into the typed report instead of reporting only the relative path.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\n[snapshot]\nallow_sensitive = [\".env\"]\n",
    );
    repo.write(".env", b"DOCTOR_PLANTED_SECRET=never-report-this\n");
    repo.commit_all("allowed sensitive fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &[])]);
    let runner = DoctorRunner::new(vec![ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();
    let serialized = serde_json::to_string(&report).unwrap();

    assert!(report.ready);
    assert!(report.snapshot.is_some());
    assert_eq!(report.issues.len(), 1);
    assert_eq!(report.issues[0].severity, IssueSeverity::Warning);
    assert_eq!(report.issues[0].code, "SENSITIVE_PATH_ALLOWED");
    assert_eq!(report.issues[0].paths, [".env"]);
    assert!(!serialized.contains("DOCTOR_PLANTED_SECRET"));
    assert!(!serialized.contains("never-report-this"));
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn unsupported_submodule_lfs_and_custom_filters_block_before_capture() {
    // Catches weakening unsupported Git features into warnings or attempting
    // a partial capture after input preflight has already failed.
    for feature in ["submodule", "lfs", "filter"] {
        let repo = GitRepo::init();
        let expected = match feature {
            "submodule" => {
                repo.write("README.md", b"root\n");
                repo.commit_all("submodule fixture");
                let head = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
                    .unwrap()
                    .trim()
                    .to_owned();
                assert!(
                    repo.git(&[
                        "update-index",
                        "--add",
                        "--cacheinfo",
                        &format!("160000,{head},vendor/submodule"),
                    ])
                    .status
                    .success()
                );
                "UNSUPPORTED_SUBMODULE"
            }
            "lfs" => {
                repo.write(".gitattributes", b"*.bin filter=lfs\n");
                repo.write("asset.bin", b"materialized bytes\n");
                repo.commit_all("lfs fixture");
                "UNSUPPORTED_LFS"
            }
            "filter" => {
                assert!(
                    repo.git(&["config", "filter.custom.clean", "cat"])
                        .status
                        .success()
                );
                "UNSUPPORTED_FILTER"
            }
            _ => unreachable!(),
        };
        let state = tempfile::tempdir().unwrap();
        let config = config(vec![worker("mini-1", "mac1", &[])]);
        let runner = DoctorRunner::new(vec![ready_probe(&[])]);

        let report = inspect(&repo, state.path(), &config, &runner).unwrap();

        assert!(!report.ready, "{feature}");
        assert!(report.snapshot.is_none(), "{feature}");
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.code == expected && issue.severity == IssueSeverity::Blocker),
            "{feature}: {:?}",
            report.issues
        );
        assert_eq!(report.workers[0].status, HealthStatus::Ready);
        assert_no_doctor_snapshot(&state.path().join("cache"));
    }
}

#[test]
fn zero_eligible_workers_adds_a_blocker_without_losing_worker_diagnostics() {
    // Catches treating an all-ineligible inventory as harmless warnings or
    // collapsing its distinct capability and transport diagnostics.
    let repo = GitRepo::init();
    repo.write(".worker.toml", b"version = 1\nrequires = [\"node\"]\n");
    repo.write("README.md", b"tracked\n");
    repo.commit_all("ineligible workers fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![
        worker("missing", "mac1", &["darwin-arm64"]),
        worker("offline", "mac2", &["darwin-arm64"]),
    ]);
    let runner = DoctorRunner::new(vec![ready_probe(&["darwin-arm64"]), offline_probe()]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(!report.ready);
    assert!(report.snapshot.is_some());
    assert_eq!(report.workers.len(), 2);
    assert_eq!(
        report.workers[0].error_code.as_deref(),
        Some("MISSING_CAPABILITIES")
    );
    assert_eq!(report.workers[0].missing_capabilities, ["node"]);
    assert_eq!(
        report.workers[1].error_code.as_deref(),
        Some("SSH_UNAVAILABLE")
    );
    assert!(report.issues.iter().any(|issue| {
        issue.code == "NO_ELIGIBLE_WORKER" && issue.severity == IssueSeverity::Blocker
    }));
    assert!(!report.issues.iter().any(|issue| {
        issue.code == "SSH_UNAVAILABLE" && issue.severity == IssueSeverity::Warning
    }));
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn offline_worker_is_a_sorted_warning_when_another_worker_is_eligible() {
    // Catches one failed probe blocking a usable inventory and catches issue
    // ordering drifting away from severity, code, then first path.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\n[snapshot]\nallow_sensitive = [\".env\"]\n",
    );
    repo.write(".env", b"fixture\n");
    repo.commit_all("partial worker health fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![
        worker("offline", "mac1", &[]),
        worker("ready", "mac2", &[]),
    ]);
    let runner = DoctorRunner::new(vec![offline_probe(), ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(report.ready);
    assert_eq!(report.workers[0].status, HealthStatus::Unavailable);
    assert_eq!(report.workers[1].status, HealthStatus::Ready);
    assert_eq!(
        report
            .issues
            .iter()
            .map(|issue| (issue.severity, issue.code.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (IssueSeverity::Warning, "SENSITIVE_PATH_ALLOWED"),
            (IssueSeverity::Warning, "SSH_UNAVAILABLE"),
        ]
    );
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn snapshot_change_is_a_typed_blocker_with_no_published_snapshot() {
    // Catches returning a stale summary after the selected source changes and
    // catches local integrity failure erasing independent worker health.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"initial bytes\n");
    repo.commit_all("snapshot mutation fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &[])]);
    let runner = DoctorRunner::mutating_snapshot(repo.root(), vec![ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(!report.ready);
    assert!(report.snapshot.is_none());
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert!(report.issues.iter().any(|issue| {
        issue.code == "SNAPSHOT_CHANGED" && issue.severity == IssueSeverity::Blocker
    }));
    assert_no_doctor_snapshot(&state.path().join("cache"));
}
