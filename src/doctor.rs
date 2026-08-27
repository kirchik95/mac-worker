use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use crate::{
    config::Config,
    error::WorkerError,
    inputs::{InputSelector, SelectionFailure, SelectionWarning},
    paths::PathLayout,
    process::ProcessRunner,
    project::{ProjectContext, ProjectInspector},
    project_config::ProjectSettings,
    protocol::{
        DoctorIssue, DoctorProject, DoctorReport, HealthStatus, IssueSeverity, WorkerHealth,
    },
    requirements::RequirementDetector,
    snapshot::SnapshotBuilder,
    transport::{SshTransport, WorkersService},
};

const DOCTOR_REPORT_VERSION: u32 = 1;
const MAX_ISSUE_PATHS: usize = 100;
const MAX_ISSUE_MESSAGE_CHARACTERS: usize = 4_096;
const MAX_ISSUE_PATH_CHARACTERS: usize = 1_024;
const MAX_PROJECT_FIELD_CHARACTERS: usize = 1_024;
const MAX_DISPLAY_NAME_CHARACTERS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorRequest {
    pub project: PathBuf,
    pub cli_includes: Vec<String>,
}

pub struct DoctorService<'a> {
    pub runner: &'a dyn ProcessRunner,
    pub config: &'a Config,
    pub paths: &'a PathLayout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalProjectState {
    context: ProjectContext,
    settings: ProjectSettings,
    requirements: Vec<String>,
}

impl LocalProjectState {
    fn load(
        runner: &dyn ProcessRunner,
        project: &Path,
        cli_includes: &[String],
    ) -> Result<Self, WorkerError> {
        let context = ProjectInspector::new(runner).inspect(project)?;
        let settings = ProjectSettings::load(&context.root, cli_includes)?;
        let detected = RequirementDetector::detect(&context.root)?;
        let requirements = merge_project_requirements(&settings.requires, detected);
        Ok(Self {
            context,
            settings,
            requirements,
        })
    }
}

impl DoctorService<'_> {
    pub fn inspect(&self, request: DoctorRequest) -> Result<DoctorReport, WorkerError> {
        let before_probes =
            LocalProjectState::load(self.runner, &request.project, &request.cli_includes)?;

        let mut workers = WorkersService::new(SshTransport::new(self.runner))
            .inspect_with_requirements(self.config, &before_probes.requirements)
            .workers;
        let after_probes =
            LocalProjectState::load(self.runner, &request.project, &request.cli_includes)?;
        let eligible_worker_count = workers.iter().filter(|worker| is_eligible(worker)).count();
        let mut issues = worker_issues(&mut workers, eligible_worker_count);
        if before_probes != after_probes {
            issues.push(local_state_changed_issue());
            sort_issues(&mut issues);
            return Ok(doctor_report(
                &before_probes,
                None,
                workers,
                issues,
                eligible_worker_count,
            ));
        }

        let snapshot = match InputSelector::new(self.runner)
            .select(&after_probes.context, &after_probes.settings.snapshot)
        {
            Ok(selection) => {
                issues.extend(selection.warnings.iter().map(selection_warning_issue));
                match SnapshotBuilder::new(self.runner, &self.paths.cache).capture(
                    &after_probes.context,
                    &after_probes.settings.snapshot,
                    selection,
                ) {
                    Ok(snapshot) => {
                        let after_capture = match LocalProjectState::load(
                            self.runner,
                            &request.project,
                            &request.cli_includes,
                        ) {
                            Ok(state) => state,
                            Err(error) => {
                                return match snapshot.cleanup() {
                                    Ok(()) => Err(error),
                                    Err(cleanup_error) => Err(cleanup_error),
                                };
                            }
                        };
                        if after_probes == after_capture {
                            let summary = snapshot.summary();
                            snapshot.cleanup()?;
                            Some(summary)
                        } else {
                            snapshot.cleanup()?;
                            issues.push(local_state_changed_issue());
                            None
                        }
                    }
                    Err(WorkerError::Snapshot { code, message }) => {
                        issues.push(issue(IssueSeverity::Blocker, code, &message, Vec::new()));
                        None
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(failure) => {
                issues.push(selection_failure_issue(failure));
                None
            }
        };

        sort_issues(&mut issues);
        Ok(doctor_report(
            &after_probes,
            snapshot,
            workers,
            issues,
            eligible_worker_count,
        ))
    }
}

fn doctor_report(
    state: &LocalProjectState,
    snapshot: Option<crate::snapshot::SnapshotSummary>,
    workers: Vec<WorkerHealth>,
    issues: Vec<DoctorIssue>,
    eligible_worker_count: usize,
) -> DoctorReport {
    let ready = snapshot.is_some()
        && eligible_worker_count > 0
        && !issues
            .iter()
            .any(|issue| issue.severity == IssueSeverity::Blocker);
    DoctorReport {
        version: DOCTOR_REPORT_VERSION,
        ready,
        project: doctor_project(&state.context),
        requirements: state.requirements.clone(),
        snapshot,
        workers,
        issues,
    }
}

fn local_state_changed_issue() -> DoctorIssue {
    issue(
        IssueSeverity::Blocker,
        "SNAPSHOT_CHANGED",
        "project policy, requirements, or Git metadata changed during doctor inspection",
        Vec::new(),
    )
}

fn merge_project_requirements(configured: &[String], mut detected: Vec<String>) -> Vec<String> {
    detected.sort();
    detected.dedup();

    let mut seen = HashSet::new();
    configured
        .iter()
        .chain(&detected)
        .filter(|requirement| seen.insert(requirement.as_str()))
        .cloned()
        .collect()
}

fn doctor_project(context: &ProjectContext) -> DoctorProject {
    let display_name = context
        .root
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "project".into());
    DoctorProject {
        display_name: sanitize_bounded(&display_name, MAX_DISPLAY_NAME_CHARACTERS),
        project_id: context.project_id.clone(),
        worktree_id: context.worktree_id.clone(),
        head: context
            .head
            .as_deref()
            .map(|head| sanitize_bounded(head, MAX_PROJECT_FIELD_CHARACTERS)),
        branch: context
            .branch
            .as_deref()
            .map(|branch| sanitize_bounded(branch, MAX_PROJECT_FIELD_CHARACTERS)),
        dirty: context.dirty,
        relative_working_dir: sanitize_bounded(
            &context.relative_cwd.to_string_lossy(),
            MAX_PROJECT_FIELD_CHARACTERS,
        ),
    }
}

fn worker_issues(workers: &mut [WorkerHealth], eligible_worker_count: usize) -> Vec<DoctorIssue> {
    let mut issues = Vec::new();
    for worker in workers.iter_mut().filter(|worker| !is_eligible(worker)) {
        let code = worker
            .error_code
            .as_deref()
            .unwrap_or("WORKER_UNAVAILABLE")
            .to_owned();
        let message = safe_worker_message(worker, &code);
        worker.error_message = Some(message.clone());
        if eligible_worker_count > 0 {
            issues.push(issue(IssueSeverity::Warning, &code, &message, Vec::new()));
        }
    }
    if eligible_worker_count == 0 {
        issues.push(issue(
            IssueSeverity::Blocker,
            "NO_ELIGIBLE_WORKER",
            "no configured worker is ready with every required capability",
            Vec::new(),
        ));
    }
    issues
}

fn is_eligible(worker: &WorkerHealth) -> bool {
    worker.status == HealthStatus::Ready
        && worker.probe.is_some()
        && worker.missing_capabilities.is_empty()
}

fn safe_worker_message(worker: &WorkerHealth, code: &str) -> String {
    let name = sanitize_bounded(&worker.name, MAX_DISPLAY_NAME_CHARACTERS);
    let message = match code {
        "MISSING_CAPABILITIES" => format!(
            "worker {name} is missing required capabilities: {}",
            worker
                .missing_capabilities
                .iter()
                .map(|capability| sanitize_bounded(capability, MAX_ISSUE_PATH_CHARACTERS))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        "SSH_UNAVAILABLE" => format!("worker {name} could not be reached by the SSH probe"),
        "INVALID_RESPONSE" => format!("worker {name} returned an invalid probe response"),
        "PROTOCOL_MISMATCH" => format!("worker {name} uses an incompatible protocol version"),
        _ => format!("worker {name} is not eligible"),
    };
    sanitize_bounded(&message, MAX_ISSUE_MESSAGE_CHARACTERS)
}

fn selection_warning_issue(warning: &SelectionWarning) -> DoctorIssue {
    issue(
        IssueSeverity::Warning,
        warning.code,
        &warning.message,
        vec![warning.path.escaped_display()],
    )
}

fn selection_failure_issue(failure: SelectionFailure) -> DoctorIssue {
    let omitted = failure.total_path_count.saturating_sub(failure.paths.len());
    let message = if omitted == 0 {
        failure.message
    } else {
        format!("{} ({omitted} additional paths omitted)", failure.message)
    };
    issue(
        IssueSeverity::Blocker,
        failure.code,
        &message,
        failure
            .paths
            .iter()
            .take(MAX_ISSUE_PATHS)
            .map(|path| path.escaped_display())
            .collect(),
    )
}

fn issue(severity: IssueSeverity, code: &str, message: &str, paths: Vec<String>) -> DoctorIssue {
    DoctorIssue {
        severity,
        code: sanitize_bounded(code, MAX_PROJECT_FIELD_CHARACTERS),
        message: sanitize_bounded(message, MAX_ISSUE_MESSAGE_CHARACTERS),
        paths: paths
            .into_iter()
            .take(MAX_ISSUE_PATHS)
            .map(|path| sanitize_bounded(&path, MAX_ISSUE_PATH_CHARACTERS))
            .collect(),
    }
}

fn sort_issues(issues: &mut [DoctorIssue]) {
    issues.sort_by(|left, right| {
        left.severity
            .cmp(&right.severity)
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.paths.first().cmp(&right.paths.first()))
    });
}

fn sanitize_bounded(value: &str, limit: usize) -> String {
    let mut tokens = Vec::new();
    let mut used = 0;
    let mut truncated = false;

    for character in value.chars() {
        let escaped = match character {
            '\n' => "\\n".to_owned(),
            '\r' => "\\r".to_owned(),
            '\t' => "\\t".to_owned(),
            character if character.is_control() => character.escape_unicode().to_string(),
            character => character.to_string(),
        };
        let escaped_length = escaped.chars().count();
        if used + escaped_length > limit {
            truncated = true;
            break;
        }
        tokens.push(escaped);
        used += escaped_length;
    }

    if truncated {
        if limit == 0 {
            return String::new();
        }
        while used + 1 > limit {
            let removed = tokens
                .pop()
                .expect("a full output limit always contains at least one token");
            used -= removed.chars().count();
        }
        tokens.push("…".to_owned());
    }
    tokens.concat()
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsStr,
        fs,
        os::unix::process::ExitStatusExt,
        path::{Path, PathBuf},
        process::{Command, ExitStatus, Output},
        sync::atomic::{AtomicUsize, Ordering},
    };

    use crate::{
        config::{Config, WorkerEntry},
        error::WorkerError,
        paths::PathLayout,
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    };

    use super::{
        DoctorRequest, DoctorService, IssueSeverity, issue, sanitize_bounded, sort_issues,
    };

    struct ReadyDoctorRunner;

    struct AfterCapturePolicyRunner {
        root: PathBuf,
        project_inspections: AtomicUsize,
    }

    impl ProcessRunner for ReadyDoctorRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.program == OsStr::new("/usr/bin/ssh") {
                return Ok(ready_probe_result());
            }
            SystemProcessRunner.run(request)
        }
    }

    impl ProcessRunner for AfterCapturePolicyRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.program == OsStr::new("/usr/bin/ssh") {
                return Ok(ready_probe_result());
            }
            if request.program == OsStr::new("/usr/bin/git")
                && request
                    .args
                    .iter()
                    .any(|argument| argument == "--show-toplevel")
                && self.project_inspections.fetch_add(1, Ordering::SeqCst) == 2
            {
                fs::write(self.root.join(".worker.toml"), b"version = 1\n")?;
            }
            SystemProcessRunner.run(request)
        }
    }

    fn ready_probe_result() -> ProcessResult {
        let response = serde_json::json!({
            "protocol_version": 1,
            "hostname": "mini.local",
            "arch": "arm64",
            "os_version": "26.2",
            "free_disk_bytes": 536_870_912_u64,
            "memory_pressure": "normal",
            "swap_used_bytes": 134_217_728_u64,
            "capabilities": [],
        });
        ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: serde_json::to_vec(&response).unwrap(),
            stderr: Vec::new(),
        }
    }

    fn git(root: &Path, arguments: &[&str]) -> Output {
        let mut command = Command::new("/usr/bin/git");
        command
            .current_dir(root)
            .env("HOME", root.join("home"))
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1");
        for name in [
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_COMMON_DIR",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_CEILING_DIRECTORIES",
            "GIT_DISCOVERY_ACROSS_FILESYSTEM",
            "GIT_CONFIG_COUNT",
            "GIT_CONFIG_PARAMETERS",
        ] {
            command.env_remove(name);
        }
        command
            .args(arguments)
            .output()
            .expect("run isolated Git fixture command")
    }

    fn ready_config() -> Config {
        Config {
            version: 1,
            workers: vec![WorkerEntry {
                name: "mini-1".into(),
                ssh: "mac1".into(),
                slots: 1,
                capabilities: Vec::new(),
                remote_binary: "~/.local/bin/worker".into(),
            }],
        }
    }

    #[test]
    fn bounded_sanitization_reserves_the_marker_and_keeps_escape_tokens_atomic() {
        // Catches appending a truncation marker after already filling the
        // limit, and catches truncation splitting an escaped control token.
        let cases = [
            ("abcde", 5, "abcde"),
            ("abcdef", 5, "abcd…"),
            ("abc\nz", 5, "abc…"),
            ("\n", 2, "\\n"),
            ("\nX", 2, "…"),
            ("\n", 1, "…"),
            ("x", 0, ""),
        ];

        for (value, limit, expected) in cases {
            let actual = sanitize_bounded(value, limit);
            assert_eq!(actual, expected, "value={value:?}, limit={limit}");
            assert!(
                actual.chars().count() <= limit,
                "value={value:?}, limit={limit}, actual={actual:?}"
            );
        }
    }

    #[test]
    fn issue_sorting_uses_the_first_path_after_severity_and_code() {
        // Catches dropping the final ordering key when upstream producers
        // happen to already emit equal-code issues in lexical path order.
        let mut issues = vec![
            issue(
                IssueSeverity::Warning,
                "SAME_CODE",
                "later path",
                vec!["z/path".into()],
            ),
            issue(
                IssueSeverity::Warning,
                "SAME_CODE",
                "earlier path",
                vec!["a/path".into()],
            ),
            issue(
                IssueSeverity::Warning,
                "ALPHA_CODE",
                "earlier code",
                Vec::new(),
            ),
            issue(IssueSeverity::Blocker, "ZZZ_CODE", "blocker", Vec::new()),
        ];

        sort_issues(&mut issues);

        assert_eq!(
            issues
                .iter()
                .map(|issue| (
                    issue.severity,
                    issue.code.as_str(),
                    issue.paths.first().map(String::as_str),
                ))
                .collect::<Vec<_>>(),
            vec![
                (IssueSeverity::Blocker, "ZZZ_CODE", None),
                (IssueSeverity::Warning, "ALPHA_CODE", None),
                (IssueSeverity::Warning, "SAME_CODE", Some("a/path")),
                (IssueSeverity::Warning, "SAME_CODE", Some("z/path")),
            ]
        );
    }

    #[test]
    fn doctor_cleanup_failure_is_an_io_error_and_retains_only_private_empty_residue() {
        // Catches swallowing a post-summary exact-owned cleanup error and
        // returning a ready report. The injected final-remove failure happens
        // after the capture is privately acquired and emptied.
        let repository = tempfile::tempdir().unwrap();
        fs::create_dir(repository.path().join("home")).unwrap();
        assert!(
            git(repository.path(), &["init", "--initial-branch=main"])
                .status
                .success()
        );
        assert!(
            git(
                repository.path(),
                &["config", "user.name", "Doctor Cleanup Test"]
            )
            .status
            .success()
        );
        assert!(
            git(
                repository.path(),
                &["config", "user.email", "doctor-cleanup@example.test"]
            )
            .status
            .success()
        );
        fs::write(repository.path().join("README.md"), b"tracked\n").unwrap();
        assert!(git(repository.path(), &["add", "--all"]).status.success());
        assert!(
            git(repository.path(), &["commit", "-m", "cleanup fixture"])
                .status
                .success()
        );
        let state = tempfile::tempdir().unwrap();
        let paths = PathLayout {
            config: state.path().join("config.toml"),
            state: state.path().join("state"),
            cache: state.path().join("cache"),
            data: state.path().join("data"),
        };
        let config = ready_config();
        let _fault = crate::rooted_fs::fail_next_cleanup_before_final_root_removal(libc::EIO);

        let error = DoctorService {
            runner: &ReadyDoctorRunner,
            config: &config,
            paths: &paths,
        }
        .inspect(DoctorRequest {
            project: repository.path().to_path_buf(),
            cli_includes: Vec::new(),
        })
        .expect_err("cleanup failure must take precedence over a ready report");

        match error {
            WorkerError::Io(error) => assert_eq!(error.raw_os_error(), Some(libc::EIO)),
            other => panic!("cleanup failure had wrong classification: {other}"),
        }
        let ready = paths.cache.join("snapshots/ready");
        let ready_entries = fs::read_dir(&ready)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(ready_entries.len(), 1);
        assert_eq!(
            ready_entries[0].file_name().unwrap(),
            ".mac-worker-rooted-fs"
        );
        let retained = fs::read_dir(&ready_entries[0])
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(retained.len(), 1);
        assert!(retained[0].is_dir());
        assert_eq!(fs::read_dir(&retained[0]).unwrap().count(), 0);

        // The injected kernel-boundary failure intentionally leaves the empty
        // privately acquired capture as evidence. The fixture removes exactly
        // that private namespace only after validating the service contract.
        fs::remove_dir_all(&ready_entries[0]).unwrap();
    }

    #[test]
    fn post_capture_state_change_cleanup_failure_keeps_io_precedence() {
        // Catches replacing an exact-owned cleanup failure with the typed
        // SNAPSHOT_CHANGED report produced by the post-capture state check.
        let repository = tempfile::tempdir().unwrap();
        fs::create_dir(repository.path().join("home")).unwrap();
        assert!(
            git(repository.path(), &["init", "--initial-branch=main"])
                .status
                .success()
        );
        assert!(
            git(
                repository.path(),
                &["config", "user.name", "Doctor Cleanup Test"]
            )
            .status
            .success()
        );
        assert!(
            git(
                repository.path(),
                &["config", "user.email", "doctor-cleanup@example.test"]
            )
            .status
            .success()
        );
        fs::write(
            repository.path().join(".worker.toml"),
            b"version = 1\n[snapshot]\nallow_sensitive = [\".env\"]\n",
        )
        .unwrap();
        fs::write(repository.path().join(".env"), b"fixture secret\n").unwrap();
        fs::write(repository.path().join("README.md"), b"tracked\n").unwrap();
        assert!(git(repository.path(), &["add", "--all"]).status.success());
        assert!(
            git(repository.path(), &["commit", "-m", "cleanup fixture"])
                .status
                .success()
        );
        fs::write(repository.path().join("README.md"), b"already dirty\n").unwrap();
        let state = tempfile::tempdir().unwrap();
        let paths = PathLayout {
            config: state.path().join("config.toml"),
            state: state.path().join("state"),
            cache: state.path().join("cache"),
            data: state.path().join("data"),
        };
        let config = ready_config();
        let runner = AfterCapturePolicyRunner {
            root: repository.path().to_path_buf(),
            project_inspections: AtomicUsize::new(0),
        };
        let _fault = crate::rooted_fs::fail_next_cleanup_before_final_root_removal(libc::EIO);

        let error = DoctorService {
            runner: &runner,
            config: &config,
            paths: &paths,
        }
        .inspect(DoctorRequest {
            project: repository.path().to_path_buf(),
            cli_includes: Vec::new(),
        })
        .expect_err("cleanup failure must override a post-capture state change report");

        match error {
            WorkerError::Io(error) => assert_eq!(error.raw_os_error(), Some(libc::EIO)),
            other => panic!("cleanup failure had wrong classification: {other}"),
        }
        let ready = paths.cache.join("snapshots/ready");
        let ready_entries = fs::read_dir(&ready)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(ready_entries.len(), 1);
        assert_eq!(
            ready_entries[0].file_name().unwrap(),
            ".mac-worker-rooted-fs"
        );
        let retained = fs::read_dir(&ready_entries[0])
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(retained.len(), 1);
        assert!(retained[0].is_dir());
        assert_eq!(fs::read_dir(&retained[0]).unwrap().count(), 0);

        fs::remove_dir_all(&ready_entries[0]).unwrap();
    }
}
