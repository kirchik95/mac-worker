use std::{collections::HashSet, path::PathBuf};

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

impl DoctorService<'_> {
    pub fn inspect(&self, request: DoctorRequest) -> Result<DoctorReport, WorkerError> {
        let context = ProjectInspector::new(self.runner).inspect(&request.project)?;
        let settings = ProjectSettings::load(&context.root, &request.cli_includes)?;
        let detected = RequirementDetector::detect(&context.root)?;
        let requirements = merge_project_requirements(&settings.requires, detected);

        let mut workers = WorkersService::new(SshTransport::new(self.runner))
            .inspect_with_requirements(self.config, &requirements)
            .workers;
        let eligible_worker_count = workers.iter().filter(|worker| is_eligible(worker)).count();
        let mut issues = worker_issues(&mut workers, eligible_worker_count);

        let snapshot = match InputSelector::new(self.runner).select(&context, &settings.snapshot) {
            Ok(selection) => {
                issues.extend(selection.warnings.iter().map(selection_warning_issue));
                match SnapshotBuilder::new(self.runner, &self.paths.cache).capture(
                    &context,
                    &settings.snapshot,
                    selection,
                ) {
                    Ok(snapshot) => {
                        let summary = snapshot.summary();
                        snapshot.cleanup()?;
                        Some(summary)
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
        let ready = snapshot.is_some()
            && eligible_worker_count > 0
            && !issues
                .iter()
                .any(|issue| issue.severity == IssueSeverity::Blocker);

        Ok(DoctorReport {
            version: DOCTOR_REPORT_VERSION,
            ready,
            project: doctor_project(&context),
            requirements,
            snapshot,
            workers,
            issues,
        })
    }
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
    let mut sanitized = String::new();
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
        sanitized.push_str(&escaped);
        used += escaped_length;
    }

    if truncated {
        sanitized.push('…');
    }
    sanitized
}
