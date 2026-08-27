#[derive(Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandOutput {
    Setup(crate::protocol::SetupReport),
    Doctor(crate::protocol::DoctorReport),
    Workers(crate::protocol::WorkersReport),
    Probe(crate::protocol::ProbeResponse),
}

impl CommandOutput {
    pub fn render_json(&self) -> Result<String, crate::error::WorkerError> {
        serde_json::to_string(self).map_err(|error| {
            crate::error::WorkerError::Protocol(format!(
                "failed to serialize command output: {error}"
            ))
        })
    }

    pub fn render_human(&self) -> String {
        match self {
            Self::Setup(report) => report
                .workers
                .iter()
                .map(|worker| {
                    let mut rendered = if worker.installed {
                        match worker.protocol_version {
                            Some(version) => {
                                format!("{}: installed (protocol {version})", worker.name)
                            }
                            None => format!("{}: installed", worker.name),
                        }
                    } else {
                        let code = worker.error_code.as_deref().unwrap_or("UNKNOWN");
                        let message = worker.error_message.as_deref().unwrap_or("setup failed");
                        format!("{}: failed [{code}]: {message}", worker.name)
                    };
                    for warning in &worker.warnings {
                        rendered.push_str("\n  warning [");
                        rendered.push_str(setup_warning_code(&warning.code));
                        rendered.push_str("]: ");
                        rendered.push_str(&warning.message);
                    }
                    rendered
                })
                .collect::<Vec<_>>()
                .join("\n"),
            Self::Workers(report) => report
                .workers
                .iter()
                .map(render_worker_health)
                .collect::<Vec<_>>()
                .join("\n"),
            Self::Doctor(report) => render_doctor_report(report),
            Self::Probe(probe) => format!(
                "{}: {} (macOS {}; protocol {})",
                probe.hostname, probe.arch, probe.os_version, probe.protocol_version
            ),
        }
    }

    pub fn write_to(
        &self,
        writer: &mut dyn std::io::Write,
        json: bool,
        raw_probe: bool,
    ) -> Result<(), crate::error::WorkerError> {
        let rendered = if raw_probe {
            let Self::Probe(probe) = self else {
                return Err(crate::error::WorkerError::Protocol(
                    "host probe returned the wrong output type".into(),
                ));
            };
            serde_json::to_string(probe).map_err(|error| {
                crate::error::WorkerError::Protocol(format!(
                    "failed to serialize host probe: {error}"
                ))
            })?
        } else if json {
            self.render_json()?
        } else {
            self.render_human()
        };

        writer.write_all(rendered.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    pub fn aggregate_exit_kind(&self) -> Option<crate::error::ExitKind> {
        match self {
            Self::Setup(report) => report
                .workers
                .iter()
                .filter(|worker| !worker.installed)
                .map(|worker| {
                    worker
                        .failure_kind
                        .unwrap_or(crate::protocol::SetupFailureKind::Infrastructure)
                })
                .max_by_key(|kind| match kind {
                    crate::protocol::SetupFailureKind::Unavailable => 0,
                    crate::protocol::SetupFailureKind::Infrastructure => 1,
                    crate::protocol::SetupFailureKind::Io => 2,
                })
                .map(crate::protocol::SetupFailureKind::exit_kind),
            Self::Doctor(report) if report.ready => None,
            Self::Doctor(report)
                if report.issues.iter().any(|issue| {
                    issue.severity == crate::protocol::IssueSeverity::Blocker
                        && issue.code.starts_with("SNAPSHOT_")
                }) =>
            {
                Some(crate::error::ExitKind::Infrastructure)
            }
            Self::Doctor(_) => Some(crate::error::ExitKind::Usage),
            _ => None,
        }
    }
}

fn render_doctor_report(report: &crate::protocol::DoctorReport) -> String {
    let mut lines = vec![format!(
        "doctor: {}",
        if report.ready { "ready" } else { "blocked" }
    )];
    lines.push(format!("project: {}", report.project.display_name));
    lines.push(format!(
        "  project id: {}",
        short_identifier(&report.project.project_id)
    ));
    lines.push(format!(
        "  worktree id: {}",
        short_identifier(&report.project.worktree_id)
    ));
    lines.push(format!(
        "  source: {}",
        render_source(&report.project.branch, &report.project.head)
    ));
    lines.push(format!(
        "  dirty: {}",
        if report.project.dirty { "yes" } else { "no" }
    ));
    lines.push(format!(
        "  working directory: {}",
        if report.project.relative_working_dir.is_empty() {
            "."
        } else {
            &report.project.relative_working_dir
        }
    ));
    lines.push(format!(
        "requirements: {}",
        render_list(&report.requirements)
    ));

    if let Some(snapshot) = &report.snapshot {
        lines.push("snapshot:".into());
        lines.push(format!("  digest: {}", snapshot.digest));
        lines.push(format!("  file count: {}", snapshot.file_count));
        lines.push(format!("  total bytes: {}", snapshot.total_bytes));
        lines.push(format!(
            "  tracked deletions: {}",
            snapshot.tracked_deletion_count
        ));
        lines.push(format!(
            "  included untracked: {}",
            snapshot.included_untracked_count
        ));
        lines.push(format!("  warnings: {}", snapshot.warning_count));
    } else {
        lines.push("snapshot: unavailable".into());
    }

    if report.workers.is_empty() {
        lines.push("workers: none".into());
    } else {
        lines.push("workers:".into());
        for worker in &report.workers {
            extend_indented(&mut lines, &render_doctor_worker_health(worker));
        }
    }

    if report.issues.is_empty() {
        lines.push("issues: none".into());
    } else {
        lines.push("issues:".into());
        for issue in &report.issues {
            lines.push(format!(
                "  {} [{}]: {}",
                issue_severity_name(issue.severity),
                issue.code,
                issue.message
            ));
            for path in &issue.paths {
                lines.push(format!("    path: {path}"));
            }
        }
    }

    lines.join("\n")
}

fn render_source(branch: &Option<String>, head: &Option<String>) -> String {
    match (branch, head) {
        (Some(branch), Some(head)) => {
            format!("branch {branch} at {}", short_identifier(head))
        }
        (Some(branch), None) => format!("branch {branch} at unborn HEAD"),
        (None, Some(head)) => format!("detached HEAD at {}", short_identifier(head)),
        (None, None) => "detached HEAD unavailable".into(),
    }
}

fn short_identifier(identifier: &str) -> String {
    identifier.chars().take(12).collect()
}

fn render_list(values: &[String]) -> String {
    if values.is_empty() {
        "none".into()
    } else {
        values.join(", ")
    }
}

fn extend_indented(lines: &mut Vec<String>, rendered: &str) {
    lines.extend(rendered.lines().map(|line| format!("  {line}")));
}

fn render_doctor_worker_health(worker: &crate::protocol::WorkerHealth) -> String {
    render_worker_health_with_labels(worker, "eligible", "ineligible")
}

fn render_worker_health(worker: &crate::protocol::WorkerHealth) -> String {
    render_worker_health_with_labels(worker, "ready", "unavailable")
}

fn render_worker_health_with_labels(
    worker: &crate::protocol::WorkerHealth,
    ready_label: &str,
    unavailable_label: &str,
) -> String {
    let mut lines = vec![match &worker.probe {
        Some(probe) if worker.status == crate::protocol::HealthStatus::Ready => format!(
            "{}: {ready_label} ({}; {}; macOS {}; protocol {})",
            worker.name, probe.hostname, probe.arch, probe.os_version, probe.protocol_version,
        ),
        _ => {
            let code = worker.error_code.as_deref().unwrap_or("UNAVAILABLE");
            let message = worker
                .error_message
                .as_deref()
                .unwrap_or("worker is unavailable");
            format!("{}: {unavailable_label} [{code}]: {message}", worker.name)
        }
    }];

    if !worker.missing_capabilities.is_empty() {
        lines.push(format!(
            "  missing capabilities: {}",
            worker.missing_capabilities.join(", ")
        ));
    }
    if let Some(probe) = &worker.probe {
        let capabilities = if probe.capabilities.is_empty() {
            "none".into()
        } else {
            probe.capabilities.join(", ")
        };
        lines.push(format!("  capabilities: {capabilities}"));
        lines.push(format!("  free disk bytes: {}", probe.free_disk_bytes));
        lines.push(format!(
            "  memory pressure: {}",
            memory_pressure_name(&probe.memory_pressure)
        ));
        lines.push(format!(
            "  swap used bytes: {}",
            probe
                .swap_used_bytes
                .map_or_else(|| "unavailable".into(), |bytes| bytes.to_string())
        ));
    }

    lines.join("\n")
}

fn issue_severity_name(severity: crate::protocol::IssueSeverity) -> &'static str {
    match severity {
        crate::protocol::IssueSeverity::Blocker => "blocker",
        crate::protocol::IssueSeverity::Warning => "warning",
    }
}

fn memory_pressure_name(pressure: &crate::protocol::MemoryPressure) -> &'static str {
    match pressure {
        crate::protocol::MemoryPressure::Normal => "normal",
        crate::protocol::MemoryPressure::Warn => "warn",
        crate::protocol::MemoryPressure::Critical => "critical",
        crate::protocol::MemoryPressure::Unknown => "unknown",
    }
}

fn setup_warning_code(code: &crate::protocol::SetupWarningCode) -> &'static str {
    match code {
        crate::protocol::SetupWarningCode::CleanupFailed => "CLEANUP_FAILED",
        crate::protocol::SetupWarningCode::RollbackFailed => "ROLLBACK_FAILED",
    }
}
