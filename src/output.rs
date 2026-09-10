#[derive(Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandOutput {
    Init(crate::onboarding::InitReport),
    Setup(crate::protocol::SetupReport),
    Doctor(crate::protocol::DoctorReport),
    Workers(crate::protocol::WorkersReport),
    Probe(crate::protocol::ProbeResponse),
    Status(crate::run::StatusReport),
    Cancel(crate::run::CancelReport),
    /// Laptop-only Markdown or name list. Not a host/task wire type.
    Plain {
        text: String,
    },
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
            Self::Init(report) => report.render_human(),
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
            Self::Status(report) => render_status_report(report),
            Self::Plain { text } => text.clone(),
            Self::Cancel(report) => match report {
                crate::run::CancelReport::QueuedCancelled { job_id } => {
                    format!("job {job_id} cancelled while queued")
                }
                crate::run::CancelReport::RemoteCancelled { response } => format!(
                    "job {} cancellation resolved as {}",
                    response.status().meta().job_id(),
                    job_state_name(response.status().status().state())
                ),
            },
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
            Self::Init(report) => report.exit_kind,
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

fn render_agent_auth(auth: crate::agent_facts::AgentAuth) -> String {
    match auth.reason() {
        Some(reason) => format!("{} ({reason})", auth.as_str()),
        None => auth.as_str().to_owned(),
    }
}

/// Spec 5.3: `available (0.9.0)`, `not installed`, `installed (0.9.0), no
/// socket`, `installed (0.9.0), no response`, or `unknown` when the facts are
/// missing or predate the fact. A known fact older than the TTL is still
/// printed, with a `stale` age suffix, so the operator can see what last
/// collected rather than a hole. Capability derivation still uses
/// [`crate::protocol::ProbeResponse::herdr_fact`], which ignores stale facts.
/// A known non-zero interactive-agent count is appended so the operator can
/// see load that is not a pool turn.
fn render_herdr_fact(probe: &crate::protocol::ProbeResponse) -> String {
    use crate::agent_facts::{FACTS_TTL, HerdrFactState};

    let Some(facts) = &probe.agent_facts else {
        return "unknown".into();
    };
    let Some(herdr) = &facts.herdr else {
        return "unknown".into();
    };
    let Some(age_millis) = probe.facts_age_millis else {
        return "unknown".into();
    };
    let versioned = |label: &str| match &herdr.version {
        Some(version) => format!("{label} ({version})"),
        None => label.to_owned(),
    };
    let mut rendered = match herdr.state {
        HerdrFactState::Available => versioned("available"),
        HerdrFactState::NotInstalled => "not installed".into(),
        HerdrFactState::NoSocket => format!("{}, no socket", versioned("installed")),
        HerdrFactState::NoResponse => format!("{}, no response", versioned("installed")),
    };
    if let Some(count) = herdr.interactive_agents.filter(|count| *count > 0) {
        rendered.push_str(&format!(
            ", {count} interactive agent{}",
            if count == 1 { "" } else { "s" }
        ));
    }
    if age_millis > FACTS_TTL {
        rendered.push_str(&format!(", stale {}", render_compact_age(age_millis)));
    }
    rendered
}

/// Compact age for the `stale` suffix: `15m`, `69m`, `2h`. Minutes stay
/// minutes through the first two hours so a 69-minute fact is `69m`, not
/// a rounded `1h`, matching the dashboard chip.
fn render_compact_age(age_millis: u64) -> String {
    let seconds = age_millis.saturating_add(500) / 1000;
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds.saturating_add(30) / 60;
    if minutes < 120 {
        return format!("{minutes}m");
    }
    let hours = minutes.saturating_add(30) / 60;
    if hours < 48 {
        return format!("{hours}h");
    }
    format!("{}d", hours.saturating_add(12) / 24)
}

fn render_doctor_worker_health(worker: &crate::protocol::WorkerHealth) -> String {
    if worker.status == crate::protocol::HealthStatus::Ready
        && worker
            .probe
            .as_ref()
            .is_some_and(|probe| probe.slot_state == crate::lease::SlotState::Busy)
    {
        return render_worker_health_with_labels(worker, "busy", "ineligible");
    }
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
        lines.push(format!(
            "  slot: {}",
            match probe.slot_state {
                crate::lease::SlotState::Idle => "idle",
                crate::lease::SlotState::Busy => "busy",
            }
        ));
        if let Some(lease) = &probe.active_lease {
            lines.push(format!("  active job: {}", lease.job_id));
            lines.push(format!(
                "  active project: {}",
                short_identifier(&lease.project_id)
            ));
            lines.push(format!(
                "  active worktree: {}",
                short_identifier(&lease.worktree_id)
            ));
        }
        let capabilities = if probe.capabilities.is_empty() {
            "none".into()
        } else {
            probe.capabilities.join(", ")
        };
        lines.push(format!("  capabilities: {capabilities}"));
        if let Some(facts) = &probe.agent_facts {
            lines.push(format!(
                "  agent facts age millis: {}",
                probe.facts_age_millis.unwrap_or(0)
            ));
            lines.push(format!(
                "  git identity: {}",
                if facts.git_identity {
                    "configured"
                } else {
                    "unconfigured"
                }
            ));
            if facts.agents.is_empty() {
                lines.push("  agents: none".into());
            } else {
                lines.push("  agents:".into());
                for agent in &facts.agents {
                    let version = agent.version.as_deref().unwrap_or("unknown");
                    lines.push(format!(
                        "    {} {}: {}",
                        agent.name,
                        version,
                        render_agent_auth(agent.auth)
                    ));
                    for (profile, auth) in &agent.auth_by_profile {
                        lines.push(format!("      {profile}: {}", render_agent_auth(*auth)));
                    }
                }
            }
            lines.push(format!("  herdr: {}", render_herdr_fact(probe)));
            if facts.env_profiles.is_empty() {
                lines.push("  profiles: none".into());
            } else {
                lines.push("  profiles:".into());
                for profile in &facts.env_profiles {
                    lines.push(format!(
                        "    {}: {}",
                        profile.name,
                        if profile.secure { "secure" } else { "insecure" }
                    ));
                }
            }
        } else {
            lines.push(format!("  herdr: {}", render_herdr_fact(probe)));
        }
        lines.push(format!("  free disk bytes: {}", probe.free_disk_bytes));
        lines.push(format!("  total disk bytes: {}", probe.total_disk_bytes));
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
        crate::protocol::SetupWarningCode::HerdrUnavailable => {
            crate::protocol::HERDR_UNAVAILABLE_CODE
        }
        crate::protocol::SetupWarningCode::WarmupFailed => "WARMUP_FAILED",
        crate::protocol::SetupWarningCode::FactsRefreshFailed => "FACTS_REFRESH_FAILED",
    }
}

fn render_status_report(report: &crate::run::StatusReport) -> String {
    let mut lines = report
        .queued
        .iter()
        .map(render_queued_status_row)
        .chain(report.jobs.iter().map(render_status_row))
        .collect::<Vec<_>>();
    if report.omitted > 0 {
        lines.push(format!("{} older jobs omitted", report.omitted));
    }
    lines.join("\n")
}

fn render_queued_status_row(row: &crate::run::QueuedStatusRow) -> String {
    let command = match row.command_summary.arg_count() {
        Some(count) => format!("argv {count}"),
        None => "shell".into(),
    };
    let requirements = if row.requirements.is_empty() {
        "-".to_owned()
    } else {
        row.requirements.join(",")
    };
    let blocking = row
        .blocking_reason
        .as_ref()
        .map_or_else(|| "none".to_owned(), |reason| reason.render_human());
    format!(
        "queued {} {} {} {command} {requirements} {blocking} age {}",
        row.position, row.job_id, row.project_id, row.age_millis
    )
}

fn render_status_row(row: &crate::run::StatusRow) -> String {
    let state = row
        .status
        .as_ref()
        .map_or("unknown", |status| job_state_name(status.state()));
    let uncertainty = match &row.remote_uncertainty {
        crate::job::RemoteUncertainty::None => "none".to_owned(),
        crate::job::RemoteUncertainty::UnknownRemote { code } => {
            format!("unknown_remote {code}")
        }
        crate::job::RemoteUncertainty::CleanupPending { code } => {
            format!("cleanup_pending {code}")
        }
    };
    let command = match row.command_summary.arg_count() {
        Some(count) => format!("argv {count}"),
        None => "shell".into(),
    };
    format!(
        "{} {} {state} {uncertainty} {} {} {command}",
        row.job_id, row.worker, row.created_at_millis, row.manifest_digest
    )
}

fn job_state_name(state: crate::job::JobState) -> &'static str {
    match state {
        crate::job::JobState::Uploading => "uploading",
        crate::job::JobState::Verified => "verified",
        crate::job::JobState::Accepted => "accepted",
        crate::job::JobState::Running => "running",
        crate::job::JobState::Succeeded => "succeeded",
        crate::job::JobState::Failed => "failed",
        crate::job::JobState::Cancelled => "cancelled",
        crate::job::JobState::TimedOut => "timed_out",
        crate::job::JobState::Lost => "lost",
    }
}
