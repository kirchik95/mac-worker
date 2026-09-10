//! A resumable first-run flow built on the existing helper installer and probes.
mod inventory;

use crate::{
    agent_facts::{AgentAuth, FACTS_TTL},
    config::{WorkerEntry, valid_ssh_destination},
    error::{ExitKind, WorkerError},
    install::Installer,
    process::{ProcessPolicy, ProcessRunner},
    protocol::HealthStatus,
    transport::{SshTransport, ssh_request},
};
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

pub struct InitRequest {
    pub destination: String,
    pub name: Option<String>,
    pub agent: String,
    pub env_profile: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InitStep {
    pub name: &'static str,
    pub status: &'static str,
    pub code: Option<String>,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct InitReport {
    pub ready: bool,
    pub worker: String,
    pub ssh: String,
    pub agent: String,
    pub config_path: PathBuf,
    pub steps: Vec<InitStep>,
    pub next_steps: Vec<String>,
    #[serde(skip)]
    pub exit_kind: Option<ExitKind>,
}

impl InitReport {
    fn passed(&mut self, name: &'static str, message: impl Into<String>) {
        self.steps.push(InitStep {
            name,
            status: "ok",
            code: None,
            message: message.into(),
        });
    }

    fn blocked(
        &mut self,
        name: &'static str,
        code: &str,
        message: &str,
        next: Vec<String>,
        kind: ExitKind,
    ) {
        self.steps.push(InitStep {
            name,
            status: "blocked",
            code: Some(code.into()),
            message: message.into(),
        });
        self.next_steps = next;
        self.exit_kind = Some(kind);
    }

    pub fn render_human(&self) -> String {
        let mut lines = vec![format!("Connecting {} ({})", self.worker, self.ssh)];
        for step in &self.steps {
            let code = step
                .code
                .as_ref()
                .map(|code| format!(" [{code}]"))
                .unwrap_or_default();
            lines.push(format!(
                "  {} {}{}: {}",
                step.status, step.name, code, step.message
            ));
        }
        lines.push(String::new());
        lines.push(if self.ready {
            format!("{} is configured for {} tasks.", self.worker, self.agent)
        } else {
            "One more step is needed. Complete the instructions below, then rerun init.".into()
        });
        lines.extend(self.next_steps.iter().map(|step| format!("  {step}")));
        lines.join("\n")
    }
}

pub fn identifier(value: &str) -> Result<String, String> {
    if value.is_empty()
        || value.len() > 128
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
    {
        return Err("use 1–128 letters, numbers, dots, underscores or hyphens".into());
    }
    Ok(value.into())
}

pub fn worker_name(value: &str) -> Result<String, String> {
    if !crate::config::valid_identifier(value) {
        return Err("use a non-empty worker name containing letters, numbers, dots, underscores, hyphens or @".into());
    }
    Ok(value.into())
}

pub fn ssh_destination(value: &str) -> Result<String, String> {
    if value.len() > 253
        || !valid_ssh_destination(value)
        || value.matches('@').count() > 1
        || value.ends_with('@')
    {
        return Err(
            "use an SSH alias or user@hostname (configure ports and keys in ~/.ssh/config)".into(),
        );
    }
    Ok(value.into())
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn remote_ok(runner: &dyn ProcessRunner, worker: &WorkerEntry, command: &str) -> Option<Vec<u8>> {
    let request = ssh_request(
        worker,
        command.into(),
        ProcessPolicy {
            stdout_limit: 4096,
            stderr_limit: 4096,
            deadline: Duration::from_secs(15),
        },
    )
    .ok()?;
    runner
        .run(&request)
        .ok()
        .filter(|r| r.status.success() && r.stdout.len() <= 4096 && r.stderr.len() <= 4096)
        .map(|r| r.stdout)
}

pub fn initialize(
    runner: &dyn ProcessRunner,
    config_path: &Path,
    request: InitRequest,
) -> Result<InitReport, WorkerError> {
    ssh_destination(&request.destination).map_err(WorkerError::Config)?;
    if let Some(name) = &request.name {
        worker_name(name).map_err(WorkerError::Config)?;
    }
    if let Some(profile) = &request.env_profile {
        identifier(profile).map_err(WorkerError::Config)?;
    }
    if !matches!(
        request.agent.as_str(),
        "codex" | "cursor" | "opencode" | "claude"
    ) {
        return Err(WorkerError::Config("unknown agent".into()));
    }
    let mut worker = inventory::preview(config_path, &request)?;
    let mut report = InitReport {
        ready: false,
        worker: worker.name.clone(),
        ssh: worker.ssh.clone(),
        agent: request.agent.clone(),
        config_path: config_path.into(),
        steps: Vec::new(),
        next_steps: Vec::new(),
        exit_kind: None,
    };
    let options = format!(
        "--agent {}{}",
        request.agent,
        request
            .env_profile
            .as_ref()
            .map(|p| format!(" --env-profile={p}"))
            .unwrap_or_default()
    );
    let retry = format!(
        "worker init {} --config {} --name={} {options}",
        request.destination,
        quote(&config_path.to_string_lossy()),
        worker.name
    );
    let login_shell = format!(
        "ssh -t -o ForwardAgent=no -o ClearAllForwardings=yes -- {}",
        request.destination
    );

    if !cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        report.blocked(
            "platform",
            "PLATFORM_UNSUPPORTED",
            "First-run installation currently requires an Apple Silicon Mac as the controller.",
            vec!["Run init from an Apple Silicon Mac using the arm64 release binary.".into()],
            ExitKind::Usage,
        );
        return Ok(report);
    }
    if remote_ok(runner, &worker, "/usr/bin/true").is_none() {
        report.blocked("ssh", "SSH_UNAVAILABLE", "Passwordless SSH is not ready. Enable Remote Login on the Mac and authorize your SSH key.", vec![format!("Connect once, verify the host fingerprint and check your account: {login_shell}"), "SSH setup guide: https://github.com/kirchik95/mac-worker/blob/main/docs/setup-macos-worker.md#2-connect-over-ssh".into(), retry], ExitKind::Unavailable);
        return Ok(report);
    }
    report.passed("ssh", "Connected without a password prompt");
    if remote_ok(runner, &worker, "/usr/bin/uname -s && /usr/bin/uname -m").as_deref()
        != Some(b"Darwin\narm64\n")
    {
        report.blocked(
            "platform",
            "PLATFORM_UNSUPPORTED",
            "The worker must be an Apple Silicon Mac (Darwin/arm64). No helper was uploaded.",
            vec!["Choose an Apple Silicon Mac and rerun init.".into()],
            ExitKind::Usage,
        );
        return Ok(report);
    }
    report.passed("platform", "Apple Silicon Mac");
    if remote_ok(runner, &worker, "/usr/bin/git --version").is_none() {
        report.blocked("git", "GIT_MISSING", "Git is not available to the worker account.", vec!["On the Mac, run xcode-select --install and complete the Command Line Tools installer.".into(), retry], ExitKind::Unavailable);
        return Ok(report);
    }
    report.passed("git", "Git is available");
    worker = inventory::register(config_path, &request)?;
    report.worker = worker.name.clone();
    report.passed(
        "config",
        format!(
            "Registered {}; inventory: {}",
            worker.name,
            config_path.display()
        ),
    );

    let transport = SshTransport::new(runner);
    let initial = transport.probe(&worker);
    if initial.status != HealthStatus::Ready {
        let setup = Installer::new(runner).install(&std::env::current_exe()?, &worker);
        for warning in &setup.warnings {
            report.steps.push(InitStep { name: "helper", status: "warning", code: Some(format!("{:?}", warning.code)), message: "Setup retained recovery state; see docs/setup-recovery.md before another installation.".into() });
        }
        if !setup.installed {
            report.blocked("helper", setup.error_code.as_deref().unwrap_or("INSTALL_FAILED"), "The helper could not be installed. The worker configuration is saved.", vec!["Installation recovery: https://github.com/kirchik95/mac-worker/blob/main/docs/setup-recovery.md".into(), format!("After resolving the reported error: {retry}")], setup.failure_kind.map_or(ExitKind::Infrastructure, |k| k.exit_kind()));
            return Ok(report);
        }
    }
    report.passed("helper", "Compatible helper installed");
    if transport.refresh_facts(&worker).is_err() {
        report.blocked(
            "agent",
            "REFRESH_FACTS_FAILED",
            "Could not refresh agent checks on the worker.",
            vec![retry],
            ExitKind::Unavailable,
        );
        return Ok(report);
    }
    let health = transport.probe(&worker);
    let facts = health
        .probe
        .as_ref()
        .filter(|_| health.status == HealthStatus::Ready)
        .filter(|p| p.facts_age_millis.is_some_and(|age| age <= FACTS_TTL))
        .and_then(|p| p.agent_facts.as_ref());
    let Some(facts) = facts else {
        report.blocked(
            "agent",
            "AGENT_FACTS_UNAVAILABLE",
            "The worker did not return fresh agent checks.",
            vec![retry],
            ExitKind::Unavailable,
        );
        return Ok(report);
    };
    let Some(agent) = facts.agents.iter().find(|a| a.name == request.agent) else {
        report.blocked(
            "agent",
            "AGENT_MISSING",
            "The selected agent is not on the worker account's login PATH.",
            vec![
                format!("Open a shell on the worker: {login_shell}"),
                format!(
                    "Install {} there: {}",
                    request.agent,
                    install_guide(&request.agent)
                ),
                retry,
            ],
            ExitKind::Unavailable,
        );
        return Ok(report);
    };
    let auth = if let Some(profile) = &request.env_profile {
        if !facts
            .env_profiles
            .iter()
            .any(|p| &p.name == profile && p.secure)
        {
            report.blocked("agent", "ENV_PROFILE_UNAVAILABLE", "The requested environment profile is missing or has insecure permissions.", vec![format!("On the worker, provision ~/.config/mac-worker/env/{profile}.env as the worker account with mode 0600."), "Profile guide: https://github.com/kirchik95/mac-worker/blob/main/docs/setup-macos-worker.md#environment-profiles".into(), retry], ExitKind::Unavailable);
            return Ok(report);
        }
        agent
            .auth_by_profile
            .iter()
            .find(|(p, _)| p == profile)
            .map(|(_, a)| *a)
    } else {
        Some(agent.auth)
    };
    if auth != Some(AgentAuth::Authenticated) {
        report.blocked("agent", "AGENT_AUTH_REQUIRED", "The selected agent has not confirmed a headless login for this account/profile.", vec![format!("Open a shell on the worker: {login_shell}"), format!("Complete login there: {}", login_command(&request.agent)), "If you use a profile, check its credentials locally on the worker. Never paste them into a task.".into(), retry], ExitKind::Unavailable);
        return Ok(report);
    }
    report.passed("agent", format!("{} authenticated over SSH", request.agent));
    report.ready = true;
    report.next_steps = vec!["In a Git repository with a commit, run:".into(), format!("worker task submit --config {} --worker={} {options} --wait --prompt \"Create SETUP_CHECK.md containing: mac-worker works.\"", quote(&config_path.to_string_lossy()), worker.name), format!("worker task fetch <task-id> --config {}", quote(&config_path.to_string_lossy())), "The worker also needs your project's toolchain and dependencies. Keep the Mac awake and connected.".into()];
    Ok(report)
}

fn install_guide(agent: &str) -> &'static str {
    match agent {
        "codex" => "brew install --cask codex (https://developers.openai.com/codex/cli/)",
        "cursor" => "https://cursor.com/docs/cli/installation",
        "opencode" => "https://opencode.ai/docs/",
        "claude" => "https://code.claude.com/docs/en/setup",
        _ => unreachable!("validated agent"),
    }
}

fn login_command(agent: &str) -> &'static str {
    match agent {
        "codex" => "codex login --device-auth (or codex login in a desktop session)",
        "cursor" => {
            "cursor-agent login; see the environment-profile guide for headless Keychain access"
        }
        "opencode" => "opencode auth login",
        "claude" => "claude auth login; see the environment-profile guide if using an API key",
        _ => unreachable!("validated agent"),
    }
}
