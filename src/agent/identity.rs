use std::{path::Path, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    error::{ProcessError, WorkerError},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    redaction::RedactionBoundary,
};

pub(crate) const IDENTITY_FILE: &str = "agent-identity.json";
/// Written by the launch wrapper inside the turn's disposable `tmp` scope.
pub(crate) const STAGED_IDENTITY_FILE: &str = "mac-worker-agent-identity.json";
pub(crate) const IDENTITY_MAX_BYTES: u64 = 8192;
pub(crate) const VERSION_DEADLINE: Duration = Duration::from_secs(2);
/// Bound for a launch whose argv depends on the version. It waits longer
/// than the diagnostic probe so a slow host is not mistaken for an unknown
/// agent.
pub(crate) const REQUIRED_VERSION_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentIdentity {
    pub executable: String,
    pub version: Option<String>,
    pub version_observation: VersionObservation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionObservation {
    Observed,
    Unavailable,
    TimedOut,
    OutputLimit,
}

impl AgentIdentity {
    pub(crate) fn version_display(&self) -> &str {
        self.version
            .as_deref()
            .unwrap_or(match self.version_observation {
                VersionObservation::TimedOut => "version unavailable(timeout)",
                VersionObservation::OutputLimit => "version unavailable(output_too_large)",
                VersionObservation::Observed | VersionObservation::Unavailable => {
                    "version unavailable"
                }
            })
    }

    pub(crate) fn redacted(&self, boundary: &RedactionBoundary) -> Self {
        // Keep the useful path below the account home while hiding its owner.
        let relative = self
            .executable
            .strip_prefix("~/")
            .map(str::to_owned)
            .or_else(|| {
                std::env::var_os("HOME")
                    .and_then(|home| std::fs::canonicalize(home).ok())
                    .and_then(|home| {
                        Path::new(&self.executable)
                            .strip_prefix(home)
                            .ok()
                            .map(|p| p.to_string_lossy().into_owned())
                    })
            });
        // Redact the suffix before adding the public home marker; the generic
        // boundary deliberately removes entire tilde-prefixed paths.
        let executable = relative.map_or_else(
            || boundary.text(&self.executable, 4096),
            |relative| format!("~/{}", boundary.text(&relative, 4094)),
        );
        Self {
            executable,
            version: self.version.as_ref().map(|v| boundary.text(v, 128)),
            version_observation: self.version_observation,
        }
    }
}

/// Called by the helper *after* the final login shell, with that shell's
/// environment. It probes the same resolved path that the helper then execs.
/// Retain only a bounded version token, never arbitrary --version output.
pub(crate) fn observe_identity(
    runner: &dyn ProcessRunner,
    executable: &Path,
    deadline: Duration,
) -> AgentIdentity {
    observe(runner, executable, deadline.min(VERSION_DEADLINE))
}

/// [`observe_identity`] for a launch that is checked against the version:
/// the same probe under [`REQUIRED_VERSION_DEADLINE`].
pub(crate) fn observe_required_identity(
    runner: &dyn ProcessRunner,
    executable: &Path,
    deadline: Duration,
) -> AgentIdentity {
    observe(runner, executable, deadline.min(REQUIRED_VERSION_DEADLINE))
}

fn observe(runner: &dyn ProcessRunner, executable: &Path, deadline: Duration) -> AgentIdentity {
    let result = runner.run(&ProcessRequest {
        program: executable.into(),
        args: vec!["--version".into()],
        environment: vec![],
        environment_remove: vec![],
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: 4096,
            stderr_limit: 4096,
            deadline,
        },
        isolate_parent_environment: false,
    });
    let (version, version_observation) = match result {
        Ok(result) if result.status.success() => {
            let version = crate::agent_facts::parse_version(&result.stdout)
                .or_else(|| crate::agent_facts::parse_version(&result.stderr));
            let observation = if version.is_some() {
                VersionObservation::Observed
            } else {
                VersionObservation::Unavailable
            };
            (version, observation)
        }
        Err(WorkerError::Process(ProcessError::DeadlineExceeded { .. })) => {
            (None, VersionObservation::TimedOut)
        }
        Err(WorkerError::Process(ProcessError::OutputLimitExceeded { .. })) => {
            (None, VersionObservation::OutputLimit)
        }
        _ => (None, VersionObservation::Unavailable),
    };
    AgentIdentity {
        executable: executable.to_string_lossy().into_owned(),
        version,
        version_observation,
    }
}

/// The adapter for a command this host runs on its own account outside a
/// turn, such as a session delete. An agent with dialects is asked for its
/// `--version` in the account's login shell first, so the command takes the
/// form of the generation installed now rather than of a recorded one.
pub(crate) fn installed_adapter(
    kind: super::AgentKind,
    runner: &dyn ProcessRunner,
    account_home: &Path,
) -> &'static dyn super::AgentAdapter {
    if !super::has_dialects(kind) {
        return super::adapter_for(kind);
    }
    let version = login_shell_version(runner, super::adapter_for(kind).binary(), account_home);
    super::adapter_for_host(kind, version.as_deref())
}

/// `--version` of `binary` as the account's login shell resolves it. `None`
/// when the command fails, exceeds its bound or prints no version.
fn login_shell_version(
    runner: &dyn ProcessRunner,
    binary: &str,
    account_home: &Path,
) -> Option<String> {
    let argv = [binary.to_owned(), "--version".to_owned()];
    let request = super::prebind_login_request(&argv, account_home, &[]).ok()?;
    let result = runner.run(&request).ok()?;
    if !result.status.success() {
        return None;
    }
    crate::agent_facts::parse_version(&result.stdout)
        .or_else(|| crate::agent_facts::parse_version(&result.stderr))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DeadlineRunner;
    impl ProcessRunner for DeadlineRunner {
        fn run(
            &self,
            request: &ProcessRequest,
        ) -> Result<crate::process::ProcessResult, WorkerError> {
            assert_eq!(request.program, "/resolved/agent");
            assert_eq!(request.args, ["--version"]);
            assert!(!request.isolate_parent_environment);
            assert!(request.environment.is_empty());
            assert_eq!(request.policy.deadline, Duration::from_millis(500));
            assert_eq!(request.policy.stdout_limit, 4096);
            Err(ProcessError::DeadlineExceeded {
                deadline: request.policy.deadline,
            }
            .into())
        }
    }

    #[test]
    fn public_home_path_preserves_agent_location_and_redacts_secrets_idempotently() {
        let home = std::fs::canonicalize(std::env::var_os("HOME").unwrap()).unwrap();
        let identity = AgentIdentity {
            executable: home
                .join(".local/redaction-test-marker/bin/agent")
                .to_string_lossy()
                .into_owned(),
            version: Some("2.3.4".into()),
            version_observation: VersionObservation::Observed,
        };
        let boundary = RedactionBoundary::from_env().with_secrets(["redaction-test-marker"]);
        let public = identity.redacted(&boundary);
        assert_eq!(public.executable, "~/.local/[token]/bin/agent");
        assert_eq!(public.redacted(&boundary), public);
    }

    #[test]
    fn version_observation_preserves_launch_environment_and_remaining_deadline() {
        let identity = observe_identity(
            &DeadlineRunner,
            Path::new("/resolved/agent"),
            Duration::from_millis(500),
        );
        assert_eq!(identity.version_observation, VersionObservation::TimedOut);
        assert!(identity.version.is_none());
    }

    struct BoundRunner(std::sync::Mutex<Vec<Duration>>);
    impl ProcessRunner for BoundRunner {
        fn run(
            &self,
            request: &ProcessRequest,
        ) -> Result<crate::process::ProcessResult, WorkerError> {
            assert_eq!(request.args, ["--version"]);
            self.0.lock().unwrap().push(request.policy.deadline);
            Err(ProcessError::DeadlineExceeded {
                deadline: request.policy.deadline,
            }
            .into())
        }
    }

    #[test]
    fn a_required_version_waits_longer_than_a_diagnostic_one_and_never_past_the_turn() {
        let runner = BoundRunner(std::sync::Mutex::new(Vec::new()));
        let agent = Path::new("/resolved/agent");
        let minute = Duration::from_secs(60);
        let short = Duration::from_millis(500);
        observe_identity(&runner, agent, minute);
        observe_required_identity(&runner, agent, minute);
        observe_identity(&runner, agent, short);
        let last = observe_required_identity(&runner, agent, short);
        assert_eq!(
            *runner.0.lock().unwrap(),
            [VERSION_DEADLINE, REQUIRED_VERSION_DEADLINE, short, short]
        );
        assert!(REQUIRED_VERSION_DEADLINE > VERSION_DEADLINE);
        assert_eq!(last.version_observation, VersionObservation::TimedOut);
    }

    /// The account login shell answering `<agent> --version`.
    struct VersionShell {
        /// stdout, stderr and exit status; `None` runs into the deadline.
        answer: Option<(&'static [u8], &'static [u8], i32)>,
        requests: std::sync::Mutex<Vec<ProcessRequest>>,
    }

    impl ProcessRunner for VersionShell {
        fn run(
            &self,
            request: &ProcessRequest,
        ) -> Result<crate::process::ProcessResult, WorkerError> {
            use std::os::unix::process::ExitStatusExt;
            self.requests.lock().unwrap().push(request.clone());
            match self.answer {
                Some((stdout, stderr, status)) => Ok(crate::process::ProcessResult {
                    status: std::process::ExitStatus::from_raw(status << 8),
                    stdout: stdout.to_vec(),
                    stderr: stderr.to_vec(),
                }),
                None => Err(ProcessError::DeadlineExceeded {
                    deadline: request.policy.deadline,
                }
                .into()),
            }
        }
    }

    #[test]
    fn installed_adapter_takes_its_dialect_from_the_login_shell_version() {
        use crate::agent::AgentKind;
        let home = Path::new("/Users/worker");
        let shell = |answer| VersionShell {
            answer,
            requests: std::sync::Mutex::new(Vec::new()),
        };
        for (answer, standalone) in [
            (Some((&b"1.18.32\n"[..], &b""[..], 0)), false),
            (Some((b"", b"1.18.32\n", 0)), false),
            (Some((b"opencode v2.0.18\n", b"", 0)), true),
            // Without a version the generation is unknown and may be v2.
            (Some((b"development build\n", b"", 0)), true),
            (Some((b"", b"", 0)), true),
            (Some((b"1.18.32\n", b"", 3)), true),
            (None, true),
        ] {
            let shell = shell(answer);
            let delete = installed_adapter(AgentKind::Opencode, &shell, home)
                .delete_session("ses_1")
                .unwrap();
            assert_eq!(
                delete.last().map(String::as_str) == Some("--standalone"),
                standalone,
                "{answer:?}"
            );
            let requests = shell.requests.lock().unwrap();
            assert_eq!(requests.len(), 1, "{answer:?}");
            assert_eq!(requests[0].program, "/bin/zsh");
            assert_eq!(requests[0].args, ["-lc", "exec 'opencode' '--version'"]);
            assert!(requests[0].isolate_parent_environment);
            assert!(
                requests[0]
                    .environment
                    .iter()
                    .any(|(name, value)| name == "HOME" && value == home.as_os_str())
            );
        }
        // An agent with one dialect is not asked for its version.
        for kind in [AgentKind::Codex, AgentKind::Claude, AgentKind::Cursor] {
            let shell = shell(None);
            installed_adapter(kind, &shell, home);
            assert!(shell.requests.lock().unwrap().is_empty(), "{kind:?}");
        }
    }
}
