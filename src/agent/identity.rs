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

/// The launch wrapper runs in the agent's process group, so a cancel can kill
/// it at any instruction. It must never write into the retained turn
/// directory, where an interrupted private write is cleanup residue. It stages
/// the record in `tmp`, which terminal cleanup removes whole, and the
/// supervisor adopts it after the child is gone.
pub(crate) fn stage_identity(tmp: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let partial = tmp.join(format!(".{STAGED_IDENTITY_FILE}.{}", std::process::id()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&partial)?;
    file.write_all(bytes)?;
    drop(file);
    std::fs::rename(&partial, tmp.join(STAGED_IDENTITY_FILE))
}

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
    let result = runner.run(&ProcessRequest {
        program: executable.into(),
        args: vec!["--version".into()],
        environment: vec![],
        environment_remove: vec![],
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: 4096,
            stderr_limit: 4096,
            deadline: deadline.min(VERSION_DEADLINE),
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
}
