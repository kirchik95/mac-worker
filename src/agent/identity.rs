use std::{path::Path, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    error::{ProcessError, WorkerError},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    redaction::RedactionBoundary,
};

pub(crate) const IDENTITY_FILE: &str = "agent-identity.json";
pub(crate) const VERSION_DEADLINE: Duration = Duration::from_secs(2);

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
    pub(crate) fn redacted(&self, boundary: &RedactionBoundary) -> Self {
        // Keep the useful path below the account home while hiding its owner.
        let executable = std::env::var_os("HOME")
            .and_then(|home| std::fs::canonicalize(home).ok())
            .and_then(|home| {
                Path::new(&self.executable)
                    .strip_prefix(home)
                    .ok()
                    .map(|p| format!("~/{}", p.display()))
            })
            .unwrap_or_else(|| self.executable.clone());
        Self {
            executable: boundary.text(&executable, 4096),
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
