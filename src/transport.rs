use std::{collections::HashSet, time::Duration};

use crate::{
    config::{Config, WorkerEntry},
    error::{ProcessError, ProcessStream, WorkerError},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    protocol::{
        HealthStatus, PROTOCOL_VERSION, ProbeResponse, SetupFailureKind, WorkerHealth,
        WorkersReport, missing_capabilities,
    },
};

const SSH_PROGRAM: &str = "/usr/bin/ssh";
const REMOTE_PROBE_COMMAND: &str = "~/.local/bin/worker host probe";
const MAX_PROBE_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_HOSTNAME_BYTES: usize = 253;
const MAX_ARCH_BYTES: usize = 32;
const MAX_OS_VERSION_BYTES: usize = 64;
const MAX_CAPABILITY_BYTES: usize = 64;
const MAX_CAPABILITY_COUNT: usize = 64;

pub struct SshTransport<R> {
    runner: R,
}

pub struct WorkersService<R> {
    transport: SshTransport<R>,
}

impl<R: ProcessRunner> WorkersService<R> {
    pub fn new(transport: SshTransport<R>) -> Self {
        Self { transport }
    }

    pub fn inspect(&self, config: &Config) -> WorkersReport {
        WorkersReport {
            protocol_version: PROTOCOL_VERSION,
            workers: config
                .workers
                .iter()
                .map(|worker| self.transport.probe(worker))
                .collect(),
        }
    }

    pub fn inspect_with_requirements(
        &self,
        config: &Config,
        requirements: &[String],
    ) -> WorkersReport {
        WorkersReport {
            protocol_version: PROTOCOL_VERSION,
            workers: config
                .workers
                .iter()
                .map(|worker| {
                    let required = stable_required_capabilities(worker, requirements);
                    self.transport.probe_with_failure_kind(worker, &required).0
                })
                .collect(),
        }
    }
}

impl<R: ProcessRunner> SshTransport<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }

    pub fn probe(&self, worker: &WorkerEntry) -> WorkerHealth {
        self.probe_with_failure_kind(worker, &worker.capabilities).0
    }

    pub(crate) fn probe_with_failure_kind(
        &self,
        worker: &WorkerEntry,
        required_capabilities: &[String],
    ) -> (WorkerHealth, Option<SetupFailureKind>) {
        self.probe_with_required_command_and_failure_kind(
            worker,
            required_capabilities,
            REMOTE_PROBE_COMMAND.into(),
        )
    }

    pub(crate) fn probe_with_command_and_failure_kind(
        &self,
        worker: &WorkerEntry,
        remote_command: String,
    ) -> (WorkerHealth, Option<SetupFailureKind>) {
        self.probe_with_required_command_and_failure_kind(
            worker,
            &worker.capabilities,
            remote_command,
        )
    }

    fn probe_with_required_command_and_failure_kind(
        &self,
        worker: &WorkerEntry,
        required_capabilities: &[String],
        remote_command: String,
    ) -> (WorkerHealth, Option<SetupFailureKind>) {
        let result = match self.runner.run(&ssh_request(
            worker,
            remote_command,
            ProcessPolicy {
                stdout_limit: MAX_PROBE_RESPONSE_BYTES,
                stderr_limit: MAX_PROBE_RESPONSE_BYTES,
                deadline: Duration::from_secs(15),
            },
        )) {
            Ok(result) => result,
            Err(error) => {
                if matches!(
                    &error,
                    WorkerError::Process(ProcessError::OutputLimitExceeded {
                        stream: ProcessStream::Stdout,
                        ..
                    })
                ) {
                    return (
                        unavailable(
                            worker,
                            "INVALID_RESPONSE",
                            format!("SSH probe response exceeded {MAX_PROBE_RESPONSE_BYTES} bytes"),
                            None,
                            Vec::new(),
                        ),
                        Some(SetupFailureKind::Infrastructure),
                    );
                }
                let failure_kind = if matches!(&error, WorkerError::Io(_)) {
                    SetupFailureKind::Io
                } else {
                    SetupFailureKind::Infrastructure
                };
                return (
                    unavailable(
                        worker,
                        "SSH_UNAVAILABLE",
                        format!("failed to launch SSH probe: {error}"),
                        None,
                        Vec::new(),
                    ),
                    Some(failure_kind),
                );
            }
        };

        if !result.status.success() {
            let status = result
                .status
                .code()
                .map_or_else(|| "signal".into(), |code| format!("exit {code}"));
            let stderr = String::from_utf8_lossy(&result.stderr);
            let detail = stderr.trim();
            let message = if detail.is_empty() {
                format!("SSH probe failed with {status}")
            } else {
                format!("SSH probe failed with {status}: {detail}")
            };
            return (
                unavailable(worker, "SSH_UNAVAILABLE", message, None, Vec::new()),
                None,
            );
        }

        if result.stdout.len() > MAX_PROBE_RESPONSE_BYTES {
            return (
                unavailable(
                    worker,
                    "INVALID_RESPONSE",
                    format!("SSH probe response exceeded {MAX_PROBE_RESPONSE_BYTES} bytes"),
                    None,
                    Vec::new(),
                ),
                None,
            );
        }

        let response = match std::str::from_utf8(&result.stdout) {
            Ok(response) => response,
            Err(error) => {
                return (
                    unavailable(
                        worker,
                        "INVALID_RESPONSE",
                        format!("SSH probe response was not valid UTF-8: {error}"),
                        None,
                        Vec::new(),
                    ),
                    None,
                );
            }
        };
        let probe: ProbeResponse = match serde_json::from_str(response) {
            Ok(probe) => probe,
            Err(error) => {
                return (
                    unavailable(
                        worker,
                        "INVALID_RESPONSE",
                        format!("SSH probe response was not valid JSON: {error}"),
                        None,
                        Vec::new(),
                    ),
                    None,
                );
            }
        };
        if !valid_probe_response(&probe) {
            return (
                unavailable(
                    worker,
                    "INVALID_RESPONSE",
                    "SSH probe response contained invalid structured fields".into(),
                    None,
                    Vec::new(),
                ),
                None,
            );
        }

        if probe.protocol_version != PROTOCOL_VERSION {
            return (
                unavailable(
                    worker,
                    "PROTOCOL_MISMATCH",
                    format!(
                        "worker protocol version {} does not match required version {PROTOCOL_VERSION}",
                        probe.protocol_version
                    ),
                    Some(probe),
                    Vec::new(),
                ),
                None,
            );
        }

        let missing = missing_capabilities(required_capabilities, &probe);
        if !missing.is_empty() {
            let capability_kind = if required_capabilities == worker.capabilities.as_slice() {
                "declared"
            } else {
                "required"
            };
            return (
                unavailable(
                    worker,
                    "MISSING_CAPABILITIES",
                    format!(
                        "worker is missing {capability_kind} capabilities: {}",
                        missing.join(", ")
                    ),
                    Some(probe),
                    missing,
                ),
                None,
            );
        }

        (
            WorkerHealth {
                name: worker.name.clone(),
                ssh: worker.ssh.clone(),
                status: HealthStatus::Ready,
                probe: Some(probe),
                missing_capabilities: Vec::new(),
                error_code: None,
                error_message: None,
            },
            None,
        )
    }
}

fn valid_probe_response(probe: &ProbeResponse) -> bool {
    valid_hostname(&probe.hostname)
        && valid_arch(&probe.arch)
        && valid_os_version(&probe.os_version)
        && probe.capabilities.len() <= MAX_CAPABILITY_COUNT
        && probe
            .capabilities
            .iter()
            .all(|capability| valid_capability(capability))
        && probe.capabilities.iter().collect::<HashSet<_>>().len() == probe.capabilities.len()
}

fn valid_hostname(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_HOSTNAME_BYTES
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn valid_arch(value: &str) -> bool {
    valid_lowercase_identifier(value, MAX_ARCH_BYTES)
}

fn valid_os_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_OS_VERSION_BYTES
        && value.split('.').all(|component| {
            !component.is_empty() && component.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn valid_capability(value: &str) -> bool {
    valid_lowercase_identifier(value, MAX_CAPABILITY_BYTES)
}

fn valid_lowercase_identifier(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn stable_required_capabilities(worker: &WorkerEntry, requirements: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    worker
        .capabilities
        .iter()
        .chain(requirements)
        .filter(|capability| seen.insert(capability.as_str()))
        .cloned()
        .collect()
}

pub(crate) fn ssh_request(
    worker: &WorkerEntry,
    remote_command: String,
    policy: ProcessPolicy,
) -> ProcessRequest {
    ProcessRequest {
        program: SSH_PROGRAM.into(),
        args: vec![
            "-o".into(),
            "BatchMode=yes".into(),
            "-o".into(),
            "ConnectTimeout=5".into(),
            "-o".into(),
            "ForwardAgent=no".into(),
            "-o".into(),
            "ClearAllForwardings=yes".into(),
            "--".into(),
            worker.ssh.clone().into(),
            remote_command.into(),
        ],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy,
    }
}

fn unavailable(
    worker: &WorkerEntry,
    error_code: &str,
    error_message: String,
    probe: Option<ProbeResponse>,
    missing_capabilities: Vec<String>,
) -> WorkerHealth {
    WorkerHealth {
        name: worker.name.clone(),
        ssh: worker.ssh.clone(),
        status: HealthStatus::Unavailable,
        probe,
        missing_capabilities,
        error_code: Some(error_code.into()),
        error_message: Some(error_message),
    }
}
