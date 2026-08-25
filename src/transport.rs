use std::time::Duration;

use crate::{
    config::{Config, WorkerEntry},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    protocol::{
        HealthStatus, PROTOCOL_VERSION, ProbeResponse, WorkerHealth, WorkersReport,
        missing_capabilities,
    },
};

const SSH_PROGRAM: &str = "/usr/bin/ssh";
const REMOTE_PROBE_COMMAND: &str = "~/.local/bin/worker host probe";
const MAX_PROBE_RESPONSE_BYTES: usize = 1024 * 1024;

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
}

impl<R: ProcessRunner> SshTransport<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }

    pub fn probe(&self, worker: &WorkerEntry) -> WorkerHealth {
        let result = match self.runner.run(&ProcessRequest {
            program: SSH_PROGRAM.into(),
            args: vec![
                "-o".into(),
                "BatchMode=yes".into(),
                "-o".into(),
                "ConnectTimeout=5".into(),
                "--".into(),
                worker.ssh.clone().into(),
                REMOTE_PROBE_COMMAND.into(),
            ],
            stdin: None,
            policy: ProcessPolicy {
                stdout_limit: MAX_PROBE_RESPONSE_BYTES,
                stderr_limit: MAX_PROBE_RESPONSE_BYTES,
                deadline: Duration::from_secs(15),
            },
        }) {
            Ok(result) => result,
            Err(error) => {
                return unavailable(
                    worker,
                    "SSH_UNAVAILABLE",
                    format!("failed to launch SSH probe: {error}"),
                    None,
                    Vec::new(),
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
            return unavailable(worker, "SSH_UNAVAILABLE", message, None, Vec::new());
        }

        if result.stdout.len() > MAX_PROBE_RESPONSE_BYTES {
            return unavailable(
                worker,
                "INVALID_RESPONSE",
                format!("SSH probe response exceeded {MAX_PROBE_RESPONSE_BYTES} bytes"),
                None,
                Vec::new(),
            );
        }

        let response = match std::str::from_utf8(&result.stdout) {
            Ok(response) => response,
            Err(error) => {
                return unavailable(
                    worker,
                    "INVALID_RESPONSE",
                    format!("SSH probe response was not valid UTF-8: {error}"),
                    None,
                    Vec::new(),
                );
            }
        };
        let probe: ProbeResponse = match serde_json::from_str(response) {
            Ok(probe) => probe,
            Err(error) => {
                return unavailable(
                    worker,
                    "INVALID_RESPONSE",
                    format!("SSH probe response was not valid JSON: {error}"),
                    None,
                    Vec::new(),
                );
            }
        };

        if probe.protocol_version != PROTOCOL_VERSION {
            return unavailable(
                worker,
                "PROTOCOL_MISMATCH",
                format!(
                    "worker protocol version {} does not match required version {PROTOCOL_VERSION}",
                    probe.protocol_version
                ),
                Some(probe),
                Vec::new(),
            );
        }

        let missing = missing_capabilities(&worker.capabilities, &probe);
        if !missing.is_empty() {
            return unavailable(
                worker,
                "MISSING_CAPABILITIES",
                format!(
                    "worker is missing declared capabilities: {}",
                    missing.join(", ")
                ),
                Some(probe),
                missing,
            );
        }

        WorkerHealth {
            name: worker.name.clone(),
            ssh: worker.ssh.clone(),
            status: HealthStatus::Ready,
            probe: Some(probe),
            missing_capabilities: Vec::new(),
            error_code: None,
            error_message: None,
        }
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
