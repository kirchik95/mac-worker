use std::{os::unix::fs::PermissionsExt, path::Path};

use uuid::Uuid;

use crate::{
    config::WorkerEntry,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{HealthStatus, PROTOCOL_VERSION, ProbeResponse, SetupHostResult},
    transport::SshTransport,
};

const SSH_PROGRAM: &str = "/usr/bin/ssh";
const SCP_PROGRAM: &str = "/usr/bin/scp";

pub struct Installer<'a> {
    runner: &'a dyn ProcessRunner,
    installation_id: Uuid,
}

impl<'a> Installer<'a> {
    pub fn new(runner: &'a dyn ProcessRunner) -> Self {
        Self {
            runner,
            installation_id: Uuid::new_v4(),
        }
    }

    #[doc(hidden)]
    pub fn with_installation_id(runner: &'a dyn ProcessRunner, installation_id: Uuid) -> Self {
        Self {
            runner,
            installation_id,
        }
    }

    pub fn install(&self, current_exe: &Path, worker: &WorkerEntry) -> SetupHostResult {
        if let Err(message) = self.preflight(current_exe) {
            return failed(worker, "LOCAL_BINARY_INVALID", message);
        }

        let id = self.installation_id.simple().to_string();
        let staging = format!("~/.local/share/mac-worker/setup/{id}");
        let steps = [
            ProcessRequest {
                program: SSH_PROGRAM.into(),
                args: ssh_args(
                    worker,
                    format!("umask 077 && mkdir -p ~/.local/bin {staging}"),
                ),
                stdin: None,
            },
            ProcessRequest {
                program: SCP_PROGRAM.into(),
                args: vec![
                    "-q".into(),
                    "-o".into(),
                    "BatchMode=yes".into(),
                    "-o".into(),
                    "ConnectTimeout=5".into(),
                    current_exe.as_os_str().into(),
                    format!("{}:{staging}/worker.new", worker.ssh).into(),
                ],
                stdin: None,
            },
            ProcessRequest {
                program: SSH_PROGRAM.into(),
                args: ssh_args(
                    worker,
                    format!(
                        "if [ -f ~/.local/bin/worker ]; then cp -p ~/.local/bin/worker {staging}/worker.previous; fi && chmod 0755 {staging}/worker.new && mv {staging}/worker.new ~/.local/bin/worker"
                    ),
                ),
                stdin: None,
            },
        ];

        for step in &steps {
            if let Err(message) = run_success(self.runner, step) {
                return failed(worker, "INSTALL_FAILED", message);
            }
        }

        let health = SshTransport::new(self.runner).probe(worker);
        if health.status != HealthStatus::Ready {
            let mut message = health
                .error_message
                .unwrap_or_else(|| "installed worker failed verification".into());
            let rollback = ProcessRequest {
                program: SSH_PROGRAM.into(),
                args: ssh_args(
                    worker,
                    format!(
                        "rm -f ~/.local/bin/worker && if [ -f {staging}/worker.previous ]; then mv {staging}/worker.previous ~/.local/bin/worker; fi && rm -f {staging}/worker.new && rmdir {staging}"
                    ),
                ),
                stdin: None,
            };
            if let Err(rollback_error) = run_success(self.runner, &rollback) {
                message.push_str("; rollback failed: ");
                message.push_str(&rollback_error);
            }
            return failed(worker, "VERIFICATION_FAILED", message);
        }

        let cleanup = ProcessRequest {
            program: SSH_PROGRAM.into(),
            args: ssh_args(
                worker,
                format!("rm -f {staging}/worker.previous && rmdir {staging}"),
            ),
            stdin: None,
        };
        if let Err(message) = run_success(self.runner, &cleanup) {
            return failed(worker, "INSTALL_FAILED", message);
        }

        SetupHostResult {
            name: worker.name.clone(),
            ssh: worker.ssh.clone(),
            installed: true,
            protocol_version: health.probe.map(|probe| probe.protocol_version),
            error_code: None,
            error_message: None,
        }
    }

    fn preflight(&self, current_exe: &Path) -> Result<(), String> {
        let metadata = current_exe.metadata().map_err(|error| {
            format!(
                "failed to inspect candidate executable {}: {error}",
                current_exe.display()
            )
        })?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
            return Err(format!(
                "candidate executable {} is not a regular executable file",
                current_exe.display()
            ));
        }

        let result = self
            .runner
            .run(&ProcessRequest {
                program: current_exe.as_os_str().into(),
                args: vec!["host".into(), "probe".into()],
                stdin: None,
            })
            .map_err(|error| format!("failed to launch candidate executable: {error}"))?;
        if !result.status.success() {
            return Err(process_failure("candidate executable probe", &result));
        }
        let probe: ProbeResponse = serde_json::from_slice(&result.stdout)
            .map_err(|error| format!("candidate executable returned an invalid probe: {error}"))?;
        if probe.protocol_version != PROTOCOL_VERSION {
            return Err(format!(
                "candidate protocol version {} does not match required version {PROTOCOL_VERSION}",
                probe.protocol_version
            ));
        }

        Ok(())
    }
}

fn ssh_args(worker: &WorkerEntry, command: String) -> Vec<std::ffi::OsString> {
    vec![
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ConnectTimeout=5".into(),
        worker.ssh.clone().into(),
        command.into(),
    ]
}

fn run_success(runner: &dyn ProcessRunner, request: &ProcessRequest) -> Result<(), String> {
    let result = runner.run(request).map_err(|error| {
        format!(
            "failed to launch {}: {error}",
            request.program.to_string_lossy()
        )
    })?;
    if result.status.success() {
        Ok(())
    } else {
        Err(process_failure(&request.program.to_string_lossy(), &result))
    }
}

fn process_failure(operation: &str, result: &ProcessResult) -> String {
    let status = result
        .status
        .code()
        .map_or_else(|| "signal".into(), |code| format!("exit {code}"));
    let stderr = String::from_utf8_lossy(&result.stderr);
    if stderr.trim().is_empty() {
        format!("{operation} failed with {status}")
    } else {
        format!("{operation} failed with {status}: {}", stderr.trim())
    }
}

fn failed(worker: &WorkerEntry, code: &str, message: String) -> SetupHostResult {
    SetupHostResult {
        name: worker.name.clone(),
        ssh: worker.ssh.clone(),
        installed: false,
        protocol_version: None,
        error_code: Some(code.into()),
        error_message: Some(message),
    }
}
