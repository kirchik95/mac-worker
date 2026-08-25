use std::{fs::File, io::Read, os::unix::fs::PermissionsExt, path::Path, time::Duration};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    config::WorkerEntry,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{
        HealthStatus, PROTOCOL_VERSION, ProbeResponse, SetupFailureKind, SetupHostResult,
        SetupWarning, SetupWarningCode,
    },
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
        let digest = match self.preflight(current_exe) {
            Ok(digest) => digest,
            Err(error) => {
                return failed(
                    worker,
                    "LOCAL_BINARY_INVALID",
                    error.message,
                    error.failure_kind,
                    Vec::new(),
                );
            }
        };
        let id = self.installation_id.simple().to_string();

        let acquire = ssh_request(worker, acquire_command(&id), control_policy());
        match self.runner.run(&acquire) {
            Ok(result) if result.status.success() => {}
            Ok(result) if result.status.code() == Some(75) => {
                return failed(
                    worker,
                    "INSTALL_LOCKED",
                    process_failure("installation lock", &result),
                    SetupFailureKind::Unavailable,
                    Vec::new(),
                );
            }
            Ok(result) => {
                return failed(
                    worker,
                    "UNKNOWN_INSTALLATION_STATE",
                    format!(
                        "installation lock acquisition was inconclusive; scoped state was retained: {}",
                        process_failure("installation lock", &result)
                    ),
                    SetupFailureKind::Infrastructure,
                    Vec::new(),
                );
            }
            Err(error) => {
                return failed(
                    worker,
                    "UNKNOWN_INSTALLATION_STATE",
                    format!(
                        "installation lock acquisition result was lost; scoped state was retained: {error}"
                    ),
                    SetupFailureKind::Infrastructure,
                    Vec::new(),
                );
            }
        }

        let transfer = ProcessRequest {
            program: SCP_PROGRAM.into(),
            args: vec![
                "-q".into(),
                "-o".into(),
                "BatchMode=yes".into(),
                "-o".into(),
                "ConnectTimeout=5".into(),
                "--".into(),
                current_exe.as_os_str().into(),
                format!(
                    "{}:~/.local/share/mac-worker/setup/{id}/worker.new",
                    worker.ssh
                )
                .into(),
            ],
            environment: Vec::new(),
            stdin: None,
            policy: transfer_policy(),
        };
        if let Err(message) = run_success(self.runner, &transfer) {
            let warnings = self.cleanup_warnings(worker, &id);
            return failed(
                worker,
                "TRANSFER_FAILED",
                message,
                SetupFailureKind::Unavailable,
                warnings,
            );
        }

        let digest_request = ssh_request(worker, digest_command(&id, &digest), control_policy());
        let digest_result = match self.runner.run(&digest_request) {
            Ok(result) if result.status.success() => result,
            Ok(result) => {
                let message = process_failure("staged candidate digest verification", &result);
                let warnings = self.cleanup_warnings(worker, &id);
                return failed(
                    worker,
                    "DIGEST_VERIFICATION_FAILED",
                    message,
                    SetupFailureKind::Infrastructure,
                    warnings,
                );
            }
            Err(error) => {
                let warnings = self.cleanup_warnings(worker, &id);
                return failed(
                    worker,
                    "DIGEST_VERIFICATION_FAILED",
                    format!("failed to verify staged candidate digest: {error}"),
                    SetupFailureKind::Infrastructure,
                    warnings,
                );
            }
        };
        match digest_result.stdout.as_slice() {
            b"match\n" => {}
            b"mismatch\n" => {
                let warnings = self.cleanup_warnings(worker, &id);
                return failed(
                    worker,
                    "CANDIDATE_DIGEST_MISMATCH",
                    format!("staged candidate SHA-256 did not match expected digest {digest}"),
                    SetupFailureKind::Infrastructure,
                    warnings,
                );
            }
            _ => {
                let warnings = self.cleanup_warnings(worker, &id);
                return failed(
                    worker,
                    "DIGEST_VERIFICATION_FAILED",
                    "staged candidate digest verification returned an invalid response".into(),
                    SetupFailureKind::Infrastructure,
                    warnings,
                );
            }
        }

        let prepare = ssh_request(worker, prepare_command(&id), control_policy());
        if let Err(message) = run_success(self.runner, &prepare) {
            let warnings = self.cleanup_warnings(worker, &id);
            return failed(
                worker,
                "INSTALL_FAILED",
                message,
                SetupFailureKind::Infrastructure,
                warnings,
            );
        }

        let promotion = ssh_request(worker, promotion_command(&id), control_policy());
        let promotion_result = match self.runner.run(&promotion) {
            Ok(result) if result.status.success() => PromotionResult::AcknowledgedSuccess,
            Ok(result) if matches!(result.status.code(), Some(code) if code != 255) => {
                PromotionResult::AcknowledgedFailure(process_failure("promotion", &result))
            }
            Ok(result) => PromotionResult::Ambiguous(process_failure("promotion", &result)),
            Err(error) => PromotionResult::Ambiguous(format!(
                "failed to launch {}: {error}",
                promotion.program.to_string_lossy()
            )),
        };
        let reconciliation = self.reconcile(worker, &id, &digest);
        match reconciliation {
            Reconciliation::Promoted => {}
            Reconciliation::Previous => {
                if let PromotionResult::AcknowledgedFailure(message) = promotion_result {
                    let warnings = self.cleanup_warnings(worker, &id);
                    return failed(
                        worker,
                        "PROMOTION_FAILED",
                        message,
                        SetupFailureKind::Infrastructure,
                        warnings,
                    );
                }
                let reason = match promotion_result {
                    PromotionResult::AcknowledgedSuccess => {
                        "promotion reported success but the previous target remained active".into()
                    }
                    PromotionResult::Ambiguous(error) => format!(
                        "{error}; the previous target is currently observable but promotion completion is unproven"
                    ),
                    PromotionResult::AcknowledgedFailure(_) => unreachable!(),
                };
                return failed(
                    worker,
                    "UNKNOWN_INSTALLATION_STATE",
                    format!("{reason}; installation lock and scoped state were retained"),
                    SetupFailureKind::Infrastructure,
                    Vec::new(),
                );
            }
            Reconciliation::Unknown(reason) => {
                let message = match promotion_result {
                    PromotionResult::AcknowledgedSuccess => format!(
                        "promotion reported success but reconciliation could not prove completion: {reason}; installation lock and scoped state were retained"
                    ),
                    PromotionResult::AcknowledgedFailure(error)
                    | PromotionResult::Ambiguous(error) => format!(
                        "{error}; reconciliation could not determine the active target: {reason}; installation lock and scoped state were retained"
                    ),
                };
                return failed(
                    worker,
                    "UNKNOWN_INSTALLATION_STATE",
                    message,
                    SetupFailureKind::Infrastructure,
                    Vec::new(),
                );
            }
        }

        let health = SshTransport::new(self.runner).probe(worker);
        if health.status != HealthStatus::Ready {
            let message = health
                .error_message
                .unwrap_or_else(|| "installed worker failed verification".into());
            let rollback = ssh_request(worker, rollback_command(&id), control_policy());
            let warnings = run_success(self.runner, &rollback)
                .err()
                .map(|message| SetupWarning {
                    code: SetupWarningCode::RollbackFailed,
                    message,
                })
                .into_iter()
                .collect();
            return failed(
                worker,
                "VERIFICATION_FAILED",
                message,
                SetupFailureKind::Infrastructure,
                warnings,
            );
        }

        let protocol_version = health.probe.map(|probe| probe.protocol_version);
        let warnings = self.cleanup_warnings(worker, &id);
        SetupHostResult {
            name: worker.name.clone(),
            ssh: worker.ssh.clone(),
            installed: true,
            protocol_version,
            error_code: None,
            error_message: None,
            failure_kind: None,
            warnings,
        }
    }

    fn preflight(&self, current_exe: &Path) -> Result<String, PreflightError> {
        let metadata = current_exe.metadata().map_err(|error| PreflightError {
            message: format!(
                "failed to inspect candidate executable {}: {error}",
                current_exe.display()
            ),
            failure_kind: SetupFailureKind::Io,
        })?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
            return Err(PreflightError {
                message: format!(
                    "candidate executable {} is not a regular executable file",
                    current_exe.display()
                ),
                failure_kind: SetupFailureKind::Infrastructure,
            });
        }

        let digest = sha256_file(current_exe).map_err(|error| PreflightError {
            message: format!(
                "failed to hash candidate executable {}: {error}",
                current_exe.display()
            ),
            failure_kind: SetupFailureKind::Io,
        })?;
        let result = self
            .runner
            .run(&ProcessRequest {
                program: current_exe.as_os_str().into(),
                args: vec!["host".into(), "probe".into()],
                environment: Vec::new(),
                stdin: None,
                policy: probe_policy(),
            })
            .map_err(|error| PreflightError {
                failure_kind: if matches!(error, crate::error::WorkerError::Io(_)) {
                    SetupFailureKind::Io
                } else {
                    SetupFailureKind::Infrastructure
                },
                message: format!("failed to launch candidate executable: {error}"),
            })?;
        if !result.status.success() {
            return Err(PreflightError {
                message: process_failure("candidate executable probe", &result),
                failure_kind: SetupFailureKind::Infrastructure,
            });
        }
        let probe: ProbeResponse =
            serde_json::from_slice(&result.stdout).map_err(|error| PreflightError {
                message: format!("candidate executable returned an invalid probe: {error}"),
                failure_kind: SetupFailureKind::Infrastructure,
            })?;
        if probe.protocol_version != PROTOCOL_VERSION {
            return Err(PreflightError {
                message: format!(
                    "candidate protocol version {} does not match required version {PROTOCOL_VERSION}",
                    probe.protocol_version
                ),
                failure_kind: SetupFailureKind::Infrastructure,
            });
        }

        Ok(digest)
    }

    fn reconcile(&self, worker: &WorkerEntry, id: &str, digest: &str) -> Reconciliation {
        let request = ssh_request(worker, reconciliation_command(id, digest), control_policy());
        let result = match self.runner.run(&request) {
            Ok(result) => result,
            Err(error) => return Reconciliation::Unknown(error.to_string()),
        };
        if !result.status.success() {
            return Reconciliation::Unknown(process_failure("promotion reconciliation", &result));
        }
        match result.stdout.as_slice() {
            b"promoted\n" => Reconciliation::Promoted,
            b"previous\n" => Reconciliation::Previous,
            b"unknown\n" => Reconciliation::Unknown("remote state is unknown".into()),
            _ => Reconciliation::Unknown("remote query returned an invalid response".into()),
        }
    }

    fn cleanup_warnings(&self, worker: &WorkerEntry, id: &str) -> Vec<SetupWarning> {
        let cleanup = ssh_request(worker, cleanup_command(id), control_policy());
        run_success(self.runner, &cleanup)
            .err()
            .map(|message| SetupWarning {
                code: SetupWarningCode::CleanupFailed,
                message,
            })
            .into_iter()
            .collect()
    }
}

enum Reconciliation {
    Promoted,
    Previous,
    Unknown(String),
}

enum PromotionResult {
    AcknowledgedSuccess,
    AcknowledgedFailure(String),
    Ambiguous(String),
}

struct PreflightError {
    message: String,
    failure_kind: SetupFailureKind,
}

fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn ssh_request(worker: &WorkerEntry, command: String, policy: ProcessPolicy) -> ProcessRequest {
    ProcessRequest {
        program: SSH_PROGRAM.into(),
        args: vec![
            "-o".into(),
            "BatchMode=yes".into(),
            "-o".into(),
            "ConnectTimeout=5".into(),
            "--".into(),
            worker.ssh.clone().into(),
            command.into(),
        ],
        environment: Vec::new(),
        stdin: None,
        policy,
    }
}

fn acquire_command(id: &str) -> String {
    format!(
        "umask 077 && mkdir -p ~/.local/bin ~/.local/share/mac-worker/setup && if mkdir ~/.local/share/mac-worker/setup/.install-lock; then printf '%s\\n' {id} > ~/.local/share/mac-worker/setup/.install-lock/owner; else exit 75; fi && mkdir ~/.local/share/mac-worker/setup/{id} && printf '%s\\n' acquired > ~/.local/share/mac-worker/setup/{id}/state"
    )
}

fn digest_command(id: &str, digest: &str) -> String {
    format!(
        "candidate=$(/usr/bin/shasum -a 256 ~/.local/share/mac-worker/setup/{id}/worker.new) && candidate=${{candidate%% *}} && if [ \"$candidate\" = {digest} ]; then printf '%s\\n' {digest} > ~/.local/share/mac-worker/setup/{id}/candidate.sha256 && printf '%s\\n' staged > ~/.local/share/mac-worker/setup/{id}/state && printf '%s\\n' match; else printf '%s\\n' mismatch; fi"
    )
}

fn prepare_command(id: &str) -> String {
    format!(
        "if [ -f ~/.local/bin/worker ]; then cp -p ~/.local/bin/worker ~/.local/share/mac-worker/setup/{id}/worker.previous && previous=$(/usr/bin/shasum -a 256 ~/.local/bin/worker) && printf '%s\\n' \"${{previous%% *}}\" > ~/.local/share/mac-worker/setup/{id}/previous.sha256; else : > ~/.local/share/mac-worker/setup/{id}/no-previous; fi && printf '%s\\n' prepared > ~/.local/share/mac-worker/setup/{id}/state"
    )
}

fn promotion_command(id: &str) -> String {
    format!(
        "printf '%s\\n' promoting > ~/.local/share/mac-worker/setup/{id}/state && chmod 0755 ~/.local/share/mac-worker/setup/{id}/worker.new && mv ~/.local/share/mac-worker/setup/{id}/worker.new ~/.local/bin/worker && printf '%s\\n' promoted > ~/.local/share/mac-worker/setup/{id}/state"
    )
}

fn reconciliation_command(id: &str, digest: &str) -> String {
    format!(
        "target=$(/usr/bin/shasum -a 256 ~/.local/bin/worker 2>/dev/null) && target=${{target%% *}}; state=$(/bin/cat ~/.local/share/mac-worker/setup/{id}/state 2>/dev/null) || state=; if [ \"$target\" = {digest} ] && [ \"$state\" = promoted ]; then printf '%s\\n' promoted; elif [ -f ~/.local/share/mac-worker/setup/{id}/previous.sha256 ] && [ \"$target\" = \"$(/bin/cat ~/.local/share/mac-worker/setup/{id}/previous.sha256)\" ]; then printf '%s\\n' previous; elif [ -f ~/.local/share/mac-worker/setup/{id}/no-previous ] && [ ! -e ~/.local/bin/worker ]; then printf '%s\\n' previous; else printf '%s\\n' unknown; fi"
    )
}

fn cleanup_command(id: &str) -> String {
    format!(
        "if [ \"$(/bin/cat ~/.local/share/mac-worker/setup/.install-lock/owner 2>/dev/null)\" = {id} ]; then rm -f ~/.local/share/mac-worker/setup/{id}/worker.new ~/.local/share/mac-worker/setup/{id}/worker.previous ~/.local/share/mac-worker/setup/{id}/candidate.sha256 ~/.local/share/mac-worker/setup/{id}/previous.sha256 ~/.local/share/mac-worker/setup/{id}/no-previous ~/.local/share/mac-worker/setup/{id}/state && rmdir ~/.local/share/mac-worker/setup/{id} && rm -f ~/.local/share/mac-worker/setup/.install-lock/owner && rmdir ~/.local/share/mac-worker/setup/.install-lock; else exit 76; fi"
    )
}

fn rollback_command(id: &str) -> String {
    format!(
        "if [ \"$(/bin/cat ~/.local/share/mac-worker/setup/.install-lock/owner 2>/dev/null)\" = {id} ]; then if [ -f ~/.local/share/mac-worker/setup/{id}/worker.previous ]; then mv ~/.local/share/mac-worker/setup/{id}/worker.previous ~/.local/bin/worker; elif [ -f ~/.local/share/mac-worker/setup/{id}/no-previous ]; then rm -f ~/.local/bin/worker; else exit 77; fi && printf '%s\\n' rolled_back > ~/.local/share/mac-worker/setup/{id}/state && rm -f ~/.local/share/mac-worker/setup/{id}/worker.new ~/.local/share/mac-worker/setup/{id}/candidate.sha256 ~/.local/share/mac-worker/setup/{id}/previous.sha256 ~/.local/share/mac-worker/setup/{id}/no-previous ~/.local/share/mac-worker/setup/{id}/state && rmdir ~/.local/share/mac-worker/setup/{id} && rm -f ~/.local/share/mac-worker/setup/.install-lock/owner && rmdir ~/.local/share/mac-worker/setup/.install-lock; else exit 76; fi"
    )
}

fn probe_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 1024 * 1024,
        stderr_limit: 1024 * 1024,
        deadline: Duration::from_secs(15),
    }
}

fn control_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(30),
    }
}

fn transfer_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(5 * 60),
    }
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

fn failed(
    worker: &WorkerEntry,
    code: &str,
    message: String,
    failure_kind: SetupFailureKind,
    warnings: Vec<SetupWarning>,
) -> SetupHostResult {
    SetupHostResult {
        name: worker.name.clone(),
        ssh: worker.ssh.clone(),
        installed: false,
        protocol_version: None,
        error_code: Some(code.into()),
        error_message: Some(message),
        failure_kind: Some(failure_kind),
        warnings,
    }
}
