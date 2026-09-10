use std::{fs, os::unix::fs::PermissionsExt, path::Path, time::Duration};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    config::WorkerEntry,
    error::WorkerError,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{
        FACTS_REFRESH_FAILED_MESSAGE, HERDR_UNAVAILABLE_MESSAGE, HealthStatus, PROTOCOL_VERSION,
        ProbeResponse, SetupFailureKind, SetupHostResult, SetupWarning, SetupWarningCode,
    },
    transport::{SshTransport, ssh_request},
};

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
        let candidate = match self.preflight(current_exe) {
            Ok(candidate) => candidate,
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
        let CandidatePayload { bytes, digest } = candidate;
        let id = self.installation_id.simple().to_string();

        let acquire = match ssh_request(worker, acquire_command(&id), control_policy()) {
            Ok(request) => request,
            Err(error) => {
                return failed(
                    worker,
                    "INVALID_REQUEST",
                    error.to_string(),
                    SetupFailureKind::Unavailable,
                    Vec::new(),
                );
            }
        };
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
                    runner_failure_kind(&error, SetupFailureKind::Infrastructure),
                    Vec::new(),
                );
            }
        }

        let mut transfer = match ssh_request(worker, upload_command(&id), transfer_policy()) {
            Ok(request) => request,
            Err(error) => {
                return failed(
                    worker,
                    "INVALID_REQUEST",
                    error.to_string(),
                    SetupFailureKind::Unavailable,
                    Vec::new(),
                );
            }
        };
        transfer.stdin = Some(bytes);
        let transfer_failure = match self.runner.run(&transfer) {
            Ok(result) if result.status.success() => None,
            Ok(result) => Some((
                process_failure(&transfer.program.to_string_lossy(), &result),
                SetupFailureKind::Unavailable,
            )),
            Err(error) => {
                let failure_kind = runner_failure_kind(&error, SetupFailureKind::Unavailable);
                Some((
                    format!(
                        "failed to launch {}: {error}",
                        transfer.program.to_string_lossy()
                    ),
                    failure_kind,
                ))
            }
        };
        if let Some((message, failure_kind)) = transfer_failure {
            let warnings = self.cleanup_warnings(worker, &id);
            return failed(worker, "TRANSFER_FAILED", message, failure_kind, warnings);
        }

        let digest_request =
            match ssh_request(worker, digest_command(&id, &digest), control_policy()) {
                Ok(request) => request,
                Err(error) => {
                    let warnings = self.cleanup_warnings(worker, &id);
                    return failed(
                        worker,
                        "INVALID_REQUEST",
                        error.to_string(),
                        SetupFailureKind::Unavailable,
                        warnings,
                    );
                }
            };
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
                    runner_failure_kind(&error, SetupFailureKind::Infrastructure),
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

        let prepare = match ssh_request(worker, prepare_command(&id), control_policy()) {
            Ok(request) => request,
            Err(error) => {
                let warnings = self.cleanup_warnings(worker, &id);
                return failed(
                    worker,
                    "INVALID_REQUEST",
                    error.to_string(),
                    SetupFailureKind::Unavailable,
                    warnings,
                );
            }
        };
        if let Err(failure) = run_success(self.runner, &prepare, SetupFailureKind::Infrastructure) {
            let warnings = self.cleanup_warnings(worker, &id);
            return failed(
                worker,
                "INSTALL_FAILED",
                failure.message,
                failure.kind,
                warnings,
            );
        }

        let promotion = match ssh_request(worker, promotion_command(&id), control_policy()) {
            Ok(request) => request,
            Err(error) => {
                let warnings = self.cleanup_warnings(worker, &id);
                return failed(
                    worker,
                    "INVALID_REQUEST",
                    error.to_string(),
                    SetupFailureKind::Unavailable,
                    warnings,
                );
            }
        };
        let promotion_result = match self.runner.run(&promotion) {
            Ok(result) if result.status.success() => PromotionResult::AcknowledgedSuccess,
            Ok(result) if matches!(result.status.code(), Some(code) if code != 255) => {
                PromotionResult::AcknowledgedFailure(ProcessFailure {
                    message: process_failure("promotion", &result),
                    kind: SetupFailureKind::Infrastructure,
                })
            }
            Ok(result) => PromotionResult::Ambiguous(ProcessFailure {
                message: process_failure("promotion", &result),
                kind: SetupFailureKind::Infrastructure,
            }),
            Err(error) => PromotionResult::Ambiguous(ProcessFailure {
                message: format!(
                    "failed to launch {}: {error}",
                    promotion.program.to_string_lossy()
                ),
                kind: runner_failure_kind(&error, SetupFailureKind::Infrastructure),
            }),
        };
        let reconciliation = self.reconcile(worker, &id, &digest);
        match reconciliation {
            Reconciliation::Promoted => {}
            Reconciliation::Previous => {
                if let PromotionResult::AcknowledgedFailure(failure) = promotion_result {
                    let warnings = self.cleanup_warnings(worker, &id);
                    return failed(
                        worker,
                        "PROMOTION_FAILED",
                        failure.message,
                        failure.kind,
                        warnings,
                    );
                }
                let (reason, failure_kind) = match promotion_result {
                    PromotionResult::AcknowledgedSuccess => (
                        "promotion reported success but the previous target remained active".into(),
                        SetupFailureKind::Infrastructure,
                    ),
                    PromotionResult::Ambiguous(failure) => (
                        format!(
                            "{}; the previous target is currently observable but promotion completion is unproven",
                            failure.message
                        ),
                        failure.kind,
                    ),
                    PromotionResult::AcknowledgedFailure(_) => unreachable!(),
                };
                return failed(
                    worker,
                    "UNKNOWN_INSTALLATION_STATE",
                    format!("{reason}; installation lock and scoped state were retained"),
                    failure_kind,
                    Vec::new(),
                );
            }
            Reconciliation::Unknown(reconciliation_failure) => {
                let (message, failure_kind) = match promotion_result {
                    PromotionResult::AcknowledgedSuccess => (
                        format!(
                            "promotion reported success but reconciliation could not prove completion: {}; installation lock and scoped state were retained",
                            reconciliation_failure.message
                        ),
                        reconciliation_failure.kind,
                    ),
                    PromotionResult::AcknowledgedFailure(promotion_failure)
                    | PromotionResult::Ambiguous(promotion_failure) => (
                        format!(
                            "{}; reconciliation could not determine the active target: {}; installation lock and scoped state were retained",
                            promotion_failure.message, reconciliation_failure.message
                        ),
                        strongest_failure_kind(promotion_failure.kind, reconciliation_failure.kind),
                    ),
                };
                return failed(
                    worker,
                    "UNKNOWN_INSTALLATION_STATE",
                    message,
                    failure_kind,
                    Vec::new(),
                );
            }
        }

        // Gatekeeper's first-launch assessment of a freshly copied helper can
        // exceed the 15 s verification probe. Warm the promoted binary under a
        // dedicated deadline so that later probe stays short; a failed warm-up
        // never decides the install on its own.
        let warmup = match ssh_request(worker, warmup_command(&id, &digest), warmup_policy()) {
            Ok(request) => request,
            Err(error) => {
                return self.rollback_unverified_promotion(
                    worker,
                    &id,
                    error.to_string(),
                    None,
                    SetupFailureKind::Unavailable,
                );
            }
        };
        let warmup_failure = run_success(self.runner, &warmup, SetupFailureKind::Infrastructure)
            .err()
            .map(|failure| failure.message);

        // `host refresh-facts` launches every installed agent and can take
        // ~14 s on a loaded mini; the 15 s probe deadline must not absorb
        // that. Layout migration stays a hard failure (exit 77), matching
        // the former verification script.
        let facts_refresh = match ssh_request(
            worker,
            facts_refresh_command(&id, &digest),
            facts_refresh_policy(),
        ) {
            Ok(request) => request,
            Err(error) => {
                return self.rollback_unverified_promotion(
                    worker,
                    &id,
                    error.to_string(),
                    warmup_failure.as_deref(),
                    SetupFailureKind::Unavailable,
                );
            }
        };
        let facts_refresh_failure = match run_facts_refresh(self.runner, &facts_refresh) {
            Ok(failure) => failure,
            Err(failure) => {
                return self.rollback_unverified_promotion(
                    worker,
                    &id,
                    failure.message,
                    warmup_failure.as_deref(),
                    failure.kind,
                );
            }
        };

        let (health, probe_failure_kind) = SshTransport::new(self.runner)
            .probe_with_command_and_failure_kind(worker, verification_command(&id, &digest));
        if health.status != HealthStatus::Ready {
            let message = health
                .error_message
                .unwrap_or_else(|| "installed worker failed verification".into());
            return self.rollback_unverified_promotion(
                worker,
                &id,
                message,
                warmup_failure.as_deref(),
                probe_failure_kind.unwrap_or(SetupFailureKind::Infrastructure),
            );
        }

        // Spec 5.3: the operator asked for the reporter, but the verification
        // probe's fresh herdr fact is not `available`.  A warning, never a
        // failure: the host is installed and the pool is complete without
        // herdr.
        let herdr_warning = (worker.herdr
            && !health
                .probe
                .as_ref()
                .is_some_and(ProbeResponse::herdr_available))
        .then(|| SetupWarning {
            code: SetupWarningCode::HerdrUnavailable,
            message: HERDR_UNAVAILABLE_MESSAGE.to_owned(),
        });
        let protocol_version = health.probe.map(|probe| probe.protocol_version);
        let mut warnings = Vec::new();
        if let Some(message) = warmup_failure {
            warnings.push(SetupWarning {
                code: SetupWarningCode::WarmupFailed,
                message,
            });
        }
        if let Some(message) = facts_refresh_failure {
            warnings.push(SetupWarning {
                code: SetupWarningCode::FactsRefreshFailed,
                message: format!("{FACTS_REFRESH_FAILED_MESSAGE}: {message}"),
            });
        }
        warnings.extend(self.success_cleanup_warnings(worker, &id, &digest));
        warnings.extend(herdr_warning);
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

    fn preflight(&self, current_exe: &Path) -> Result<CandidatePayload, PreflightError> {
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

        let bytes = fs::read(current_exe).map_err(|error| PreflightError {
            message: format!(
                "failed to read candidate executable {}: {error}",
                current_exe.display()
            ),
            failure_kind: SetupFailureKind::Io,
        })?;
        let digest = sha256_bytes(&bytes);
        let result = self
            .runner
            .run(&ProcessRequest {
                program: current_exe.as_os_str().into(),
                args: vec!["host".into(), "probe".into()],
                environment: Vec::new(),
                environment_remove: Vec::new(),
                stdin: None,
                policy: probe_policy(),
                isolate_parent_environment: false,
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

        Ok(CandidatePayload { bytes, digest })
    }

    fn reconcile(&self, worker: &WorkerEntry, id: &str, digest: &str) -> Reconciliation {
        let request =
            match ssh_request(worker, reconciliation_command(id, digest), control_policy()) {
                Ok(request) => request,
                Err(error) => {
                    return Reconciliation::Unknown(ProcessFailure {
                        message: error.to_string(),
                        kind: SetupFailureKind::Unavailable,
                    });
                }
            };
        let result = match self.runner.run(&request) {
            Ok(result) => result,
            Err(error) => {
                return Reconciliation::Unknown(ProcessFailure {
                    message: error.to_string(),
                    kind: runner_failure_kind(&error, SetupFailureKind::Infrastructure),
                });
            }
        };
        if !result.status.success() {
            return Reconciliation::Unknown(ProcessFailure {
                message: process_failure("promotion reconciliation", &result),
                kind: SetupFailureKind::Infrastructure,
            });
        }
        match result.stdout.as_slice() {
            b"promoted\n" => Reconciliation::Promoted,
            b"previous\n" => Reconciliation::Previous,
            b"unknown\n" => Reconciliation::Unknown(ProcessFailure {
                message: "remote state is unknown".into(),
                kind: SetupFailureKind::Infrastructure,
            }),
            _ => Reconciliation::Unknown(ProcessFailure {
                message: "remote query returned an invalid response".into(),
                kind: SetupFailureKind::Infrastructure,
            }),
        }
    }

    fn cleanup_warnings(&self, worker: &WorkerEntry, id: &str) -> Vec<SetupWarning> {
        let cleanup = match ssh_request(worker, cleanup_command(id), control_policy()) {
            Ok(request) => request,
            Err(error) => {
                return vec![SetupWarning {
                    code: SetupWarningCode::CleanupFailed,
                    message: error.to_string(),
                }];
            }
        };
        self.run_cleanup(&cleanup)
    }

    fn success_cleanup_warnings(
        &self,
        worker: &WorkerEntry,
        id: &str,
        digest: &str,
    ) -> Vec<SetupWarning> {
        let cleanup = match ssh_request(
            worker,
            success_cleanup_command(id, digest),
            control_policy(),
        ) {
            Ok(request) => request,
            Err(error) => {
                return vec![SetupWarning {
                    code: SetupWarningCode::CleanupFailed,
                    message: error.to_string(),
                }];
            }
        };
        self.run_cleanup(&cleanup)
    }

    fn run_cleanup(&self, cleanup: &ProcessRequest) -> Vec<SetupWarning> {
        run_success(self.runner, cleanup, SetupFailureKind::Infrastructure)
            .err()
            .map(|failure| SetupWarning {
                code: SetupWarningCode::CleanupFailed,
                message: failure.message,
            })
            .into_iter()
            .collect()
    }

    /// Roll back a promoted helper that did not pass verification. Layout
    /// migration failures use this same path so an outdated host is never
    /// left active.
    fn rollback_unverified_promotion(
        &self,
        worker: &WorkerEntry,
        id: &str,
        mut message: String,
        warmup_failure: Option<&str>,
        failure_kind: SetupFailureKind,
    ) -> SetupHostResult {
        if let Some(warmup) = warmup_failure {
            message = format!("{message}; first-launch warm-up: {warmup}");
        }
        let rollback = match ssh_request(worker, rollback_command(id), control_policy()) {
            Ok(request) => request,
            Err(error) => {
                return failed(
                    worker,
                    "VERIFICATION_FAILED",
                    message,
                    failure_kind,
                    vec![SetupWarning {
                        code: SetupWarningCode::RollbackFailed,
                        message: error.to_string(),
                    }],
                );
            }
        };
        let warnings = run_success(self.runner, &rollback, SetupFailureKind::Infrastructure)
            .err()
            .map(|failure| SetupWarning {
                code: SetupWarningCode::RollbackFailed,
                message: failure.message,
            })
            .into_iter()
            .collect();
        failed(
            worker,
            "VERIFICATION_FAILED",
            message,
            failure_kind,
            warnings,
        )
    }
}

enum Reconciliation {
    Promoted,
    Previous,
    Unknown(ProcessFailure),
}

enum PromotionResult {
    AcknowledgedSuccess,
    AcknowledgedFailure(ProcessFailure),
    Ambiguous(ProcessFailure),
}

struct ProcessFailure {
    message: String,
    kind: SetupFailureKind,
}

struct PreflightError {
    message: String,
    failure_kind: SetupFailureKind,
}

struct CandidatePayload {
    bytes: Vec<u8>,
    digest: String,
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

const ACQUIRE_CONTEXT: &str = r#"set -eu
LC_ALL=C
export LC_ALL
umask 077
require_directory() {
    directory_path=$1
    expected_path=$2
    [ ! -L "$directory_path" ] || return 1
    [ -d "$directory_path" ] || return 1
    physical_path=$(cd -P "$directory_path" 2>/dev/null && /bin/pwd -P) || return 1
    [ "$physical_path" = "$expected_path" ]
}
validate_directory_slot() {
    directory_path=$1
    expected_path=$2
    [ ! -L "$directory_path" ] || return 1
    if [ -e "$directory_path" ]; then
        require_directory "$directory_path" "$expected_path"
    fi
}
ensure_directory() {
    directory_path=$1
    expected_path=$2
    validate_directory_slot "$directory_path" "$expected_path" || return 1
    if [ ! -d "$directory_path" ]; then
        /bin/mkdir "$directory_path" || return 1
    fi
    require_directory "$directory_path" "$expected_path"
}
home_root=$(cd -P "$HOME" 2>/dev/null && /bin/pwd -P) || exit 78
[ -n "$home_root" ] || exit 78
local_root="$home_root/.local"
bin_root="$local_root/bin"
share_root="$local_root/share"
data_root="$share_root/mac-worker"
setup_root="$data_root/setup"
lock_dir="$setup_root/.install-lock"
transaction="$setup_root/$expected_owner"
validate_directory_slot "$local_root" "$home_root/.local" || exit 78
if [ -d "$local_root" ]; then
    validate_directory_slot "$bin_root" "$local_root/bin" || exit 78
    validate_directory_slot "$share_root" "$local_root/share" || exit 78
    if [ -d "$share_root" ]; then
        validate_directory_slot "$data_root" "$share_root/mac-worker" || exit 78
        if [ -d "$data_root" ]; then
            validate_directory_slot "$setup_root" "$data_root/setup" || exit 78
            if [ -d "$setup_root" ]; then
                validate_directory_slot "$lock_dir" "$setup_root/.install-lock" || exit 78
                validate_directory_slot "$transaction" "$setup_root/$expected_owner" || exit 78
            fi
        fi
    fi
fi
ensure_directory "$local_root" "$home_root/.local" || exit 78
ensure_directory "$bin_root" "$local_root/bin" || exit 78
ensure_directory "$share_root" "$local_root/share" || exit 78
ensure_directory "$data_root" "$share_root/mac-worker" || exit 78
ensure_directory "$setup_root" "$data_root/setup" || exit 78
validate_directory_slot "$lock_dir" "$setup_root/.install-lock" || exit 78
[ ! -d "$lock_dir" ] || exit 75
validate_directory_slot "$transaction" "$setup_root/$expected_owner" || exit 78
[ ! -d "$transaction" ] || exit 76
/bin/mkdir "$lock_dir" || exit 75
require_directory "$lock_dir" "$setup_root/.install-lock" || exit 78
printf '%s\n' "$expected_owner" > "$lock_dir/owner" || exit 76
/bin/mkdir "$transaction" || exit 76
require_directory "$transaction" "$setup_root/$expected_owner" || exit 78
printf '%s\n' acquired > "$transaction/state""#;

const LOCKED_CONTEXT: &str = r#"set -eu
LC_ALL=C
export LC_ALL
umask 077
require_directory() {
    directory_path=$1
    expected_path=$2
    [ ! -L "$directory_path" ] || return 1
    [ -d "$directory_path" ] || return 1
    physical_path=$(cd -P "$directory_path" 2>/dev/null && /bin/pwd -P) || return 1
    [ "$physical_path" = "$expected_path" ]
}
require_regular_file() {
    [ ! -L "$1" ] && [ -f "$1" ]
}
allow_absent_regular_file() {
    [ ! -L "$1" ] || return 1
    [ ! -e "$1" ] || [ -f "$1" ]
}
require_absent() {
    [ ! -L "$1" ] && [ ! -e "$1" ]
}
require_empty_regular_file() {
    require_regular_file "$1" || return 1
    file_bytes=$(/usr/bin/wc -c < "$1") || return 1
    [ "$file_bytes" -eq 0 ]
}
require_exact_file() {
    exact_path=$1
    expected_value=$2
    expected_length=$3
    require_regular_file "$exact_path" || return 1
    [ "${#expected_value}" -eq "$expected_length" ] || return 1
    printf '%s\n' "$expected_value" | /usr/bin/cmp -s - "$exact_path"
}
read_canonical_hex() {
    hex_path=$1
    hex_length=$2
    require_regular_file "$hex_path" || return 1
    file_bytes=$(/usr/bin/wc -c < "$hex_path") || return 1
    [ "$file_bytes" -eq "$((hex_length + 1))" ] || return 1
    hex_value=''
    IFS= read -r hex_value < "$hex_path" || return 1
    [ "${#hex_value}" -eq "$hex_length" ] || return 1
    case "$hex_value" in
        *[!0-9a-f]*) return 1 ;;
    esac
    canonical_hex=$hex_value
}
digest_regular_file() {
    require_regular_file "$1" || return 1
    digest_line=$(/usr/bin/shasum -a 256 "$1" 2>/dev/null) || return 1
    digest_value=${digest_line%% *}
    [ "${#digest_value}" -eq 64 ] || return 1
    case "$digest_value" in
        *[!0-9a-f]*) return 1 ;;
    esac
    canonical_digest=$digest_value
}
verify_transaction_entries() {
    /usr/bin/find "$transaction" ! -path "$transaction" -prune \
        -exec /bin/sh -c '
            transaction=$1
            shift
            for entry do
                case "$entry" in
                    "$transaction/worker.new"|"$transaction/worker.previous"|\
                    "$transaction/candidate.sha256"|"$transaction/previous.sha256"|\
                    "$transaction/no-previous"|"$transaction/state") ;;
                    *) exit 1 ;;
                esac
            done
        ' sh "$transaction" {} +
}
verify_lock_entries() {
    /usr/bin/find "$lock_dir" ! -path "$lock_dir" -prune \
        -exec /bin/sh -c '
            owner_path=$1
            shift
            for entry do
                case "$entry" in
                    "$owner_path") ;;
                    *) exit 1 ;;
                esac
            done
        ' sh "$owner_path" {} +
}
verify_known_transaction_types() {
    allow_absent_regular_file "$transaction/worker.new" || return 1
    allow_absent_regular_file "$transaction/worker.previous" || return 1
    allow_absent_regular_file "$transaction/candidate.sha256" || return 1
    allow_absent_regular_file "$transaction/previous.sha256" || return 1
    allow_absent_regular_file "$transaction/no-previous" || return 1
    allow_absent_regular_file "$transaction/state" || return 1
}
resolve_locked_context() {
    home_root=$(cd -P "$HOME" 2>/dev/null && /bin/pwd -P) || return 1
    [ -n "$home_root" ] || return 1
    local_root="$home_root/.local"
    require_directory "$local_root" "$home_root/.local" || return 1
    bin_root="$local_root/bin"
    require_directory "$bin_root" "$local_root/bin" || return 1
    share_root="$local_root/share"
    require_directory "$share_root" "$local_root/share" || return 1
    data_root="$share_root/mac-worker"
    require_directory "$data_root" "$share_root/mac-worker" || return 1
    setup_root="$data_root/setup"
    require_directory "$setup_root" "$data_root/setup" || return 1
    lock_dir="$setup_root/.install-lock"
    require_directory "$lock_dir" "$setup_root/.install-lock" || return 1
    owner_path="$lock_dir/owner"
    require_exact_file "$owner_path" "$expected_owner" 32 || return 1
    transaction="$setup_root/$expected_owner"
    require_directory "$transaction" "$setup_root/$expected_owner" || return 1
    worker_path="$bin_root/worker"
    allow_absent_regular_file "$worker_path" || return 1
    verify_known_transaction_types || return 1
    verify_transaction_entries || return 1
    verify_lock_entries || return 1
    require_directory "$lock_dir" "$setup_root/.install-lock" || return 1
    require_directory "$transaction" "$setup_root/$expected_owner" || return 1
}"#;

const UPLOAD_BODY: &str = r#"verify_upload_input() {
    require_exact_file "$transaction/state" acquired 8 || return 1
    require_absent "$transaction/worker.new" || return 1
    require_absent "$transaction/candidate.sha256" || return 1
    require_absent "$transaction/worker.previous" || return 1
    require_absent "$transaction/previous.sha256" || return 1
    require_absent "$transaction/no-previous" || return 1
}
resolve_locked_context || exit 76
verify_upload_input || exit 77
resolve_locked_context || exit 76
verify_upload_input || exit 77
(set -C; /bin/cat > "$transaction/worker.new") || exit 77
require_regular_file "$transaction/worker.new" || exit 77"#;

const DIGEST_BODY: &str = r#"verify_digest_input() {
    require_exact_file "$transaction/state" acquired 8 || return 1
    require_regular_file "$transaction/worker.new" || return 1
    require_absent "$transaction/candidate.sha256" || return 1
    require_absent "$transaction/worker.previous" || return 1
    require_absent "$transaction/previous.sha256" || return 1
    require_absent "$transaction/no-previous" || return 1
}
resolve_locked_context || exit 76
verify_digest_input || exit 77
digest_regular_file "$transaction/worker.new" || exit 77
first_digest=$canonical_digest
if [ "$first_digest" != "$expected_digest" ]; then
    printf '%s\n' mismatch
    exit 0
fi
resolve_locked_context || exit 76
verify_digest_input || exit 77
digest_regular_file "$transaction/worker.new" || exit 77
[ "$canonical_digest" = "$expected_digest" ] || {
    printf '%s\n' mismatch
    exit 0
}
printf '%s\n' "$expected_digest" > "$transaction/candidate.sha256"
printf '%s\n' staged > "$transaction/state"
printf '%s\n' match"#;

const PREPARE_BODY: &str = r#"verify_staged_state() {
    require_exact_file "$transaction/state" staged 6 || return 1
    read_canonical_hex "$transaction/candidate.sha256" 64 || return 1
    staged_digest=$canonical_hex
    digest_regular_file "$transaction/worker.new" || return 1
    [ "$canonical_digest" = "$staged_digest" ] || return 1
    require_absent "$transaction/worker.previous" || return 1
    require_absent "$transaction/previous.sha256" || return 1
    require_absent "$transaction/no-previous" || return 1
}
resolve_locked_context || exit 76
verify_staged_state || exit 77
if [ -f "$worker_path" ]; then
    digest_regular_file "$worker_path" || exit 77
    previous_digest=$canonical_digest
    resolve_locked_context || exit 76
    verify_staged_state || exit 77
    require_regular_file "$worker_path" || exit 77
    digest_regular_file "$worker_path" || exit 77
    [ "$canonical_digest" = "$previous_digest" ] || exit 77
    /bin/cp -p "$worker_path" "$transaction/worker.previous"
    printf '%s\n' "$previous_digest" > "$transaction/previous.sha256"
else
    resolve_locked_context || exit 76
    verify_staged_state || exit 77
    require_absent "$worker_path" || exit 77
    : > "$transaction/no-previous"
fi
printf '%s\n' prepared > "$transaction/state""#;

const PROMOTION_BODY: &str = r#"verify_prepared_state() {
    require_exact_file "$transaction/state" prepared 8 || return 1
    read_canonical_hex "$transaction/candidate.sha256" 64 || return 1
    candidate_digest=$canonical_hex
    digest_regular_file "$transaction/worker.new" || return 1
    [ "$canonical_digest" = "$candidate_digest" ] || return 1
    if [ -f "$transaction/worker.previous" ]; then
        require_absent "$transaction/no-previous" || return 1
        read_canonical_hex "$transaction/previous.sha256" 64 || return 1
        previous_digest=$canonical_hex
        digest_regular_file "$transaction/worker.previous" || return 1
        [ "$canonical_digest" = "$previous_digest" ] || return 1
        digest_regular_file "$worker_path" || return 1
        [ "$canonical_digest" = "$previous_digest" ] || return 1
    else
        require_absent "$transaction/previous.sha256" || return 1
        require_empty_regular_file "$transaction/no-previous" || return 1
        require_absent "$worker_path" || return 1
    fi
}
resolve_locked_context || exit 76
verify_prepared_state || exit 77
resolve_locked_context || exit 76
verify_prepared_state || exit 77
printf '%s\n' promoting > "$transaction/state"
/bin/chmod 0755 "$transaction/worker.new"
"$transaction/worker.new" host complete-protocol-upgrade "$worker_path" || exit 77
printf '%s\n' promoted > "$transaction/state""#;

const RECONCILIATION_BODY: &str = r#"resolve_locked_context || exit 76
read_canonical_hex "$transaction/candidate.sha256" 64 || exit 77
candidate_digest=$canonical_hex
[ "$candidate_digest" = "$expected_digest" ] || exit 77
if require_exact_file "$transaction/state" promoted 8 \
    && require_absent "$transaction/worker.new" \
    && digest_regular_file "$worker_path" \
    && [ "$canonical_digest" = "$candidate_digest" ]; then
    printf '%s\n' promoted
    exit 0
fi
if { require_exact_file "$transaction/state" prepared 8 \
        || require_exact_file "$transaction/state" promoting 9; } \
    && digest_regular_file "$transaction/worker.new" \
    && [ "$canonical_digest" = "$candidate_digest" ]; then
    if [ -f "$transaction/worker.previous" ]; then
        require_absent "$transaction/no-previous" || exit 77
        read_canonical_hex "$transaction/previous.sha256" 64 || exit 77
        previous_digest=$canonical_hex
        digest_regular_file "$transaction/worker.previous" || exit 77
        [ "$canonical_digest" = "$previous_digest" ] || exit 77
        if digest_regular_file "$worker_path" \
            && [ "$canonical_digest" = "$previous_digest" ]; then
            printf '%s\n' previous
            exit 0
        fi
    elif require_absent "$transaction/previous.sha256" \
        && require_empty_regular_file "$transaction/no-previous" \
        && require_absent "$worker_path"; then
        printf '%s\n' previous
        exit 0
    fi
fi
printf '%s\n' unknown"#;

const WARMUP_BODY: &str = r#"resolve_locked_context || exit 76
require_exact_file "$transaction/state" promoted 8 || exit 77
require_absent "$transaction/worker.new" || exit 77
read_canonical_hex "$transaction/candidate.sha256" 64 || exit 77
[ "$canonical_hex" = "$expected_digest" ] || exit 77
digest_regular_file "$worker_path" || exit 77
[ "$canonical_digest" = "$expected_digest" ] || exit 77
"$worker_path" --version"#;

const FACTS_REFRESH_BODY: &str = r#"resolve_locked_context || exit 76
require_exact_file "$transaction/state" promoted 8 || exit 77
require_absent "$transaction/worker.new" || exit 77
read_canonical_hex "$transaction/candidate.sha256" 64 || exit 77
[ "$canonical_hex" = "$expected_digest" ] || exit 77
digest_regular_file "$worker_path" || exit 77
[ "$canonical_digest" = "$expected_digest" ] || exit 77
"$worker_path" host migrate-layout || exit 77
"$worker_path" host refresh-facts"#;

const VERIFICATION_BODY: &str = r#"resolve_locked_context || exit 76
require_exact_file "$transaction/state" promoted 8 || exit 77
require_absent "$transaction/worker.new" || exit 77
read_canonical_hex "$transaction/candidate.sha256" 64 || exit 77
[ "$canonical_hex" = "$expected_digest" ] || exit 77
digest_regular_file "$worker_path" || exit 77
[ "$canonical_digest" = "$expected_digest" ] || exit 77
exec "$worker_path" host probe"#;

const CLEANUP_BODY: &str = r#"verify_cleanup_state() {
    require_exact_file "$transaction/state" acquired 8 \
        || require_exact_file "$transaction/state" staged 6 \
        || require_exact_file "$transaction/state" prepared 8 \
        || require_exact_file "$transaction/state" promoting 9 \
        || require_exact_file "$transaction/state" promoted 8 \
        || return 1
    if [ -f "$transaction/candidate.sha256" ]; then
        read_canonical_hex "$transaction/candidate.sha256" 64 || return 1
    fi
    if [ -f "$transaction/previous.sha256" ]; then
        read_canonical_hex "$transaction/previous.sha256" 64 || return 1
        [ -f "$transaction/worker.previous" ] || return 1
    fi
    if [ -f "$transaction/no-previous" ]; then
        require_empty_regular_file "$transaction/no-previous" || return 1
        require_absent "$transaction/worker.previous" || return 1
        require_absent "$transaction/previous.sha256" || return 1
    fi
}
resolve_locked_context || exit 76
verify_cleanup_state || exit 77
resolve_locked_context || exit 76
verify_cleanup_state || exit 77
/bin/rm -f "$transaction/worker.new" "$transaction/worker.previous" \
    "$transaction/candidate.sha256" "$transaction/previous.sha256" \
    "$transaction/no-previous" "$transaction/state"
/bin/rmdir "$transaction"
/bin/rm -f "$owner_path"
/bin/rmdir "$lock_dir""#;

const SUCCESS_CLEANUP_BODY: &str = r#"verify_success_cleanup_state() {
    require_exact_file "$transaction/state" promoted 8 || return 1
    require_absent "$transaction/worker.new" || return 1
    read_canonical_hex "$transaction/candidate.sha256" 64 || return 1
    [ "$canonical_hex" = "$expected_digest" ] || return 1
    digest_regular_file "$worker_path" || return 1
    [ "$canonical_digest" = "$expected_digest" ] || return 1
    if [ -f "$transaction/worker.previous" ]; then
        require_absent "$transaction/no-previous" || return 1
        read_canonical_hex "$transaction/previous.sha256" 64 || return 1
        previous_digest=$canonical_hex
        digest_regular_file "$transaction/worker.previous" || return 1
        [ "$canonical_digest" = "$previous_digest" ] || return 1
    else
        require_absent "$transaction/previous.sha256" || return 1
        require_empty_regular_file "$transaction/no-previous" || return 1
    fi
}
resolve_locked_context || exit 76
verify_success_cleanup_state || exit 77
resolve_locked_context || exit 76
verify_success_cleanup_state || exit 77
/bin/rm -f "$transaction/worker.new" "$transaction/worker.previous" \
    "$transaction/candidate.sha256" "$transaction/previous.sha256" \
    "$transaction/no-previous" "$transaction/state"
/bin/rmdir "$transaction"
/bin/rm -f "$owner_path"
/bin/rmdir "$lock_dir""#;

const ROLLBACK_BODY: &str = r#"verify_rollback_state() {
    require_exact_file "$transaction/state" promoted 8 || return 1
    require_absent "$transaction/worker.new" || return 1
    read_canonical_hex "$transaction/candidate.sha256" 64 || return 1
    candidate_digest=$canonical_hex
    digest_regular_file "$worker_path" || return 1
    [ "$canonical_digest" = "$candidate_digest" ] || return 1
    if [ -f "$transaction/worker.previous" ]; then
        require_absent "$transaction/no-previous" || return 1
        read_canonical_hex "$transaction/previous.sha256" 64 || return 1
        previous_digest=$canonical_hex
        digest_regular_file "$transaction/worker.previous" || return 1
        [ "$canonical_digest" = "$previous_digest" ] || return 1
        rollback_previous=1
    else
        require_absent "$transaction/previous.sha256" || return 1
        require_empty_regular_file "$transaction/no-previous" || return 1
        rollback_previous=0
    fi
}
verify_rolled_back_state() {
    require_exact_file "$transaction/state" rolled_back 11 || return 1
    require_absent "$transaction/worker.new" || return 1
    require_absent "$transaction/worker.previous" || return 1
    read_canonical_hex "$transaction/candidate.sha256" 64 || return 1
    if [ -f "$transaction/previous.sha256" ]; then
        read_canonical_hex "$transaction/previous.sha256" 64 || return 1
        restored_digest=$canonical_hex
        digest_regular_file "$worker_path" || return 1
        [ "$canonical_digest" = "$restored_digest" ] || return 1
        require_absent "$transaction/no-previous" || return 1
    else
        require_empty_regular_file "$transaction/no-previous" || return 1
        require_absent "$worker_path" || return 1
    fi
}
resolve_locked_context || exit 76
verify_rollback_state || exit 77
resolve_locked_context || exit 76
verify_rollback_state || exit 77
if [ "$rollback_previous" -eq 1 ]; then
    "$worker_path" host complete-unverified-rollback "$worker_path" \
        "$transaction/worker.previous" || exit 77
else
    "$worker_path" host complete-unverified-rollback "$worker_path" || exit 77
fi
printf '%s\n' rolled_back > "$transaction/state"
resolve_locked_context || exit 76
verify_rolled_back_state || exit 77
/bin/rm -f "$transaction/worker.new" "$transaction/worker.previous" \
    "$transaction/candidate.sha256" "$transaction/previous.sha256" \
    "$transaction/no-previous" "$transaction/state"
/bin/rmdir "$transaction"
/bin/rm -f "$owner_path"
/bin/rmdir "$lock_dir""#;

fn acquire_command(id: &str) -> String {
    format!("expected_owner='{id}'\n{ACQUIRE_CONTEXT}")
}

fn locked_command(id: &str, body: &str) -> String {
    format!("{LOCKED_CONTEXT}\nexpected_owner='{id}'\n{body}")
}

fn upload_command(id: &str) -> String {
    locked_command(id, UPLOAD_BODY)
}

fn digest_command(id: &str, digest: &str) -> String {
    locked_command(id, &format!("expected_digest='{digest}'\n{DIGEST_BODY}"))
}

fn prepare_command(id: &str) -> String {
    locked_command(id, PREPARE_BODY)
}

fn promotion_command(id: &str) -> String {
    locked_command(id, PROMOTION_BODY)
}

fn reconciliation_command(id: &str, digest: &str) -> String {
    locked_command(
        id,
        &format!("expected_digest='{digest}'\n{RECONCILIATION_BODY}"),
    )
}

fn warmup_command(id: &str, digest: &str) -> String {
    locked_command(id, &format!("expected_digest='{digest}'\n{WARMUP_BODY}"))
}

fn facts_refresh_command(id: &str, digest: &str) -> String {
    locked_command(
        id,
        &format!("expected_digest='{digest}'\n{FACTS_REFRESH_BODY}"),
    )
}

fn verification_command(id: &str, digest: &str) -> String {
    locked_command(
        id,
        &format!("expected_digest='{digest}'\n{VERIFICATION_BODY}"),
    )
}

fn cleanup_command(id: &str) -> String {
    locked_command(id, CLEANUP_BODY)
}

fn success_cleanup_command(id: &str, digest: &str) -> String {
    locked_command(
        id,
        &format!("expected_digest='{digest}'\n{SUCCESS_CLEANUP_BODY}"),
    )
}

fn rollback_command(id: &str) -> String {
    locked_command(id, ROLLBACK_BODY)
}

fn probe_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 1024 * 1024,
        stderr_limit: 1024 * 1024,
        deadline: Duration::from_secs(15),
    }
}

/// Generous deadline for the one-shot first launch of a freshly copied helper.
/// macOS Gatekeeper may spend tens of seconds looking up a notarization ticket
/// before the process may run; that verdict is then cached by code hash. The
/// later verification probe keeps its own 15 s cap so ordinary
/// `worker workers` probes stay protected.
fn warmup_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(90),
    }
}

/// Generous deadline for post-promotion layout migration and agent-facts
/// collection. `host refresh-facts` launches every installed agent (version
/// and auth probes, per env profile, with a keychain unlock) and pings herdr;
/// on 2026-09-09 that took 14.0 s and 13.6 s on mini-3, versus 0.15 s for
/// `host probe` alone, so the 15 s probe deadline fired on most setup
/// attempts. This step is allowed 120 s so a slow facts refresh cannot be
/// mistaken for a failed install. The later verification probe keeps its
/// own 15 s cap.
fn facts_refresh_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(120),
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

/// Run the locked facts-refresh step. Exit 76/77 are the script's lock and
/// layout-invariant failures, including `host migrate-layout`; those still
/// roll the promotion back. Any other failure, including a 120 s timeout of
/// `host refresh-facts`, is a warning on an otherwise successful install.
fn run_facts_refresh(
    runner: &dyn ProcessRunner,
    request: &ProcessRequest,
) -> Result<Option<String>, ProcessFailure> {
    match runner.run(request) {
        Ok(result) if result.status.success() => Ok(None),
        Ok(result) if matches!(result.status.code(), Some(76 | 77)) => Err(ProcessFailure {
            message: process_failure(&request.program.to_string_lossy(), &result),
            kind: SetupFailureKind::Infrastructure,
        }),
        Ok(result) => Ok(Some(process_failure(
            &request.program.to_string_lossy(),
            &result,
        ))),
        Err(error) => Ok(Some(format!(
            "failed to launch {}: {error}",
            request.program.to_string_lossy()
        ))),
    }
}

fn run_success(
    runner: &dyn ProcessRunner,
    request: &ProcessRequest,
    default_kind: SetupFailureKind,
) -> Result<(), ProcessFailure> {
    let result = runner.run(request).map_err(|error| ProcessFailure {
        message: format!(
            "failed to launch {}: {error}",
            request.program.to_string_lossy()
        ),
        kind: runner_failure_kind(&error, default_kind),
    })?;
    if result.status.success() {
        Ok(())
    } else {
        Err(ProcessFailure {
            message: process_failure(&request.program.to_string_lossy(), &result),
            kind: default_kind,
        })
    }
}

fn runner_failure_kind(error: &WorkerError, default_kind: SetupFailureKind) -> SetupFailureKind {
    if matches!(error, WorkerError::Io(_)) {
        SetupFailureKind::Io
    } else {
        default_kind
    }
}

fn strongest_failure_kind(first: SetupFailureKind, second: SetupFailureKind) -> SetupFailureKind {
    fn rank(kind: SetupFailureKind) -> u8 {
        match kind {
            SetupFailureKind::Unavailable => 0,
            SetupFailureKind::Infrastructure => 1,
            SetupFailureKind::Io => 2,
        }
    }

    if rank(first) >= rank(second) {
        first
    } else {
        second
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
