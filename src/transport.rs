use std::{
    cell::RefCell,
    collections::HashSet,
    ffi::{OsStr, OsString},
    fs, io,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(debug_assertions)]
use std::path::Component;

use crate::{
    config::{Config, WorkerEntry, valid_ssh_destination},
    error::{ProcessError, ProcessStream, WorkerError},
    lease::{LeaseSummary, MAX_HOST_SLOTS, SlotState},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    protocol::{
        HealthStatus, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION, SetupFailureKind,
        WorkerHealth, WorkersReport, missing_capabilities,
    },
    transfer::HostOperation,
};

const SSH_PROGRAM: &str = "/usr/bin/ssh";
/// Fixture-only. Debug/test-profile binaries honor an absolute replacement
/// for JSON SSH hops. Release ignores this variable. Not a public config key.
#[cfg(debug_assertions)]
const TEST_SSH_ENV: &str = "MAC_WORKER_TEST_SSH";
const REMOTE_PROBE_COMMAND: &str = "~/.local/bin/worker host probe";
const MAX_PROBE_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_HOSTNAME_BYTES: usize = 253;
const MAX_ARCH_BYTES: usize = 32;
const MAX_OS_VERSION_BYTES: usize = 64;
const MAX_CAPABILITY_BYTES: usize = 64;
const MAX_CAPABILITY_COUNT: usize = 64;
const MAX_CONCURRENT_PROBES: usize = 3;
const MAX_PROBE_DEADLINE: Duration = Duration::from_secs(15);
const REFRESH_FACTS_POLICY: ProcessPolicy = ProcessPolicy {
    stdout_limit: 64 * 1024,
    stderr_limit: 64 * 1024,
    deadline: Duration::from_secs(30),
};

/// Prefix a non-default collection budget. The 30s SSH deadline leaves the
/// historical command unchanged so existing exact-match checks keep passing.
fn facts_refresh_remote_command(base: &str, deadline: Duration) -> String {
    let ssh_deadline = deadline.min(REFRESH_FACTS_POLICY.deadline);
    let budget = ssh_deadline.saturating_sub(crate::agent_facts::FACTS_REFRESH_MARGIN);
    if budget == crate::agent_facts::DEFAULT_FACTS_REFRESH_BUDGET {
        base.to_owned()
    } else {
        format!(
            "{}={} {base}",
            crate::agent_facts::FACTS_BUDGET_ENV,
            budget.as_millis()
        )
    }
}

#[doc(hidden)]
pub trait ProbeClock: Send + Sync {
    fn now(&self) -> Duration;
}

struct SystemProbeClock {
    origin: Instant,
}

impl SystemProbeClock {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl ProbeClock for SystemProbeClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

pub struct SshTransport<R> {
    runner: R,
}

pub struct WorkersService<R> {
    transport: SshTransport<R>,
    clock: Arc<dyn ProbeClock>,
}

impl<R: ProcessRunner> WorkersService<R> {
    pub fn new(transport: SshTransport<R>) -> Self {
        Self {
            transport,
            clock: Arc::new(SystemProbeClock::new()),
        }
    }

    #[doc(hidden)]
    pub fn with_clock(transport: SshTransport<R>, clock: Arc<dyn ProbeClock>) -> Self {
        Self { transport, clock }
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

    pub fn inspect_with_budget(&self, config: &Config, budget: Duration) -> WorkersReport {
        self.inspect_budgeted(config, &[], budget, false)
    }

    pub fn inspect_with_requirements_and_budget(
        &self,
        config: &Config,
        requirements: &[String],
        budget: Duration,
    ) -> WorkersReport {
        self.inspect_budgeted(config, requirements, budget, true)
    }

    pub(crate) fn map_workers_budgeted<T, F>(
        &self,
        workers: &[WorkerEntry],
        budget: Duration,
        work: F,
    ) -> Vec<Option<T>>
    where
        T: Send,
        F: Fn(&WorkerEntry, Duration) -> T + Sync,
    {
        if workers.is_empty() {
            return Vec::new();
        }
        let started = self.clock.now();
        let next = AtomicUsize::new(0);
        let results = Mutex::new(
            workers
                .iter()
                .map(|_| None)
                .collect::<Vec<Option<Option<T>>>>(),
        );

        thread::scope(|scope| {
            for _ in 0..MAX_CONCURRENT_PROBES.min(workers.len()) {
                scope.spawn(|| {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(worker) = workers.get(index) else {
                            break;
                        };
                        let outcome = catch_unwind(AssertUnwindSafe(|| {
                            let elapsed = self.clock.now().saturating_sub(started);
                            let remaining = budget.saturating_sub(elapsed);
                            work(worker, remaining)
                        }))
                        .ok();
                        results
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())[index] =
                            Some(outcome);
                    }
                });
            }
        });

        results
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .into_iter()
            .map(Option::flatten)
            .collect()
    }

    fn inspect_budgeted(
        &self,
        config: &Config,
        requirements: &[String],
        budget: Duration,
        include_project_requirements: bool,
    ) -> WorkersReport {
        let outcomes = self.map_workers_budgeted(&config.workers, budget, |worker, remaining| {
            if include_project_requirements {
                let required = stable_required_capabilities(worker, requirements);
                self.transport
                    .probe_with_required_deadline_and_failure_kind(worker, &required, remaining)
                    .0
            } else {
                self.transport.probe_with_deadline(worker, remaining)
            }
        });
        WorkersReport {
            protocol_version: PROTOCOL_VERSION,
            workers: outcomes
                .into_iter()
                .zip(&config.workers)
                .map(|(health, worker)| health.unwrap_or_else(|| probe_aborted(worker)))
                .collect(),
        }
    }
}

impl<R: ProcessRunner> SshTransport<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }

    pub fn git_ssh_command(&self, worker: &WorkerEntry) -> Result<String, WorkerError> {
        if !valid_ssh_destination(&worker.ssh) || worker.remote_binary != "~/.local/bin/worker" {
            return Err(WorkerError::Transport {
                code: "INVALID_REQUEST",
                message: "worker transport configuration is invalid".into(),
            });
        }
        git_ssh_command_line(SshTarget::Worker, &ssh_program()?)
    }

    pub fn refresh_facts(&self, worker: &WorkerEntry) -> Result<(), WorkerError> {
        self.refresh_facts_cleared(worker, false)
    }

    pub(crate) fn refresh_facts_cleared(
        &self,
        worker: &WorkerEntry,
        clear_auth_incidents: bool,
    ) -> Result<(), WorkerError> {
        self.refresh_facts_with_deadline(
            worker,
            REFRESH_FACTS_POLICY.deadline,
            clear_auth_incidents,
        )
    }

    pub(crate) fn refresh_facts_with_deadline(
        &self,
        worker: &WorkerEntry,
        deadline: Duration,
        clear_auth_incidents: bool,
    ) -> Result<(), WorkerError> {
        if !valid_ssh_destination(&worker.ssh) || worker.remote_binary != "~/.local/bin/worker" {
            return Err(WorkerError::Transport {
                code: "INVALID_REQUEST",
                message: "worker transport configuration is invalid".into(),
            });
        }
        let mut policy = REFRESH_FACTS_POLICY;
        policy.deadline = deadline.min(REFRESH_FACTS_POLICY.deadline);
        if policy.deadline.is_zero() {
            return Err(WorkerError::Transport {
                code: "REFRESH_FACTS_FAILED",
                message: "worker fact refresh failed".into(),
            });
        }
        let command = facts_refresh_remote_command(
            if clear_auth_incidents {
                HostOperation::RefreshFactsClear.command()
            } else {
                HostOperation::RefreshFacts.command()
            },
            policy.deadline,
        );
        let request = ssh_request(worker, command, policy)?;
        let result = self
            .runner
            .run(&request)
            .map_err(|_| WorkerError::Transport {
                code: "REFRESH_FACTS_FAILED",
                message: "worker fact refresh could not be started".into(),
            })?;
        if !result.status.success()
            || result.stdout.len() > REFRESH_FACTS_POLICY.stdout_limit
            || result.stderr.len() > REFRESH_FACTS_POLICY.stderr_limit
        {
            return Err(WorkerError::Transport {
                code: "REFRESH_FACTS_FAILED",
                message: "worker fact refresh failed".into(),
            });
        }
        Ok(())
    }

    pub fn probe(&self, worker: &WorkerEntry) -> WorkerHealth {
        self.probe_with_failure_kind(worker, &worker.capabilities).0
    }

    pub fn probe_with_deadline(&self, worker: &WorkerEntry, deadline: Duration) -> WorkerHealth {
        self.probe_with_required_deadline_and_failure_kind(worker, &worker.capabilities, deadline)
            .0
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
            MAX_PROBE_DEADLINE,
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
            MAX_PROBE_DEADLINE,
        )
    }

    fn probe_with_required_deadline_and_failure_kind(
        &self,
        worker: &WorkerEntry,
        required_capabilities: &[String],
        deadline: Duration,
    ) -> (WorkerHealth, Option<SetupFailureKind>) {
        self.probe_with_required_command_and_failure_kind(
            worker,
            required_capabilities,
            REMOTE_PROBE_COMMAND.into(),
            deadline,
        )
    }

    fn probe_with_required_command_and_failure_kind(
        &self,
        worker: &WorkerEntry,
        required_capabilities: &[String],
        remote_command: String,
        deadline: Duration,
    ) -> (WorkerHealth, Option<SetupFailureKind>) {
        if deadline.is_zero() {
            return (probe_budget_exhausted(worker), None);
        }
        let request = match ssh_request(
            worker,
            remote_command,
            ProcessPolicy {
                stdout_limit: MAX_PROBE_RESPONSE_BYTES,
                stderr_limit: MAX_PROBE_RESPONSE_BYTES,
                deadline: deadline.min(MAX_PROBE_DEADLINE),
            },
        ) {
            Ok(request) => request,
            Err(error) => {
                let code = error.public_code();
                return (
                    unavailable(worker, &code, error.to_string(), None, Vec::new()),
                    None,
                );
            }
        };
        let result = match self.runner.run(&request) {
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
        let mut probe: ProbeResponse = match decode_probe_response(response) {
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
        if probe.supervision_version != SUPERVISION_VERSION {
            return (
                unavailable(
                    worker,
                    "PROTOCOL_MISMATCH",
                    format!(
                        "worker supervision version {} does not match required version {SUPERVISION_VERSION}",
                        probe.supervision_version
                    ),
                    Some(probe),
                    Vec::new(),
                ),
                None,
            );
        }

        append_inventory_origin_capabilities(worker, &mut probe);
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
    (probe.protocol_version != PROTOCOL_VERSION
        || (probe.total_disk_bytes > 0 && probe.free_disk_bytes <= probe.total_disk_bytes))
        && valid_hostname(&probe.hostname)
        && valid_arch(&probe.arch)
        && valid_os_version(&probe.os_version)
        && probe.capabilities.len() <= MAX_CAPABILITY_COUNT
        && probe
            .capabilities
            .iter()
            .all(|capability| valid_capability(capability))
        && probe.capabilities.iter().collect::<HashSet<_>>().len() == probe.capabilities.len()
        && valid_probe_occupancy(probe)
}

fn valid_lease_summary(summary: &LeaseSummary) -> bool {
    summary.project_id.len() == 64
        && summary.project_id.bytes().all(is_lower_hex)
        && summary.worktree_id.len() == 64
        && summary.worktree_id.bytes().all(is_lower_hex)
}

fn valid_probe_occupancy(probe: &ProbeResponse) -> bool {
    if probe.configured_slots == 0 {
        // Legacy one-slot wire: occupancy is only slot_state + active_lease.
        return match (probe.slot_state, probe.active_lease.as_ref()) {
            (SlotState::Idle, None) => true,
            (SlotState::Busy, Some(summary)) => valid_lease_summary(summary),
            _ => false,
        };
    }
    if probe.configured_slots > MAX_HOST_SLOTS || probe.busy_slots > probe.configured_slots {
        return false;
    }
    let free = probe.busy_slots < probe.configured_slots;
    match (
        probe.slot_state,
        free,
        probe.busy_slots,
        probe.active_lease.as_ref(),
    ) {
        (SlotState::Idle, true, 0, None) => true,
        (SlotState::Idle, true, busy, Some(summary)) if busy > 0 => valid_lease_summary(summary),
        (SlotState::Busy, false, busy, Some(summary)) if busy > 0 => valid_lease_summary(summary),
        _ => false,
    }
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn decode_probe_response(response: &str) -> Result<ProbeResponse, serde_json::Error> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct LegacyProbe {
        protocol_version: u32,
        hostname: String,
        arch: String,
        os_version: String,
        free_disk_bytes: u64,
        memory_pressure: crate::protocol::MemoryPressure,
        swap_used_bytes: Option<u64>,
        capabilities: Vec<String>,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ProbeWithoutSupervision {
        protocol_version: u32,
        hostname: String,
        arch: String,
        os_version: String,
        free_disk_bytes: u64,
        total_disk_bytes: u64,
        memory_pressure: crate::protocol::MemoryPressure,
        swap_used_bytes: Option<u64>,
        #[serde(default)]
        available_memory_bytes: Option<u64>,
        #[serde(default)]
        cpu_counters: Option<crate::protocol::CpuCounters>,
        slot_state: crate::lease::SlotState,
        active_lease: Option<crate::lease::LeaseSummary>,
        capabilities: Vec<String>,
        #[serde(default)]
        agent_facts: Option<crate::agent_facts::AgentFacts>,
        #[serde(default)]
        facts_age_millis: Option<u64>,
    }

    let without_supervision = |legacy: ProbeWithoutSupervision| ProbeResponse {
        features: None,
        protocol_version: legacy.protocol_version,
        supervision_version: 0,
        hostname: legacy.hostname,
        arch: legacy.arch,
        os_version: legacy.os_version,
        free_disk_bytes: legacy.free_disk_bytes,
        total_disk_bytes: legacy.total_disk_bytes,
        memory_pressure: legacy.memory_pressure,
        swap_used_bytes: legacy.swap_used_bytes,
        available_memory_bytes: legacy.available_memory_bytes,
        cpu_counters: legacy.cpu_counters,
        slot_state: legacy.slot_state,
        active_lease: legacy.active_lease,
        capabilities: legacy.capabilities,
        agent_facts: legacy.agent_facts,
        facts_age_millis: legacy.facts_age_millis,
        configured_slots: 0,
        busy_slots: 0,
        build_id: None,
        binary_sha256: None,
    };
    match serde_json::from_str::<ProbeResponse>(response) {
        Ok(probe) => Ok(probe),
        Err(full_error) => match serde_json::from_str::<ProbeWithoutSupervision>(response) {
            Ok(legacy) => Ok(without_supervision(legacy)),
            Err(_) => match serde_json::from_str::<LegacyProbe>(response) {
                Ok(legacy) => Ok(ProbeResponse {
                    features: None,
                    protocol_version: legacy.protocol_version,
                    supervision_version: 0,
                    hostname: legacy.hostname,
                    arch: legacy.arch,
                    os_version: legacy.os_version,
                    free_disk_bytes: legacy.free_disk_bytes,
                    total_disk_bytes: 0,
                    memory_pressure: legacy.memory_pressure,
                    swap_used_bytes: legacy.swap_used_bytes,
                    available_memory_bytes: None,
                    cpu_counters: None,
                    slot_state: crate::lease::SlotState::Idle,
                    active_lease: None,
                    capabilities: legacy.capabilities,
                    agent_facts: None,
                    facts_age_millis: None,
                    configured_slots: 0,
                    busy_slots: 0,
                    build_id: None,
                    binary_sha256: None,
                }),
                Err(_) => Err(full_error),
            },
        },
    }
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

fn append_inventory_origin_capabilities(worker: &WorkerEntry, probe: &mut ProbeResponse) {
    for capability in worker
        .capabilities
        .iter()
        .filter(|capability| is_origin_capability(capability))
    {
        if !probe.capabilities.contains(capability) {
            probe.capabilities.push(capability.clone());
        }
    }
}

fn is_origin_capability(capability: &str) -> bool {
    capability
        .strip_prefix("origin:")
        .is_some_and(|host| !host.is_empty())
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

/// Runtime SSH client plan. `control_dir` overrides `~/.cache/mac-worker/ssh`
/// in tests. Production leaves it empty and uses the account cache.
#[derive(Clone, Debug)]
struct SshSettings {
    multiplex: bool,
    config_file: Option<PathBuf>,
    control_dir: Option<PathBuf>,
}

impl SshSettings {
    #[cfg(test)]
    fn direct() -> Self {
        Self {
            multiplex: false,
            config_file: None,
            control_dir: None,
        }
    }

    fn from_installed() -> Self {
        let ssh = crate::config::installed_ssh();
        Self {
            multiplex: ssh.multiplex,
            config_file: ssh.config_file,
            control_dir: None,
        }
    }
}

/// Worker traffic uses the controller's managed identity/trust. Origin traffic
/// always uses the account's own SSH configuration (including git-shell keys).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SshTarget {
    Worker,
    Origin,
}

enum SshRoute {
    Direct,
    Multiplexed { control_path: String },
}

thread_local! {
    static SSH_OVERRIDE: RefCell<Option<SshSettings>> = const { RefCell::new(None) };
}

#[cfg(test)]
struct RestoreSshOverride(Option<SshSettings>);

#[cfg(test)]
impl Drop for RestoreSshOverride {
    fn drop(&mut self) {
        let previous = self.0.take();
        SSH_OVERRIDE.with(|slot| *slot.borrow_mut() = previous);
    }
}

#[cfg(test)]
fn with_ssh_settings<T>(settings: SshSettings, body: impl FnOnce() -> T) -> T {
    let previous = SSH_OVERRIDE.with(|slot| slot.replace(Some(settings)));
    let _restore = RestoreSshOverride(previous);
    body()
}

fn current_ssh_settings() -> SshSettings {
    SSH_OVERRIDE.with(|slot| {
        slot.borrow()
            .clone()
            .unwrap_or_else(SshSettings::from_installed)
    })
}

fn ssh_exec_args(
    target: SshTarget,
    destination: &str,
    remote_command: &str,
) -> Result<Vec<OsString>, WorkerError> {
    let mut args = ssh_option_args(target, true);
    args.push("--".into());
    args.push(destination.into());
    args.push(remote_command.into());
    Ok(args)
}

fn ssh_option_args(target: SshTarget, clear_forwardings: bool) -> Vec<OsString> {
    let settings = current_ssh_settings();
    let mut args = ssh_config_file_args(target, &settings);
    args.extend(
        option_values(&settings, target, clear_forwardings)
            .into_iter()
            .flat_map(|value| [OsString::from("-o"), OsString::from(value)]),
    );
    args
}

/// Dashboard local-forward options. Always a dedicated connection: multiplexing
/// would hide a dead path inside a shared ControlMaster, and the forward has
/// its own server keepalives.
fn ssh_forward_option_args(target: SshTarget) -> Vec<OsString> {
    let settings = current_ssh_settings();
    let mut args = ssh_config_file_args(target, &settings);
    args.push("-T".into());
    args.extend(
        forward_option_values()
            .into_iter()
            .flat_map(|value| [OsString::from("-o"), OsString::from(value)]),
    );
    args
}

fn ssh_config_file_args(target: SshTarget, settings: &SshSettings) -> Vec<OsString> {
    let mut args = Vec::new();
    if target == SshTarget::Worker
        && let Some(path) = &settings.config_file
    {
        args.extend([OsString::from("-F"), path.as_os_str().to_owned()]);
    }
    args
}

fn forward_option_values() -> Vec<String> {
    vec![
        "BatchMode=yes".to_owned(),
        "ConnectTimeout=5".to_owned(),
        "ForwardAgent=no".to_owned(),
        "ExitOnForwardFailure=yes".to_owned(),
        "ControlMaster=no".to_owned(),
        "ControlPath=none".to_owned(),
        "ServerAliveInterval=15".to_owned(),
        "ServerAliveCountMax=3".to_owned(),
    ]
}

fn ssh_shell_options(target: SshTarget, clear_forwardings: bool) -> Result<String, WorkerError> {
    ssh_option_args(target, clear_forwardings)
        .iter()
        .map(|arg| {
            arg.to_str()
                .map_or_else(|| Err(invalid_test_ssh()), shell_option_value)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join(" "))
}

fn option_values(
    settings: &SshSettings,
    target: SshTarget,
    clear_forwardings: bool,
) -> Vec<String> {
    let mut values = vec![
        "BatchMode=yes".to_owned(),
        "ConnectTimeout=5".to_owned(),
        "ForwardAgent=no".to_owned(),
    ];
    if clear_forwardings {
        values.push("ClearAllForwardings=yes".to_owned());
    } else {
        values.push("ExitOnForwardFailure=yes".to_owned());
    }
    if let SshRoute::Multiplexed { control_path } = resolve_ssh_route(settings, target) {
        values.push("ControlMaster=auto".to_owned());
        values.push(format!("ControlPath={control_path}"));
        values.push("ControlPersist=60".to_owned());
        values.push("ServerAliveInterval=10".to_owned());
        values.push("ServerAliveCountMax=3".to_owned());
    }
    values
}

fn shell_option_value(value: &str) -> Result<String, WorkerError> {
    if value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-' | b'=' | b'%')
    }) {
        return Ok(value.to_owned());
    }
    posix_shell_quote(OsStr::new(value))
}

/// Direct when multiplexing is off, or when the private control directory or
/// an existing control socket cannot be used. A socket that does not accept
/// immediately is removed so the next command can open a new master instead
/// of waiting on the stuck one.
fn resolve_ssh_route(settings: &SshSettings, target: SshTarget) -> SshRoute {
    if !settings.multiplex {
        return SshRoute::Direct;
    }
    let Some(directory) = control_directory(settings, target) else {
        return SshRoute::Direct;
    };
    if prepare_control_directory(&directory).is_err() {
        return SshRoute::Direct;
    }
    if control_directory_unusable(&directory) {
        return SshRoute::Direct;
    }
    let control_path = directory.join("%C");
    let Some(control_path) = control_path.to_str() else {
        return SshRoute::Direct;
    };
    SshRoute::Multiplexed {
        control_path: control_path.to_owned(),
    }
}

fn control_directory(settings: &SshSettings, target: SshTarget) -> Option<PathBuf> {
    let directory = if let Some(directory) = &settings.control_dir {
        directory.clone()
    } else {
        let home = std::env::var_os("HOME")?;
        if home.is_empty() {
            return None;
        }
        PathBuf::from(home)
            .join(".cache")
            .join("mac-worker")
            .join("ssh")
    };
    if target == SshTarget::Worker
        && let Some(path) = &settings.config_file
    {
        use sha2::{Digest, Sha256};
        let digest = format!("{:x}", Sha256::digest(path.as_os_str().as_encoded_bytes()));
        return Some(directory.with_file_name(format!("ssh-{}", &digest[..16])));
    }
    Some(directory)
}

fn prepare_control_directory(directory: &Path) -> io::Result<()> {
    if directory.exists() {
        if !directory.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "ssh control path is not a directory",
            ));
        }
    } else {
        fs::create_dir_all(directory)?;
    }
    let mut permissions = fs::metadata(directory)?.permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(directory, permissions)?;
    let mode = fs::metadata(directory)?.permissions().mode() & 0o777;
    if mode != 0o700 {
        return Err(io::Error::other("ssh control directory is not private"));
    }
    Ok(())
}

fn control_directory_unusable(directory: &Path) -> bool {
    let Ok(entries) = fs::read_dir(directory) else {
        return true;
    };
    let mut unusable = false;
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_socket() {
            continue;
        }
        if unix_socket_unusable(&path) {
            let _ = fs::remove_file(&path);
            unusable = true;
        }
    }
    unusable
}

fn unix_socket_unusable(path: &Path) -> bool {
    let Some(text) = path.to_str() else {
        return true;
    };
    let bytes = text.as_bytes();
    if bytes.is_empty() || bytes.len() >= 104 {
        return true;
    }
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return true;
    }
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags >= 0 {
        unsafe {
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (index, byte) in bytes.iter().enumerate() {
        address.sun_path[index] = *byte as libc::c_char;
    }
    let path_len = bytes.len();
    let addr_len = std::mem::offset_of!(libc::sockaddr_un, sun_path) + path_len + 1;
    address.sun_len = u8::try_from(addr_len).unwrap_or(u8::MAX);
    let length = libc::socklen_t::try_from(addr_len).unwrap_or(0);
    let connected =
        unsafe { libc::connect(fd, (&raw const address).cast::<libc::sockaddr>(), length) };
    unsafe {
        libc::close(fd);
    }
    connected != 0
}

pub(crate) fn ssh_request(
    worker: &WorkerEntry,
    remote_command: String,
    policy: ProcessPolicy,
) -> Result<ProcessRequest, WorkerError> {
    ssh_exec_request(
        SshTarget::Worker,
        &worker.ssh,
        &remote_command,
        policy,
        None,
    )
}

pub(crate) fn ssh_exec_request(
    target: SshTarget,
    destination: &str,
    remote_command: &str,
    policy: ProcessPolicy,
    stdin: Option<Vec<u8>>,
) -> Result<ProcessRequest, WorkerError> {
    Ok(ProcessRequest {
        program: ssh_program()?,
        args: ssh_exec_args(target, destination, remote_command)?,
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin,
        policy,
        isolate_parent_environment: false,
    })
}

// Channel bootstrap deliberately does not use resolve_ssh_route: its directory
// scan can unlink unrelated stale sockets. The channel retains one endpoint.
pub(crate) fn channel_control_directory(ssh: &crate::config::SshConfig) -> Option<PathBuf> {
    control_directory(
        &SshSettings {
            multiplex: ssh.multiplex,
            config_file: ssh.config_file.clone(),
            control_dir: None,
        },
        SshTarget::Worker,
    )
}

fn channel_master_args(ssh: &crate::config::SshConfig, control_path: &Path) -> Vec<OsString> {
    let mut args = Vec::new();
    if let Some(config) = &ssh.config_file {
        args.extend([OsString::from("-F"), config.as_os_str().to_owned()]);
    }
    for option in [
        "BatchMode=yes",
        "ConnectTimeout=5",
        "ForwardAgent=no",
        "ClearAllForwardings=yes",
        "ExitOnForwardFailure=yes",
        "ControlMaster=auto",
        "ControlPersist=60",
        "StreamLocalBindMask=0177",
        "StreamLocalBindUnlink=no",
        "ServerAliveInterval=10",
        "ServerAliveCountMax=3",
    ] {
        args.extend([OsString::from("-o"), OsString::from(option)]);
    }
    // -S preserves paths with spaces/quotes as a single argv value. OpenSSH,
    // rather than Rust, expands the template during -G resolution.
    args.extend([OsString::from("-S"), control_path.as_os_str().to_owned()]);
    args
}

pub(crate) fn channel_resolution_request(
    ssh: &crate::config::SshConfig,
    destination: &str,
    directory: &Path,
    policy: ProcessPolicy,
) -> Result<ProcessRequest, WorkerError> {
    let mut args = channel_master_args(ssh, &directory.join("%C"));
    args.extend([
        OsString::from("-G"),
        OsString::from("--"),
        destination.into(),
    ]);
    Ok(ProcessRequest {
        program: ssh_program()?,
        args,
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy,
        isolate_parent_environment: false,
    })
}

pub(crate) fn channel_bootstrap_request(
    ssh: &crate::config::SshConfig,
    destination: &str,
    endpoint: &Path,
    policy: ProcessPolicy,
) -> Result<ProcessRequest, WorkerError> {
    let mut args = channel_master_args(ssh, endpoint);
    args.extend([
        OsString::from("--"),
        destination.into(),
        HostOperation::ControllerRpc.command().into(),
    ]);
    Ok(ProcessRequest {
        program: ssh_program()?,
        args,
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy,
        isolate_parent_environment: false,
    })
}

pub(crate) fn channel_control_request(
    master: &crate::controller::channel::contracts::MasterPlan,
    operation: &str,
    forward: Option<&str>,
    policy: ProcessPolicy,
) -> Result<ProcessRequest, WorkerError> {
    let destination = master
        .bootstrap_request
        .args
        .windows(2)
        .find(|pair| pair[0] == "--")
        .and_then(|pair| pair[1].to_str())
        .filter(|destination| valid_ssh_destination(destination))
        .ok_or_else(|| {
            WorkerError::Protocol("CONTROLLER_UNAVAILABLE: invalid captured SSH destination".into())
        })?;
    let mut args = vec![OsString::from("-F"), OsString::from("/dev/null")];
    for option in [
        "BatchMode=yes",
        "ConnectTimeout=5",
        "ForwardAgent=no",
        "ExitOnForwardFailure=yes",
        "ControlMaster=no",
        "StreamLocalBindMask=0177",
        "StreamLocalBindUnlink=no",
        "ServerAliveInterval=10",
        "ServerAliveCountMax=3",
    ] {
        args.extend([OsString::from("-o"), option.into()]);
    }
    args.extend([
        OsString::from("-S"),
        master.control_path.as_os_str().to_owned(),
        OsString::from("-O"),
        operation.into(),
    ]);
    if let Some(forward) = forward {
        args.extend([OsString::from("-L"), forward.into()]);
    }
    args.extend([OsString::from("--"), destination.into()]);
    // Config edits cannot redirect cancellation's program, endpoint or pair.
    Ok(ProcessRequest {
        program: master.bootstrap_request.program.clone(),
        args,
        environment: master.bootstrap_request.environment.clone(),
        environment_remove: master.bootstrap_request.environment_remove.clone(),
        stdin: None,
        policy,
        isolate_parent_environment: master.bootstrap_request.isolate_parent_environment,
    })
}

/// SSH argv for a same-port local forward. Unlike [`ssh_request`], this keeps
/// local forwards (`ExitOnForwardFailure=yes`, no `ClearAllForwardings`) so the
/// dashboard tunnel can bind `-L 127.0.0.1:N:127.0.0.1:N`. The forward is always
/// its own connection (`-T`, `ControlMaster=no`, `ControlPath=none`) with
/// server keepalives, even when `[ssh] multiplex` is on.
pub(crate) fn ssh_local_forward_request(
    target: SshTarget,
    destination: &str,
    port: u16,
    remote_command: String,
    policy: ProcessPolicy,
) -> Result<ProcessRequest, WorkerError> {
    let forward = format!("127.0.0.1:{port}:127.0.0.1:{port}");
    let mut args = ssh_forward_option_args(target);
    args.push("-L".into());
    args.push(forward.into());
    args.push("--".into());
    args.push(destination.into());
    args.push(remote_command.into());
    Ok(ProcessRequest {
        program: ssh_program()?,
        args,
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy,
        isolate_parent_environment: false,
    })
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

fn probe_budget_exhausted(worker: &WorkerEntry) -> WorkerHealth {
    unavailable(
        worker,
        "SSH_UNAVAILABLE",
        "worker probe budget was exhausted".into(),
        None,
        Vec::new(),
    )
}

fn probe_aborted(worker: &WorkerEntry) -> WorkerHealth {
    unavailable(
        worker,
        "SSH_UNAVAILABLE",
        "worker probe aborted unexpectedly".into(),
        None,
        Vec::new(),
    )
}

/// JSON/control SSH executable and the Git `GIT_SSH_COMMAND` program.
///
/// Stock path is `/usr/bin/ssh`. Debug builds may replace it when the child
/// env sets `MAC_WORKER_TEST_SSH` to an absolute path. Release builds never
/// read the variable. A set-but-invalid value fails closed and does not fall
/// through to live `/usr/bin/ssh`.
pub(crate) fn ssh_program() -> Result<OsString, WorkerError> {
    #[cfg(debug_assertions)]
    {
        ssh_program_from_override(std::env::var_os(TEST_SSH_ENV))
    }
    #[cfg(not(debug_assertions))]
    {
        Ok(OsString::from(SSH_PROGRAM))
    }
}

// Release builds never read the test override; only tests call this there.
#[cfg(any(test, debug_assertions))]
pub(crate) fn ssh_program_from_override(value: Option<OsString>) -> Result<OsString, WorkerError> {
    #[cfg(debug_assertions)]
    {
        match value {
            None => Ok(OsString::from(SSH_PROGRAM)),
            Some(value) if fixture_ssh_path_ok(&value) => Ok(value),
            Some(_) => Err(invalid_test_ssh()),
        }
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = value;
        Ok(OsString::from(SSH_PROGRAM))
    }
}

#[cfg(debug_assertions)]
fn fixture_ssh_path_ok(value: &OsStr) -> bool {
    let path = Path::new(value);
    path.is_absolute()
        && !value.as_encoded_bytes().contains(&0)
        && path
            .components()
            .all(|component| component != Component::ParentDir)
}

fn invalid_test_ssh() -> WorkerError {
    WorkerError::Protocol(
        "CONTROLLER_UNAVAILABLE: MAC_WORKER_TEST_SSH must be an absolute executable path".into(),
    )
}

/// Quote an absolute SSH executable for `GIT_SSH_COMMAND` / rsync `-e`.
/// Safe unquoted paths (`/usr/bin/ssh`) stay unquoted so stock Git command
/// lines do not change.
pub(crate) fn posix_shell_quote(value: &OsStr) -> Result<String, WorkerError> {
    let Some(text) = value.to_str() else {
        return Err(invalid_test_ssh());
    };
    if !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        return Ok(text.to_owned());
    }
    let mut quoted = String::from("'");
    for ch in text.chars() {
        if ch == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('\'');
    Ok(quoted)
}

pub(crate) fn git_ssh_command_line(
    target: SshTarget,
    program: &OsStr,
) -> Result<String, WorkerError> {
    Ok(format!(
        "{} {}",
        posix_shell_quote(program)?,
        ssh_shell_options(target, true)?
    ))
}

pub(crate) fn rsync_ssh_shell(target: SshTarget) -> Result<OsString, WorkerError> {
    let program = ssh_program()?;
    let shell = format!(
        "{} {} --",
        posix_shell_quote(&program)?,
        ssh_shell_options(target, true)?
    );
    // rsync's -e parser understands quote alternation but not backslash
    // escapes. Keep Git's POSIX encoding and use literal apostrophes here.
    Ok(shell.replace("'\\''", "'\"'\"'").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::unix::io::IntoRawFd;

    /// A sibling test can inherit this listener before `CLOEXEC` sticks. Wait
    /// until connect fails with `ConnectionRefused` so the stale-socket
    /// assertions are not racing a still-live fd.
    fn wait_until_unix_connect_refused(path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match std::os::unix::net::UnixStream::connect(path) {
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => return,
                _ => {
                    if Instant::now() >= deadline {
                        panic!(
                            "stale control socket {} stayed connectable for 5s",
                            path.display()
                        );
                    }
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }

    #[test]
    fn test_ssh_override_accepts_only_absolute_paths() {
        assert_eq!(
            ssh_program_from_override(None).unwrap().as_os_str(),
            "/usr/bin/ssh"
        );
        #[cfg(debug_assertions)]
        {
            assert_eq!(
                ssh_program_from_override(Some(OsString::from("fake-ssh")))
                    .unwrap_err()
                    .public_code(),
                "CONTROLLER_UNAVAILABLE"
            );
            assert_eq!(
                ssh_program_from_override(Some(OsString::from("/tmp/../usr/bin/ssh")))
                    .unwrap_err()
                    .public_code(),
                "CONTROLLER_UNAVAILABLE"
            );
            assert_eq!(
                ssh_program_from_override(Some(OsString::from("/tmp/mac-worker-fake-ssh")))
                    .unwrap()
                    .as_os_str(),
                "/tmp/mac-worker-fake-ssh"
            );
        }
        #[cfg(not(debug_assertions))]
        {
            assert_eq!(
                ssh_program_from_override(Some(OsString::from("fake-ssh")))
                    .unwrap()
                    .as_os_str(),
                "/usr/bin/ssh"
            );
            assert_eq!(
                ssh_program_from_override(Some(OsString::from("/tmp/mac-worker-fake-ssh")))
                    .unwrap()
                    .as_os_str(),
                "/usr/bin/ssh"
            );
        }
    }

    #[test]
    fn git_ssh_command_quotes_paths_that_need_a_shell() {
        assert_eq!(
            git_ssh_command_line(SshTarget::Worker, OsStr::new("/usr/bin/ssh")).unwrap(),
            "/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes"
        );
        assert_eq!(
            git_ssh_command_line(SshTarget::Worker, OsStr::new("/tmp/mac-worker fake-ssh"))
                .unwrap(),
            "'/tmp/mac-worker fake-ssh' -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes"
        );
        assert_eq!(
            git_ssh_command_line(SshTarget::Worker, OsStr::new("/tmp/mac-worker's-ssh")).unwrap(),
            "'/tmp/mac-worker'\\''s-ssh' -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes"
        );
    }

    #[test]
    fn ssh_request_program_is_stock_without_override() {
        let worker = WorkerEntry {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            slots: 1,
            capabilities: Vec::new(),
            remote_binary: "~/.local/bin/worker".into(),
            herdr: false,
        };
        let request = ssh_request(
            &worker,
            HostOperation::ControllerRpc.command().into(),
            ProcessPolicy {
                stdout_limit: 1024,
                stderr_limit: 1024,
                deadline: Duration::from_secs(1),
            },
        )
        .unwrap();
        assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
        assert_eq!(request.args[request.args.len() - 2], "mac1");
    }

    #[test]
    fn default_refresh_deadline_keeps_the_historical_command() {
        let command = facts_refresh_remote_command(
            HostOperation::RefreshFacts.command(),
            Duration::from_secs(30),
        );
        assert_eq!(command, HostOperation::RefreshFacts.command());
    }

    #[test]
    fn short_refresh_deadline_prefixes_the_collection_budget() {
        let command = facts_refresh_remote_command(
            HostOperation::RefreshFacts.command(),
            Duration::from_secs(10),
        );
        assert!(
            command.starts_with("MAC_WORKER_FACTS_BUDGET_MS=5000 "),
            "{command}"
        );
        assert!(command.contains("host refresh-facts"), "{command}");
        let cleared = facts_refresh_remote_command(
            HostOperation::RefreshFactsClear.command(),
            Duration::from_secs(10),
        );
        assert!(
            cleared.starts_with("MAC_WORKER_FACTS_BUDGET_MS=5000 "),
            "{cleared}"
        );
        assert!(
            cleared.contains("host refresh-facts --clear-auth-incidents"),
            "{cleared}"
        );
    }

    fn tiny_policy() -> ProcessPolicy {
        ProcessPolicy {
            stdout_limit: 64,
            stderr_limit: 64,
            deadline: Duration::from_secs(1),
        }
    }

    fn arg_strings(args: &[OsString]) -> Vec<String> {
        args.iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn direct_ssh_argv_matches_the_historical_option_list() {
        with_ssh_settings(SshSettings::direct(), || {
            let exec =
                ssh_exec_request(SshTarget::Worker, "mac1", "true", tiny_policy(), None).unwrap();
            assert_eq!(exec.program, OsString::from("/usr/bin/ssh"));
            assert_eq!(
                arg_strings(&exec.args),
                [
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "ConnectTimeout=5",
                    "-o",
                    "ForwardAgent=no",
                    "-o",
                    "ClearAllForwardings=yes",
                    "--",
                    "mac1",
                    "true",
                ]
            );
            assert_eq!(
                git_ssh_command_line(SshTarget::Worker, OsStr::new("/usr/bin/ssh")).unwrap(),
                "/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes"
            );
            assert_eq!(
                rsync_ssh_shell(SshTarget::Worker).unwrap(),
                OsString::from(
                    "/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes --"
                )
            );
            let forward = ssh_local_forward_request(
                SshTarget::Worker,
                "mac1",
                9,
                "true".into(),
                tiny_policy(),
            )
            .unwrap();
            assert_dedicated_forward(&arg_strings(&forward.args));
        });
    }

    #[test]
    fn dashboard_forward_stays_dedicated_when_multiplex_is_enabled() {
        let dir = tempfile::tempdir().unwrap();
        with_ssh_settings(
            SshSettings {
                multiplex: true,
                config_file: None,
                control_dir: Some(dir.path().to_path_buf()),
            },
            || {
                let forward = ssh_local_forward_request(
                    SshTarget::Worker,
                    "mac1",
                    9173,
                    "true".into(),
                    tiny_policy(),
                )
                .unwrap();
                let forward_args = arg_strings(&forward.args);
                assert_dedicated_forward(&forward_args);
                assert!(
                    !forward_args.iter().any(|arg| {
                        arg == "ControlMaster=auto"
                            || arg == "ControlPersist=60"
                            || arg == "ServerAliveInterval=10"
                            || (arg.starts_with("ControlPath=") && arg != "ControlPath=none")
                    }),
                    "forward joined the multiplex master: {forward_args:?}"
                );

                let worker = WorkerEntry {
                    name: "mini-1".into(),
                    ssh: "mac1".into(),
                    slots: 1,
                    capabilities: Vec::new(),
                    remote_binary: "~/.local/bin/worker".into(),
                    herdr: false,
                };
                let rpc = ssh_request(&worker, "true".into(), tiny_policy()).unwrap();
                let rpc_args = arg_strings(&rpc.args);
                assert!(rpc_args.iter().any(|arg| arg == "ControlMaster=auto"));
                assert!(rpc_args.iter().any(|arg| arg == "ControlPersist=60"));
                assert!(rpc_args.iter().any(|arg| arg == "ServerAliveInterval=10"));
                assert!(rpc_args.iter().any(|arg| arg == "ClearAllForwardings=yes"));
                assert!(
                    !rpc_args
                        .iter()
                        .any(|arg| arg == "-T" || arg == "ControlMaster=no")
                );
                assert!(
                    rpc_args
                        .iter()
                        .any(|arg| arg.starts_with("ControlPath=") && arg != "ControlPath=none")
                );
            },
        );
    }

    fn assert_dedicated_forward(args: &[String]) {
        assert!(args.iter().any(|arg| arg == "-T"), "{args:?}");
        assert!(args.iter().any(|arg| arg == "-L"), "{args:?}");
        for option in [
            "BatchMode=yes",
            "ConnectTimeout=5",
            "ForwardAgent=no",
            "ExitOnForwardFailure=yes",
            "ControlMaster=no",
            "ControlPath=none",
            "ServerAliveInterval=15",
            "ServerAliveCountMax=3",
        ] {
            assert!(
                args.iter().any(|arg| arg == option),
                "missing {option} in {args:?}"
            );
        }
        assert!(!args.iter().any(|arg| arg.contains("ClearAllForwardings")));
        assert!(!args.iter().any(|arg| arg == "ControlMaster=auto"));
        assert!(!args.iter().any(|arg| arg == "ControlPersist=60"));
    }

    #[test]
    fn managed_config_is_used_by_every_worker_builder_and_never_by_origin() {
        let path = "/tmp/controller owner's/.ssh/mac-worker-controller.conf";
        with_ssh_settings(
            SshSettings {
                config_file: Some(path.into()),
                ..SshSettings::direct()
            },
            || {
                let worker = WorkerEntry {
                    name: "mini-1".into(),
                    ssh: "managed-mini-1".into(),
                    slots: 1,
                    capabilities: vec![],
                    remote_binary: "~/.local/bin/worker".into(),
                    herdr: false,
                };
                for request in [
                    ssh_request(&worker, "true".into(), tiny_policy()).unwrap(),
                    ssh_exec_request(SshTarget::Worker, &worker.ssh, "true", tiny_policy(), None)
                        .unwrap(),
                    ssh_local_forward_request(
                        SshTarget::Worker,
                        &worker.ssh,
                        9,
                        "true".into(),
                        tiny_policy(),
                    )
                    .unwrap(),
                ] {
                    assert_eq!(&arg_strings(&request.args)[..2], &["-F", path]);
                    assert_eq!(request.args[request.args.len() - 2], worker.ssh.as_str());
                }
                let prefix = "/usr/bin/ssh -F '/tmp/controller owner'\\''s/.ssh/mac-worker-controller.conf' -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes";
                let git = SshTransport::new(crate::process::SystemProcessRunner)
                    .git_ssh_command(&worker)
                    .unwrap();
                assert_eq!(git, prefix);
                assert_eq!(
                    rsync_ssh_shell(SshTarget::Worker).unwrap(),
                    OsString::from(
                        "/usr/bin/ssh -F '/tmp/controller owner'\"'\"'s/.ssh/mac-worker-controller.conf' -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes --"
                    )
                );
                assert_eq!(
                    git_ssh_command_line(SshTarget::Origin, OsStr::new("/usr/bin/ssh")).unwrap(),
                    "/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes"
                );
                let origin = ssh_exec_request(
                    SshTarget::Origin,
                    "git@example.test",
                    "true",
                    tiny_policy(),
                    None,
                )
                .unwrap();
                assert!(!origin.args.iter().any(|arg| arg == "-F"));
            },
        );
    }

    #[test]
    fn managed_rsync_config_path_reaches_the_child_as_one_literal_argument() {
        if !Path::new("/usr/bin/rsync").is_file() {
            eprintln!("skipping rsync argv capture: /usr/bin/rsync is missing");
            return;
        }
        let temp = tempfile::tempdir_in("/tmp").unwrap();
        let fake = temp.path().join("fake-transport");
        fs::write(
            &fake,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$0.args\"\nexit 1\n",
        )
        .unwrap();
        fs::set_permissions(&fake, fs::Permissions::from_mode(0o700)).unwrap();
        let source = temp.path().join("source");
        fs::write(&source, b"offline fixture\n").unwrap();
        let config_file = "/tmp/controller owner's/\"managed config\".conf";
        with_ssh_settings(
            SshSettings {
                config_file: Some(config_file.into()),
                ..SshSettings::direct()
            },
            || {
                // Replace only the executable, so even a parser failure can never
                // fall through to a real SSH invocation or host connection.
                let shell = rsync_ssh_shell(SshTarget::Worker)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replacen("/usr/bin/ssh", fake.to_str().unwrap(), 1);
                let output = std::process::Command::new("/usr/bin/rsync")
                    .env_clear()
                    .env("PATH", "/usr/bin:/bin")
                    .arg("-e")
                    .arg(shell)
                    .arg(&source)
                    .arg("offline-worker:incoming")
                    .stdin(std::process::Stdio::null())
                    .output()
                    .unwrap();
                let captured = fs::read_to_string(fake.with_extension("args")).unwrap_or_default();
                let args: Vec<_> = captured.lines().collect();
                assert!(
                    args.windows(2).any(|pair| pair == ["-F", config_file]),
                    "rsync child argv: {args:?}; stderr: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            },
        );
    }

    #[test]
    fn managed_multiplex_connections_cannot_reuse_shared_or_other_config_masters() {
        let temp = tempfile::tempdir_in("/tmp").unwrap();
        let shared = temp.path().join("ssh");
        let settings = SshSettings {
            multiplex: true,
            config_file: Some("/tmp/controller-a.conf".into()),
            control_dir: Some(shared.clone()),
        };
        let control = |target| {
            let request = ssh_exec_request(target, "mac1", "true", tiny_policy(), None).unwrap();
            arg_strings(&request.args)
                .into_iter()
                .find_map(|arg| arg.strip_prefix("ControlPath=").map(str::to_owned))
                .unwrap()
        };
        let managed = with_ssh_settings(settings.clone(), || {
            let managed = control(SshTarget::Worker);
            assert_ne!(managed, shared.join("%C").to_str().unwrap());
            assert_eq!(
                control(SshTarget::Origin),
                shared.join("%C").to_str().unwrap()
            );
            assert_eq!(control(SshTarget::Worker), managed);
            managed
        });
        let directory = Path::new(&managed).parent().unwrap();
        assert_eq!(directory.parent(), shared.parent());
        assert_eq!(
            fs::metadata(directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let other = SshSettings {
            config_file: Some("/tmp/controller-b.conf".into()),
            ..settings.clone()
        };
        assert_ne!(
            with_ssh_settings(other, || control(SshTarget::Worker)),
            managed
        );
        let direct = SshSettings {
            config_file: None,
            ..settings.clone()
        };
        assert_eq!(
            with_ssh_settings(direct, || control(SshTarget::Worker)),
            shared.join("%C").to_str().unwrap()
        );

        let socket = directory.join("stale.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let fd = listener.into_raw_fd();
        unsafe { libc::close(fd) };
        wait_until_unix_connect_refused(&socket);
        with_ssh_settings(settings, || {
            let request =
                ssh_exec_request(SshTarget::Worker, "mac1", "true", tiny_policy(), None).unwrap();
            assert_eq!(request.args[0], "-F");
            assert!(
                !arg_strings(&request.args)
                    .iter()
                    .any(|arg| arg == "ControlMaster=auto")
            );
            assert!(!socket.exists());
            assert_eq!(control(SshTarget::Worker), managed);
        });
    }

    #[test]
    fn multiplex_ssh_argv_uses_a_private_control_directory() {
        let dir = tempfile::tempdir().unwrap();
        with_ssh_settings(
            SshSettings {
                multiplex: true,
                config_file: None,
                control_dir: Some(dir.path().to_path_buf()),
            },
            || {
                let exec = ssh_exec_request(SshTarget::Worker, "mac1", "true", tiny_policy(), None)
                    .unwrap();
                let args = arg_strings(&exec.args);
                assert!(args.iter().any(|arg| arg == "ControlMaster=auto"));
                assert!(args.iter().any(|arg| arg == "ControlPersist=60"));
                assert!(args.iter().any(|arg| arg == "ServerAliveInterval=10"));
                assert!(args.iter().any(|arg| arg == "ServerAliveCountMax=3"));
                let control = args
                    .iter()
                    .find(|arg| arg.starts_with("ControlPath="))
                    .expect("control path");
                assert!(control.ends_with("/%C"), "{control}");
                assert!(control.contains(dir.path().to_str().unwrap()));
                let mode = fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777;
                assert_eq!(mode, 0o700);
                let shell =
                    git_ssh_command_line(SshTarget::Worker, OsStr::new("/usr/bin/ssh")).unwrap();
                assert!(shell.contains("ControlMaster=auto"), "{shell}");
                assert!(shell.contains("/%C"), "{shell}");
            },
        );
    }

    #[test]
    fn unusable_control_socket_falls_back_to_a_direct_connection() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("stale.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let raw = listener.into_raw_fd();
        unsafe { libc::close(raw) };
        wait_until_unix_connect_refused(&socket);
        assert!(socket.exists());
        with_ssh_settings(
            SshSettings {
                multiplex: true,
                config_file: None,
                control_dir: Some(dir.path().to_path_buf()),
            },
            || {
                let first =
                    ssh_exec_request(SshTarget::Worker, "mac1", "true", tiny_policy(), None)
                        .unwrap();
                assert!(
                    !arg_strings(&first.args)
                        .iter()
                        .any(|arg| arg == "ControlMaster=auto")
                );
                assert!(!socket.exists(), "stale control socket must be removed");
                let second =
                    ssh_exec_request(SshTarget::Worker, "mac1", "true", tiny_policy(), None)
                        .unwrap();
                assert!(
                    arg_strings(&second.args)
                        .iter()
                        .any(|arg| arg == "ControlMaster=auto")
                );
            },
        );
    }

    #[test]
    fn control_path_that_is_not_a_directory_stays_direct() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not-a-directory");
        fs::write(&file, b"blocked").unwrap();
        with_ssh_settings(
            SshSettings {
                multiplex: true,
                config_file: None,
                control_dir: Some(file.clone()),
            },
            || {
                let exec = ssh_exec_request(SshTarget::Worker, "mac1", "true", tiny_policy(), None)
                    .unwrap();
                assert!(
                    !arg_strings(&exec.args)
                        .iter()
                        .any(|arg| arg == "ControlMaster=auto")
                );
            },
        );
        assert_eq!(fs::read(&file).unwrap(), b"blocked");
    }

    #[test]
    fn listening_control_socket_stays_in_place_and_multiplexes() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("live.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        with_ssh_settings(
            SshSettings {
                multiplex: true,
                config_file: None,
                control_dir: Some(dir.path().to_path_buf()),
            },
            || {
                let exec = ssh_exec_request(SshTarget::Worker, "mac1", "true", tiny_policy(), None)
                    .unwrap();
                assert!(
                    arg_strings(&exec.args)
                        .iter()
                        .any(|arg| arg == "ControlMaster=auto")
                );
            },
        );
        assert!(socket.exists());
        drop(listener);
    }
}
