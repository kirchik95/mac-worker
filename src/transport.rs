use std::{
    collections::HashSet,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crate::{
    config::{Config, WorkerEntry, valid_ssh_destination},
    error::{ProcessError, ProcessStream, WorkerError},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    protocol::{
        HealthStatus, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION, SetupFailureKind,
        WorkerHealth, WorkersReport, missing_capabilities,
    },
    transfer::HostOperation,
};

const SSH_PROGRAM: &str = "/usr/bin/ssh";
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
        Ok(format!(
            "{SSH_PROGRAM} -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes"
        ))
    }

    pub fn refresh_facts(&self, worker: &WorkerEntry) -> Result<(), WorkerError> {
        self.refresh_facts_with_deadline(worker, REFRESH_FACTS_POLICY.deadline)
    }

    pub(crate) fn refresh_facts_with_deadline(
        &self,
        worker: &WorkerEntry,
        deadline: Duration,
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
        let request = ssh_request(worker, HostOperation::RefreshFacts.command().into(), policy);
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
        let request = ssh_request(
            worker,
            remote_command,
            ProcessPolicy {
                stdout_limit: MAX_PROBE_RESPONSE_BYTES,
                stderr_limit: MAX_PROBE_RESPONSE_BYTES,
                deadline: deadline.min(MAX_PROBE_DEADLINE),
            },
        );
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
        && match (probe.slot_state, probe.active_lease.as_ref()) {
            (crate::lease::SlotState::Idle, None) => true,
            (crate::lease::SlotState::Busy, Some(summary)) => {
                summary.project_id.len() == 64
                    && summary.project_id.bytes().all(is_lower_hex)
                    && summary.worktree_id.len() == 64
                    && summary.worktree_id.bytes().all(is_lower_hex)
            }
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
    };
    match serde_json::from_str::<ProbeResponse>(response) {
        Ok(probe) => Ok(probe),
        Err(full_error) => match serde_json::from_str::<ProbeWithoutSupervision>(response) {
            Ok(legacy) => Ok(without_supervision(legacy)),
            Err(_) => match serde_json::from_str::<LegacyProbe>(response) {
                Ok(legacy) => Ok(ProbeResponse {
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
        isolate_parent_environment: false,
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
