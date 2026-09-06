use std::{
    fs, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(target_os = "macos")]
use std::ffi::{c_char, c_int, c_void};

use crate::{
    agent_facts::{AgentFacts, EnvProfile as AgentEnvProfile, collect_agent_facts},
    error::WorkerError,
    host_store::HostStore,
    lease::{LeaseService, SlotState},
    paths::PathLayout,
    process::{ProcessPolicy, ProcessRequest, ProcessRunner, SystemProcessRunner},
    protocol::{CpuCounters, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    rooted_fs::RootedDir,
    turn::EnvProfile as TurnEnvProfile,
};

const CONTROLLED_HOST_PATH: &str = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin";
const CONTROLLED_HOST_PATHS: &[&str] = &[
    "/opt/homebrew/bin",
    "/usr/local/bin",
    "/usr/bin",
    "/bin",
    "/usr/sbin",
    "/sbin",
];
const HOST_COMMAND_POLICY: ProcessPolicy = ProcessPolicy {
    stdout_limit: 64 * 1024,
    stderr_limit: 64 * 1024,
    deadline: Duration::from_secs(5),
};
const FACTS_FILE: &str = "facts.json";
const MAX_FACTS_BYTES: u64 = 1024 * 1024;
const FACTS_WRITE_RETRIES: usize = 3;

struct ProcessOutput {
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

trait CommandExecutor {
    fn output(&self, program: &Path, args: &[&str]) -> io::Result<ProcessOutput>;
}

trait MemoryPressureQuery {
    fn current_level(&self) -> io::Result<u32>;
}

struct SystemCommandExecutor {
    policy: ProcessPolicy,
}

struct SystemMemoryPressureQuery;

impl CommandExecutor for SystemCommandExecutor {
    fn output(&self, program: &Path, args: &[&str]) -> io::Result<ProcessOutput> {
        let output = SystemProcessRunner
            .run(&ProcessRequest {
                program: program.as_os_str().into(),
                args: args.iter().map(|argument| (*argument).into()).collect(),
                environment: vec![("PATH".into(), CONTROLLED_HOST_PATH.into())],
                environment_remove: Vec::new(),
                stdin: None,
                policy: self.policy,
            })
            .map_err(io::Error::other)?;

        Ok(ProcessOutput {
            code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

impl Default for SystemCommandExecutor {
    fn default() -> Self {
        Self {
            policy: HOST_COMMAND_POLICY,
        }
    }
}

impl SystemCommandExecutor {
    #[cfg(test)]
    fn with_policy(policy: ProcessPolicy) -> Self {
        Self { policy }
    }
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn sysctlbyname(
        name: *const c_char,
        old_value: *mut c_void,
        old_length: *mut usize,
        new_value: *mut c_void,
        new_length: usize,
    ) -> c_int;
    fn mach_host_self() -> u32;
    fn host_statistics64(
        host_priv: u32,
        flavor: c_int,
        host_info_out: *mut c_int,
        host_info_out_count: *mut u32,
    ) -> c_int;
}

impl MemoryPressureQuery for SystemMemoryPressureQuery {
    fn current_level(&self) -> io::Result<u32> {
        #[cfg(target_os = "macos")]
        {
            let mut level = 0_u32;
            let mut length = size_of::<u32>();
            // SAFETY: the name is a static NUL-terminated C string, and the output pointers
            // reference live, correctly sized values for the duration of the call.
            let result = unsafe {
                sysctlbyname(
                    c"kern.memorystatus_vm_pressure_level".as_ptr(),
                    (&raw mut level).cast(),
                    &raw mut length,
                    std::ptr::null_mut(),
                    0,
                )
            };
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
            if length != size_of::<u32>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected macOS memory-pressure level size",
                ));
            }

            Ok(level)
        }

        #[cfg(not(target_os = "macos"))]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "memory-pressure queries require macOS",
        ))
    }
}

pub struct ProbeCollector;

impl ProbeCollector {
    pub fn collect() -> Result<ProbeResponse, WorkerError> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default();
        let env = std::env::vars_os().collect();
        let paths = PathLayout::discover(None, &env, &home)?;
        Self::collect_for_paths(&paths)
    }

    pub(crate) fn collect_for_paths(paths: &PathLayout) -> Result<ProbeResponse, WorkerError> {
        Self::collect_at(&paths.host_state_root())
    }

    /// Collects host facts and occupancy for an already selected host-state root.
    ///
    /// This low-level boundary does not accept the enclosing installer data container.
    pub fn collect_at(host_state_root: &Path) -> Result<ProbeResponse, WorkerError> {
        let search_paths = CONTROLLED_HOST_PATHS
            .iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        let mut response = Self::collect_with(
            &SystemCommandExecutor::default(),
            &SystemMemoryPressureQuery,
            collect_cpu_counters(),
            &search_paths,
            std::env::consts::OS,
            std::env::consts::ARCH,
        )?;
        let (free, total) = filesystem_capacity(host_state_root)?;
        response.free_disk_bytes = free;
        response.total_disk_bytes = total;
        let occupancy = LeaseService::load_if_present(host_state_root)?;
        response.slot_state = occupancy.slot_state;
        response.active_lease = occupancy.active_lease;
        response.agent_facts = Self::cached_facts_at(host_state_root)?;
        response.facts_age_millis = response
            .agent_facts
            .as_ref()
            .map(|facts| facts.age_millis(current_time_millis()));
        Ok(response)
    }

    /// Refreshes the cached agent, profile, and Git-identity facts. This is
    /// deliberately separate from [`Self::collect_at`] so the probe hot path
    /// never starts an agent process.
    pub fn refresh_facts_at(
        host_state_root: &Path,
        home: &Path,
        runner: &dyn ProcessRunner,
    ) -> Result<AgentFacts, WorkerError> {
        HostStore::open(host_state_root)?;
        let profiles = load_env_profiles(home)?;
        let facts = collect_agent_facts(runner, &profiles);
        write_cached_facts(host_state_root, &facts)?;
        Ok(facts)
    }

    /// Reads the facts cache without launching any agent binary.
    pub fn cached_facts_at(host_state_root: &Path) -> Result<Option<AgentFacts>, WorkerError> {
        let Some(_store) = HostStore::open_if_present(host_state_root)? else {
            return Ok(None);
        };
        let root = RootedDir::open(host_state_root).map_err(WorkerError::Io)?;
        if !root.entry_exists(FACTS_FILE)? {
            return Ok(None);
        }
        read_cached_facts(&root).map(Some)
    }

    fn collect_with(
        executor: &impl CommandExecutor,
        memory_pressure_query: &impl MemoryPressureQuery,
        cpu_counters: Option<CpuCounters>,
        search_paths: &[PathBuf],
        os: &str,
        arch: &str,
    ) -> Result<ProbeResponse, WorkerError> {
        let hostname = required_text(executor, Path::new("/bin/hostname"), &[], "hostname")?;
        let arch = normalize_arch(arch)?;
        let os_version = required_text(
            executor,
            Path::new("/usr/bin/sw_vers"),
            &["-productVersion"],
            "OS version",
        )?;
        let disk = required_text(
            executor,
            Path::new("/bin/df"),
            &["-k", "/"],
            "free disk space",
        )?;
        let (free_disk_bytes, total_disk_bytes) = parse_disk_bytes(&disk)?;
        let memory_pressure = collect_memory_pressure(memory_pressure_query);
        let swap_used_bytes = collect_swap_used_bytes(executor);
        let available_memory_bytes = collect_available_memory_bytes(executor);
        let capabilities = collect_capabilities(executor, search_paths, os, &arch);

        Ok(ProbeResponse {
            protocol_version: PROTOCOL_VERSION,
            supervision_version: SUPERVISION_VERSION,
            hostname,
            arch,
            os_version,
            free_disk_bytes,
            total_disk_bytes,
            memory_pressure,
            swap_used_bytes,
            available_memory_bytes,
            cpu_counters,
            slot_state: SlotState::Idle,
            active_lease: None,
            capabilities,
            agent_facts: None,
            facts_age_millis: None,
        })
    }
}

fn load_env_profiles(home: &Path) -> Result<Vec<AgentEnvProfile>, WorkerError> {
    let directory = home.join(".config").join("mac-worker").join("env");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(WorkerError::Io(error)),
    };
    let mut profiles = Vec::new();
    for entry in entries {
        let entry = entry.map_err(WorkerError::Io)?;
        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(name) = file_name.strip_suffix(".env") else {
            continue;
        };
        if !valid_profile_name(name) {
            continue;
        }
        let profile = match TurnEnvProfile::load_for_home(&entry.path(), home) {
            Ok(profile) => AgentEnvProfile::new(name, true, profile.all_entries()),
            Err(_) => AgentEnvProfile::new(name, false, Vec::new()),
        };
        profiles.push(profile);
    }
    profiles.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(profiles)
}

fn valid_profile_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.contains('/')
        && !value.contains('\\')
        && value != "."
        && value != ".."
        && !value.chars().any(char::is_control)
}

fn read_cached_facts(root: &RootedDir) -> Result<AgentFacts, WorkerError> {
    let bytes = root
        .read_private_regular(FACTS_FILE, MAX_FACTS_BYTES)
        .map_err(WorkerError::Io)?;
    let facts: AgentFacts = serde_json::from_slice(&bytes)
        .map_err(|_| WorkerError::Protocol("cached agent facts are invalid".into()))?;
    let canonical = facts
        .canonical_bytes()
        .map_err(|_| WorkerError::Protocol("cached agent facts are invalid".into()))?;
    if canonical != bytes {
        return Err(WorkerError::Protocol(
            "cached agent facts are not canonical".into(),
        ));
    }
    Ok(facts)
}

fn write_cached_facts(host_state_root: &Path, facts: &AgentFacts) -> Result<(), WorkerError> {
    let bytes = facts
        .canonical_bytes()
        .map_err(|_| WorkerError::Protocol("failed to serialize cached agent facts".into()))?;
    if bytes.len() as u64 > MAX_FACTS_BYTES {
        return Err(WorkerError::Protocol(
            "cached agent facts exceed 1 MiB".into(),
        ));
    }
    let root = RootedDir::open(host_state_root).map_err(WorkerError::Io)?;
    for _ in 0..FACTS_WRITE_RETRIES {
        if root.entry_exists(FACTS_FILE)? {
            let previous = root
                .read_private_regular(FACTS_FILE, MAX_FACTS_BYTES)
                .map_err(WorkerError::Io)?;
            match root.replace_private_regular_exact(FACTS_FILE, &previous, &bytes) {
                Ok(()) => return Ok(()),
                Err(error) if error.raw_os_error() == Some(libc::ESTALE) => continue,
                Err(error) => return Err(WorkerError::Io(error)),
            }
        } else {
            match root.write_private_atomic_no_replace(FACTS_FILE, &bytes) {
                Ok(()) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(WorkerError::Io(error)),
            }
        }
    }
    Err(WorkerError::Io(io::Error::from_raw_os_error(libc::EAGAIN)))
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub fn collect_json() -> Result<Vec<u8>, WorkerError> {
    let response = ProbeCollector::collect()?;
    serde_json::to_vec(&response)
        .map_err(|error| WorkerError::Protocol(format!("failed to serialize host probe: {error}")))
}

fn required_text(
    executor: &impl CommandExecutor,
    program: &Path,
    args: &[&str],
    fact: &str,
) -> Result<String, WorkerError> {
    let output = executor.output(program, args).map_err(|error| {
        WorkerError::Protocol(format!(
            "failed to determine {fact} using {}: {error}",
            program.display()
        ))
    })?;
    if output.code != Some(0) {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(WorkerError::Protocol(format!(
            "failed to determine {fact} using {} (exit {:?}): {}",
            program.display(),
            output.code,
            stderr.trim()
        )));
    }

    let text = String::from_utf8(output.stdout)
        .map_err(|error| WorkerError::Protocol(format!("{fact} was not valid UTF-8: {error}")))?;
    let text = text.trim();
    if text.is_empty() {
        return Err(WorkerError::Protocol(format!(
            "failed to determine {fact}: command returned no value"
        )));
    }

    Ok(text.into())
}

fn normalize_arch(arch: &str) -> Result<String, WorkerError> {
    let arch = arch.trim().to_ascii_lowercase();
    if arch.is_empty() {
        return Err(WorkerError::Protocol(
            "failed to determine architecture".into(),
        ));
    }

    Ok(match arch.as_str() {
        "aarch64" | "arm64" => "arm64".into(),
        "amd64" | "x86_64" => "x86_64".into(),
        _ => arch,
    })
}

fn parse_disk_bytes(output: &str) -> Result<(u64, u64), WorkerError> {
    let line = output
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| WorkerError::Protocol("failed to parse disk space".into()))?;
    let fields = line.split_whitespace().collect::<Vec<_>>();
    let total_kib = fields
        .get(1)
        .ok_or_else(|| WorkerError::Protocol("failed to parse total disk space".into()))?
        .parse::<u64>()
        .map_err(|error| {
            WorkerError::Protocol(format!("failed to parse total disk space: {error}"))
        })?;
    let available_kib = fields
        .get(3)
        .ok_or_else(|| WorkerError::Protocol("failed to parse free disk space".into()))?
        .parse::<u64>()
        .map_err(|error| {
            WorkerError::Protocol(format!("failed to parse free disk space: {error}"))
        })?;
    let total = total_kib
        .checked_mul(1024)
        .ok_or_else(|| WorkerError::Protocol("total disk space overflowed u64".into()))?;
    let free = available_kib
        .checked_mul(1024)
        .ok_or_else(|| WorkerError::Protocol("free disk space overflowed u64".into()))?;
    Ok((free, total))
}

fn filesystem_capacity(path: &Path) -> Result<(u64, u64), WorkerError> {
    let existing = path
        .ancestors()
        .find(|candidate| candidate.exists())
        .ok_or_else(|| WorkerError::Protocol("host data root has no existing ancestor".into()))?;
    let c_path = std::ffi::CString::new(existing.as_os_str().as_encoded_bytes())
        .map_err(|_| WorkerError::Protocol("host data path contains NUL".into()))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(c_path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(WorkerError::Io(std::io::Error::last_os_error()));
    }
    let stats = unsafe { stats.assume_init() };
    let block_size = stats.f_frsize;
    let free_blocks: u64 = stats.f_bavail.into();
    let total_blocks: u64 = stats.f_blocks.into();
    let free = free_blocks
        .checked_mul(block_size)
        .ok_or_else(|| WorkerError::Protocol("free disk capacity overflowed".into()))?;
    let total = total_blocks
        .checked_mul(block_size)
        .ok_or_else(|| WorkerError::Protocol("total disk capacity overflowed".into()))?;
    Ok((free, total))
}

fn collect_memory_pressure(query: &impl MemoryPressureQuery) -> MemoryPressure {
    match query.current_level() {
        Ok(0) => MemoryPressure::Normal,
        Ok(1 | 2) => MemoryPressure::Warn,
        Ok(3) => MemoryPressure::Critical,
        _ => MemoryPressure::Unknown,
    }
}

fn collect_swap_used_bytes(executor: &impl CommandExecutor) -> Option<u64> {
    let output = executor
        .output(Path::new("/usr/sbin/sysctl"), &["-n", "vm.swapusage"])
        .ok()?;
    if output.code != Some(0) {
        return None;
    }
    let output = String::from_utf8(output.stdout).ok()?;
    let used = output.split("used =").nth(1)?.split_whitespace().next()?;
    parse_byte_quantity(used)
}

fn collect_available_memory_bytes(executor: &impl CommandExecutor) -> Option<u64> {
    let output = executor.output(Path::new("/usr/bin/vm_stat"), &[]).ok()?;
    if output.code != Some(0) {
        return None;
    }
    let output = String::from_utf8(output.stdout).ok()?;
    parse_available_memory_bytes(&output)
}

fn parse_available_memory_bytes(output: &str) -> Option<u64> {
    let page_size = output
        .lines()
        .next()?
        .split("page size of ")
        .nth(1)?
        .split(" bytes")
        .next()?
        .trim()
        .parse::<u64>()
        .ok()?;
    if page_size == 0 {
        return None;
    }

    let page_count = |label: &str| {
        output
            .lines()
            .find_map(|line| line.strip_prefix(label))?
            .trim()
            .trim_end_matches('.')
            .parse::<u64>()
            .ok()
    };
    let pages = page_count("Pages free:")?.checked_add(page_count("Pages speculative:")?)?;
    pages.checked_mul(page_size)
}

fn collect_cpu_counters() -> Option<CpuCounters> {
    #[cfg(target_os = "macos")]
    {
        const HOST_CPU_LOAD_INFO: c_int = 3;
        const HOST_CPU_LOAD_INFO_COUNT: u32 = 4;
        let mut ticks = [0_u32; HOST_CPU_LOAD_INFO_COUNT as usize];
        let mut count = HOST_CPU_LOAD_INFO_COUNT;
        // SAFETY: `ticks` is a live buffer sized for HOST_CPU_LOAD_INFO and `count` describes
        // that buffer for the duration of the macOS kernel call.
        let status = unsafe {
            host_statistics64(
                mach_host_self(),
                HOST_CPU_LOAD_INFO,
                ticks.as_mut_ptr().cast(),
                &raw mut count,
            )
        };
        cpu_counters_from_host_statistics(status, count, ticks)
    }

    #[cfg(not(target_os = "macos"))]
    None
}

fn cpu_counters_from_host_statistics(
    status: i32,
    count: u32,
    ticks: [u32; 4],
) -> Option<CpuCounters> {
    (status == 0 && count == 4).then(|| CpuCounters {
        user_ticks: u64::from(ticks[0]),
        system_ticks: u64::from(ticks[1]),
        idle_ticks: u64::from(ticks[2]),
        nice_ticks: u64::from(ticks[3]),
    })
}

fn parse_byte_quantity(value: &str) -> Option<u64> {
    let (number, multiplier) = match value.as_bytes().last().copied()? {
        b'K' | b'k' => (&value[..value.len() - 1], 1024_f64),
        b'M' | b'm' => (&value[..value.len() - 1], 1024_f64.powi(2)),
        b'G' | b'g' => (&value[..value.len() - 1], 1024_f64.powi(3)),
        b'T' | b't' => (&value[..value.len() - 1], 1024_f64.powi(4)),
        _ => (value, 1_f64),
    };
    let bytes = number.parse::<f64>().ok()? * multiplier;
    if !bytes.is_finite() || bytes < 0.0 || bytes > u64::MAX as f64 {
        return None;
    }

    Some(bytes.round() as u64)
}

struct ToolProbe {
    capability: &'static str,
    executables: &'static [&'static str],
    argv: &'static [&'static str],
}

const TOOL_PROBES: &[ToolProbe] = &[
    ToolProbe {
        capability: "git",
        executables: &["git"],
        argv: &["--version"],
    },
    ToolProbe {
        capability: "rsync",
        executables: &["rsync"],
        argv: &["--version"],
    },
    ToolProbe {
        capability: "node",
        executables: &["node"],
        argv: &["--version"],
    },
    ToolProbe {
        capability: "ruby",
        executables: &["ruby"],
        argv: &["--version"],
    },
    ToolProbe {
        capability: "python",
        executables: &["python3", "python"],
        argv: &["--version"],
    },
    ToolProbe {
        capability: "go",
        executables: &["go"],
        argv: &["version"],
    },
    ToolProbe {
        capability: "dotnet",
        executables: &["dotnet"],
        argv: &["--version"],
    },
    ToolProbe {
        capability: "swift",
        executables: &["swift"],
        argv: &["--version"],
    },
    ToolProbe {
        capability: "docker",
        executables: &["docker"],
        argv: &["--version"],
    },
];

fn collect_capabilities(
    executor: &impl CommandExecutor,
    search_paths: &[PathBuf],
    os: &str,
    arch: &str,
) -> Vec<String> {
    let mut capabilities = Vec::new();
    if matches!(os.to_ascii_lowercase().as_str(), "macos" | "darwin") && arch == "arm64" {
        capabilities.push("darwin-arm64".into());
    }

    for probe in TOOL_PROBES {
        let detected = probe.executables.iter().any(|executable| {
            find_executable(search_paths, executable).is_some_and(|program| {
                executor
                    .output(&program, probe.argv)
                    .is_ok_and(|output| output.code == Some(0))
            })
        });
        if detected {
            capabilities.push(probe.capability.into());
        }
    }

    capabilities
}

fn find_executable(search_paths: &[PathBuf], executable: &str) -> Option<PathBuf> {
    search_paths.iter().find_map(|directory| {
        let candidate = directory.join(executable);
        fs::metadata(&candidate)
            .ok()
            .filter(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
            .map(|_| candidate)
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs, io,
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
        sync::Mutex,
        time::{Duration, Instant},
    };

    use tempfile::tempdir;

    use super::{
        CommandExecutor, MemoryPressureQuery, ProbeCollector, ProcessOutput, SystemCommandExecutor,
        collect_json, collect_memory_pressure, cpu_counters_from_host_statistics,
        parse_available_memory_bytes,
    };
    use crate::{
        error::{ProcessError, ProcessStream, WorkerError},
        lease::SlotState,
        process::ProcessPolicy,
        protocol::{MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    };

    #[test]
    fn vm_stat_available_memory_includes_free_and_speculative_pages() {
        // Dropping speculative pages would understate scheduler-available memory.
        let output = "Mach Virtual Memory Statistics: (page size of 4096 bytes)\nPages free:                               1048576.\nPages speculative:                         524288.\n";

        assert_eq!(
            parse_available_memory_bytes(output),
            Some(6 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn malformed_or_overflowing_vm_stat_is_unavailable_not_zero() {
        // Returning zero for bad host data would fabricate a scheduler fact.
        assert_eq!(parse_available_memory_bytes("Pages free: 1.\n"), None);
        assert_eq!(
            parse_available_memory_bytes(
                "Mach Virtual Memory Statistics: (page size of 4096 bytes)\nPages free: 18446744073709551615.\nPages speculative: 1.\n"
            ),
            None
        );
    }

    #[test]
    fn host_cpu_statistics_requires_success_and_complete_tick_count() {
        // Accepting a failed or truncated host_statistics64 response would fabricate CPU data.
        assert_eq!(
            cpu_counters_from_host_statistics(0, 4, [10, 20, 30, 40]).map(|counters| (
                counters.user_ticks,
                counters.system_ticks,
                counters.idle_ticks,
                counters.nice_ticks
            )),
            Some((10, 20, 30, 40))
        );
        assert_eq!(
            cpu_counters_from_host_statistics(1, 4, [10, 20, 30, 40]),
            None
        );
        assert_eq!(
            cpu_counters_from_host_statistics(0, 3, [10, 20, 30, 40]),
            None
        );
    }

    fn host_test_policy(stdout_limit: usize, deadline: Duration) -> ProcessPolicy {
        ProcessPolicy {
            stdout_limit,
            stderr_limit: 1024,
            deadline,
        }
    }

    #[derive(Default)]
    struct FixtureExecutor {
        failed_command: Option<(&'static str, Vec<&'static str>)>,
        fail_swap_metric: bool,
    }

    struct FixtureMemoryPressureQuery {
        level: Option<u32>,
    }

    #[derive(Default)]
    struct CapabilityRecordingExecutor {
        calls: Mutex<Vec<(PathBuf, Vec<String>)>>,
    }

    impl FixtureMemoryPressureQuery {
        fn available(level: u32) -> Self {
            Self { level: Some(level) }
        }

        fn unavailable() -> Self {
            Self { level: None }
        }
    }

    impl MemoryPressureQuery for FixtureMemoryPressureQuery {
        fn current_level(&self) -> io::Result<u32> {
            self.level
                .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "fixture failure"))
        }
    }

    impl CommandExecutor for FixtureExecutor {
        fn output(&self, program: &Path, args: &[&str]) -> io::Result<ProcessOutput> {
            let name = program
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            if self
                .failed_command
                .as_ref()
                .is_some_and(|(failed_name, failed_args)| {
                    name == *failed_name && args == failed_args.as_slice()
                })
                || (self.fail_swap_metric && name == "sysctl")
            {
                return Err(io::Error::new(io::ErrorKind::NotFound, "fixture failure"));
            }

            let (code, stdout) = match (name, args) {
                ("hostname", []) => (Some(0), "mini-1.local\n"),
                ("sw_vers", ["-productVersion"]) => (Some(0), "26.2\n"),
                ("sysctl", ["-n", "vm.swapusage"]) => {
                    (Some(0), "total = 4096.00M  used = 1.50G  free = 2560.00M\n")
                }
                ("df", ["-k", "/"]) => (
                    Some(0),
                    "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/disk 1000 100 333 24% /\n",
                ),
                ("git", ["--version"]) => (Some(0), "git version 2.50.1\n"),
                ("python3", ["--version"]) => (Some(0), "Python 3.14.0\n"),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("unexpected fixture command: {} {args:?}", program.display()),
                    ));
                }
            };

            Ok(ProcessOutput {
                code,
                stdout: stdout.as_bytes().to_vec(),
                stderr: Vec::new(),
            })
        }
    }

    impl CommandExecutor for CapabilityRecordingExecutor {
        fn output(&self, program: &Path, args: &[&str]) -> io::Result<ProcessOutput> {
            self.calls.lock().unwrap().push((
                program.to_path_buf(),
                args.iter().map(|arg| (*arg).to_owned()).collect(),
            ));
            let name = program
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            let stdout = match (name, args) {
                ("hostname", []) => "mini-1.local\n",
                ("sw_vers", ["-productVersion"]) => "26.2\n",
                ("sysctl", ["-n", "vm.swapusage"]) => {
                    "total = 4096.00M  used = 1.50G  free = 2560.00M\n"
                }
                ("df", ["-k", "/"]) => {
                    "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/disk 1000 100 333 24% /\n"
                }
                _ if program.is_absolute() => "tool version\n",
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("unexpected fixture command: {} {args:?}", program.display()),
                    ));
                }
            };
            Ok(ProcessOutput {
                code: Some(0),
                stdout: stdout.as_bytes().to_vec(),
                stderr: Vec::new(),
            })
        }
    }

    fn executable(directory: &Path, name: &str) -> PathBuf {
        let path = directory.join(name);
        fs::write(&path, b"fixture").unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).unwrap();
        path
    }

    #[test]
    fn system_executor_bounds_host_command_stdout() {
        // This catches routing host facts through an unbounded capture path.
        let executor =
            SystemCommandExecutor::with_policy(host_test_policy(32, Duration::from_secs(1)));

        let error = match executor.output(
            Path::new("/bin/sh"),
            &["-c", "while :; do printf 0123456789; done"],
        ) {
            Ok(_) => panic!("a host command exceeding stdout policy must fail"),
            Err(error) => error,
        };

        assert!(
            matches!(
                error
                    .get_ref()
                    .and_then(|source| source.downcast_ref::<WorkerError>()),
                Some(WorkerError::Process(ProcessError::OutputLimitExceeded {
                    stream: ProcessStream::Stdout,
                    limit: 32,
                }))
            ),
            "unexpected host command error: {error:?}"
        );
    }

    #[test]
    fn system_executor_enforces_host_command_deadline() {
        // This catches a host fact command keeping the helper alive past its policy deadline.
        let executor =
            SystemCommandExecutor::with_policy(host_test_policy(1024, Duration::from_millis(100)));
        let started = Instant::now();

        let error = match executor.output(Path::new("/bin/sleep"), &["5"]) {
            Ok(_) => panic!("a non-exiting host command must hit its deadline"),
            Err(error) => error,
        };

        assert!(matches!(
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<WorkerError>()),
            Some(WorkerError::Process(ProcessError::DeadlineExceeded { .. }))
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn system_executor_kills_descendants_that_retain_host_command_pipes() {
        // This catches killing only the direct child after it exits while a descendant
        // keeps its inherited output pipes open.
        let executor =
            SystemCommandExecutor::with_policy(host_test_policy(1024, Duration::from_millis(100)));
        let started = Instant::now();

        let error = match executor.output(Path::new("/bin/sh"), &["-c", "sleep 5 & exit 0"]) {
            Ok(_) => panic!("a pipe-holding descendant must be terminated at the deadline"),
            Err(error) => error,
        };

        assert!(matches!(
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<WorkerError>()),
            Some(WorkerError::Process(ProcessError::DeadlineExceeded { .. }))
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn collection_normalizes_arch_and_uses_only_the_controlled_path() {
        let directory = tempdir().unwrap();
        executable(directory.path(), "git");
        executable(directory.path(), "python3");

        let response = ProbeCollector::collect_with(
            &FixtureExecutor::default(),
            &FixtureMemoryPressureQuery::available(1),
            None,
            &[directory.path().to_path_buf()],
            "macos",
            "aarch64",
        )
        .unwrap();

        assert_eq!(
            response,
            ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: SUPERVISION_VERSION,
                hostname: "mini-1.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 333 * 1024,
                total_disk_bytes: 1000 * 1024,
                memory_pressure: MemoryPressure::Warn,
                swap_used_bytes: Some(1_610_612_736),
                available_memory_bytes: None,
                cpu_counters: None,
                slot_state: SlotState::Idle,
                active_lease: None,
                capabilities: vec!["darwin-arm64".into(), "git".into(), "python".into()],
                agent_facts: None,
                facts_age_millis: None,
            }
        );
    }

    #[test]
    fn capability_detection_uses_each_tools_literal_version_argv_without_a_shell() {
        // Catches applying a universal --version convention to Go or routing
        // controlled executable paths through a shell.
        let directory = tempdir().unwrap();
        for name in [
            "git", "rsync", "node", "ruby", "python3", "python", "go", "dotnet", "swift", "docker",
        ] {
            executable(directory.path(), name);
        }
        let executor = CapabilityRecordingExecutor::default();

        let response = ProbeCollector::collect_with(
            &executor,
            &FixtureMemoryPressureQuery::available(0),
            None,
            &[directory.path().to_path_buf()],
            "macos",
            "arm64",
        )
        .unwrap();

        assert_eq!(
            response.capabilities,
            vec![
                "darwin-arm64",
                "git",
                "rsync",
                "node",
                "ruby",
                "python",
                "go",
                "dotnet",
                "swift",
                "docker",
            ]
        );
        let capability_calls = executor
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(program, _)| program.starts_with(directory.path()))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            capability_calls,
            vec![
                (directory.path().join("git"), vec!["--version".into()]),
                (directory.path().join("rsync"), vec!["--version".into()]),
                (directory.path().join("node"), vec!["--version".into()]),
                (directory.path().join("ruby"), vec!["--version".into()]),
                (directory.path().join("python3"), vec!["--version".into()]),
                (directory.path().join("go"), vec!["version".into()]),
                (directory.path().join("dotnet"), vec!["--version".into()]),
                (directory.path().join("swift"), vec!["--version".into()]),
                (directory.path().join("docker"), vec!["--version".into()]),
            ]
        );
    }

    #[test]
    fn documented_macos_pressure_levels_map_to_wire_states() {
        for (level, expected) in [
            (0, MemoryPressure::Normal),
            (1, MemoryPressure::Warn),
            (2, MemoryPressure::Warn),
            (3, MemoryPressure::Critical),
        ] {
            assert_eq!(
                collect_memory_pressure(&FixtureMemoryPressureQuery::available(level)),
                expected
            );
        }
    }

    #[test]
    fn optional_metric_failures_degrade_to_typed_unknown_values() {
        let response = ProbeCollector::collect_with(
            &FixtureExecutor {
                fail_swap_metric: true,
                ..FixtureExecutor::default()
            },
            &FixtureMemoryPressureQuery::unavailable(),
            None,
            &[],
            "macos",
            "arm64",
        )
        .unwrap();

        assert_eq!(response.memory_pressure, MemoryPressure::Unknown);
        assert_eq!(response.swap_used_bytes, None);
    }

    #[test]
    fn mandatory_command_failures_are_protocol_errors() {
        for failed_command in [
            ("hostname", vec![]),
            ("sw_vers", vec!["-productVersion"]),
            ("df", vec!["-k", "/"]),
        ] {
            let error = ProbeCollector::collect_with(
                &FixtureExecutor {
                    failed_command: Some(failed_command),
                    ..FixtureExecutor::default()
                },
                &FixtureMemoryPressureQuery::available(0),
                None,
                &[],
                "macos",
                "arm64",
            )
            .expect_err("mandatory facts must not produce a partial probe");

            assert!(matches!(error, WorkerError::Protocol(_)));
        }
    }

    #[test]
    fn an_empty_architecture_is_a_protocol_error() {
        let error = ProbeCollector::collect_with(
            &FixtureExecutor::default(),
            &FixtureMemoryPressureQuery::available(0),
            None,
            &[],
            "macos",
            "",
        )
        .expect_err("an unknown architecture must not produce a partial probe");

        assert!(matches!(error, WorkerError::Protocol(_)));
    }

    #[test]
    fn collect_json_serializes_one_complete_local_probe() {
        let bytes = collect_json().unwrap();
        let response: ProbeResponse = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(response.protocol_version, PROTOCOL_VERSION);
        assert!(!response.hostname.is_empty());
        assert!(!bytes.contains(&b'\n'));
    }
}
