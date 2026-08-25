use std::{
    fs, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

#[cfg(target_os = "macos")]
use std::ffi::{c_char, c_int, c_void};

use crate::{
    error::WorkerError,
    protocol::{MemoryPressure, PROTOCOL_VERSION, ProbeResponse},
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

struct SystemCommandExecutor;

struct SystemMemoryPressureQuery;

impl CommandExecutor for SystemCommandExecutor {
    fn output(&self, program: &Path, args: &[&str]) -> io::Result<ProcessOutput> {
        let output = Command::new(program)
            .args(args)
            .env("PATH", CONTROLLED_HOST_PATH)
            .output()?;

        Ok(ProcessOutput {
            code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
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
        let search_paths = CONTROLLED_HOST_PATHS
            .iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        Self::collect_with(
            &SystemCommandExecutor,
            &SystemMemoryPressureQuery,
            &search_paths,
            std::env::consts::OS,
            std::env::consts::ARCH,
        )
    }

    fn collect_with(
        executor: &impl CommandExecutor,
        memory_pressure_query: &impl MemoryPressureQuery,
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
        let free_disk_bytes = parse_free_disk_bytes(&disk)?;
        let memory_pressure = collect_memory_pressure(memory_pressure_query);
        let swap_used_bytes = collect_swap_used_bytes(executor);
        let capabilities = collect_capabilities(executor, search_paths, os, &arch);

        Ok(ProbeResponse {
            protocol_version: PROTOCOL_VERSION,
            hostname,
            arch,
            os_version,
            free_disk_bytes,
            memory_pressure,
            swap_used_bytes,
            capabilities,
        })
    }
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

fn parse_free_disk_bytes(output: &str) -> Result<u64, WorkerError> {
    let available_kib = output
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .and_then(|line| line.split_whitespace().nth(3))
        .ok_or_else(|| WorkerError::Protocol("failed to parse free disk space".into()))?
        .parse::<u64>()
        .map_err(|error| {
            WorkerError::Protocol(format!("failed to parse free disk space: {error}"))
        })?;

    available_kib
        .checked_mul(1024)
        .ok_or_else(|| WorkerError::Protocol("free disk space overflowed u64".into()))
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
}

const TOOL_PROBES: &[ToolProbe] = &[
    ToolProbe {
        capability: "git",
        executables: &["git"],
    },
    ToolProbe {
        capability: "rsync",
        executables: &["rsync"],
    },
    ToolProbe {
        capability: "node",
        executables: &["node"],
    },
    ToolProbe {
        capability: "ruby",
        executables: &["ruby"],
    },
    ToolProbe {
        capability: "python",
        executables: &["python3", "python"],
    },
    ToolProbe {
        capability: "go",
        executables: &["go"],
    },
    ToolProbe {
        capability: "dotnet",
        executables: &["dotnet"],
    },
    ToolProbe {
        capability: "swift",
        executables: &["swift"],
    },
    ToolProbe {
        capability: "docker",
        executables: &["docker"],
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
                    .output(&program, &["--version"])
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
    };

    use tempfile::tempdir;

    use super::{
        CommandExecutor, MemoryPressureQuery, ProbeCollector, ProcessOutput, collect_json,
        collect_memory_pressure,
    };
    use crate::{
        error::WorkerError,
        protocol::{MemoryPressure, PROTOCOL_VERSION, ProbeResponse},
    };

    #[derive(Default)]
    struct FixtureExecutor {
        failed_command: Option<(&'static str, Vec<&'static str>)>,
        fail_swap_metric: bool,
    }

    struct FixtureMemoryPressureQuery {
        level: Option<u32>,
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

    fn executable(directory: &Path, name: &str) -> PathBuf {
        let path = directory.join(name);
        fs::write(&path, b"fixture").unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).unwrap();
        path
    }

    #[test]
    fn collection_normalizes_arch_and_uses_only_the_controlled_path() {
        let directory = tempdir().unwrap();
        executable(directory.path(), "git");
        executable(directory.path(), "python3");

        let response = ProbeCollector::collect_with(
            &FixtureExecutor::default(),
            &FixtureMemoryPressureQuery::available(1),
            &[directory.path().to_path_buf()],
            "macos",
            "aarch64",
        )
        .unwrap();

        assert_eq!(
            response,
            ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                hostname: "mini-1.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 333 * 1024,
                memory_pressure: MemoryPressure::Warn,
                swap_used_bytes: Some(1_610_612_736),
                capabilities: vec!["darwin-arm64".into(), "git".into(), "python".into()],
            }
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

        assert_eq!(response.protocol_version, 1);
        assert!(!response.hostname.is_empty());
        assert!(!bytes.contains(&b'\n'));
    }
}
