use std::{
    fs, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

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

struct SystemCommandExecutor;

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

pub struct ProbeCollector;

impl ProbeCollector {
    pub fn collect() -> Result<ProbeResponse, WorkerError> {
        let search_paths = CONTROLLED_HOST_PATHS
            .iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        Self::collect_with(
            &SystemCommandExecutor,
            &search_paths,
            std::env::consts::OS,
            std::env::consts::ARCH,
        )
    }

    fn collect_with(
        executor: &impl CommandExecutor,
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
        let memory_pressure = collect_memory_pressure(executor);
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

fn collect_memory_pressure(executor: &impl CommandExecutor) -> MemoryPressure {
    let output = executor.output(Path::new("/usr/bin/memory_pressure"), &["-Q"]);
    match output.ok().and_then(|output| output.code) {
        Some(0) => MemoryPressure::Normal,
        Some(1) => MemoryPressure::Warn,
        Some(2) => MemoryPressure::Critical,
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

    use super::{CommandExecutor, ProbeCollector, ProcessOutput, collect_json};
    use crate::{
        error::WorkerError,
        protocol::{MemoryPressure, PROTOCOL_VERSION, ProbeResponse},
    };

    #[derive(Default)]
    struct FixtureExecutor {
        failed_command: Option<(&'static str, Vec<&'static str>)>,
        fail_optional_metrics: bool,
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
                || (self.fail_optional_metrics && matches!(name, "memory_pressure" | "sysctl"))
            {
                return Err(io::Error::new(io::ErrorKind::NotFound, "fixture failure"));
            }

            let (code, stdout) = match (name, args) {
                ("hostname", []) => (Some(0), "mini-1.local\n"),
                ("sw_vers", ["-productVersion"]) => (Some(0), "26.2\n"),
                ("memory_pressure", ["-Q"]) => (Some(1), ""),
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
    fn optional_metric_failures_degrade_to_typed_unknown_values() {
        let response = ProbeCollector::collect_with(
            &FixtureExecutor {
                fail_optional_metrics: true,
                ..FixtureExecutor::default()
            },
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
        let error = ProbeCollector::collect_with(&FixtureExecutor::default(), &[], "macos", "")
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
