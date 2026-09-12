//! Laptop-side binary identity and process-table helpers.
//!
//! A host may add fields to facts and probe JSON. A still-running laptop
//! process cannot pick up a replaced binary, so doctor/setup warn when
//! `worker dashboard` or `worker controller run` started before the installed
//! file's mtime, and the dashboard snapshot can flag the same mismatch.

use std::{
    ffi::OsString,
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    error::WorkerError,
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
};

/// Identity of the CLI file a long-running laptop process started from.
///
/// Compared against the file currently at that path (inode, size, mtime).
/// A replaced install changes at least one of those without needing a
/// build-id in the binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryIdentity {
    pub path: PathBuf,
    pub inode: u64,
    pub size: u64,
    pub mtime_millis: u64,
}

impl BinaryIdentity {
    pub fn from_path(path: &Path) -> Option<Self> {
        let metadata = fs::metadata(path).ok()?;
        let mtime_millis = metadata
            .modified()
            .ok()?
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis()
            .try_into()
            .ok()?;
        Some(Self {
            path: path.to_path_buf(),
            inode: metadata.ino(),
            size: metadata.len(),
            mtime_millis,
        })
    }
}

/// Started-versus-installed identity for a long-running laptop process.
pub trait BinaryIdentitySource: Send + Sync + 'static {
    fn started(&self) -> Option<BinaryIdentity>;
    fn installed(&self) -> Option<BinaryIdentity>;
}

/// Records the executable at construction and restats that same path later.
pub struct SystemBinaryIdentitySource {
    started: Option<BinaryIdentity>,
}

impl SystemBinaryIdentitySource {
    pub fn capture() -> Self {
        let started = std::env::current_exe()
            .ok()
            .and_then(|path| BinaryIdentity::from_path(&path));
        Self { started }
    }
}

impl BinaryIdentitySource for SystemBinaryIdentitySource {
    fn started(&self) -> Option<BinaryIdentity> {
        self.started.clone()
    }

    fn installed(&self) -> Option<BinaryIdentity> {
        self.started
            .as_ref()
            .and_then(|started| BinaryIdentity::from_path(&started.path))
    }
}

/// Test double that returns fixed identities.
#[derive(Debug, Clone)]
pub struct FixedBinaryIdentitySource {
    pub started: Option<BinaryIdentity>,
    pub installed: Option<BinaryIdentity>,
}

impl BinaryIdentitySource for FixedBinaryIdentitySource {
    fn started(&self) -> Option<BinaryIdentity> {
        self.started.clone()
    }

    fn installed(&self) -> Option<BinaryIdentity> {
        self.installed.clone()
    }
}

/// True when both identities are known and they differ.
pub fn binary_is_outdated(source: &dyn BinaryIdentitySource) -> bool {
    match (source.started(), source.installed()) {
        (Some(started), Some(installed)) => started != installed,
        _ => false,
    }
}

/// One row from the laptop process table. Args are `ps` argv tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaptopProcess {
    pub pid: u32,
    pub started_at: SystemTime,
    pub args: Vec<String>,
}

pub trait LaptopProcessTable: Send + Sync {
    fn list(&self) -> Result<Vec<LaptopProcess>, WorkerError>;
}

const LAPTOP_PS_POLICY: ProcessPolicy = ProcessPolicy {
    stdout_limit: 1024 * 1024,
    stderr_limit: 64 * 1024,
    deadline: Duration::from_secs(5),
};

/// Production table: `/bin/ps` through the injected `ProcessRunner`.
///
/// Library code never spawns `ps` itself; tests' fake runners therefore
/// cannot observe the real machine.
pub struct SystemLaptopProcessTable<'a> {
    runner: &'a dyn ProcessRunner,
}

impl<'a> SystemLaptopProcessTable<'a> {
    pub fn new(runner: &'a dyn ProcessRunner) -> Self {
        Self { runner }
    }
}

impl LaptopProcessTable for SystemLaptopProcessTable<'_> {
    fn list(&self) -> Result<Vec<LaptopProcess>, WorkerError> {
        let result = self.runner.run(&laptop_ps_request())?;
        if !result.status.success() {
            return Err(WorkerError::Protocol(
                "laptop process table could not be listed".into(),
            ));
        }
        Ok(parse_ps_table(&result.stdout, SystemTime::now()))
    }
}

fn laptop_ps_request() -> ProcessRequest {
    ProcessRequest {
        program: OsString::from("/bin/ps"),
        args: vec![
            OsString::from("-axww"),
            OsString::from("-o"),
            OsString::from("pid="),
            OsString::from("-o"),
            OsString::from("etime="),
            OsString::from("-o"),
            OsString::from("args="),
        ],
        environment: vec![
            (OsString::from("LANG"), OsString::from("C")),
            (OsString::from("LC_ALL"), OsString::from("C")),
        ],
        environment_remove: Vec::new(),
        stdin: None,
        policy: LAPTOP_PS_POLICY,
        isolate_parent_environment: false,
    }
}

/// Empty table for tests that must not observe the real machine.
pub struct EmptyLaptopProcessTable;

impl LaptopProcessTable for EmptyLaptopProcessTable {
    fn list(&self) -> Result<Vec<LaptopProcess>, WorkerError> {
        Ok(Vec::new())
    }
}

/// Test double that returns a fixed process list.
#[derive(Debug, Clone)]
pub struct FixedLaptopProcessTable {
    pub processes: Vec<LaptopProcess>,
}

impl LaptopProcessTable for FixedLaptopProcessTable {
    fn list(&self) -> Result<Vec<LaptopProcess>, WorkerError> {
        Ok(self.processes.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaptopCliKind {
    Dashboard,
    ControllerRun,
}

impl LaptopCliKind {
    pub fn command(self) -> &'static str {
        match self {
            Self::Dashboard => "worker dashboard",
            Self::ControllerRun => "worker controller run",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutdatedLaptopCli {
    pub pid: u32,
    pub kind: LaptopCliKind,
}

/// Long-running laptop CLIs whose start time is older than the installed
/// binary mtime. Fail-open callers treat an empty result as "no warning".
pub fn outdated_laptop_cli(
    processes: &[LaptopProcess],
    installed_mtime: SystemTime,
) -> Vec<OutdatedLaptopCli> {
    let mut outdated = Vec::new();
    for process in processes {
        let Some(kind) = laptop_cli_kind(&process.args) else {
            continue;
        };
        if process.started_at < installed_mtime {
            outdated.push(OutdatedLaptopCli {
                pid: process.pid,
                kind,
            });
            if outdated.len() == 8 {
                break;
            }
        }
    }
    outdated
}

pub fn format_outdated_laptop_cli(outdated: &[OutdatedLaptopCli]) -> Option<String> {
    if outdated.is_empty() {
        return None;
    }
    let names = outdated
        .iter()
        .map(|item| format!("{} (pid {})", item.kind.command(), item.pid))
        .collect::<Vec<_>>();
    Some(match names.as_slice() {
        [one] => format!("{one} was started before the installed binary; restart it"),
        many => format!(
            "{} were started before the installed binary; restart them",
            many.join(" and ")
        ),
    })
}

fn laptop_cli_kind(args: &[String]) -> Option<LaptopCliKind> {
    let argv0 = args.first()?;
    let name = Path::new(argv0).file_name()?.to_str()?;
    if name != "worker" {
        return None;
    }
    match args.get(1).map(String::as_str) {
        Some("dashboard") => Some(LaptopCliKind::Dashboard),
        Some("controller") if args.get(2).map(String::as_str) == Some("run") => {
            Some(LaptopCliKind::ControllerRun)
        }
        _ => None,
    }
}

fn parse_ps_table(stdout: &[u8], now: SystemTime) -> Vec<LaptopProcess> {
    String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|line| parse_ps_line(line, now))
        .collect()
}

fn parse_ps_line(line: &str, now: SystemTime) -> Option<LaptopProcess> {
    let mut tokens = line.split_whitespace();
    let pid = tokens.next()?.parse().ok()?;
    let etime = tokens.next()?;
    let args = tokens.map(str::to_owned).collect::<Vec<_>>();
    if args.is_empty() {
        return None;
    }
    let age = parse_etime(etime)?;
    let started_at = now.checked_sub(age)?;
    Some(LaptopProcess {
        pid,
        started_at,
        args,
    })
}

/// macOS `etime` is `[[dd-]hh:]mm:ss` (sometimes `mm:ss` only).
fn parse_etime(value: &str) -> Option<Duration> {
    let (days, hms) = match value.split_once('-') {
        Some((days, hms)) => (days.parse::<u64>().ok()?, hms),
        None => (0, value),
    };
    let parts = hms
        .split(':')
        .map(|part| part.parse::<u64>().ok())
        .collect::<Option<Vec<_>>>()?;
    let seconds = match parts.as_slice() {
        [seconds] => *seconds,
        [minutes, seconds] => minutes.saturating_mul(60).saturating_add(*seconds),
        [hours, minutes, seconds] => hours
            .saturating_mul(3600)
            .saturating_add(minutes.saturating_mul(60))
            .saturating_add(*seconds),
        _ => return None,
    };
    Some(Duration::from_secs(
        days.saturating_mul(86_400).saturating_add(seconds),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{os::unix::process::ExitStatusExt, process::ExitStatus, sync::Mutex};

    use crate::process::ProcessResult;

    #[test]
    fn laptop_cli_kind_matches_dashboard_and_controller_run_only() {
        assert_eq!(
            laptop_cli_kind(&["/opt/bin/worker".into(), "dashboard".into()]),
            Some(LaptopCliKind::Dashboard)
        );
        assert_eq!(
            laptop_cli_kind(&["/opt/bin/worker".into(), "controller".into(), "run".into()]),
            Some(LaptopCliKind::ControllerRun)
        );
        assert_eq!(
            laptop_cli_kind(&["/opt/bin/worker".into(), "doctor".into()]),
            None
        );
        assert_eq!(
            laptop_cli_kind(&["/opt/bin/worker".into(), "setup".into()]),
            None
        );
        assert_eq!(
            laptop_cli_kind(&["/opt/bin/other".into(), "dashboard".into()]),
            None
        );
    }

    #[test]
    fn outdated_cli_uses_start_time_against_binary_mtime() {
        let installed = UNIX_EPOCH + Duration::from_secs(50);
        let processes = vec![
            LaptopProcess {
                pid: 11,
                started_at: UNIX_EPOCH + Duration::from_secs(10),
                args: vec!["/bin/worker".into(), "dashboard".into()],
            },
            LaptopProcess {
                pid: 12,
                started_at: UNIX_EPOCH + Duration::from_secs(80),
                args: vec!["/bin/worker".into(), "dashboard".into()],
            },
            LaptopProcess {
                pid: 13,
                started_at: UNIX_EPOCH + Duration::from_secs(10),
                args: vec!["/bin/worker".into(), "doctor".into()],
            },
        ];
        let outdated = outdated_laptop_cli(&processes, installed);
        assert_eq!(
            outdated,
            vec![OutdatedLaptopCli {
                pid: 11,
                kind: LaptopCliKind::Dashboard,
            }]
        );
        assert_eq!(
            format_outdated_laptop_cli(&outdated).as_deref(),
            Some("worker dashboard (pid 11) was started before the installed binary; restart it")
        );
    }

    #[test]
    fn ps_etime_lines_become_start_times() {
        let now = UNIX_EPOCH + Duration::from_secs(10_000);
        let parsed = parse_ps_table(
            b"  42  01:00 /Users/me/.local/bin/worker dashboard --port 9173\n",
            now,
        );
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].pid, 42);
        assert_eq!(parsed[0].started_at, now - Duration::from_secs(60));
        assert_eq!(parsed[0].args[1], "dashboard");
    }

    struct RecordingPsRunner {
        requests: Mutex<Vec<ProcessRequest>>,
        stdout: Vec<u8>,
    }

    impl ProcessRunner for RecordingPsRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: self.stdout.clone(),
                stderr: Vec::new(),
            })
        }
    }

    #[test]
    fn process_table_lists_through_the_injected_runner() {
        let runner = RecordingPsRunner {
            requests: Mutex::new(Vec::new()),
            stdout: b"  42  01:00 /Users/me/.local/bin/worker dashboard\n".to_vec(),
        };
        let processes = SystemLaptopProcessTable::new(&runner).list().unwrap();
        let requests = runner.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].program, OsString::from("/bin/ps"));
        assert_eq!(
            requests[0].args,
            vec![
                OsString::from("-axww"),
                OsString::from("-o"),
                OsString::from("pid="),
                OsString::from("-o"),
                OsString::from("etime="),
                OsString::from("-o"),
                OsString::from("args="),
            ]
        );
        assert_eq!(processes[0].pid, 42);
        assert_eq!(processes[0].args[1], "dashboard");
    }

    struct FailingPsRunner;

    impl ProcessRunner for FailingPsRunner {
        fn run(&self, _request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            Err(WorkerError::Protocol("ps refused".into()))
        }
    }

    #[test]
    fn process_table_surfaces_runner_errors_instead_of_spawning() {
        assert!(
            SystemLaptopProcessTable::new(&FailingPsRunner)
                .list()
                .is_err()
        );
    }
}
