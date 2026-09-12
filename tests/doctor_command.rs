#[allow(dead_code)]
mod support;

use std::{
    collections::{BTreeMap, VecDeque},
    ffi::{OsStr, OsString},
    fs,
    io::{self, Write},
    os::unix::{fs::symlink, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, ExitStatus},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};

use mac_worker::{
    RuntimeContext,
    agent_facts::{AgentAuth, AgentFacts, AgentProbe, turn_auth_failure_reason},
    cli::{Cli, Command},
    config::{Config, WorkerEntry},
    doctor::{DoctorRequest, DoctorService},
    error::{ExitKind, WorkerError},
    inputs::InputSelector,
    laptop::{FixedLaptopProcessTable, LaptopProcess},
    lease::SlotState,
    output::CommandOutput,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project::ProjectInspector,
    project_config::SnapshotSettings,
    protocol::{
        DoctorIssue, DoctorProject, DoctorReport, HealthStatus, IssueSeverity, MemoryPressure,
        PROTOCOL_VERSION, ProbeResponse, WorkerHealth,
    },
    run_with_io_in_context,
    snapshot::SnapshotSummary,
};

use support::GitRepo;

#[derive(Clone)]
struct DoctorRunner {
    ssh_results: Arc<Mutex<VecDeque<Result<ProcessResult, WorkerError>>>>,
    mutation: Option<Arc<SnapshotMutation>>,
    probe_mutation: Option<Arc<ProbeWindowMutation>>,
    after_capture_mutation: Option<Arc<AfterCaptureMutation>>,
}

struct SnapshotMutation {
    root: PathBuf,
    stage_queries: AtomicUsize,
}

struct ProbeWindowMutation {
    root: PathBuf,
    kind: ProbeWindowMutationKind,
    applied: AtomicBool,
}

enum ProbeWindowMutationKind {
    RevokeSensitivePolicy,
    CommitNodeRequirement,
}

struct AfterCaptureMutation {
    root: PathBuf,
    project_inspections: AtomicUsize,
}

struct BrokenWriter;

struct IsolatedRuntime {
    context: RuntimeContext,
    cache: PathBuf,
    fallback_cache: PathBuf,
}

impl Write for BrokenWriter {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "writer closed"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl DoctorRunner {
    fn new(ssh_results: Vec<Result<ProcessResult, WorkerError>>) -> Self {
        Self {
            ssh_results: Arc::new(Mutex::new(ssh_results.into())),
            mutation: None,
            probe_mutation: None,
            after_capture_mutation: None,
        }
    }

    fn mutating_snapshot(
        root: &Path,
        ssh_results: Vec<Result<ProcessResult, WorkerError>>,
    ) -> Self {
        Self {
            ssh_results: Arc::new(Mutex::new(ssh_results.into())),
            mutation: Some(Arc::new(SnapshotMutation {
                root: root.to_path_buf(),
                stage_queries: AtomicUsize::new(0),
            })),
            probe_mutation: None,
            after_capture_mutation: None,
        }
    }

    fn mutating_probe_policy(
        root: &Path,
        ssh_results: Vec<Result<ProcessResult, WorkerError>>,
    ) -> Self {
        Self {
            ssh_results: Arc::new(Mutex::new(ssh_results.into())),
            mutation: None,
            probe_mutation: Some(Arc::new(ProbeWindowMutation {
                root: root.to_path_buf(),
                kind: ProbeWindowMutationKind::RevokeSensitivePolicy,
                applied: AtomicBool::new(false),
            })),
            after_capture_mutation: None,
        }
    }

    fn mutating_probe_requirement_and_head(
        root: &Path,
        ssh_results: Vec<Result<ProcessResult, WorkerError>>,
    ) -> Self {
        Self {
            ssh_results: Arc::new(Mutex::new(ssh_results.into())),
            mutation: None,
            probe_mutation: Some(Arc::new(ProbeWindowMutation {
                root: root.to_path_buf(),
                kind: ProbeWindowMutationKind::CommitNodeRequirement,
                applied: AtomicBool::new(false),
            })),
            after_capture_mutation: None,
        }
    }

    fn mutating_policy_after_capture(
        root: &Path,
        ssh_results: Vec<Result<ProcessResult, WorkerError>>,
    ) -> Self {
        Self {
            ssh_results: Arc::new(Mutex::new(ssh_results.into())),
            mutation: None,
            probe_mutation: None,
            after_capture_mutation: Some(Arc::new(AfterCaptureMutation {
                root: root.to_path_buf(),
                project_inspections: AtomicUsize::new(0),
            })),
        }
    }
}

impl ProcessRunner for DoctorRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/ssh") {
            let result = self
                .ssh_results
                .lock()
                .unwrap()
                .pop_front()
                .expect("one SSH result per configured worker");
            if let Some(mutation) = &self.probe_mutation
                && !mutation.applied.swap(true, Ordering::SeqCst)
            {
                match mutation.kind {
                    ProbeWindowMutationKind::RevokeSensitivePolicy => {
                        fs::write(mutation.root.join(".worker.toml"), b"version = 1\n")?
                    }
                    ProbeWindowMutationKind::CommitNodeRequirement => {
                        fs::write(mutation.root.join("package.json"), b"{}\n")?;
                        run_fixture_git(&mutation.root, &["add", "package.json"])?;
                        run_fixture_git(
                            &mutation.root,
                            &["commit", "-m", "probe-window requirement mutation"],
                        )?;
                    }
                }
            }
            return result;
        }

        if request.program == OsStr::new("/usr/bin/git")
            && request
                .args
                .iter()
                .any(|argument| argument == "--show-toplevel")
            && let Some(mutation) = &self.after_capture_mutation
            && mutation.project_inspections.fetch_add(1, Ordering::SeqCst) == 3
        {
            fs::write(mutation.root.join(".worker.toml"), b"version = 1\n")?;
        }

        if request.program == OsStr::new("/usr/bin/git")
            && request.args.iter().any(|argument| argument == "ls-files")
            && request.args.iter().any(|argument| argument == "--stage")
            && let Some(mutation) = &self.mutation
            && mutation.stage_queries.fetch_add(1, Ordering::SeqCst) == 1
        {
            fs::write(
                mutation.root.join("tracked.txt"),
                b"changed after materialization\n",
            )?;
        }

        SystemProcessRunner.run(request)
    }
}

fn run_fixture_git(root: &Path, arguments: &[&str]) -> io::Result<()> {
    let mut command = ProcessCommand::new("/usr/bin/git");
    command
        .current_dir(root)
        .env("HOME", root.join("home"))
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1");
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_CEILING_DIRECTORIES",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_PARAMETERS",
    ] {
        command.env_remove(name);
    }
    let output = command.args(arguments).output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(io::Error::other("fixture Git mutation failed"))
    }
}

fn exit_status(code: i32) -> ExitStatus {
    ExitStatus::from_raw(code << 8)
}

fn ready_probe(capabilities: &[&str]) -> Result<ProcessResult, WorkerError> {
    let response = serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "supervision_version": mac_worker::protocol::SUPERVISION_VERSION,
        "hostname": "mini.local",
        "arch": "arm64",
        "os_version": "26.2",
        "free_disk_bytes": 536_870_912_u64,
        "total_disk_bytes": 1_073_741_824_u64,
        "memory_pressure": "normal",
        "swap_used_bytes": 134_217_728_u64,
        "slot_state": "idle",
        "active_lease": null,
        "capabilities": capabilities,
    });
    Ok(ProcessResult {
        status: exit_status(0),
        stdout: serde_json::to_vec(&response).unwrap(),
        stderr: Vec::new(),
    })
}

fn busy_probe(capabilities: &[&str]) -> Result<ProcessResult, WorkerError> {
    let mut result = ready_probe(capabilities)?;
    let mut value: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    value["slot_state"] = serde_json::json!("busy");
    value["active_lease"] = serde_json::json!({
        "job_id": "00000000000000000000000000000001",
        "project_id": "a".repeat(64),
        "worktree_id": "b".repeat(64),
        "created_at_millis": 10,
    });
    result.stdout = serde_json::to_vec(&value).unwrap();
    Ok(result)
}

fn structured_probe(
    protocol_version: u32,
    hostname: &str,
    arch: &str,
    os_version: &str,
    capabilities: Vec<String>,
) -> Result<ProcessResult, WorkerError> {
    let mut response = serde_json::json!({
        "protocol_version": protocol_version,
        "hostname": hostname,
        "arch": arch,
        "os_version": os_version,
        "free_disk_bytes": 536_870_912_u64,
        "memory_pressure": "normal",
        "swap_used_bytes": 134_217_728_u64,
        "capabilities": capabilities,
    });
    if protocol_version == PROTOCOL_VERSION {
        response["supervision_version"] =
            serde_json::json!(mac_worker::protocol::SUPERVISION_VERSION);
        response["total_disk_bytes"] = serde_json::json!(1_073_741_824_u64);
        response["slot_state"] = serde_json::json!("idle");
        response["active_lease"] = serde_json::Value::Null;
    }
    Ok(ProcessResult {
        status: exit_status(0),
        stdout: serde_json::to_vec(&response).unwrap(),
        stderr: Vec::new(),
    })
}

fn offline_probe() -> Result<ProcessResult, WorkerError> {
    offline_probe_with_stderr(b"offline")
}

fn offline_probe_with_stderr(stderr: &[u8]) -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: exit_status(255),
        stdout: Vec::new(),
        stderr: stderr.to_vec(),
    })
}

fn invalid_probe() -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: exit_status(0),
        stdout: b"not JSON".to_vec(),
        stderr: Vec::new(),
    })
}

fn worker(name: &str, ssh: &str, capabilities: &[&str]) -> WorkerEntry {
    WorkerEntry {
        name: name.into(),
        ssh: ssh.into(),
        slots: 1,
        capabilities: capabilities.iter().map(|value| (*value).into()).collect(),
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    }
}

fn config(workers: Vec<WorkerEntry>) -> Config {
    Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        controller: Default::default(),
        workers,
    }
}

fn paths(root: &Path) -> PathLayout {
    PathLayout {
        config: root.join("config.toml"),
        state: root.join("state"),
        cache: root.join("cache"),
        data: root.join("data"),
    }
}

fn inspect(
    repo: &GitRepo,
    state_root: &Path,
    config: &Config,
    runner: &DoctorRunner,
) -> Result<mac_worker::protocol::DoctorReport, WorkerError> {
    let paths = paths(state_root);
    DoctorService {
        runner,
        config,
        paths: &paths,
        laptop_processes: &mac_worker::laptop::EmptyLaptopProcessTable,
        installed_binary_mtime: None,
    }
    .inspect(DoctorRequest {
        project: repo.root().to_path_buf(),
        cli_includes: Vec::new(),
    })
}

fn assert_no_doctor_snapshot(cache: &Path) {
    for directory in [
        cache.join("snapshots/staging"),
        cache.join("snapshots/ready"),
    ] {
        if directory.exists() {
            for entry in fs::read_dir(&directory).unwrap() {
                let entry = entry.unwrap();
                assert_eq!(
                    entry.file_name(),
                    ".mac-worker-rooted-fs",
                    "doctor left an owned capture in {}",
                    directory.display()
                );
                assert!(entry.file_type().unwrap().is_dir());
                assert!(fs::read_dir(entry.path()).unwrap().next().is_none());
            }
        }
    }
}

fn ready_output_report() -> DoctorReport {
    DoctorReport {
        version: 1,
        ready: true,
        project: DoctorProject {
            display_name: "demo".into(),
            project_id: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
            worktree_id: "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210".into(),
            head: Some("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".into()),
            branch: Some("main".into()),
            dirty: true,
            relative_working_dir: "crates/app".into(),
        },
        requirements: vec!["node".into(), "docker".into()],
        snapshot: Some(SnapshotSummary {
            digest: "9999999999999999999999999999999999999999999999999999999999999999".into(),
            file_count: 3,
            total_bytes: 42,
            tracked_deletion_count: 1,
            included_untracked_count: 1,
            warning_count: 1,
        }),
        workers: vec![WorkerHealth {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            status: HealthStatus::Ready,
            probe: Some(ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: mac_worker::protocol::SUPERVISION_VERSION,
                hostname: "mini-1.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 536_870_912,
                total_disk_bytes: 1_073_741_824,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(134_217_728),
                available_memory_bytes: Some(12 * 1024 * 1024 * 1024),
                cpu_counters: Some(mac_worker::protocol::CpuCounters {
                    user_ticks: 10,
                    system_ticks: 20,
                    idle_ticks: 30,
                    nice_ticks: 40,
                }),
                slot_state: SlotState::Idle,
                active_lease: None,
                capabilities: vec!["node".into(), "docker".into()],
                agent_facts: None,
                facts_age_millis: None,
                configured_slots: 0,
                busy_slots: 0,
            }),
            missing_capabilities: Vec::new(),
            error_code: None,
            error_message: None,
        }],
        issues: vec![DoctorIssue {
            severity: IssueSeverity::Warning,
            code: "SENSITIVE_PATH_ALLOWED".into(),
            message: "allowed sensitive input".into(),
            paths: vec![".env".into()],
        }],
    }
}

fn blocked_output_report(code: &str) -> DoctorReport {
    DoctorReport {
        version: 1,
        ready: false,
        project: DoctorProject {
            display_name: "demo".into(),
            project_id: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
            worktree_id: "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210".into(),
            head: Some("deadbeefcafe0123456789abcdef0123456789abcdef0123456789abcdef0123".into()),
            branch: None,
            dirty: false,
            relative_working_dir: String::new(),
        },
        requirements: Vec::new(),
        snapshot: None,
        workers: vec![WorkerHealth {
            name: "mini-2".into(),
            ssh: "mac2".into(),
            status: HealthStatus::Unavailable,
            probe: Some(ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: mac_worker::protocol::SUPERVISION_VERSION,
                hostname: "mini-2.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 268_435_456,
                total_disk_bytes: 1_073_741_824,
                memory_pressure: MemoryPressure::Warn,
                swap_used_bytes: None,
                available_memory_bytes: None,
                cpu_counters: None,
                slot_state: SlotState::Idle,
                active_lease: None,
                capabilities: vec!["node".into()],
                agent_facts: None,
                facts_age_millis: None,
                configured_slots: 0,
                busy_slots: 0,
            }),
            missing_capabilities: vec!["docker".into()],
            error_code: Some("MISSING_CAPABILITIES".into()),
            error_message: Some("worker mini-2 is missing required capabilities: docker".into()),
        }],
        issues: vec![
            DoctorIssue {
                severity: IssueSeverity::Blocker,
                code: code.into(),
                message: "selected input changed during capture".into(),
                paths: vec!["src/lib.rs".into()],
            },
            DoctorIssue {
                severity: IssueSeverity::Warning,
                code: "MISSING_CAPABILITIES".into(),
                message: "worker mini-2 is missing required capabilities: docker".into(),
                paths: Vec::new(),
            },
        ],
    }
}

fn write_inventory(root: &Path) -> PathBuf {
    let path = root.join("config.toml");
    fs::write(
        &path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();
    path
}

fn doctor_cli(config: PathBuf, project: Option<&Path>, includes: Vec<String>, json: bool) -> Cli {
    Cli {
        config: Some(config),
        json,
        command: Command::Doctor {
            project: project.map(Path::to_path_buf),
            includes,
        },
    }
}

fn isolated_runtime(root: &Path, current_dir: &Path) -> IsolatedRuntime {
    let home = root.join("runtime-home");
    let config_home = root.join("xdg-config");
    let state_home = root.join("xdg-state");
    let cache_home = root.join("xdg-cache");
    let data_home = root.join("xdg-data");
    fs::create_dir(&home).unwrap();
    let environment = BTreeMap::from([
        (
            OsString::from("XDG_CONFIG_HOME"),
            config_home.into_os_string(),
        ),
        (
            OsString::from("XDG_STATE_HOME"),
            state_home.into_os_string(),
        ),
        (
            OsString::from("XDG_CACHE_HOME"),
            cache_home.clone().into_os_string(),
        ),
        (OsString::from("XDG_DATA_HOME"), data_home.into_os_string()),
    ]);

    IsolatedRuntime {
        context: RuntimeContext::isolated(environment, home.clone(), current_dir.to_path_buf()),
        cache: cache_home.join("mac-worker"),
        fallback_cache: home.join(".cache/mac-worker"),
    }
}

fn assert_isolated_snapshot_state(runtime: &IsolatedRuntime, infrastructure_expected: bool) {
    assert_eq!(runtime.cache.exists(), infrastructure_expected);
    if infrastructure_expected {
        assert_no_doctor_snapshot(&runtime.cache);
    }
    assert!(
        !runtime.fallback_cache.exists(),
        "doctor selected the runtime home's fallback cache instead of isolated XDG cache"
    );
}

fn regular_files_below(root: &Path, files: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(error) => panic!("could not inspect {}: {error}", root.display()),
    };
    for entry in entries {
        let entry = entry.unwrap();
        let metadata = fs::symlink_metadata(entry.path()).unwrap();
        if metadata.file_type().is_dir() {
            regular_files_below(&entry.path(), files);
        } else if metadata.file_type().is_file() {
            files.push(entry.path());
        }
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

#[test]
fn doctor_output_json_is_one_compact_tagged_sanitized_report() {
    let ready = CommandOutput::Doctor(ready_output_report());
    let blocked = CommandOutput::Doctor(blocked_output_report("SNAPSHOT_CHANGED"));

    for (output, expected_codes) in [
        (&ready, vec!["SENSITIVE_PATH_ALLOWED"]),
        (&blocked, vec!["SNAPSHOT_CHANGED", "MISSING_CAPABILITIES"]),
    ] {
        let rendered = output.render_json().unwrap();
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();

        assert_eq!(rendered.lines().count(), 1);
        assert!(!rendered.contains(['\n', '\r']));
        assert_eq!(value["kind"], "doctor");
        assert_eq!(
            value["issues"]
                .as_array()
                .unwrap()
                .iter()
                .map(|issue| issue["code"].as_str().unwrap())
                .collect::<Vec<_>>(),
            expected_codes
        );
        for forbidden in [
            "git@github.com:private/example.git",
            "DOCTOR_PLANTED_SECRET=never-report-this",
            "/Users/alice/private/example",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "doctor JSON leaked {forbidden:?}"
            );
        }
    }
}

#[test]
fn doctor_output_human_renders_complete_ready_and_blocked_reports_without_ssh() {
    let ready = CommandOutput::Doctor(ready_output_report()).render_human();
    let blocked = CommandOutput::Doctor(blocked_output_report("SNAPSHOT_CHANGED")).render_human();

    assert_eq!(
        ready,
        format!(
            concat!(
                "doctor: ready\n",
                "project: demo\n",
                "  project id: 0123456789ab\n",
                "  worktree id: fedcba987654\n",
                "  source: branch main at abcdef012345\n",
                "  dirty: yes\n",
                "  working directory: crates/app\n",
                "requirements: node, docker\n",
                "snapshot:\n",
                "  digest: 9999999999999999999999999999999999999999999999999999999999999999\n",
                "  file count: 3\n",
                "  total bytes: 42\n",
                "  tracked deletions: 1\n",
                "  included untracked: 1\n",
                "  warnings: 1\n",
                "workers:\n",
                "  mini-1: eligible (mini-1.local; arm64; macOS 26.2; protocol {})\n",
                "    slot: idle\n",
                "    capabilities: node, docker\n",
                "    herdr: unknown\n",
                "    free disk bytes: 536870912\n",
                "    total disk bytes: 1073741824\n",
                "    memory pressure: normal\n",
                "    swap used bytes: 134217728\n",
                "issues:\n",
                "  warning [SENSITIVE_PATH_ALLOWED]: allowed sensitive input\n",
                "    path: .env",
            ),
            PROTOCOL_VERSION,
        )
    );
    assert_eq!(
        blocked,
        concat!(
            "doctor: blocked\n",
            "project: demo\n",
            "  project id: 0123456789ab\n",
            "  worktree id: fedcba987654\n",
            "  source: detached HEAD at deadbeefcafe\n",
            "  dirty: no\n",
            "  working directory: .\n",
            "requirements: none\n",
            "snapshot: unavailable\n",
            "workers:\n",
            "  mini-2: ineligible [MISSING_CAPABILITIES]: worker mini-2 is missing required capabilities: docker\n",
            "    missing capabilities: docker\n",
            "    slot: idle\n",
            "    capabilities: node\n",
            "    herdr: unknown\n",
            "    free disk bytes: 268435456\n",
            "    total disk bytes: 1073741824\n",
            "    memory pressure: warn\n",
            "    swap used bytes: unavailable\n",
            "issues:\n",
            "  blocker [SNAPSHOT_CHANGED]: selected input changed during capture\n",
            "    path: src/lib.rs\n",
            "  warning [MISSING_CAPABILITIES]: worker mini-2 is missing required capabilities: docker",
        )
    );
    for forbidden in ["mac1", "mac2", "/Users/alice/private/example"] {
        assert!(
            !ready.contains(forbidden),
            "ready human output leaked {forbidden}"
        );
        assert!(
            !blocked.contains(forbidden),
            "blocked human output leaked {forbidden}"
        );
    }
}

#[test]
fn doctor_output_human_renders_every_bounded_issue_path() {
    let mut report = blocked_output_report("UNTRACKED_INPUT");
    report.issues = vec![DoctorIssue {
        severity: IssueSeverity::Blocker,
        code: "UNTRACKED_INPUT".into(),
        message: "untracked inputs need an explicit policy".into(),
        paths: vec![
            "first/input.txt".into(),
            "second/input.txt".into(),
            "third/input.txt".into(),
        ],
    }];

    let human = CommandOutput::Doctor(report).render_human();

    for path in [
        "path: first/input.txt",
        "path: second/input.txt",
        "path: third/input.txt",
    ] {
        assert!(human.contains(path), "missing issue {path:?} in {human:?}");
    }
}

#[test]
fn doctor_output_human_labels_an_unborn_branch_without_inventing_a_head() {
    let mut report = ready_output_report();
    report.project.branch = Some("main".into());
    report.project.head = None;

    let human = CommandOutput::Doctor(report).render_human();

    assert!(human.contains("source: branch main at unborn HEAD"));
    assert!(!human.contains("source: detached HEAD"));
}

#[test]
fn doctor_aggregate_exit_uses_ready_usage_and_snapshot_integrity_categories() {
    assert_eq!(
        CommandOutput::Doctor(ready_output_report()).aggregate_exit_kind(),
        None
    );
    assert_eq!(
        CommandOutput::Doctor(blocked_output_report("NO_ELIGIBLE_WORKER")).aggregate_exit_kind(),
        Some(ExitKind::Usage)
    );
    assert_eq!(
        CommandOutput::Doctor(blocked_output_report("SNAPSHOT_CHANGED")).aggregate_exit_kind(),
        Some(ExitKind::Infrastructure)
    );
}

#[test]
fn doctor_renders_a_turn_auth_failure_reason() {
    let reason = turn_auth_failure_reason(1_704_067_200_000).unwrap();
    let mut report = ready_output_report();
    let probe = report.workers[0].probe.as_mut().unwrap();
    probe.agent_facts = Some(AgentFacts {
        agents: vec![AgentProbe {
            name: "codex".into(),
            version: Some("0.152.1".into()),
            auth: AgentAuth::UnknownWithReason(reason),
            auth_by_profile: Vec::new(),
        }],
        env_profiles: Vec::new(),
        git_identity: true,
        collected_at_millis: 1,
        herdr: None,
        origin_https_helpers: Default::default(),
    });
    probe.facts_age_millis = Some(0);
    let human = CommandOutput::Doctor(report).render_human();
    assert!(
        human.contains("codex 0.152.1: unknown (auth failed in a turn at 2024-01-01T00:00Z)"),
        "{human}"
    );
}

#[test]
fn executable_doctor_ready_report_goes_to_stdout_and_exits_zero() {
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("ready executable doctor fixture");
    repo.write("fixtures/generated/data.txt", b"included untracked\n");
    let state = tempfile::tempdir().unwrap();
    let config_path = write_inventory(state.path());
    let runtime = isolated_runtime(state.path(), repo.root());
    let runner = DoctorRunner::new(vec![ready_probe(&[])]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io_in_context(
        doctor_cli(
            config_path,
            Some(repo.root()),
            vec!["fixtures/generated/**".into()],
            true,
        ),
        &runner,
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 0);
    assert!(stderr.is_empty());
    assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["kind"], "doctor");
    assert_eq!(value["ready"], true);
    assert_eq!(value["snapshot"]["included_untracked_count"], 1);
    assert_isolated_snapshot_state(&runtime, true);
}

#[test]
fn executable_doctor_project_blocker_goes_to_stdout_and_exits_usage() {
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("blocked executable doctor fixture");
    repo.write("local-input.txt", b"uncovered untracked input\n");
    let state = tempfile::tempdir().unwrap();
    let config_path = write_inventory(state.path());
    let runtime = isolated_runtime(state.path(), repo.root());
    let runner = DoctorRunner::new(vec![ready_probe(&[])]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io_in_context(
        doctor_cli(config_path, Some(repo.root()), Vec::new(), true),
        &runner,
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 64);
    assert!(stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["kind"], "doctor");
    assert_eq!(value["ready"], false);
    assert_eq!(value["issues"][0]["code"], "UNTRACKED_INPUT");
    assert_isolated_snapshot_state(&runtime, false);
}

#[test]
fn executable_doctor_rejects_external_project_policy_without_leaking_diagnostics() {
    // This catches a symlinked policy authorizing tracked .env bytes and
    // catches its target, contents, or project root reaching CLI stderr.
    let repo = GitRepo::init();
    let external = tempfile::tempdir().unwrap();
    let external_secret = "CLI_EXTERNAL_POLICY_SECRET_3617a4e2";
    let project_secret = "CLI_PROJECT_ENV_SECRET_e2999421";
    let external_policy = external.path().join("outside-policy.toml");
    fs::write(
        &external_policy,
        format!("version = 1\n# {external_secret}\n[snapshot]\nallow_sensitive = [\".env\"]\n"),
    )
    .unwrap();
    symlink(&external_policy, repo.root().join(".worker.toml")).unwrap();
    repo.write(".env", format!("TOKEN={project_secret}\n").as_bytes());
    repo.write("README.md", b"tracked\n");
    repo.commit_all("external policy CLI fixture");
    let state = tempfile::tempdir().unwrap();
    let config_path = write_inventory(state.path());
    let runtime = isolated_runtime(state.path(), repo.root());
    let runner = DoctorRunner::new(Vec::new());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io_in_context(
        doctor_cli(config_path, Some(repo.root()), Vec::new(), true),
        &runner,
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, ExitKind::Usage as u8);
    assert!(stdout.is_empty());
    assert_eq!(
        String::from_utf8_lossy(&stderr),
        "configuration error: project configuration must be a regular file\n"
    );
    for forbidden in [
        external_secret,
        project_secret,
        external_policy.to_string_lossy().as_ref(),
        repo.root().to_string_lossy().as_ref(),
        "allow_sensitive",
    ] {
        assert!(
            !contains_bytes(&stderr, forbidden.as_bytes()),
            "CLI diagnostics leaked {forbidden:?}"
        );
    }
    assert_isolated_snapshot_state(&runtime, false);
}

#[test]
fn executable_doctor_snapshot_change_goes_to_stdout_and_exits_infrastructure() {
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"initial bytes\n");
    repo.commit_all("mutating executable doctor fixture");
    let state = tempfile::tempdir().unwrap();
    let config_path = write_inventory(state.path());
    let runtime = isolated_runtime(state.path(), repo.root());
    let runner = DoctorRunner::mutating_snapshot(repo.root(), vec![ready_probe(&[])]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io_in_context(
        doctor_cli(config_path, Some(repo.root()), Vec::new(), true),
        &runner,
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 70);
    assert!(stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["kind"], "doctor");
    assert_eq!(value["ready"], false);
    assert_eq!(value["issues"][0]["code"], "SNAPSHOT_CHANGED");
    assert_isolated_snapshot_state(&runtime, true);
}

#[test]
fn executable_doctor_broken_stdout_is_an_io_error_on_stderr_and_exit_74() {
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("broken writer executable doctor fixture");
    repo.write("local-input.txt", b"uncovered untracked input\n");
    let state = tempfile::tempdir().unwrap();
    let config_path = write_inventory(state.path());
    let runtime = isolated_runtime(state.path(), repo.root());
    let runner = DoctorRunner::new(vec![ready_probe(&[])]);
    let mut stderr = Vec::new();

    let exit = run_with_io_in_context(
        doctor_cli(config_path, Some(repo.root()), Vec::new(), true),
        &runner,
        &runtime.context,
        &mut BrokenWriter,
        &mut stderr,
    );

    assert_eq!(exit, 74);
    assert!(String::from_utf8(stderr).unwrap().contains("I/O error"));
    assert_isolated_snapshot_state(&runtime, false);
}

#[test]
fn executable_doctor_resolves_an_omitted_project_from_the_injected_current_dir() {
    // Catches parse-time cwd capture or dispatch falling back to the process
    // cwd instead of the isolated runtime context.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("omitted project executable doctor fixture");
    let state = tempfile::tempdir().unwrap();
    let config_path = write_inventory(state.path());
    let runtime = isolated_runtime(state.path(), repo.root());
    let runner = DoctorRunner::new(vec![ready_probe(&[])]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io_in_context(
        doctor_cli(config_path, None, Vec::new(), true),
        &runner,
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 0);
    assert!(stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["kind"], "doctor");
    assert_eq!(value["ready"], true);
    assert_eq!(
        value["project"]["display_name"],
        repo.root().file_name().unwrap().to_string_lossy().as_ref()
    );
    assert_isolated_snapshot_state(&runtime, true);
}

#[test]
fn clean_project_snapshot_and_eligible_worker_are_ready_and_cleanup_the_capture() {
    // Catches losing configured requirement order, publishing a doctor-owned
    // capture, or deciding readiness without both snapshot and worker proof.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\nrequires = [\"swift\", \"darwin-arm64\", \"node\"]\n",
    );
    repo.write("README.md", b"tracked\n");
    repo.write("Dockerfile", b"FROM scratch\n");
    repo.write("Package.swift", b"// package\n");
    repo.write("package.json", b"{}\n");
    repo.write("playwright.config.ts", b"export default {};\n");
    repo.write("go.mod", b"module example.test/fixture\n");
    repo.commit_all("ready doctor fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &["darwin-arm64", "git"])]);
    let runner = DoctorRunner::new(vec![ready_probe(&[
        "darwin-arm64",
        "git",
        "swift",
        "node",
        "browser",
        "docker",
        "go",
    ])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert_eq!(report.version, 1);
    assert!(report.ready);
    assert_eq!(
        report.requirements,
        ["swift", "darwin-arm64", "node", "browser", "docker", "go"]
    );
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert!(report.workers[0].missing_capabilities.is_empty());
    assert!(
        report
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| { snapshot.file_count == 7 && !snapshot.digest.is_empty() })
    );
    assert!(report.issues.is_empty());
    assert_eq!(
        report.project.display_name,
        repo.root().file_name().unwrap().to_string_lossy()
    );
    assert_eq!(report.project.relative_working_dir, "");
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains(&repo.root().to_string_lossy().into_owned())
    );
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn busy_worker_is_healthy_but_not_immediately_eligible() {
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("busy worker fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &["darwin-arm64"])]);
    let runner = DoctorRunner::new(vec![busy_probe(&["darwin-arm64"])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(!report.ready);
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert_eq!(
        report.workers[0].probe.as_ref().unwrap().slot_state,
        SlotState::Busy
    );
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.code == "NO_ELIGIBLE_WORKER")
    );
    let human = CommandOutput::Doctor(report).render_human();
    assert!(human.contains("mini-1: busy"));
    assert!(human.contains("slot: busy"));
}

#[test]
fn uncovered_untracked_input_is_a_bounded_typed_blocker_but_keeps_worker_health() {
    // Catches converting selection failures into an opaque error, leaking raw
    // control characters, or skipping independent worker diagnostics.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("untracked doctor fixture");
    repo.write(
        "local\ninput.txt",
        b"untracked contents must not be reported\n",
    );
    for index in 0..105 {
        repo.write(&format!("untracked/{index:03}.txt"), b"local\n");
    }
    let state = tempfile::tempdir().unwrap();
    let poisoned_cache = state.path().join("cache");
    fs::write(&poisoned_cache, b"capture must never touch this file\n").unwrap();
    let config = config(vec![worker("mini-1", "mac1", &[])]);
    let runner = DoctorRunner::new(vec![ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(!report.ready);
    assert!(report.snapshot.is_none());
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert_eq!(report.issues.len(), 1);
    assert_eq!(report.issues[0].severity, IssueSeverity::Blocker);
    assert_eq!(report.issues[0].code, "UNTRACKED_INPUT");
    assert_eq!(report.issues[0].paths.len(), 100);
    assert_eq!(report.issues[0].paths[0], "local\\ninput.txt");
    assert!(!report.issues[0].message.contains("untracked contents"));
    assert!(!report.issues[0].message.contains('\n'));
    assert!(poisoned_cache.is_file());
    assert!(!poisoned_cache.join("snapshots").exists());
}

#[test]
fn explicitly_allowed_sensitive_path_is_only_a_content_free_warning() {
    // Catches allowlisting either blocking readiness or copying secret bytes
    // into the typed report instead of reporting only the relative path.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\n[snapshot]\nallow_sensitive = [\".env\"]\n",
    );
    repo.write(".env", b"DOCTOR_PLANTED_SECRET=never-report-this\n");
    repo.commit_all("allowed sensitive fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &[])]);
    let runner = DoctorRunner::new(vec![ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();
    let serialized = serde_json::to_string(&report).unwrap();

    assert!(report.ready);
    assert!(report.snapshot.is_some());
    assert_eq!(report.issues.len(), 1);
    assert_eq!(report.issues[0].severity, IssueSeverity::Warning);
    assert_eq!(report.issues[0].code, "SENSITIVE_PATH_ALLOWED");
    assert_eq!(report.issues[0].paths, [".env"]);
    assert!(!serialized.contains("DOCTOR_PLANTED_SECRET"));
    assert!(!serialized.contains("never-report-this"));
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn blocked_sensitive_path_matrix_leaks_no_secret_bytes_or_snapshot_tree() {
    // Catches any sensitive-path policy gap or diagnostic path that copies
    // tracked credential contents into typed, human, JSON, error, or cache
    // surfaces. Relative paths may be named; their bytes never may be.
    let repo = GitRepo::init();
    let secrets = [
        (".env", "ENV_SECRET_MATRIX_71f5920d"),
        (".npmrc", "NPM_SECRET_MATRIX_28ca864e"),
        (".pypirc", "PYPI_SECRET_MATRIX_43bd709a"),
        (".ssh/id_ed25519", "SSH_SECRET_MATRIX_9ed1435c"),
        (".aws/credentials", "AWS_SECRET_MATRIX_56ac208f"),
    ];
    assert_eq!(
        secrets
            .iter()
            .map(|(_, bytes)| *bytes)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        secrets.len()
    );
    for (path, secret) in secrets {
        repo.write(path, format!("{secret}\n").as_bytes());
    }
    repo.commit_all("track blocked sensitive matrix");

    let context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .unwrap();
    let selection_error = InputSelector::new(&SystemProcessRunner)
        .select(
            &context,
            &SnapshotSettings {
                include_untracked: Vec::new(),
                include_empty_dirs: Vec::new(),
                allow_sensitive: Vec::new(),
            },
        )
        .expect_err("tracked sensitive paths must block selection");
    assert_eq!(selection_error.code, "SENSITIVE_PATH");

    let state = tempfile::tempdir().unwrap();
    let service_state = state.path().join("service");
    let service_config = config(vec![worker("mini-1", "mac1", &[])]);
    let service_runner = DoctorRunner::new(vec![ready_probe(&[])]);
    let report = inspect(&repo, &service_state, &service_config, &service_runner).unwrap();
    let expected_paths = [
        ".aws/credentials",
        ".env",
        ".npmrc",
        ".pypirc",
        ".ssh/id_ed25519",
    ];
    assert!(!report.ready);
    assert!(report.snapshot.is_none());
    assert_eq!(report.issues.len(), 1);
    assert_eq!(report.issues[0].code, "SENSITIVE_PATH");
    assert_eq!(report.issues[0].paths, expected_paths);

    let output = CommandOutput::Doctor(report.clone());
    let typed = format!("{report:?}");
    let human = output.render_human();
    let json = output.render_json().unwrap();
    let selection_display = selection_error.to_string();
    let selection_debug = format!("{selection_error:?}");
    let issue_errors = report
        .issues
        .iter()
        .map(|issue| format!("[{}] {}", issue.code, issue.message))
        .collect::<Vec<_>>()
        .join("\n");

    let config_path = write_inventory(state.path());
    let runtime = isolated_runtime(state.path(), repo.root());
    let mut executable_surfaces = Vec::new();
    for json_mode in [false, true] {
        let runner = DoctorRunner::new(vec![ready_probe(&[])]);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = run_with_io_in_context(
            doctor_cli(
                config_path.clone(),
                Some(repo.root()),
                Vec::new(),
                json_mode,
            ),
            &runner,
            &runtime.context,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(exit, ExitKind::Usage as u8);
        assert!(stderr.is_empty());
        executable_surfaces.push((stdout, stderr));
    }

    for path in expected_paths {
        assert!(
            human.contains(path),
            "human output omitted blocked path {path}"
        );
        assert!(
            json.contains(path),
            "JSON output omitted blocked path {path}"
        );
    }
    let text_surfaces = [
        ("typed debug", typed.as_bytes()),
        ("human", human.as_bytes()),
        ("JSON", json.as_bytes()),
        ("selection display", selection_display.as_bytes()),
        ("selection debug", selection_debug.as_bytes()),
        ("issue error strings", issue_errors.as_bytes()),
    ];
    for (path, secret) in secrets {
        for (label, surface) in text_surfaces {
            assert!(
                !contains_bytes(surface, secret.as_bytes()),
                "{label} leaked secret bytes from {path}"
            );
        }
        for (stdout, stderr) in &executable_surfaces {
            assert!(
                !contains_bytes(stdout, secret.as_bytes()),
                "executable stdout leaked secret bytes from {path}"
            );
            assert!(
                !contains_bytes(stderr, secret.as_bytes()),
                "executable stderr leaked secret bytes from {path}"
            );
        }
    }

    assert!(
        !service_state.join("cache").exists(),
        "blocked service doctor created a snapshot cache"
    );
    assert_isolated_snapshot_state(&runtime, false);
    let mut state_files = Vec::new();
    regular_files_below(state.path(), &mut state_files);
    assert!(
        state_files
            .iter()
            .all(|path| path.file_name() != Some(OsStr::new("manifest.json")))
    );
    for file in state_files {
        let bytes = fs::read(&file).unwrap();
        for (path, secret) in secrets {
            assert!(
                !contains_bytes(&bytes, secret.as_bytes()),
                "local state file {} leaked secret bytes from {path}",
                file.display()
            );
        }
    }
}

#[test]
fn unsupported_submodule_lfs_and_custom_filters_block_before_capture() {
    // Catches weakening unsupported Git features into warnings or attempting
    // a partial capture after input preflight has already failed.
    for feature in ["submodule", "lfs", "filter"] {
        let repo = GitRepo::init();
        let expected = match feature {
            "submodule" => {
                repo.write("README.md", b"root\n");
                repo.commit_all("submodule fixture");
                let head = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
                    .unwrap()
                    .trim()
                    .to_owned();
                assert!(
                    repo.git(&[
                        "update-index",
                        "--add",
                        "--cacheinfo",
                        &format!("160000,{head},vendor/submodule"),
                    ])
                    .status
                    .success()
                );
                "UNSUPPORTED_SUBMODULE"
            }
            "lfs" => {
                repo.write(".gitattributes", b"*.bin filter=lfs\n");
                repo.write("asset.bin", b"materialized bytes\n");
                repo.commit_all("lfs fixture");
                "UNSUPPORTED_LFS"
            }
            "filter" => {
                assert!(
                    repo.git(&["config", "filter.custom.clean", "cat"])
                        .status
                        .success()
                );
                "UNSUPPORTED_FILTER"
            }
            _ => unreachable!(),
        };
        let state = tempfile::tempdir().unwrap();
        let poisoned_cache = state.path().join("cache");
        fs::write(&poisoned_cache, b"capture must never touch this file\n").unwrap();
        let config = config(vec![worker("mini-1", "mac1", &[])]);
        let runner = DoctorRunner::new(vec![ready_probe(&[])]);

        let report = inspect(&repo, state.path(), &config, &runner).unwrap();

        assert!(!report.ready, "{feature}");
        assert!(report.snapshot.is_none(), "{feature}");
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.code == expected && issue.severity == IssueSeverity::Blocker),
            "{feature}: {:?}",
            report.issues
        );
        assert_eq!(report.workers[0].status, HealthStatus::Ready);
        assert!(poisoned_cache.is_file(), "{feature}");
        assert!(
            !poisoned_cache.join("snapshots").exists(),
            "{feature}: snapshot hierarchy was created"
        );
    }
}

#[test]
fn doctor_replaces_untrusted_ssh_diagnostics_everywhere_in_the_typed_report() {
    // Catches raw SSH stderr surviving in WorkerHealth even if the duplicate
    // issue message is genericized. The JSON and typed report share this data.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("SSH diagnostic redaction fixture");
    let state = tempfile::tempdir().unwrap();
    let secret_token = "ssh-token-DOCTOR-should-never-report";
    let environment_value = "DEPLOY_PASSWORD=planted-environment-secret";
    let absolute_path = state
        .path()
        .join("private/credentials.txt")
        .to_string_lossy()
        .into_owned();
    let stderr = format!(
        "authentication failed: {secret_token}\n{environment_value}\r\nlocal={absolute_path}\tCONTROL_MARKER\u{1b}"
    );
    let config = config(vec![
        worker("offline", "mac1", &[]),
        worker("ready", "mac2", &[]),
    ]);
    let runner = DoctorRunner::new(vec![
        offline_probe_with_stderr(stderr.as_bytes()),
        ready_probe(&[]),
    ]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();
    let typed = format!("{report:?}");
    let json = serde_json::to_string(&report).unwrap();
    let safe_message = "worker offline could not be reached by the SSH probe";

    assert!(report.ready);
    assert_eq!(
        report.workers[0].error_code.as_deref(),
        Some("SSH_UNAVAILABLE")
    );
    assert_eq!(
        report.workers[0].error_message.as_deref(),
        Some(safe_message)
    );
    let issue = report
        .issues
        .iter()
        .find(|issue| issue.code == "SSH_UNAVAILABLE")
        .unwrap();
    assert_eq!(issue.message, safe_message);
    for forbidden in [
        secret_token,
        environment_value,
        absolute_path.as_str(),
        "CONTROL_MARKER",
    ] {
        assert!(
            !typed.contains(forbidden),
            "typed report leaked {forbidden:?}"
        );
        assert!(
            !json.contains(forbidden),
            "JSON report leaked {forbidden:?}"
        );
    }
    for escaped_control in ["\\n", "\\r", "\\t", "\\u001b", "\\u{1b}"] {
        assert!(
            !typed.contains(escaped_control),
            "typed report leaked control data {escaped_control:?}"
        );
        assert!(
            !json.contains(escaped_control),
            "JSON report leaked control data {escaped_control:?}"
        );
    }
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn doctor_rejects_invalid_probe_fields_without_retaining_or_rendering_them() {
    // Catches structured SSH probe data reaching the typed report before
    // protocol and required-capability classification validates it.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("invalid structured probe fixture");
    let state = tempfile::tempdir().unwrap();
    let oversized_hostname = format!("{}-hostname-doctor-secret", "h".repeat(280));
    let oversized_capability = format!("{}-capability-doctor-secret", "c".repeat(100));
    let forbidden = [
        oversized_hostname.clone(),
        "ARCH_DOCTOR_SECRET".into(),
        "OS_DOCTOR_SECRET=value".into(),
        oversized_capability.clone(),
    ];
    let config = config(vec![
        worker("invalid-hostname", "mac1", &["required-safe"]),
        worker("invalid-arch", "mac2", &["required-safe"]),
        worker("invalid-os", "mac3", &["required-safe"]),
        worker("invalid-capability", "mac4", &["required-safe"]),
    ]);
    let runner = DoctorRunner::new(vec![
        structured_probe(2, &oversized_hostname, "arm64", "26.2", Vec::new()),
        structured_probe(
            1,
            "mini.local",
            "arm64\u{1b}]0;ARCH_DOCTOR_SECRET\u{7}",
            "26.2",
            Vec::new(),
        ),
        structured_probe(
            1,
            "mini.local",
            "arm64",
            "26.2\nOS_DOCTOR_SECRET=value",
            Vec::new(),
        ),
        structured_probe(1, "mini.local", "arm64", "26.2", vec![oversized_capability]),
    ]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();
    let typed = format!("{report:?}");
    let output = CommandOutput::Doctor(report.clone());
    let json = output.render_json().unwrap();
    let human = output.render_human();

    assert!(!report.ready);
    assert!(report.snapshot.is_some());
    let expected_messages = [
        "worker invalid-hostname returned an invalid probe response",
        "worker invalid-arch returned an invalid probe response",
        "worker invalid-os returned an invalid probe response",
        "worker invalid-capability returned an invalid probe response",
    ];
    for (worker, expected_message) in report.workers.iter().zip(expected_messages) {
        assert_eq!(worker.status, HealthStatus::Unavailable);
        assert_eq!(worker.error_code.as_deref(), Some("INVALID_RESPONSE"));
        assert_eq!(worker.error_message.as_deref(), Some(expected_message));
        assert!(worker.probe.is_none());
        assert!(worker.missing_capabilities.is_empty());
    }
    assert!(report.issues.iter().any(|issue| {
        issue.severity == IssueSeverity::Blocker && issue.code == "NO_ELIGIBLE_WORKER"
    }));
    assert_eq!(output.aggregate_exit_kind(), Some(ExitKind::Usage));
    for value in forbidden {
        assert!(!typed.contains(&value), "typed report leaked {value:?}");
        assert!(!json.contains(&value), "doctor JSON leaked {value:?}");
        assert!(!human.contains(&value), "human output leaked {value:?}");
    }
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn zero_eligible_workers_adds_a_blocker_without_losing_worker_diagnostics() {
    // Catches treating an all-ineligible inventory as harmless warnings or
    // collapsing its distinct capability and transport diagnostics.
    let repo = GitRepo::init();
    repo.write(".worker.toml", b"version = 1\nrequires = [\"node\"]\n");
    repo.write("README.md", b"tracked\n");
    repo.commit_all("ineligible workers fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![
        worker("missing", "mac1", &["darwin-arm64"]),
        worker("offline", "mac2", &["darwin-arm64"]),
    ]);
    let runner = DoctorRunner::new(vec![ready_probe(&["darwin-arm64"]), offline_probe()]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(!report.ready);
    assert!(report.snapshot.is_some());
    assert_eq!(report.workers.len(), 2);
    assert_eq!(
        report.workers[0].error_code.as_deref(),
        Some("MISSING_CAPABILITIES")
    );
    assert_eq!(report.workers[0].missing_capabilities, ["node"]);
    assert_eq!(
        report.workers[1].error_code.as_deref(),
        Some("SSH_UNAVAILABLE")
    );
    assert!(report.issues.iter().any(|issue| {
        issue.code == "NO_ELIGIBLE_WORKER" && issue.severity == IssueSeverity::Blocker
    }));
    assert!(!report.issues.iter().any(|issue| {
        issue.code == "SSH_UNAVAILABLE" && issue.severity == IssueSeverity::Warning
    }));
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn offline_worker_is_a_sorted_warning_when_another_worker_is_eligible() {
    // Catches one failed probe blocking a usable inventory and catches issue
    // ordering drifting away from severity, code, then first path.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\n[snapshot]\nallow_sensitive = [\".env\"]\n",
    );
    repo.write(".env", b"fixture\n");
    repo.commit_all("partial worker health fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![
        worker("offline", "mac1", &[]),
        worker("ready", "mac2", &[]),
    ]);
    let runner = DoctorRunner::new(vec![offline_probe(), ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(report.ready);
    assert_eq!(report.workers[0].status, HealthStatus::Unavailable);
    assert_eq!(report.workers[1].status, HealthStatus::Ready);
    assert_eq!(
        report
            .issues
            .iter()
            .map(|issue| (issue.severity, issue.code.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (IssueSeverity::Warning, "SENSITIVE_PATH_ALLOWED"),
            (IssueSeverity::Warning, "SSH_UNAVAILABLE"),
        ]
    );
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn policy_change_during_worker_probe_blocks_before_capture_without_leaking_allowed_secret() {
    // This catches probing under one authorization policy and capturing under
    // stale settings after that policy has revoked a sensitive path.
    let repo = GitRepo::init();
    let secret = "PROBE_WINDOW_POLICY_SECRET_f2e57941";
    repo.write(
        ".worker.toml",
        b"version = 1\n[snapshot]\nallow_sensitive = [\".env\"]\n",
    );
    repo.write(".env", format!("TOKEN={secret}\n").as_bytes());
    repo.write("README.md", b"tracked\n");
    repo.commit_all("probe-window policy fixture");
    repo.write("README.md", b"already dirty before probe\n");
    let state = tempfile::tempdir().unwrap();
    let poisoned_cache = state.path().join("cache");
    let poison = b"capture must never open this cache\n";
    fs::write(&poisoned_cache, poison).unwrap();
    let config = config(vec![worker("mini-1", "mac1", &[])]);
    let runner = DoctorRunner::mutating_probe_policy(repo.root(), vec![ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(!report.ready);
    assert!(report.snapshot.is_none());
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert_eq!(
        report
            .issues
            .iter()
            .map(|issue| (issue.severity, issue.code.as_str()))
            .collect::<Vec<_>>(),
        vec![(IssueSeverity::Blocker, "SNAPSHOT_CHANGED")]
    );
    let output = CommandOutput::Doctor(report.clone());
    assert_eq!(output.aggregate_exit_kind(), Some(ExitKind::Infrastructure));
    let surfaces = [
        format!("{report:?}"),
        output.render_human(),
        output.render_json().unwrap(),
    ];
    for forbidden in [secret, repo.root().to_string_lossy().as_ref()] {
        assert!(
            surfaces.iter().all(|surface| !surface.contains(forbidden)),
            "probe-window report leaked {forbidden:?}"
        );
    }
    assert_eq!(fs::read(&poisoned_cache).unwrap(), poison);
    assert!(!poisoned_cache.join("snapshots").exists());
}

#[test]
fn requirement_and_head_change_during_worker_probe_blocks_before_capture() {
    // This catches accepting worker eligibility measured for an old HEAD and
    // requirement set, even when the later project state is otherwise clean.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("probe-window Git context fixture");
    let original_head = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    let state = tempfile::tempdir().unwrap();
    let poisoned_cache = state.path().join("cache");
    let poison = b"capture must never open this cache\n";
    fs::write(&poisoned_cache, poison).unwrap();
    let config = config(vec![worker("mini-1", "mac1", &[])]);
    let runner =
        DoctorRunner::mutating_probe_requirement_and_head(repo.root(), vec![ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();
    let changed_head = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();

    assert_ne!(changed_head, original_head);
    assert!(!report.ready);
    assert!(report.snapshot.is_none());
    assert_eq!(report.project.head.as_deref(), Some(original_head.as_str()));
    assert!(report.requirements.is_empty());
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert_eq!(
        report
            .issues
            .iter()
            .map(|issue| (issue.severity, issue.code.as_str()))
            .collect::<Vec<_>>(),
        vec![(IssueSeverity::Blocker, "SNAPSHOT_CHANGED")]
    );
    assert_eq!(fs::read(&poisoned_cache).unwrap(), poison);
    assert!(!poisoned_cache.join("snapshots").exists());
}

#[test]
fn policy_change_after_capture_discards_summary_and_cleans_secret_bytes() {
    // This catches returning a ready summary when policy changes after the
    // snapshot's own byte verification but before Doctor returns.
    let repo = GitRepo::init();
    let secret = "POST_CAPTURE_POLICY_SECRET_6d67d751";
    repo.write(
        ".worker.toml",
        b"version = 1\n[snapshot]\nallow_sensitive = [\".env\"]\n",
    );
    repo.write(".env", format!("TOKEN={secret}\n").as_bytes());
    repo.write("README.md", b"tracked\n");
    repo.commit_all("post-capture policy fixture");
    repo.write("README.md", b"already dirty before capture\n");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &[])]);
    let runner = DoctorRunner::mutating_policy_after_capture(repo.root(), vec![ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(!report.ready);
    assert!(report.snapshot.is_none());
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert_eq!(
        report
            .issues
            .iter()
            .map(|issue| (issue.severity, issue.code.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (IssueSeverity::Blocker, "SNAPSHOT_CHANGED"),
            (IssueSeverity::Warning, "SENSITIVE_PATH_ALLOWED"),
        ]
    );
    let output = CommandOutput::Doctor(report.clone());
    for surface in [
        format!("{report:?}"),
        output.render_human(),
        output.render_json().unwrap(),
    ] {
        assert!(!surface.contains(secret), "report leaked captured secret");
    }
    assert_no_doctor_snapshot(&state.path().join("cache"));
    let mut cache_files = Vec::new();
    regular_files_below(&state.path().join("cache"), &mut cache_files);
    for file in cache_files {
        assert!(
            !contains_bytes(&fs::read(&file).unwrap(), secret.as_bytes()),
            "cleaned cache file {} retained secret bytes",
            file.display()
        );
    }
}

#[test]
fn snapshot_change_is_a_typed_blocker_with_no_published_snapshot() {
    // Catches returning a stale summary after the selected source changes and
    // catches local integrity failure erasing independent worker health.
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"initial bytes\n");
    repo.commit_all("snapshot mutation fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &[])]);
    let runner = DoctorRunner::mutating_snapshot(repo.root(), vec![ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(!report.ready);
    assert!(report.snapshot.is_none());
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert!(report.issues.iter().any(|issue| {
        issue.code == "SNAPSHOT_CHANGED" && issue.severity == IssueSeverity::Blocker
    }));
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

#[test]
fn issues_sort_by_severity_then_code_then_first_path() {
    // Catches warning codes outranking blockers or equal-code issues retaining
    // producer order instead of sorting by their first escaped path.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\n[snapshot]\nallow_sensitive = [\".npmrc\", \".env\"]\n",
    );
    repo.write(".env", b"first planted secret\n");
    repo.write(".npmrc", b"second planted secret\n");
    repo.write("tracked.txt", b"initial bytes\n");
    repo.commit_all("issue ordering fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![
        worker("invalid", "mac1", &[]),
        worker("ready", "mac2", &[]),
    ]);
    let runner =
        DoctorRunner::mutating_snapshot(repo.root(), vec![invalid_probe(), ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(!report.ready);
    assert!(report.snapshot.is_none());
    assert_eq!(
        report
            .issues
            .iter()
            .map(|issue| (
                issue.severity,
                issue.code.as_str(),
                issue.paths.first().map(String::as_str),
            ))
            .collect::<Vec<_>>(),
        vec![
            (IssueSeverity::Blocker, "SNAPSHOT_CHANGED", None),
            (IssueSeverity::Warning, "INVALID_RESPONSE", None),
            (
                IssueSeverity::Warning,
                "SENSITIVE_PATH_ALLOWED",
                Some(".env"),
            ),
            (
                IssueSeverity::Warning,
                "SENSITIVE_PATH_ALLOWED",
                Some(".npmrc"),
            ),
        ]
    );
    assert_no_doctor_snapshot(&state.path().join("cache"));
}

fn ready_probe_with_facts(
    capabilities: &[&str],
    herdr: Option<serde_json::Value>,
    facts_age_millis: u64,
) -> Result<ProcessResult, WorkerError> {
    let mut result = ready_probe(capabilities)?;
    let mut value: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    let mut facts = serde_json::json!({
        "agents": [],
        "env_profiles": [],
        "git_identity": true,
        "collected_at_millis": 10,
    });
    if let Some(herdr) = herdr {
        facts["herdr"] = herdr;
    }
    value["agent_facts"] = facts;
    value["facts_age_millis"] = serde_json::json!(facts_age_millis);
    result.stdout = serde_json::to_vec(&value).unwrap();
    Ok(result)
}

fn herdr_worker(name: &str, ssh: &str, herdr: bool) -> WorkerEntry {
    WorkerEntry {
        herdr,
        ..worker(name, ssh, &[])
    }
}

fn herdr_repo() -> GitRepo {
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("herdr doctor fixture");
    repo
}

fn issue_triples(report: &DoctorReport) -> Vec<(IssueSeverity, &str, &str)> {
    report
        .issues
        .iter()
        .map(|issue| (issue.severity, issue.code.as_str(), issue.message.as_str()))
        .collect()
}

/// Every probe whose herdr fact is not `available`, with the line spec 5.3
/// renders for it: three collected states, then facts missing, predating
/// the fact, and stale.
fn unavailable_herdr_probes() -> Vec<(Result<ProcessResult, WorkerError>, &'static str)> {
    vec![
        (
            ready_probe_with_facts(
                &[],
                Some(serde_json::json!({ "state": "not_installed" })),
                0,
            ),
            "    herdr: not installed",
        ),
        (
            ready_probe_with_facts(
                &[],
                Some(serde_json::json!({ "state": "no_socket", "version": "0.9.0" })),
                0,
            ),
            "    herdr: installed (0.9.0), no socket",
        ),
        (
            ready_probe_with_facts(
                &[],
                Some(serde_json::json!({ "state": "no_response", "version": "0.9.0" })),
                0,
            ),
            "    herdr: installed (0.9.0), no response",
        ),
        (ready_probe(&[]), "    herdr: unknown"),
        (ready_probe_with_facts(&[], None, 0), "    herdr: unknown"),
        (
            ready_probe_with_facts(
                &[],
                Some(serde_json::json!({ "state": "available", "version": "0.9.0" })),
                4_120_000,
            ),
            "    herdr: available (0.9.0), stale 69m",
        ),
    ]
}

#[test]
fn herdr_true_worker_gets_a_warning_for_every_fact_state_but_available_and_stays_ready() {
    // Spec 5.3 and 12: HERDR_UNAVAILABLE is a warning, never a blocker, and
    // it never touches readiness or eligibility.
    use mac_worker::protocol::{HERDR_FACTS_STALE_MESSAGE, HERDR_UNAVAILABLE_MESSAGE};

    let repo = herdr_repo();
    let config = config(vec![herdr_worker("mini-1", "mac1", true)]);
    for (probe, line) in unavailable_herdr_probes() {
        let state = tempfile::tempdir().unwrap();
        let runner = DoctorRunner::new(vec![probe]);
        // A fact that is missing renders `unknown` and a stale fact keeps the
        // last collected value with a `stale` suffix; both get the message
        // that names the refresh. A fresh fact that is not `available` gets
        // the unreachable message.
        let expected_message = if line.contains("unknown") || line.contains("stale") {
            HERDR_FACTS_STALE_MESSAGE
        } else {
            HERDR_UNAVAILABLE_MESSAGE
        };

        let report = inspect(&repo, state.path(), &config, &runner).unwrap();

        assert!(report.ready, "{line}: the herdr warning never blocks");
        assert_eq!(report.workers[0].status, HealthStatus::Ready, "{line}");
        assert_eq!(report.workers[0].error_code, None, "{line}");
        assert_eq!(
            issue_triples(&report),
            vec![(
                IssueSeverity::Warning,
                "HERDR_UNAVAILABLE",
                expected_message,
            )],
            "{line}"
        );

        let human = CommandOutput::Doctor(report.clone()).render_human();
        assert!(human.starts_with("doctor: ready\n"), "{line}: {human}");
        assert!(human.contains(line), "{line} missing in {human}");
        assert!(
            human.contains(&format!(
                "  warning [HERDR_UNAVAILABLE]: {expected_message}"
            )),
            "{human}"
        );
        let json: serde_json::Value =
            serde_json::from_str(&CommandOutput::Doctor(report).render_json().unwrap()).unwrap();
        assert_eq!(json["ready"], true, "{line}");
        assert_eq!(
            json["issues"],
            serde_json::json!([{
                "severity": "warning",
                "code": "HERDR_UNAVAILABLE",
                "message": expected_message,
                "paths": [],
            }]),
            "{line}"
        );
    }
}

#[test]
fn herdr_true_worker_with_an_available_fact_gets_no_herdr_warning() {
    use mac_worker::agent_facts::FACTS_TTL;

    let repo = herdr_repo();
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![herdr_worker("mini-1", "mac1", true)]);
    let runner = DoctorRunner::new(vec![ready_probe_with_facts(
        &[],
        Some(serde_json::json!({ "state": "available", "version": "0.9.0" })),
        FACTS_TTL,
    )]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(report.ready);
    assert!(report.issues.is_empty(), "{:?}", report.issues);
    let human = CommandOutput::Doctor(report.clone()).render_human();
    assert!(human.contains("    herdr: available (0.9.0)"), "{human}");
    assert!(!human.contains("HERDR_UNAVAILABLE"));
    let json = CommandOutput::Doctor(report).render_json().unwrap();
    assert!(json.contains(r#""herdr":{"state":"available","version":"0.9.0"}"#));
    assert!(!json.contains("HERDR_UNAVAILABLE"));
}

#[test]
fn herdr_false_worker_never_gets_the_herdr_warning() {
    let repo = herdr_repo();
    let config = config(vec![herdr_worker("mini-1", "mac1", false)]);
    for (probe, line) in unavailable_herdr_probes() {
        let state = tempfile::tempdir().unwrap();
        let runner = DoctorRunner::new(vec![probe]);

        let report = inspect(&repo, state.path(), &config, &runner).unwrap();

        assert!(report.ready, "{line}");
        assert!(report.issues.is_empty(), "{line}: {:?}", report.issues);
        let human = CommandOutput::Doctor(report.clone()).render_human();
        assert!(human.contains(line), "{line} missing in {human}");
        assert!(!human.contains("HERDR_UNAVAILABLE"), "{human}");
        let json = CommandOutput::Doctor(report).render_json().unwrap();
        assert!(!json.contains("HERDR_UNAVAILABLE"), "{json}");
    }
}

#[test]
fn herdr_warning_for_an_unreachable_herdr_worker_sorts_with_the_other_warnings() {
    // An unreachable worker has no fresh fact; asking for herdr on it adds
    // the facts warning beside SSH_UNAVAILABLE and leaves the ready pool
    // ready.
    use mac_worker::protocol::HERDR_FACTS_STALE_MESSAGE;

    let repo = herdr_repo();
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![
        herdr_worker("offline", "mac1", true),
        herdr_worker("ready", "mac2", false),
    ]);
    let runner = DoctorRunner::new(vec![offline_probe(), ready_probe(&[])]);

    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(report.ready);
    assert_eq!(report.workers[0].status, HealthStatus::Unavailable);
    assert_eq!(report.workers[1].status, HealthStatus::Ready);
    assert_eq!(
        issue_triples(&report),
        vec![
            (
                IssueSeverity::Warning,
                "HERDR_UNAVAILABLE",
                HERDR_FACTS_STALE_MESSAGE,
            ),
            (
                IssueSeverity::Warning,
                "SSH_UNAVAILABLE",
                "worker offline could not be reached by the SSH probe",
            ),
        ]
    );
}

#[test]
fn declared_origin_without_a_helper_warns_origin_helper_missing_and_stays_ready() {
    let repo = herdr_repo();
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &["origin:github.com"])]);
    let runner = DoctorRunner::new(vec![ready_probe_with_facts(&[], None, 0)]);
    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(report.ready);
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert_eq!(
        issue_triples(&report),
        vec![(
            IssueSeverity::Warning,
            "ORIGIN_HELPER_MISSING",
            "worker mini-1 has no HTTPS credential helper for origin:github.com",
        )]
    );
    let human = CommandOutput::Doctor(report).render_human();
    assert!(human.starts_with("doctor: ready\n"), "{human}");
    assert!(
        human.contains("warning [ORIGIN_HELPER_MISSING]: worker mini-1 has no HTTPS credential helper for origin:github.com"),
        "{human}"
    );
}

#[test]
fn declared_origin_with_a_helper_gets_no_origin_helper_warning() {
    let repo = herdr_repo();
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &["origin:github.com"])]);
    let mut probe = ready_probe_with_facts(&[], None, 0).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&probe.stdout).unwrap();
    value["agent_facts"]["origin_https_helpers"] = serde_json::json!({ "generic": true });
    probe.stdout = serde_json::to_vec(&value).unwrap();
    let runner = DoctorRunner::new(vec![Ok(probe)]);
    let report = inspect(&repo, state.path(), &config, &runner).unwrap();

    assert!(report.ready);
    assert!(
        report
            .issues
            .iter()
            .all(|issue| issue.code != "ORIGIN_HELPER_MISSING"),
        "{:?}",
        report.issues
    );
}

#[test]
fn doctor_warns_when_a_running_dashboard_is_older_than_the_installed_binary() {
    let repo = GitRepo::init();
    repo.write("tracked.txt", b"ok\n");
    repo.commit_all("laptop binary warning fixture");
    let state = tempfile::tempdir().unwrap();
    let config = config(vec![worker("mini-1", "mac1", &[])]);
    let runner = DoctorRunner::new(vec![ready_probe(&[])]);
    let processes = FixedLaptopProcessTable {
        processes: vec![LaptopProcess {
            pid: 4242,
            started_at: UNIX_EPOCH + Duration::from_secs(10),
            args: vec!["/Users/me/.local/bin/worker".into(), "dashboard".into()],
        }],
    };
    let paths = paths(state.path());
    let report = DoctorService {
        runner: &runner,
        config: &config,
        paths: &paths,
        laptop_processes: &processes,
        installed_binary_mtime: Some(UNIX_EPOCH + Duration::from_secs(50)),
    }
    .inspect(DoctorRequest {
        project: repo.root().to_path_buf(),
        cli_includes: Vec::new(),
    })
    .unwrap();

    assert!(report.ready);
    let warning = report
        .issues
        .iter()
        .find(|issue| issue.code == "LAPTOP_BINARY_OUTDATED")
        .expect("outdated dashboard must be a warning");
    assert_eq!(warning.severity, IssueSeverity::Warning);
    assert!(
        warning
            .message
            .contains("worker dashboard (pid 4242) was started before the installed binary"),
        "{}",
        warning.message
    );
}
