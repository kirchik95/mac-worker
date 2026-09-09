use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{Arc, Mutex},
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    agent_facts::{AgentAuth, AgentFacts, AgentProbe, FACTS_TTL, ProfileProbe},
    cli::{Cli, Command, HostCommand},
    config::{Config, WorkerEntry},
    host_store::HostStore,
    lease::SlotState,
    output::CommandOutput,
    probe::ProbeCollector,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, WorkerHealth},
    scheduler_adapter::SchedulerProbeAdapter,
    transfer::HostOperation,
};

const SECRET: &str = "agent-probe-secret-that-must-never-escape";
const INSECURE_SECRET: &str = "insecure-profile-value-that-must-never-be-applied";

#[derive(Clone, Default)]
struct RecordingRunner {
    requests: Arc<Mutex<Vec<ProcessRequest>>>,
}

impl RecordingRunner {
    fn requests(&self) -> Vec<ProcessRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl ProcessRunner for RecordingRunner {
    fn run(
        &self,
        request: &ProcessRequest,
    ) -> Result<ProcessResult, mac_worker::error::WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        let program = request.program.to_string_lossy();
        let args = request
            .args
            .iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        if program == "/bin/zsh" && args.first().map(String::as_str) == Some("-lc") {
            let shell = args.last().map(String::as_str).unwrap_or_default();
            // The herdr lookups prefix the command with a PATH extension for
            // `~/.local/bin`; the command proper follows the last `; `.
            let shell = shell
                .rsplit_once("; ")
                .map_or(shell, |(_, command)| command);
            if let Some(binary) = command_v_binary(shell) {
                let success = binary == "codex";
                if !success {
                    return Ok(ProcessResult {
                        status: exit_status(1),
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    });
                }
                let mut stdout = b"/opt/tools/codex\n".to_vec();
                if shell.contains("MAC_WORKER_FACTS_VERSION") {
                    stdout.extend_from_slice(b"MAC_WORKER_FACTS_VERSION\n");
                    stdout.extend_from_slice(b"codex-cli 0.152.1\n");
                }
                return Ok(ProcessResult {
                    status: exit_status(0),
                    stdout,
                    stderr: Vec::new(),
                });
            }
            if shell.starts_with("exec ") {
                return Ok(self.exec_command(request, shell));
            }
        }

        let stdout = match (program.as_ref(), args.as_slice()) {
            ("zsh", [shell, command])
                if shell == "-lc" && command == "git config --get user.name" =>
            {
                b"Worker Account\n".to_vec()
            }
            ("zsh", [shell, command])
                if shell == "-lc" && command == "git config --get user.email" =>
            {
                b"worker@example.test\n".to_vec()
            }
            ("/usr/bin/ssh", _)
                if args
                    .last()
                    .is_some_and(|command| command.ends_with(" host probe")) =>
            {
                serde_json::to_vec(&health_with_facts(facts(1)).probe.unwrap()).unwrap()
            }
            ("/usr/bin/ssh", _) => Vec::new(),
            _ => Vec::new(),
        };
        Ok(ProcessResult {
            status: exit_status(0),
            stdout,
            stderr: Vec::new(),
        })
    }
}

impl RecordingRunner {
    fn exec_command(&self, request: &ProcessRequest, shell: &str) -> ProcessResult {
        let shell = shell.strip_prefix("exec ").unwrap_or(shell);
        let mut parts = Vec::new();
        for token in shell.split_whitespace() {
            parts.push(token.trim_matches('\''));
        }
        match parts.as_slice() {
            ["codex", "--version"] => ProcessResult {
                status: exit_status(0),
                stdout: b"codex-cli 0.152.1\n".to_vec(),
                stderr: Vec::new(),
            },
            ["codex", "login", "status"] => {
                let authenticated = request.environment.iter().any(|(_, value)| value == SECRET);
                ProcessResult {
                    status: exit_status(0),
                    stdout: if authenticated {
                        b"Logged in using ChatGPT\n".to_vec()
                    } else {
                        b"Not logged in\n".to_vec()
                    },
                    stderr: Vec::new(),
                }
            }
            _ => ProcessResult {
                status: exit_status(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            },
        }
    }
}

fn exit_status(code: i32) -> ExitStatus {
    ExitStatus::from_raw(code << 8)
}

fn command_v_binary(shell: &str) -> Option<&str> {
    let rest = shell.strip_prefix("command -v ")?;
    rest.split(|character: char| character.is_whitespace() || character == '&')
        .next()
        .filter(|binary| !binary.is_empty())
}

fn worker() -> WorkerEntry {
    WorkerEntry {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        slots: 1,
        capabilities: vec!["darwin-arm64".into()],
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    }
}

fn config() -> Config {
    Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker()],
    }
}

fn host_root(root: &Path) -> PathBuf {
    let host_root = root.join("data/mac-worker/host");
    HostStore::open(&host_root).unwrap();
    host_root
}

fn facts(collected_at_millis: u64) -> AgentFacts {
    AgentFacts {
        agents: vec![
            AgentProbe {
                name: "codex".into(),
                version: Some("0.152.1".into()),
                auth: AgentAuth::Authenticated,
                auth_by_profile: vec![("agents".into(), AgentAuth::Authenticated)],
            },
            AgentProbe {
                name: "claude".into(),
                version: Some("2.1.252".into()),
                auth: AgentAuth::Unauthenticated,
                auth_by_profile: vec![("agents".into(), AgentAuth::Authenticated)],
            },
        ],
        env_profiles: vec![
            ProfileProbe {
                name: "agents".into(),
                secure: true,
            },
            ProfileProbe {
                name: "unsafe".into(),
                secure: false,
            },
        ],
        git_identity: true,
        collected_at_millis,
        herdr: None,
    }
}

fn health_with_facts(facts: AgentFacts) -> WorkerHealth {
    WorkerHealth {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        status: HealthStatus::Ready,
        probe: Some(ProbeResponse {
            protocol_version: PROTOCOL_VERSION,
            supervision_version: mac_worker::protocol::SUPERVISION_VERSION,
            hostname: "mini-1.local".into(),
            arch: "arm64".into(),
            os_version: "26.2".into(),
            free_disk_bytes: 500,
            total_disk_bytes: 1_000,
            memory_pressure: MemoryPressure::Normal,
            swap_used_bytes: None,
            available_memory_bytes: None,
            cpu_counters: None,
            slot_state: SlotState::Idle,
            active_lease: None,
            capabilities: vec!["darwin-arm64".into()],
            agent_facts: Some(facts),
            facts_age_millis: Some(0),
        }),
        missing_capabilities: Vec::new(),
        error_code: None,
        error_message: None,
    }
}

#[test]
fn refresh_is_the_only_agent_collection_path_and_persists_no_profile_values() {
    // Break caught: moving collection back into host probe, failing to cache a
    // complete fact record, or serializing an env-profile value.
    let temporary = tempfile::tempdir().unwrap();
    let host_root = host_root(temporary.path());
    let home = temporary.path().join("home");
    let profiles = home.join(".config/mac-worker/env");
    fs::create_dir_all(&profiles).unwrap();
    let profile = profiles.join("agents.env");
    fs::write(&profile, format!("AGENT_TOKEN={SECRET}\n")).unwrap();
    fs::set_permissions(&profile, fs::Permissions::from_mode(0o600)).unwrap();
    let insecure = profiles.join("unsafe.env");
    fs::write(&insecure, format!("AGENT_TOKEN={INSECURE_SECRET}\n")).unwrap();
    fs::set_permissions(&insecure, fs::Permissions::from_mode(0o644)).unwrap();

    let runner = RecordingRunner::default();
    let facts = ProbeCollector::refresh_facts_at(&host_root, &home, &runner).unwrap();
    assert_eq!(facts.agents[0].auth, AgentAuth::Unauthenticated);
    assert_eq!(
        facts.agents[0].auth_by_profile[0].1,
        AgentAuth::Authenticated
    );
    assert!(facts.git_identity);
    assert_eq!(
        facts.env_profiles,
        vec![
            ProfileProbe {
                name: "agents".into(),
                secure: true,
            },
            ProfileProbe {
                name: "unsafe".into(),
                secure: false,
            },
        ]
    );
    assert!(!runner.requests().iter().any(|request| {
        request
            .environment
            .iter()
            .any(|(_, value)| value == INSECURE_SECRET)
    }));

    let request_count_after_refresh = runner.requests().len();
    let cached = ProbeCollector::cached_facts_at(&host_root)
        .unwrap()
        .unwrap();
    assert_eq!(cached, facts);
    assert_eq!(runner.requests().len(), request_count_after_refresh);

    let cache = fs::read_to_string(host_root.join("facts.json")).unwrap();
    assert!(!cache.contains(SECRET));
    assert!(!cache.contains(INSECURE_SECRET));
}

#[test]
fn capability_projection_is_profile_keyed_and_stale_facts_never_satisfy_it() {
    // Break caught: treating a profile authentication as plain, accepting an
    // insecure profile, or continuing to schedule with expired facts.
    let fresh =
        SchedulerProbeAdapter::observations_at(&config(), &[health_with_facts(facts(100))], 101)
            .unwrap();
    assert!(fresh[0].capabilities().contains(&"agent:codex".to_owned()));
    assert!(
        fresh[0]
            .capabilities()
            .contains(&"agent:codex@agents".to_owned())
    );
    assert!(
        fresh[0]
            .capabilities()
            .contains(&"agent:claude@agents".to_owned())
    );
    assert!(!fresh[0].capabilities().contains(&"agent:claude".to_owned()));
    assert!(
        !fresh[0]
            .capabilities()
            .contains(&"agent:codex@unsafe".to_owned())
    );

    let mut stale_health = health_with_facts(facts(0));
    stale_health.probe.as_mut().unwrap().facts_age_millis = Some(FACTS_TTL + 1);
    let stale =
        SchedulerProbeAdapter::observations_at(&config(), &[stale_health], FACTS_TTL + 1).unwrap();
    assert!(
        !stale[0]
            .capabilities()
            .iter()
            .any(|capability| capability.starts_with("agent:"))
    );
}

#[test]
fn capability_projection_uses_reported_fact_age_across_clock_skew() {
    // Break caught: comparing a remote collected-at timestamp to the scheduler
    // clock can make expired facts from a clock-ahead worker look fresh.
    let mut stale_health = health_with_facts(facts(FACTS_TTL + 10_000));
    stale_health.probe.as_mut().unwrap().facts_age_millis = Some(FACTS_TTL + 1);

    let observations =
        SchedulerProbeAdapter::observations_at(&config(), &[stale_health], 0).unwrap();

    assert!(
        !observations[0]
            .capabilities()
            .iter()
            .any(|capability| capability.starts_with("agent:"))
    );
}

#[test]
fn capability_projection_requires_a_reported_fact_age() {
    // Break caught: accepting fact payloads that omit their worker-measured age
    // bypasses the TTL boundary for a malformed current-version probe.
    let mut health = health_with_facts(facts(1));
    health.probe.as_mut().unwrap().facts_age_millis = None;

    let observations = SchedulerProbeAdapter::observations_at(&config(), &[health], 1).unwrap();

    assert!(
        !observations[0]
            .capabilities()
            .iter()
            .any(|capability| capability.starts_with("agent:"))
    );
}

#[test]
fn refresh_command_is_hidden_and_workers_refreshes_before_its_probe() {
    // Break caught: accepting a public host operation, omitting workers
    // refresh, or probing before the refresh operation completes.
    let workers = Cli::try_parse_from(["worker", "workers", "--refresh"]).unwrap();
    assert!(matches!(
        workers.command,
        Command::Workers { refresh: true }
    ));
    let host = Cli::try_parse_from(["worker", "host", "refresh-facts"]).unwrap();
    assert!(matches!(
        host.command,
        Command::Host {
            command: HostCommand::RefreshFacts { timing: false }
        }
    ));
    let timed = Cli::try_parse_from(["worker", "host", "refresh-facts", "--timing"]).unwrap();
    assert!(matches!(
        timed.command,
        Command::Host {
            command: HostCommand::RefreshFacts { timing: true }
        }
    ));

    let temporary = tempfile::tempdir().unwrap();
    let config_path = temporary.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::new(),
        temporary.path().join("home"),
        temporary.path().to_path_buf(),
    );
    let runner = RecordingRunner::default();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = mac_worker::run_with_io_in_context(
        Cli {
            config: Some(config_path),
            json: false,
            command: Command::Workers { refresh: true },
        },
        &runner,
        &runtime,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    let commands = runner
        .requests()
        .into_iter()
        .filter(|request| request.program == OsStr::new("/usr/bin/ssh"))
        .map(|request| request.args.last().unwrap().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        commands,
        vec![
            HostOperation::RefreshFacts.command().to_owned(),
            "~/.local/bin/worker host probe".to_owned(),
        ]
    );
}

#[test]
fn workers_output_reports_facts_and_age_without_profile_values() {
    // Break caught: displaying raw profile contents or silently omitting the
    // operator-visible state needed to diagnose agent eligibility.
    let report = mac_worker::protocol::WorkersReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![health_with_facts(facts(1))],
    };
    let output = CommandOutput::Workers(report).render_human();

    assert!(output.contains("agent facts age"));
    assert!(output.contains("git identity: configured"));
    assert!(output.contains("codex 0.152.1: authenticated"));
    assert!(output.contains("agents: secure"));
    assert!(output.contains("unsafe: insecure"));
    assert!(!output.contains(SECRET));
}

#[test]
fn workers_output_includes_agent_auth_reasons() {
    let mut facts = facts(1);
    facts.agents[0].auth = AgentAuth::UnknownWithReason("keychain locked");
    facts.agents[0].auth_by_profile = vec![(
        "agents".into(),
        AgentAuth::UnknownWithReason("keychain unlock failed"),
    )];
    let report = mac_worker::protocol::WorkersReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![health_with_facts(facts)],
    };

    let output = CommandOutput::Workers(report).render_human();
    assert!(output.contains("codex 0.152.1: unknown (keychain locked)"));
    assert!(output.contains("agents: unknown (keychain unlock failed)"));
}

#[test]
fn refresh_uses_the_runtime_home_without_loading_inventory() {
    // Break caught: the hidden host command reads client inventory or skips
    // the runtime account home while discovering profiles.
    let temporary = tempfile::tempdir().unwrap();
    let home = temporary.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::from([(
            OsString::from("XDG_DATA_HOME"),
            temporary.path().join("data").into_os_string(),
        )]),
        home,
        temporary.path().to_path_buf(),
    );
    let runner = RecordingRunner::default();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = mac_worker::run_with_io_in_context(
        Cli {
            config: Some("/missing/client-inventory.toml".into()),
            json: false,
            command: Command::Host {
                command: HostCommand::RefreshFacts { timing: false },
            },
        },
        &runner,
        &runtime,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    assert!(stdout.is_empty());
    assert!(
        !String::from_utf8_lossy(&stderr).contains("timing "),
        "without --timing stderr stays empty of diagnostics: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert!(
        runner
            .requests()
            .iter()
            .any(|request| request.program == OsStr::new("/bin/zsh"))
    );
}

#[test]
fn refresh_collects_the_herdr_fact_and_cached_reads_never_probe_it() {
    // Break caught: the herdr fact leaving refresh-facts for the probe hot
    // path, or the lookup bypassing the account login shell.
    use mac_worker::agent_facts::{HerdrFactState, HerdrFacts};

    let temporary = tempfile::tempdir().unwrap();
    let host_root = host_root(temporary.path());
    let home = temporary.path().join("home");
    fs::create_dir_all(&home).unwrap();

    let runner = RecordingRunner::default();
    let facts = ProbeCollector::refresh_facts_at(&host_root, &home, &runner).unwrap();
    assert_eq!(
        facts.herdr,
        Some(HerdrFacts {
            state: HerdrFactState::NotInstalled,
            version: None,
            interactive_agents: None,
        })
    );
    let lookup = runner
        .requests()
        .into_iter()
        .find(|request| {
            request
                .args
                .last()
                .and_then(|argument| argument.to_str())
                .is_some_and(|shell| shell.contains("command -v herdr"))
        })
        .expect("refresh-facts looks herdr up on the account login shell");
    assert_eq!(lookup.program, OsStr::new("/bin/zsh"));
    assert!(lookup.isolate_parent_environment);
    assert!(
        lookup
            .environment
            .iter()
            .any(|(name, value)| name == "HOME" && value == home.as_os_str())
    );

    let request_count_after_refresh = runner.requests().len();
    let cached = ProbeCollector::cached_facts_at(&host_root)
        .unwrap()
        .unwrap();
    assert_eq!(cached.herdr, facts.herdr);
    assert_eq!(runner.requests().len(), request_count_after_refresh);
}

#[test]
fn refresh_facts_without_timing_prints_nothing_extra() {
    let (exit, stdout, stderr) = run_host_refresh_facts(false);
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    assert!(stdout.is_empty(), "{}", String::from_utf8_lossy(&stdout));
    assert!(
        stderr.is_empty(),
        "without --timing the command stays silent: {}",
        String::from_utf8_lossy(&stderr)
    );
}

#[test]
fn refresh_facts_timing_prints_named_steps_on_stderr_after_writing_facts() {
    let (exit, stdout, stderr) = run_host_refresh_facts(true);
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    assert!(stdout.is_empty(), "{}", String::from_utf8_lossy(&stdout));
    let text = String::from_utf8(stderr).unwrap();
    let lines = text
        .lines()
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    assert!(
        !lines.is_empty(),
        "timing prints one line per recorded step"
    );
    let names = lines
        .iter()
        .map(|line| {
            line.split_whitespace()
                .filter(|field| {
                    *field == "timing"
                        || field.starts_with("agent=")
                        || field.starts_with("profile=")
                        || field.starts_with("step=")
                        || field.starts_with("result=")
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "timing agent=codex profile=- step=locate+version",
            "timing agent=codex profile=- step=auth result=unauthenticated",
            "timing agent=codex profile=agents step=auth result=authenticated",
            "timing agent=claude profile=- step=locate+version",
            "timing agent=cursor profile=- step=locate+version",
            "timing agent=opencode profile=- step=locate+version",
            "timing agent=herdr profile=- step=locate+version",
            "timing step=total",
        ]
    );
    for line in &lines {
        assert!(line.starts_with("timing "), "{line}");
        assert!(!line.contains('/'), "timing leaked a path: {line}");
        assert!(!line.contains(SECRET), "{line}");
        assert!(!line.contains(INSECURE_SECRET), "{line}");
        assert!(!line.contains("AGENT_TOKEN="), "{line}");
        assert!(!line.contains("command -v"), "{line}");
        assert!(!line.contains("--version"), "{line}");
        assert!(!line.contains("login status"), "{line}");
    }
    assert!(
        names.last() == Some(&"timing step=total".to_owned()),
        "total is last"
    );
}

fn run_host_refresh_facts(timing: bool) -> (u8, Vec<u8>, Vec<u8>) {
    let temporary = tempfile::tempdir().unwrap();
    let home = temporary.path().join("home");
    let profiles = home.join(".config/mac-worker/env");
    fs::create_dir_all(&profiles).unwrap();
    fs::write(
        profiles.join("agents.env"),
        format!("AGENT_TOKEN={SECRET}\n"),
    )
    .unwrap();
    fs::set_permissions(
        profiles.join("agents.env"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::from([(
            OsString::from("XDG_DATA_HOME"),
            temporary.path().join("data").into_os_string(),
        )]),
        home,
        temporary.path().to_path_buf(),
    );
    let runner = RecordingRunner::default();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = mac_worker::run_with_io_in_context(
        Cli {
            config: Some("/missing/client-inventory.toml".into()),
            json: false,
            command: Command::Host {
                command: HostCommand::RefreshFacts { timing },
            },
        },
        &runner,
        &runtime,
        &mut stdout,
        &mut stderr,
    );
    (exit, stdout, stderr)
}
