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

        let stdout = match (program.as_ref(), args.as_slice()) {
            ("zsh", [shell, command]) if shell == "-lc" && command == "command -v codex" => {
                b"/opt/tools/codex\n".to_vec()
            }
            ("zsh", [shell, command]) if shell == "-lc" && command.starts_with("command -v ") => {
                Vec::new()
            }
            (path, [version]) if path.ends_with("/codex") && version == "--version" => {
                b"codex-cli 0.152.1\n".to_vec()
            }
            (path, [login, status])
                if path.ends_with("/codex") && login == "login" && status == "status" =>
            {
                if request.environment.iter().any(|(_, value)| value == SECRET) {
                    b"Logged in using ChatGPT\n".to_vec()
                } else {
                    b"Not logged in\n".to_vec()
                }
            }
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
        let success = !(program == "zsh"
            && args
                .get(1)
                .is_some_and(|command| command.starts_with("command -v "))
            && args.get(1) != Some(&"command -v codex".to_owned()));
        Ok(ProcessResult {
            status: exit_status(if success { 0 } else { 1 }),
            stdout,
            stderr: Vec::new(),
        })
    }
}

fn exit_status(code: i32) -> ExitStatus {
    ExitStatus::from_raw(code << 8)
}

fn worker() -> WorkerEntry {
    WorkerEntry {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        slots: 1,
        capabilities: vec!["darwin-arm64".into()],
        remote_binary: "~/.local/bin/worker".into(),
    }
}

fn config() -> Config {
    Config {
        version: 1,
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

    let stale = SchedulerProbeAdapter::observations_at(
        &config(),
        &[health_with_facts(facts(0))],
        FACTS_TTL + 1,
    )
    .unwrap();
    assert!(
        !stale[0]
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
            command: HostCommand::RefreshFacts
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
                command: HostCommand::RefreshFacts,
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
        runner
            .requests()
            .iter()
            .any(|request| request.program == OsStr::new("zsh"))
    );
}
