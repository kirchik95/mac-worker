#[path = "support/fake_herdr.rs"]
mod fake_herdr;

use std::{
    ffi::OsString,
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use fake_herdr::{FakeHerdr, Reply};
use mac_worker::{
    agent::{AgentKind, adapter_for},
    agent_facts::{
        AgentAuth, AgentFacts, AgentProbe, EnvProfile, FACTS_TTL, HerdrFactState, HerdrFacts,
        PROBE_DEADLINE, ProfileProbe, collect_agent_facts_at,
        collect_agent_facts_at_host_with_timing, collect_agent_facts_at_with_timing,
        turn_auth_failure_reason,
    },
    auth_incidents::{self, AUTH_INCIDENT_TTL_MILLIS, AUTH_INCIDENTS_UNREADABLE_REASON},
    config::{Config, WorkerEntry},
    error::{ProcessError, WorkerError},
    host_store::HostStore,
    lease::SlotState,
    probe::ProbeCollector,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{
        HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        WorkerHealth,
    },
    scheduler_adapter::SchedulerProbeAdapter,
};

const COLLECTED_AT: u64 = 100_000;
const SECRET: &str = "PLANTED_PROFILE_SECRET";
const ACCOUNT_HOME: &str = "/Users/worker";
const HERDR_VERSION: &str = "0.9.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    AllAgents,
    MissingOpenCode,
    LockedClaude,
    ProfileError,
    NetworkCodex,
    AmbiguousOpenCode,
    InvalidUtf8Cursor,
    UnverifiedCursorLogin,
    NoBinariesResolve,
    KeychainUnlockFailure,
    KeychainOrdering,
    StallingCodexAuth,
    StallingLocateVersion,
}

/// What the scripted account login shell knows about a `herdr` binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HerdrBinary {
    Missing,
    Installed,
    VersionFails,
}

struct FakeProcessRunner {
    scenario: Scenario,
    herdr: HerdrBinary,
    requests: Mutex<Vec<ProcessRequest>>,
    events: Mutex<Vec<String>>,
    keychain_unlocked: Mutex<bool>,
}

impl FakeProcessRunner {
    fn new(scenario: Scenario) -> Self {
        Self::with_herdr(scenario, HerdrBinary::Missing)
    }

    fn with_herdr(scenario: Scenario, herdr: HerdrBinary) -> Self {
        Self {
            scenario,
            herdr,
            requests: Mutex::new(Vec::new()),
            events: Mutex::new(Vec::new()),
            keychain_unlocked: Mutex::new(false),
        }
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.requests
            .lock()
            .expect("request mutex poisoned")
            .clone()
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().expect("events mutex poisoned").clone()
    }

    fn record_event(&self, event: &str) {
        self.events
            .lock()
            .expect("events mutex poisoned")
            .push(event.to_owned());
    }

    fn keychain_unlocked(&self) -> bool {
        *self
            .keychain_unlocked
            .lock()
            .expect("keychain mutex poisoned")
    }

    fn set_keychain_unlocked(&self) {
        *self
            .keychain_unlocked
            .lock()
            .expect("keychain mutex poisoned") = true;
    }
}

impl ProcessRunner for FakeProcessRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests
            .lock()
            .expect("request mutex poisoned")
            .push(request.clone());

        let program = request.program.to_string_lossy();
        let args = request
            .args
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        if program == "/usr/bin/security" {
            return match self.scenario {
                Scenario::KeychainUnlockFailure => {
                    assert_eq!(request.environment, Vec::<(OsString, OsString)>::new());
                    assert_eq!(request.stdin, Some(b"profile-password\n".to_vec()));
                    Ok(failure(
                        b"security: profile-password /tmp/profile.keychain-db\n",
                    ))
                }
                Scenario::KeychainOrdering => {
                    self.record_event("keychain_unlock");
                    self.set_keychain_unlocked();
                    Ok(success(b""))
                }
                _ => panic!("unexpected security probe for scenario {:?}", self.scenario),
            };
        }

        if program == "/bin/zsh" && args.first().map(String::as_str) == Some("-lc") {
            assert!(request.isolate_parent_environment);
            let shell = args.last().map(String::as_str).unwrap_or_default();
            if shell == "git config --global --get user.name" {
                return Ok(success(b"Submitter\n"));
            }
            if shell == "git config --global --get user.email" {
                return Ok(success(b"submitter@example.test\n"));
            }
            // The herdr lookups extend PATH with `~/.local/bin` before the
            // command proper, which follows the last `; `.
            let (prefix, shell) = match shell.rsplit_once("; ") {
                Some((prefix, command)) => (Some(prefix), command),
                None => (None, shell),
            };
            if let Some(binary) = command_v_binary(shell) {
                return self.locate_or_merged(request, prefix, binary, shell);
            }
            if shell.starts_with("exec ") {
                return self.exec_command(request, shell);
            }
        }

        panic!("unexpected process request: {request:?}");
    }
}

impl FakeProcessRunner {
    fn locate_or_merged(
        &self,
        request: &ProcessRequest,
        prefix: Option<&str>,
        binary: &str,
        shell: &str,
    ) -> Result<ProcessResult, WorkerError> {
        if self.scenario == Scenario::StallingLocateVersion && binary == "codex" {
            return Err(WorkerError::Process(ProcessError::DeadlineExceeded {
                deadline: request.policy.deadline,
            }));
        }
        let locate = self.locate_binary(prefix, binary);
        if !is_merged_locate_version(shell) || !locate.status.success() {
            return Ok(locate);
        }
        let version = if binary == "herdr" {
            self.herdr_version()
        } else {
            self.exec_command(request, &format!("exec '{binary}' '--version'"))?
        };
        Ok(combine_locate_and_version(locate, version))
    }

    fn locate_binary(&self, prefix: Option<&str>, binary: &str) -> ProcessResult {
        if self.scenario == Scenario::NoBinariesResolve {
            return failure(b"command not found\n");
        }
        if binary == "herdr" {
            assert!(
                prefix.is_some_and(|prefix| prefix.contains("$HOME/.local/bin")),
                "the herdr lookup must search ~/.local/bin: {prefix:?}"
            );
            return match self.herdr {
                HerdrBinary::Missing => failure(b"herdr: command not found\n"),
                HerdrBinary::Installed | HerdrBinary::VersionFails => {
                    success(b"/Users/worker/.local/bin/herdr\n")
                }
            };
        }
        if self.scenario == Scenario::KeychainOrdering && binary != "claude" {
            return failure(b"command not found\n");
        }
        if binary == "opencode" && self.scenario == Scenario::MissingOpenCode {
            return failure(b"opencode: command not found\n");
        }
        success(format!("/opt/tools/{binary}\n").as_bytes())
    }

    fn herdr_version(&self) -> ProcessResult {
        match self.herdr {
            HerdrBinary::Installed => success(format!("herdr {HERDR_VERSION}\n").as_bytes()),
            HerdrBinary::VersionFails => failure(b"herdr: unrecognized option\n"),
            HerdrBinary::Missing => panic!("herdr --version ran without a herdr binary"),
        }
    }

    fn exec_command(
        &self,
        request: &ProcessRequest,
        shell: &str,
    ) -> Result<ProcessResult, WorkerError> {
        let shell = shell.strip_prefix("exec ").unwrap_or(shell);
        let mut parts = Vec::new();
        for token in shell.split_whitespace() {
            parts.push(token.trim_matches('\''));
        }
        match parts.as_slice() {
            ["codex", "--version"] => Ok(success(b"codex-cli 0.152.1\n")),
            ["codex", "login", "status"] => {
                if self.scenario == Scenario::StallingCodexAuth {
                    return Err(WorkerError::Process(ProcessError::DeadlineExceeded {
                        deadline: request.policy.deadline,
                    }));
                }
                if self.scenario == Scenario::NetworkCodex {
                    return Ok(success(b"Logged in\nnetwork error\n"));
                }
                Ok(success(b"Logged in using ChatGPT\n"))
            }
            ["claude", "--version"] => Ok(success(b"2.1.252 (Claude Code)\n")),
            ["claude", "auth", "status"] => {
                if self.scenario == Scenario::KeychainOrdering {
                    if has_environment(request, "CLAUDE_CODE_OAUTH_TOKEN") {
                        self.record_event("claude_profile_auth");
                        return Ok(success(br#"{"loggedIn":true}"#));
                    }
                    self.record_event("claude_base_auth");
                    if !self.keychain_unlocked() {
                        return Ok(success(b"Error: Your macOS login keychain is locked.\n"));
                    }
                    return Ok(success(br#"{"loggedIn":false}"#));
                }
                if self.scenario == Scenario::LockedClaude {
                    return Ok(success(b"Error: Your macOS login keychain is locked.\n"));
                }
                if self.scenario == Scenario::ProfileError
                    && request.environment.iter().any(|(_, value)| value == SECRET)
                {
                    return Err(WorkerError::Protocol(format!("provider rejected {SECRET}")));
                }
                if has_environment(request, "CLAUDE_CODE_OAUTH_TOKEN") {
                    Ok(success(br#"{"loggedIn":true}"#))
                } else {
                    Ok(success(br#"{"loggedIn":false}"#))
                }
            }
            ["cursor-agent", "--version"] => Ok(success(b"cursor-agent 1.3.0\n")),
            ["cursor-agent", "status"] => {
                if self.scenario == Scenario::InvalidUtf8Cursor {
                    return Ok(success(&[0xff, b'\n']));
                }
                if self.scenario == Scenario::UnverifiedCursorLogin {
                    return Ok(success(
                        "\u{1b}[32m\u{2713}\u{1b}[0m Login successful!\nLogged in (unable to fetch user details)\n"
                            .as_bytes(),
                    ));
                }
                if has_environment(request, "CURSOR_API_KEY") {
                    Ok(success(b"Authenticated as test-user\n"))
                } else {
                    Ok(success(b"Not authenticated\n"))
                }
            }
            ["opencode", "--version"] => Ok(success(b"opencode 1.0.0\n")),
            ["opencode", "auth", "list"] => {
                if self.scenario == Scenario::AmbiguousOpenCode {
                    return Ok(success(b"unexpected output\n"));
                }
                if has_environment(request, "OPENAI_API_KEY") {
                    Ok(success(br#"[{"provider":"openai"}]"#))
                } else {
                    Ok(success(b"[]"))
                }
            }
            _ => panic!("unexpected exec command: {shell:?}"),
        }
    }
}

fn account_home() -> &'static Path {
    Path::new(ACCOUNT_HOME)
}

fn success(stdout: &[u8]) -> ProcessResult {
    ProcessResult {
        status: 0_i32.into_exit_status(),
        stdout: stdout.to_vec(),
        stderr: Vec::new(),
    }
}

fn failure(stderr: &[u8]) -> ProcessResult {
    ProcessResult {
        status: 1_i32.into_exit_status(),
        stdout: Vec::new(),
        stderr: stderr.to_vec(),
    }
}

trait ExitStatusCode {
    fn into_exit_status(self) -> std::process::ExitStatus;
}

impl ExitStatusCode for i32 {
    fn into_exit_status(self) -> std::process::ExitStatus {
        ExitStatusExt::from_raw(self << 8)
    }
}

fn has_environment(request: &ProcessRequest, name: &str) -> bool {
    request
        .environment
        .iter()
        .any(|(key, _)| key == &OsString::from(name))
}

fn command_v_binary(shell: &str) -> Option<&str> {
    let rest = shell.strip_prefix("command -v ")?;
    rest.split(|character: char| character.is_whitespace() || character == '&')
        .next()
        .filter(|binary| !binary.is_empty())
}

fn is_merged_locate_version(shell: &str) -> bool {
    shell.contains("MAC_WORKER_FACTS_VERSION")
}

fn combine_locate_and_version(locate: ProcessResult, version: ProcessResult) -> ProcessResult {
    let mut stdout = locate.stdout;
    if !stdout.ends_with(b"\n") {
        stdout.push(b'\n');
    }
    stdout.extend_from_slice(b"MAC_WORKER_FACTS_VERSION\n");
    stdout.extend_from_slice(&version.stdout);
    ProcessResult {
        status: version.status,
        stdout,
        stderr: version.stderr,
    }
}

fn account_login_shells(runner: &FakeProcessRunner) -> Vec<ProcessRequest> {
    runner
        .requests()
        .into_iter()
        .filter(|request| request.program == "/bin/zsh")
        .collect()
}

fn profiles() -> Vec<EnvProfile> {
    vec![
        EnvProfile {
            name: "agents".into(),
            secure: true,
            entries: vec![
                ("CLAUDE_CODE_OAUTH_TOKEN".into(), SECRET.into()),
                ("CURSOR_API_KEY".into(), "cursor-profile-key".into()),
                ("OPENAI_API_KEY".into(), "openai-profile-key".into()),
            ],
        },
        EnvProfile {
            name: "unsafe".into(),
            secure: false,
            entries: vec![("CLAUDE_CODE_OAUTH_TOKEN".into(), "unsafe-value".into())],
        },
    ]
}

fn keychain_profiles() -> Vec<EnvProfile> {
    let mut profiles = profiles();
    profiles.push(EnvProfile {
        name: "keychain".into(),
        secure: true,
        entries: vec![
            ("CURSOR_API_KEY".into(), "cursor-profile-key".into()),
            (
                "MAC_WORKER_KEYCHAIN_PASSWORD".into(),
                "profile-password".into(),
            ),
            (
                "MAC_WORKER_KEYCHAIN_PATH".into(),
                "/tmp/profile.keychain-db".into(),
            ),
        ],
    });
    profiles
}

#[test]
fn collects_all_adapters_with_profile_keyed_auth_and_git_identity() {
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);

    assert_eq!(facts.collected_at_millis(), COLLECTED_AT);
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
    assert_eq!(
        facts.agents,
        vec![
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
            AgentProbe {
                name: "cursor".into(),
                version: Some("1.3.0".into()),
                auth: AgentAuth::Unauthenticated,
                auth_by_profile: vec![("agents".into(), AgentAuth::Authenticated)],
            },
            AgentProbe {
                name: "opencode".into(),
                version: Some("1.0.0".into()),
                auth: AgentAuth::Unauthenticated,
                auth_by_profile: vec![("agents".into(), AgentAuth::Authenticated)],
            },
        ]
    );
}

#[test]
fn checks_use_two_second_four_kib_bounds_and_never_apply_insecure_profiles() {
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    let _ = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    let requests = runner.requests();

    assert!(requests.iter().all(|request| {
        request.policy
            == ProcessPolicy {
                stdout_limit: 4 * 1024,
                stderr_limit: 4 * 1024,
                deadline: Duration::from_secs(2),
            }
    }));

    let claude_auth = requests
        .iter()
        .filter(|request| {
            request.program.to_string_lossy() == "/bin/zsh"
                && request
                    .args
                    .last()
                    .and_then(|arg| arg.to_str())
                    .is_some_and(|shell| shell.contains("'claude' 'auth' 'status'"))
                && !request
                    .environment
                    .iter()
                    .any(|(key, _)| key == "CLAUDE_CODE_OAUTH_TOKEN")
        })
        .collect::<Vec<_>>();
    assert_eq!(claude_auth.len(), 1);
    let claude_profile_auth = requests
        .iter()
        .filter(|request| {
            request.program.to_string_lossy() == "/bin/zsh"
                && request
                    .args
                    .last()
                    .and_then(|arg| arg.to_str())
                    .is_some_and(|shell| shell.contains("'claude' 'auth' 'status'"))
                && request
                    .environment
                    .iter()
                    .any(|(key, value)| key == "CLAUDE_CODE_OAUTH_TOKEN" && value == SECRET)
        })
        .collect::<Vec<_>>();
    assert_eq!(claude_profile_auth.len(), 1);
    assert!(!requests.iter().any(|request| {
        request
            .environment
            .iter()
            .any(|(_, value)| value == "unsafe-value")
    }));
}

#[test]
fn missing_binaries_are_omitted_and_auth_errors_are_unknown_without_leaking_values() {
    let runner = FakeProcessRunner::new(Scenario::MissingOpenCode);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    assert!(!facts.agents.iter().any(|agent| agent.name == "opencode"));

    let runner = FakeProcessRunner::new(Scenario::LockedClaude);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    let claude = facts
        .agents
        .iter()
        .find(|agent| agent.name == "claude")
        .unwrap();
    assert_eq!(claude.auth, AgentAuth::Unknown);

    let runner = FakeProcessRunner::new(Scenario::ProfileError);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    let bytes = facts.canonical_bytes().unwrap();
    assert!(!String::from_utf8(bytes).unwrap().contains(SECRET));
    assert!(!format!("{:?}", profiles()[0]).contains(SECRET));
}

#[test]
fn ambiguous_network_and_non_utf8_auth_outputs_are_unknown() {
    let runner = FakeProcessRunner::new(Scenario::NetworkCodex);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    let codex = facts
        .agents
        .iter()
        .find(|agent| agent.name == "codex")
        .unwrap();
    assert_eq!(codex.auth, AgentAuth::Unknown);
    assert_eq!(codex.auth_by_profile[0].1, AgentAuth::Unknown);

    let runner = FakeProcessRunner::new(Scenario::AmbiguousOpenCode);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    let opencode = facts
        .agents
        .iter()
        .find(|agent| agent.name == "opencode")
        .unwrap();
    assert_eq!(opencode.auth, AgentAuth::Unknown);
    assert_eq!(opencode.auth_by_profile[0].1, AgentAuth::Unknown);

    let runner = FakeProcessRunner::new(Scenario::InvalidUtf8Cursor);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    let cursor = facts
        .agents
        .iter()
        .find(|agent| agent.name == "cursor")
        .unwrap();
    assert_eq!(cursor.auth, AgentAuth::Unknown);
    assert_eq!(cursor.auth_by_profile[0].1, AgentAuth::Unknown);
}

#[test]
fn unverified_cursor_login_reaches_facts_and_is_not_an_agent_capability() {
    let runner = FakeProcessRunner::new(Scenario::UnverifiedCursorLogin);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    let cursor = facts
        .agents
        .iter()
        .find(|agent| agent.name == "cursor")
        .expect("cursor probe must be present");
    assert_eq!(
        cursor.auth,
        AgentAuth::UnknownWithReason("login unverified: user details unavailable")
    );
    assert_eq!(
        cursor.auth_by_profile,
        vec![(
            "agents".into(),
            AgentAuth::UnknownWithReason("login unverified: user details unavailable")
        )]
    );
    let bytes = facts.canonical_bytes().unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert!(
        text.contains(r#""state":"unknown","reason":"login unverified: user details unavailable""#)
    );
    let parsed: AgentFacts = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed.canonical_bytes().unwrap(), bytes);

    let observations = SchedulerProbeAdapter::observations(
        &Config {
            version: 1,
            notifications: mac_worker::config::NotificationsConfig::default(),
            workers: vec![WorkerEntry {
                name: "mini-1".into(),
                ssh: "mac1".into(),
                slots: 1,
                capabilities: vec!["darwin-arm64".into()],
                remote_binary: "~/.local/bin/worker".into(),
                herdr: false,
            }],
        },
        &[WorkerHealth {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            status: HealthStatus::Ready,
            probe: Some(ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: SUPERVISION_VERSION,
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
                configured_slots: 0,
                busy_slots: 0,
            }),
            missing_capabilities: Vec::new(),
            error_code: None,
            error_message: None,
        }],
    )
    .unwrap();
    let capabilities = observations[0].capabilities();
    assert!(
        !capabilities
            .iter()
            .any(|capability| capability == "agent:cursor")
    );
    assert!(
        !capabilities
            .iter()
            .any(|capability| capability == "agent:cursor@agents")
    );
}

fn keychain_unlock_ordering_profiles() -> Vec<EnvProfile> {
    vec![EnvProfile {
        name: "keychain".into(),
        secure: true,
        entries: vec![
            ("CLAUDE_CODE_OAUTH_TOKEN".into(), SECRET.into()),
            (
                "MAC_WORKER_KEYCHAIN_PASSWORD".into(),
                "profile-password".into(),
            ),
            (
                "MAC_WORKER_KEYCHAIN_PATH".into(),
                "/tmp/profile.keychain-db".into(),
            ),
        ],
    }]
}

#[test]
fn base_auth_probe_runs_before_keychain_unlock_and_profile_auth() {
    let runner = FakeProcessRunner::new(Scenario::KeychainOrdering);
    let facts = collect_agent_facts_at(
        &runner,
        account_home(),
        &keychain_unlock_ordering_profiles(),
        COLLECTED_AT,
    );
    let events = runner.events();
    let base = events
        .iter()
        .position(|event| event == "claude_base_auth")
        .expect("base claude auth probe must be observed");
    let profile = events
        .iter()
        .position(|event| event == "claude_profile_auth")
        .expect("profile claude auth probe must be observed");
    let unlock = events[..profile]
        .iter()
        .rposition(|event| event == "keychain_unlock")
        .expect("keychain unlock must be observed before profile auth");
    assert!(
        base < unlock,
        "base auth must precede the profile keychain unlock, saw {events:?}"
    );
    assert!(
        unlock < profile,
        "profile auth must follow keychain unlock, saw {events:?}"
    );
    let claude = facts
        .agents
        .iter()
        .find(|agent| agent.name == "claude")
        .expect("claude probe must be present");
    assert_eq!(
        claude.auth,
        AgentAuth::Unknown,
        "base auth must not observe an earlier keychain unlock"
    );
    assert_eq!(claude.auth_by_profile[0].1, AgentAuth::Authenticated);
}

#[test]
fn keychain_unlock_failures_are_unknown_with_a_safe_reason() {
    let runner = FakeProcessRunner::new(Scenario::KeychainUnlockFailure);
    let facts = collect_agent_facts_at(&runner, account_home(), &keychain_profiles(), COLLECTED_AT);

    for agent in &facts.agents {
        assert_eq!(
            agent.auth_by_profile.last().map(|(_, auth)| *auth),
            Some(AgentAuth::UnknownWithReason("keychain unlock failed"))
        );
    }
    let bytes = facts.canonical_bytes().unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains(r#""state":"unknown","reason":"keychain unlock failed""#));
    assert!(!text.contains("profile-password"));
    assert!(!text.contains("/tmp/profile.keychain-db"));
    let parsed: AgentFacts = serde_json::from_str(&text).unwrap();
    assert_eq!(
        parsed.canonical_bytes().unwrap(),
        facts.canonical_bytes().unwrap()
    );
}

#[test]
fn facts_are_stale_only_after_the_ttl_and_age_subtraction_is_saturating() {
    let facts = AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: false,
        collected_at_millis: COLLECTED_AT,
        herdr: None,
    };
    assert!(!facts.is_stale(COLLECTED_AT.saturating_sub(1)));
    assert!(!facts.is_stale(COLLECTED_AT + FACTS_TTL));
    assert!(facts.is_stale(COLLECTED_AT + FACTS_TTL + 1));
}

#[test]
fn dto_json_is_canonical_and_rejects_unknown_or_duplicate_fields() {
    let facts = AgentFacts {
        agents: vec![AgentProbe {
            name: "codex".into(),
            version: Some("0.152.1".into()),
            auth: AgentAuth::Authenticated,
            auth_by_profile: Vec::new(),
        }],
        env_profiles: vec![ProfileProbe {
            name: "agents".into(),
            secure: true,
        }],
        git_identity: true,
        collected_at_millis: COLLECTED_AT,
        herdr: None,
    };
    let bytes = facts.canonical_bytes().unwrap();
    let parsed: AgentFacts = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed.canonical_bytes().unwrap(), bytes);
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        r#"{"agents":[{"name":"codex","version":"0.152.1","auth":"authenticated","auth_by_profile":[]}],"env_profiles":[{"name":"agents","secure":true}],"git_identity":true,"collected_at_millis":100000}"#
    );

    let unknown = r#"{"agents":[],"env_profiles":[],"git_identity":false,"collected_at_millis":1,"unexpected":true}"#;
    assert!(serde_json::from_str::<AgentFacts>(unknown).is_err());
    let duplicate = r#"{"agents":[],"agents":[],"env_profiles":[],"git_identity":false,"collected_at_millis":1}"#;
    assert!(serde_json::from_str::<AgentFacts>(duplicate).is_err());
    let nested_unknown = r#"{"agents":[{"name":"codex","version":null,"auth":"unknown","auth_by_profile":[],"extra":1}],"env_profiles":[],"git_identity":false,"collected_at_millis":1}"#;
    assert!(serde_json::from_str::<AgentFacts>(nested_unknown).is_err());
    let nested_duplicate = r#"{"agents":[{"name":"codex","name":"claude","version":null,"auth":"unknown","auth_by_profile":[]}],"env_profiles":[],"git_identity":false,"collected_at_millis":1}"#;
    assert!(serde_json::from_str::<AgentFacts>(nested_duplicate).is_err());
}

#[test]
fn profile_probe_json_rejects_unknown_fields() {
    let profile = ProfileProbe {
        name: "agents".into(),
        secure: true,
    };
    let value = serde_json::to_value(profile).unwrap();
    assert_eq!(value, serde_json::json!({"name":"agents","secure":true}));
    assert!(
        serde_json::from_str::<ProfileProbe>(r#"{"name":"agents","secure":true,"extra":false}"#)
            .is_err()
    );
}

#[test]
fn profile_values_are_available_to_the_runner_but_not_to_dto_debug_output() {
    let profile = EnvProfile {
        name: "agents".into(),
        secure: true,
        entries: vec![("TOKEN".into(), SECRET.into())],
    };
    let debug = format!("{profile:?}");
    assert!(debug.contains("agents"));
    assert!(!debug.contains(SECRET));

    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    let _ = collect_agent_facts_at(&runner, account_home(), &[profile], COLLECTED_AT);
    assert!(
        runner
            .requests()
            .iter()
            .any(|request| { request.environment.iter().any(|(_, value)| value == SECRET) })
    );
}

#[test]
fn account_login_shell_requests_use_isolation_and_profile_entries() {
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    assert_eq!(facts.agents.len(), 4);

    let requests = runner.requests();
    let launches = requests
        .iter()
        .filter(|request| request.program.to_string_lossy() == "/bin/zsh")
        .collect::<Vec<_>>();
    assert!(!launches.is_empty());
    for request in &launches {
        assert!(request.isolate_parent_environment);
        assert_eq!(
            request.args.first().map(OsString::as_os_str),
            Some(OsString::from("-lc").as_os_str())
        );
        assert!(
            request
                .environment
                .iter()
                .any(|(name, value)| name == "HOME" && value == ACCOUNT_HOME)
        );
    }

    let path_profile = EnvProfile {
        name: "path-override".into(),
        secure: true,
        entries: vec![(OsString::from("PATH"), OsString::from("/profile/bin"))],
    };
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    collect_agent_facts_at(&runner, account_home(), &[path_profile], COLLECTED_AT);
    let overridden = runner
        .requests()
        .into_iter()
        .filter(|request| {
            request
                .args
                .last()
                .and_then(|arg| arg.to_str())
                .is_some_and(|shell| shell.contains("'codex' 'login' 'status'"))
                && request
                    .environment
                    .iter()
                    .any(|(key, value)| key == "PATH" && value == "/profile/bin")
        })
        .collect::<Vec<_>>();
    assert_eq!(overridden.len(), 1);
}

#[test]
fn five_agents_and_one_profile_use_fifteen_login_shells() {
    // Four adapters plus herdr, one secure profile that does not set PATH,
    // ZDOTDIR, HOME, or SHELL. Adapter probes: locate+version, auth, and
    // profile auth (12) plus herdr locate+version (1) = 13. Git identity
    // adds two account-login `git config --global --get` probes. Reused
    // profile resolution emits no locate/version timing line.
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    let _ = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    let shells = account_login_shells(&runner);
    assert_eq!(
        shells.len(),
        15,
        "account login shells: {:?}",
        shells.iter().map(shell_command).collect::<Vec<_>>()
    );
    let locate_version = shells
        .iter()
        .filter(|request| is_merged_locate_version(shell_command(request)))
        .count();
    assert_eq!(
        locate_version, 5,
        "one locate+version per adapter plus herdr, none for the reused profile"
    );
}

#[test]
fn profiles_that_set_path_zdotdir_home_or_shell_re_resolve_binaries() {
    for name in ["PATH", "ZDOTDIR", "HOME", "SHELL"] {
        let profile = EnvProfile {
            name: "override".into(),
            secure: true,
            entries: vec![(OsString::from(name), OsString::from("/profile/bin"))],
        };
        let runner = FakeProcessRunner::new(Scenario::AllAgents);
        collect_agent_facts_at(&runner, account_home(), &[profile], COLLECTED_AT);
        let re_resolved = account_login_shells(&runner)
            .into_iter()
            .filter(|request| {
                is_merged_locate_version(shell_command(request))
                    && request
                        .environment
                        .iter()
                        .any(|(key, value)| key == name && value == "/profile/bin")
            })
            .count();
        assert_eq!(
            re_resolved, 4,
            "{name} must re-run locate+version for each adapter"
        );
    }
}

#[test]
fn a_locate_version_deadline_is_unavailable_with_no_version() {
    let runner = FakeProcessRunner::new(Scenario::StallingLocateVersion);
    let (facts, timing) =
        collect_agent_facts_at_with_timing(&runner, account_home(), &profiles(), COLLECTED_AT);
    assert!(
        !facts.agents.iter().any(|agent| agent.name == "codex"),
        "a locate+version timeout is availability false, so the agent is omitted"
    );
    let stalled = timing
        .lines()
        .into_iter()
        .filter(|line| line.contains("agent=codex") && line.contains("step=locate+version"))
        .collect::<Vec<_>>();
    assert_eq!(
        stalled.len(),
        1,
        "the profile reuses the failed default resolution: {stalled:?}"
    );
    assert!(stalled[0].contains("hit_deadline=true"), "{}", stalled[0]);
    assert!(
        stalled[0].contains(&format!("deadline_ms={}", PROBE_DEADLINE.as_millis())),
        "{}",
        stalled[0]
    );
    assert_timing_privacy(&timing.lines());
}

#[test]
fn login_shell_command_resolution_failure_omits_all_agents() {
    let runner = FakeProcessRunner::new(Scenario::NoBinariesResolve);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);
    assert!(
        facts.agents.is_empty(),
        "when login-shell command -v fails for every adapter, no agent probes should be emitted"
    );
}

#[test]
fn herdr_facts_round_trip_and_stay_absent_for_records_that_predate_them() {
    let without = AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: false,
        collected_at_millis: COLLECTED_AT,
        herdr: None,
    };
    let bytes = without.canonical_bytes().unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("herdr"));
    let back: AgentFacts = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(back, without);

    for (state, tag) in [
        (HerdrFactState::Available, "available"),
        (HerdrFactState::NotInstalled, "not_installed"),
        (HerdrFactState::NoSocket, "no_socket"),
        (HerdrFactState::NoResponse, "no_response"),
    ] {
        let facts = AgentFacts {
            herdr: Some(HerdrFacts {
                state,
                version: Some("0.9.0".into()),
                interactive_agents: None,
            }),
            ..without.clone()
        };
        let json = String::from_utf8(facts.canonical_bytes().unwrap()).unwrap();
        assert!(
            json.contains(&format!(r#""herdr":{{"state":"{tag}","version":"0.9.0"}}"#)),
            "{json}"
        );
        let back: AgentFacts = serde_json::from_str(&json).unwrap();
        assert_eq!(back, facts);
    }

    let bad = AgentFacts {
        herdr: Some(HerdrFacts {
            state: HerdrFactState::Available,
            version: Some("0.9\u{7}".into()),
            interactive_agents: None,
        }),
        ..without
    };
    assert!(
        bad.canonical_bytes().is_err(),
        "control characters never reach the record"
    );
}

fn shell_command(request: &ProcessRequest) -> &str {
    request
        .args
        .last()
        .and_then(|argument| argument.to_str())
        .unwrap_or_default()
}

fn requests_containing(runner: &FakeProcessRunner, needle: &str) -> Vec<ProcessRequest> {
    runner
        .requests()
        .into_iter()
        .filter(|request| shell_command(request).contains(needle))
        .collect()
}

fn herdr_fact(state: HerdrFactState, version: Option<&str>) -> Option<HerdrFacts> {
    Some(HerdrFacts {
        state,
        version: version.map(str::to_owned),
        interactive_agents: None,
    })
}

#[test]
fn herdr_fact_is_not_installed_when_no_binary_resolves_on_the_login_shell() {
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    let facts = collect_agent_facts_at(&runner, account_home(), &profiles(), COLLECTED_AT);

    assert_eq!(facts.herdr, herdr_fact(HerdrFactState::NotInstalled, None));
    let lookups = requests_containing(&runner, "command -v herdr");
    assert_eq!(lookups.len(), 1, "one herdr lookup through the login shell");
    assert_eq!(lookups[0].program, OsString::from("/bin/zsh"));
    assert!(lookups[0].isolate_parent_environment);
    let shell = shell_command(&lookups[0]);
    assert!(
        shell.contains("$HOME/.local/bin"),
        "the lookup must reach herdr's install directory: {shell:?}"
    );
    assert!(
        shell.contains("'herdr' '--version'"),
        "locate and version share one login shell: {shell:?}"
    );
}

#[test]
fn herdr_fact_is_no_socket_when_the_binary_answers_without_a_socket() {
    let home = tempfile::tempdir().unwrap();
    let runner = FakeProcessRunner::with_herdr(Scenario::AllAgents, HerdrBinary::Installed);
    let facts = collect_agent_facts_at(&runner, home.path(), &profiles(), COLLECTED_AT);

    assert_eq!(
        facts.herdr,
        herdr_fact(HerdrFactState::NoSocket, Some(HERDR_VERSION))
    );
    let versions = requests_containing(&runner, "'herdr' '--version'");
    assert_eq!(versions.len(), 1, "one version probe for one binary");
    let version = &versions[0];
    assert!(
        shell_command(version).contains("command -v herdr"),
        "herdr locate and version share one login shell: {:?}",
        shell_command(version)
    );
    assert_eq!(version.program, OsString::from("/bin/zsh"));
    assert!(version.isolate_parent_environment);
    assert!(
        version
            .environment
            .iter()
            .any(|(name, value)| name == "HOME" && value == home.path().as_os_str())
    );
    assert_eq!(
        version.policy,
        ProcessPolicy {
            stdout_limit: 4 * 1024,
            stderr_limit: 4 * 1024,
            deadline: Duration::from_secs(2),
        }
    );
}

#[test]
fn herdr_fact_keeps_the_socket_state_when_the_version_probe_fails() {
    let home = tempfile::tempdir().unwrap();
    let runner = FakeProcessRunner::with_herdr(Scenario::AllAgents, HerdrBinary::VersionFails);
    let facts = collect_agent_facts_at(&runner, home.path(), &profiles(), COLLECTED_AT);

    assert_eq!(facts.herdr, herdr_fact(HerdrFactState::NoSocket, None));
}

#[test]
fn herdr_fact_is_available_when_the_socket_answers_ping_and_no_path_crosses_it() {
    let home = tempfile::tempdir().unwrap();
    let herdr = FakeHerdr::start_in_home(home.path());
    let runner = FakeProcessRunner::with_herdr(Scenario::AllAgents, HerdrBinary::Installed);
    let facts = collect_agent_facts_at(&runner, home.path(), &profiles(), COLLECTED_AT);

    assert_eq!(
        facts.herdr,
        herdr_fact(HerdrFactState::Available, Some(HERDR_VERSION))
    );
    assert_eq!(herdr.requests_for("ping").len(), 1);
    assert_eq!(herdr.requests_for("agent.list").len(), 1);
    assert_eq!(
        herdr.requests().len(),
        2,
        "available facts cost ping then a best-effort agent.list"
    );
    let wire = serde_json::to_string(&herdr.requests()).unwrap();
    let home_text = home.path().to_string_lossy().into_owned();
    for forbidden in [home_text.as_str(), "/Users/", "/home/", "~"] {
        assert!(
            !wire.contains(forbidden),
            "{forbidden:?} crossed the herdr socket: {wire}"
        );
    }
}

#[test]
fn herdr_fact_is_no_response_when_the_socket_stays_silent() {
    let home = tempfile::tempdir().unwrap();
    let herdr = FakeHerdr::start_in_home(home.path());
    herdr.reply("ping", Reply::Silence);
    let runner = FakeProcessRunner::with_herdr(Scenario::AllAgents, HerdrBinary::Installed);

    let started = Instant::now();
    let facts = collect_agent_facts_at(&runner, home.path(), &profiles(), COLLECTED_AT);

    assert_eq!(
        facts.herdr,
        herdr_fact(HerdrFactState::NoResponse, Some(HERDR_VERSION))
    );
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "the ping honours the client deadlines"
    );
}

#[test]
fn herdr_fact_counts_interactive_agents_and_stores_nothing_but_the_count() {
    let home = tempfile::tempdir().unwrap();
    let herdr = FakeHerdr::start_in_home(home.path());
    herdr.reply(
        "agent.list",
        Reply::Result(serde_json::json!({
            "type": "agent_list",
            "agents": [
                {
                    "pane_id": "w1:p1",
                    "agent_status": "working",
                    "display_agent": "codex",
                    "title": "secret review",
                    "cwd": "/Users/operator/secret"
                },
                {
                    "pane_id": "w1:p2",
                    "agent_status": "working",
                    "display_agent": "mac-worker",
                    "title": "task abc123def456 · leak"
                },
                {
                    "pane_id": "w1:p3",
                    "agent_status": "idle",
                    "display_agent": "claude"
                }
            ]
        })),
    );
    let runner = FakeProcessRunner::with_herdr(Scenario::AllAgents, HerdrBinary::Installed);
    let (facts, timing) =
        collect_agent_facts_at_with_timing(&runner, home.path(), &profiles(), COLLECTED_AT);

    let herdr_facts = facts.herdr.as_ref().expect("herdr fact");
    assert_eq!(herdr_facts.state, HerdrFactState::Available);
    assert_eq!(herdr_facts.interactive_agents, Some(2));
    let json = String::from_utf8(facts.canonical_bytes().unwrap()).unwrap();
    assert!(json.contains(r#""interactive_agents":2"#), "{json}");
    for forbidden in [
        "w1:p1",
        "w1:p2",
        "w1:p3",
        "secret review",
        "task abc123def456",
        "/Users/operator/secret",
        "cwd",
        "pane_id",
        "title",
    ] {
        assert!(
            !json.contains(forbidden),
            "{forbidden:?} leaked into facts: {json}"
        );
    }
    let names = timing_names(&timing.lines());
    assert!(
        names.iter().any(|name| name == "timing step=herdr-agents"),
        "available herdr records herdr-agents: {names:?}"
    );
    assert_timing_privacy(&timing.lines());
}

#[test]
fn herdr_fact_keeps_available_when_agent_list_fails() {
    let home = tempfile::tempdir().unwrap();
    let herdr = FakeHerdr::start_in_home(home.path());
    herdr.reply("agent.list", Reply::Silence);
    let runner = FakeProcessRunner::with_herdr(Scenario::AllAgents, HerdrBinary::Installed);

    let started = Instant::now();
    let facts = collect_agent_facts_at(&runner, home.path(), &profiles(), COLLECTED_AT);

    assert_eq!(
        facts.herdr,
        herdr_fact(HerdrFactState::Available, Some(HERDR_VERSION))
    );
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "a silent agent.list honours the client deadlines"
    );
}

#[test]
fn herdr_facts_omit_interactive_agents_from_records_that_predate_the_count() {
    let json = r#"{"agents":[],"env_profiles":[],"git_identity":false,"collected_at_millis":1,"herdr":{"state":"available","version":"0.9.0"}}"#;
    let facts: AgentFacts = serde_json::from_str(json).unwrap();
    assert_eq!(facts.herdr.unwrap().interactive_agents, None);
}

#[test]
fn timing_lines_name_each_step_in_collection_order_without_leaking_probe_details() {
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    let (_, timing) =
        collect_agent_facts_at_with_timing(&runner, account_home(), &profiles(), COLLECTED_AT);
    let lines = timing.lines();
    assert_eq!(timing_names(&lines), expected_all_agents_timing_names());
    assert!(
        lines
            .last()
            .is_some_and(|line| line.starts_with("timing step=total ms=")),
        "total is last: {lines:?}"
    );
    for line in &lines {
        if line.contains("step=total") {
            assert!(
                !line.contains("deadline_ms="),
                "total has no probe deadline: {line}"
            );
        } else {
            assert!(
                line.contains("deadline_ms="),
                "probe steps record their deadline: {line}"
            );
        }
        assert!(!line.contains("hit_deadline="), "{line}");
    }
    assert_timing_privacy(&lines);
}

#[test]
fn a_probe_that_stalls_to_its_deadline_is_marked_on_the_auth_step() {
    let runner = FakeProcessRunner::new(Scenario::StallingCodexAuth);
    let (facts, timing) =
        collect_agent_facts_at_with_timing(&runner, account_home(), &profiles(), COLLECTED_AT);
    let lines = timing.lines();
    let stalled = lines
        .iter()
        .filter(|line| line.contains("agent=codex") && line.contains("step=auth"))
        .collect::<Vec<_>>();
    assert_eq!(
        stalled.len(),
        2,
        "base and profile auth both ran: {lines:?}"
    );
    for line in stalled {
        assert!(
            line.contains("result=unknown"),
            "deadline auth is unknown: {line}"
        );
        assert!(
            line.contains(&format!("deadline_ms={}", PROBE_DEADLINE.as_millis())),
            "{line}"
        );
        assert!(line.contains("hit_deadline=true"), "{line}");
    }
    assert_eq!(facts.agents[0].name, "codex");
    assert_eq!(facts.agents[0].auth, AgentAuth::Unknown);
    assert_eq!(facts.agents[0].auth_by_profile[0].1, AgentAuth::Unknown);
    assert_timing_privacy(&lines);
}

#[test]
fn timing_lines_never_contain_paths_profile_values_or_command_text() {
    let runner = FakeProcessRunner::new(Scenario::KeychainUnlockFailure);
    let (_, timing) = collect_agent_facts_at_with_timing(
        &runner,
        account_home(),
        &keychain_profiles(),
        COLLECTED_AT,
    );
    let lines = timing.lines();
    assert!(
        lines
            .iter()
            .any(|line| line.contains("step=keychain-unlock")),
        "keychain unlock is a recorded step: {lines:?}"
    );
    assert_timing_privacy(&lines);
}

#[test]
fn available_herdr_timing_names_include_herdr_agents_after_ping() {
    let home = tempfile::tempdir().unwrap();
    let _herdr = FakeHerdr::start_in_home(home.path());
    let runner = FakeProcessRunner::with_herdr(Scenario::AllAgents, HerdrBinary::Installed);
    let (_, timing) =
        collect_agent_facts_at_with_timing(&runner, home.path(), &profiles(), COLLECTED_AT);
    let names = timing_names(&timing.lines());
    let herdr_and_total: Vec<_> = names
        .into_iter()
        .filter(|name| name.contains("herdr") || name.ends_with("step=total"))
        .collect();
    assert_eq!(herdr_and_total, expected_available_herdr_timing_names());
    assert_timing_privacy(&timing.lines());
}

fn expected_all_agents_timing_names() -> Vec<String> {
    let mut names = Vec::new();
    for agent in ["codex", "claude", "cursor", "opencode"] {
        let auth = if agent == "codex" {
            "authenticated"
        } else {
            "unauthenticated"
        };
        names.push(format!(
            "timing agent={agent} profile=- step=locate+version"
        ));
        names.push(format!(
            "timing agent={agent} profile=- step=auth result={auth}"
        ));
        names.push(format!(
            "timing agent={agent} profile=agents step=auth result=authenticated"
        ));
    }
    names.push("timing agent=herdr profile=- step=locate+version".into());
    names.push("timing step=total".into());
    names
}

fn expected_available_herdr_timing_names() -> Vec<String> {
    vec![
        "timing agent=herdr profile=- step=locate+version".into(),
        "timing step=herdr-ping".into(),
        "timing step=herdr-agents".into(),
        "timing step=total".into(),
    ]
}

fn timing_names(lines: &[String]) -> Vec<String> {
    lines
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
        .collect()
}

fn assert_timing_privacy(lines: &[String]) {
    for line in lines {
        assert!(line.starts_with("timing "), "{line}");
        assert!(!line.contains('/'), "timing must not leak a path: {line}");
        assert!(
            !line.contains(SECRET),
            "timing leaked a profile secret: {line}"
        );
        for leaked in [
            "CLAUDE_CODE_OAUTH_TOKEN=",
            "CURSOR_API_KEY=",
            "OPENAI_API_KEY=",
            "MAC_WORKER_KEYCHAIN_PASSWORD=",
            "MAC_WORKER_KEYCHAIN_PATH=",
            "cursor-profile-key",
            "openai-profile-key",
            "profile-password",
            "command -v",
            "--version",
            "login status",
            "auth status",
            "git config",
            "unlock-keychain",
        ] {
            assert!(!line.contains(leaked), "timing leaked {leaked:?}: {line}");
        }
        for field in line.split_whitespace().skip(1) {
            let Some((key, value)) = field.split_once('=') else {
                panic!("timing field is not key=value: {field} in {line}");
            };
            assert!(
                matches!(
                    key,
                    "agent" | "profile" | "step" | "ms" | "result" | "deadline_ms" | "hit_deadline"
                ),
                "unexpected timing key {key} in {line}"
            );
            assert!(!value.is_empty(), "{line}");
        }
    }
}

const CODEX_AUTH_FAILURE: &str = concat!(
    "ERROR codex_login::auth::manager: Failed to refresh token: ",
    "Your access token could not be refreshed because your refresh token was already used. ",
    "Please log out and sign in again.\n",
    r#"{"type":"error","message":"Your access token could not be refreshed because your refresh token was already used."}"#,
    "\n",
);

const CURSOR_AUTH_FAILURE: &str = "Error: Authentication required. Please run 'agent login' first, or set CURSOR_API_KEY environment variable.\n";

const TURN_AUTH_AT: u64 = 1_704_067_200_000;

fn host_state() -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    HostStore::open(&root).unwrap();
    (temp, root)
}

fn facts_with_incidents(host_state_root: &Path) -> AgentFacts {
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    collect_agent_facts_at_host_with_timing(
        &runner,
        account_home(),
        &profiles(),
        COLLECTED_AT,
        host_state_root,
    )
    .0
}

fn agent_named<'a>(facts: &'a AgentFacts, name: &str) -> &'a AgentProbe {
    facts
        .agents
        .iter()
        .find(|agent| agent.name == name)
        .unwrap_or_else(|| panic!("{name} probe must be present"))
}

fn advertised_agent_capabilities(facts: AgentFacts) -> Vec<String> {
    let observations = SchedulerProbeAdapter::observations(
        &Config {
            version: 1,
            notifications: mac_worker::config::NotificationsConfig::default(),
            workers: vec![WorkerEntry {
                name: "mini-1".into(),
                ssh: "mac1".into(),
                slots: 1,
                capabilities: vec!["darwin-arm64".into()],
                remote_binary: "~/.local/bin/worker".into(),
                herdr: false,
            }],
        },
        &[WorkerHealth {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            status: HealthStatus::Ready,
            probe: Some(ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: SUPERVISION_VERSION,
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
        }],
    )
    .unwrap();
    observations[0]
        .capabilities()
        .iter()
        .filter(|capability| capability.starts_with("agent:"))
        .cloned()
        .collect()
}

#[test]
fn turn_auth_failure_reason_is_utc_minute_and_round_trips() {
    let reason = turn_auth_failure_reason(TURN_AUTH_AT).expect("bounded interned reason");
    assert_eq!(reason, "auth failed in a turn at 2024-01-01T00:00Z");
    let encoded = serde_json::to_vec(&AgentAuth::UnknownWithReason(reason)).unwrap();
    let parsed: AgentAuth = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(parsed, AgentAuth::UnknownWithReason(reason));
    assert!(
        serde_json::from_str::<AgentAuth>(r#""authenticated""#).unwrap()
            == AgentAuth::Authenticated
    );
}

#[test]
fn scripted_codex_and_cursor_turn_output_records_an_incident_and_drops_the_capability() {
    let codex = adapter_for(AgentKind::Codex);
    assert!(codex.output_shows_auth_failure(CODEX_AUTH_FAILURE, ""));
    assert!(codex.output_shows_auth_failure(
        "",
        "ERROR codex_login::auth::manager: HTTP error: 401 Unauthorized\n"
    ));
    assert!(!codex.output_shows_auth_failure("HTTP error: 401 Unauthorized\n", ""));
    assert!(!codex.output_shows_auth_failure("command failed\n", ""));

    let cursor = adapter_for(AgentKind::Cursor);
    assert!(cursor.output_shows_auth_failure(CURSOR_AUTH_FAILURE, ""));
    assert!(!cursor.output_shows_auth_failure("command failed\n", ""));
    assert!(
        adapter_for(AgentKind::Claude)
            .auth_failure_signatures()
            .is_empty()
    );
    assert!(
        adapter_for(AgentKind::Opencode)
            .auth_failure_signatures()
            .is_empty()
    );

    let (_temp, host) = host_state();
    auth_incidents::record_incident(&host, AgentKind::Codex, None, TURN_AUTH_AT).unwrap();
    auth_incidents::record_incident(&host, AgentKind::Cursor, Some("agents"), TURN_AUTH_AT)
        .unwrap();
    let stored = fs::read_to_string(host.join(auth_incidents::AUTH_INCIDENTS_FILE)).unwrap();
    assert!(stored.contains(r#""reason":"auth failed in a turn""#));
    assert!(!stored.contains("refresh token"));
    assert!(!stored.contains("CURSOR_API_KEY"));
    assert!(!stored.contains("/Users/"));

    let facts = facts_with_incidents(&host);
    let reason = turn_auth_failure_reason(TURN_AUTH_AT).unwrap();
    assert_eq!(
        agent_named(&facts, "codex").auth,
        AgentAuth::UnknownWithReason(reason)
    );
    assert_eq!(
        agent_named(&facts, "cursor").auth_by_profile,
        vec![("agents".into(), AgentAuth::UnknownWithReason(reason))]
    );
    assert_eq!(
        agent_named(&facts, "cursor").auth,
        AgentAuth::Unauthenticated
    );
    let capabilities = advertised_agent_capabilities(facts);
    assert!(
        !capabilities
            .iter()
            .any(|capability| capability == "agent:codex"),
        "{capabilities:?}"
    );
    assert!(
        !capabilities
            .iter()
            .any(|capability| capability == "agent:cursor@agents"),
        "{capabilities:?}"
    );
}

#[test]
fn later_success_clears_the_matching_auth_incident() {
    let (_temp, host) = host_state();
    auth_incidents::record_incident(&host, AgentKind::Codex, None, TURN_AUTH_AT).unwrap();
    auth_incidents::record_success(&host, AgentKind::Codex, None, TURN_AUTH_AT + 60_000).unwrap();
    let facts = facts_with_incidents(&host);
    assert_eq!(agent_named(&facts, "codex").auth, AgentAuth::Authenticated);
    assert!(
        advertised_agent_capabilities(facts)
            .iter()
            .any(|capability| capability == "agent:codex")
    );
}

#[test]
fn clear_auth_incidents_flag_restores_the_status_probe() {
    let (_temp, host) = host_state();
    auth_incidents::record_incident(&host, AgentKind::Codex, None, TURN_AUTH_AT).unwrap();
    auth_incidents::clear_all(&host).unwrap();
    let facts = facts_with_incidents(&host);
    assert_eq!(agent_named(&facts, "codex").auth, AgentAuth::Authenticated);
}

#[test]
fn newer_codex_auth_json_clears_the_incident() {
    let home = tempfile::tempdir().unwrap();
    let (_host_temp, host) = host_state();
    auth_incidents::record_incident(&host, AgentKind::Codex, None, 1).unwrap();
    std::fs::create_dir_all(home.path().join(".codex")).unwrap();
    std::fs::write(home.path().join(".codex/auth.json"), b"{}").unwrap();
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    let facts = collect_agent_facts_at_host_with_timing(
        &runner,
        home.path(),
        &profiles(),
        COLLECTED_AT,
        &host,
    )
    .0;
    assert_eq!(agent_named(&facts, "codex").auth, AgentAuth::Authenticated);
}

#[test]
fn expired_auth_incident_is_not_overlaid() {
    let (_temp, host) = host_state();
    auth_incidents::record_incident(&host, AgentKind::Codex, None, 1).unwrap();
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    let facts = collect_agent_facts_at_host_with_timing(
        &runner,
        account_home(),
        &profiles(),
        1 + AUTH_INCIDENT_TTL_MILLIS,
        &host,
    )
    .0;
    assert_eq!(agent_named(&facts, "codex").auth, AgentAuth::Authenticated);
}

static AUTH_HOOK_TESTS: Mutex<()> = Mutex::new(());
const HOOK_WAIT: Duration = Duration::from_secs(10);

fn recv_bounded<T>(rx: &mpsc::Receiver<T>, what: &str) -> T {
    rx.recv_timeout(HOOK_WAIT)
        .unwrap_or_else(|error| panic!("{what} timed out: {error}"))
}

struct MutateInterleave {
    go_lock: Option<mpsc::Sender<()>>,
    go_publish: Option<mpsc::Sender<()>>,
}

impl Drop for MutateInterleave {
    fn drop(&mut self) {
        if let Some(go) = self.go_lock.take() {
            let _ = go.send(());
        }
        if let Some(go) = self.go_publish.take() {
            let _ = go.send(());
        }
        auth_incidents::set_before_lock_hook(None);
        auth_incidents::set_after_load_hook(None);
        auth_incidents::set_before_publish_hook(None);
    }
}

fn install_paused_writer_hooks() -> (mpsc::Receiver<()>, mpsc::Receiver<()>, MutateInterleave) {
    let (arrived_tx, arrived_rx) = mpsc::channel();
    let (paused_tx, paused_rx) = mpsc::channel();
    let (go_lock_tx, go_lock_rx) = mpsc::channel();
    let (go_publish_tx, go_publish_rx) = mpsc::channel();
    let go_lock_rx = Mutex::new(go_lock_rx);
    let go_publish_rx = Mutex::new(go_publish_rx);
    let first_lock = Arc::new(AtomicBool::new(true));
    let first_publish = Arc::new(AtomicBool::new(true));
    auth_incidents::set_before_lock_hook(Some(Arc::new({
        let first_lock = Arc::clone(&first_lock);
        move || {
            if first_lock.swap(false, Ordering::SeqCst) {
                let _ = arrived_tx.send(());
                let _ = go_lock_rx
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .recv();
            }
        }
    })));
    auth_incidents::set_before_publish_hook(Some(Arc::new({
        let first_publish = Arc::clone(&first_publish);
        move || {
            if first_publish.swap(false, Ordering::SeqCst) {
                let _ = paused_tx.send(());
                let _ = go_publish_rx
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .recv();
            }
        }
    })));
    (
        arrived_rx,
        paused_rx,
        MutateInterleave {
            go_lock: Some(go_lock_tx),
            go_publish: Some(go_publish_tx),
        },
    )
}

fn peer_must_stay_blocked(
    done: &mpsc::Receiver<Result<(), WorkerError>>,
    store: &Path,
    forbidden: &str,
) {
    let hold = Instant::now();
    while hold.elapsed() < Duration::from_millis(400) {
        match done.try_recv() {
            Ok(result) => panic!("peer finished while the writer still held the lock: {result:?}"),
            Err(mpsc::TryRecvError::Empty) => {}
            Err(error) => panic!("peer thread dropped: {error}"),
        }
        if store.exists() {
            let stored = fs::read_to_string(store).unwrap();
            assert!(
                !stored.contains(forbidden),
                "peer published under the held lock: {stored}"
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn write_corrupt_incidents(host: &Path) {
    let path = host.join(auth_incidents::AUTH_INCIDENTS_FILE);
    fs::write(&path, b"{not-canonical").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn invalid_turn_auth_dates_do_not_round_trip_through_facts() {
    assert!(
        serde_json::from_str::<AgentAuth>(
            r#"{"state":"unknown","reason":"auth failed in a turn at 2024-13-01T00:00Z"}"#
        )
        .is_err()
    );
    let parsed: AgentAuth =
        serde_json::from_str(r#"{"state":"unknown","reason":"auth failed in a turn"}"#).unwrap();
    assert_eq!(
        parsed,
        AgentAuth::UnknownWithReason(auth_incidents::AUTH_INCIDENT_REASON)
    );
}

#[test]
fn unreadable_incident_store_is_conservative_and_observable() {
    let (_temp, host) = host_state();
    write_corrupt_incidents(&host);
    let facts = facts_with_incidents(&host);
    assert_eq!(
        agent_named(&facts, "codex").auth,
        AgentAuth::UnknownWithReason(AUTH_INCIDENTS_UNREADABLE_REASON)
    );
    assert_eq!(
        agent_named(&facts, "cursor")
            .auth_by_profile
            .iter()
            .find(|(name, _)| name == "agents")
            .map(|(_, auth)| *auth),
        Some(AgentAuth::UnknownWithReason(
            AUTH_INCIDENTS_UNREADABLE_REASON
        ))
    );
    let capabilities = advertised_agent_capabilities(facts);
    assert!(
        !capabilities
            .iter()
            .any(|capability| capability.starts_with("agent:codex")
                || capability.starts_with("agent:cursor")),
        "{capabilities:?}"
    );
}

#[test]
fn clear_all_recovers_a_corrupt_store_so_refresh_can_advertise() {
    let home = tempfile::tempdir().unwrap();
    let (_temp, host) = host_state();
    write_corrupt_incidents(&host);
    auth_incidents::clear_all(&host).unwrap();
    let facts = ProbeCollector::refresh_facts_at(
        &host,
        home.path(),
        &FakeProcessRunner::new(Scenario::AllAgents),
    )
    .unwrap();
    assert_eq!(agent_named(&facts, "codex").auth, AgentAuth::Authenticated);
    assert!(
        advertised_agent_capabilities(facts)
            .iter()
            .any(|capability| capability == "agent:codex")
    );
}

#[test]
fn refresh_does_not_erase_an_incident_recorded_after_it_loaded() {
    let _guard = AUTH_HOOK_TESTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (_temp, host) = host_state();
    let (arrived_rx, paused_rx, mut interleave) = install_paused_writer_hooks();
    let host_writer = host.clone();
    let writer = thread::spawn(move || {
        auth_incidents::enable_after_load_hook_on_this_thread();
        collect_agent_facts_at_host_with_timing(
            &FakeProcessRunner::new(Scenario::AllAgents),
            account_home(),
            &profiles(),
            COLLECTED_AT,
            &host_writer,
        )
    });
    recv_bounded(&arrived_rx, "refresh reached mutate before the lock");
    let _ = interleave.go_lock.take().expect("go_lock").send(());
    recv_bounded(&paused_rx, "refresh paused before publish");

    let (done_tx, done_rx) = mpsc::channel();
    let host_peer = host.clone();
    thread::spawn(move || {
        let _ = done_tx.send(auth_incidents::record_incident(
            &host_peer,
            AgentKind::Codex,
            None,
            TURN_AUTH_AT,
        ));
    });
    peer_must_stay_blocked(
        &done_rx,
        &host.join(auth_incidents::AUTH_INCIDENTS_FILE),
        r#""agent":"codex""#,
    );

    drop(interleave);
    recv_bounded(&done_rx, "peer record_incident").unwrap();
    writer.join().expect("refresh thread");
    let facts = facts_with_incidents(&host);
    let reason = turn_auth_failure_reason(TURN_AUTH_AT).unwrap();
    assert_eq!(
        agent_named(&facts, "codex").auth,
        AgentAuth::UnknownWithReason(reason)
    );
    let stored = fs::read_to_string(host.join(auth_incidents::AUTH_INCIDENTS_FILE)).unwrap();
    assert!(stored.contains(r#""agent":"codex""#), "{stored}");
}

#[test]
fn concurrent_records_for_distinct_profiles_both_survive() {
    let _guard = AUTH_HOOK_TESTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (_temp, host) = host_state();
    let (arrived_rx, paused_rx, mut interleave) = install_paused_writer_hooks();
    let host_writer = host.clone();
    let writer = thread::spawn(move || {
        auth_incidents::enable_after_load_hook_on_this_thread();
        auth_incidents::record_incident(&host_writer, AgentKind::Codex, None, TURN_AUTH_AT)
    });
    recv_bounded(&arrived_rx, "writer reached mutate before the lock");
    let _ = interleave.go_lock.take().expect("go_lock").send(());
    recv_bounded(&paused_rx, "writer paused before publish");

    let (done_tx, done_rx) = mpsc::channel();
    let host_peer = host.clone();
    thread::spawn(move || {
        let _ = done_tx.send(auth_incidents::record_incident(
            &host_peer,
            AgentKind::Cursor,
            Some("agents"),
            TURN_AUTH_AT,
        ));
    });
    peer_must_stay_blocked(
        &done_rx,
        &host.join(auth_incidents::AUTH_INCIDENTS_FILE),
        r#""agent":"cursor""#,
    );

    drop(interleave);
    recv_bounded(&done_rx, "peer record_incident").unwrap();
    writer.join().expect("codex record").unwrap();
    let facts = facts_with_incidents(&host);
    let reason = turn_auth_failure_reason(TURN_AUTH_AT).unwrap();
    assert_eq!(
        agent_named(&facts, "codex").auth,
        AgentAuth::UnknownWithReason(reason)
    );
    assert_eq!(
        agent_named(&facts, "cursor").auth_by_profile,
        vec![("agents".into(), AgentAuth::UnknownWithReason(reason))]
    );
}

const AUTH_INCIDENT_PEER_HOST: &str = "MAC_WORKER_AUTH_INCIDENT_PEER_HOST";

#[test]
fn before_publish_lock_keeps_a_peer_process_from_erasing_an_incident() {
    if let Ok(host) = std::env::var(AUTH_INCIDENT_PEER_HOST) {
        let host = Path::new(&host);
        let parent = host.parent().expect("host has a parent");
        fs::write(parent.join("peer-started"), b"1").unwrap();
        auth_incidents::record_incident(host, AgentKind::Cursor, Some("agents"), TURN_AUTH_AT)
            .unwrap();
        fs::write(parent.join("peer-done"), b"1").unwrap();
        return;
    }

    let _guard = AUTH_HOOK_TESTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (_temp, host) = host_state();
    auth_incidents::record_incident(&host, AgentKind::Claude, None, TURN_AUTH_AT).unwrap();

    let (at_publish_tx, at_publish_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let go_rx = Mutex::new(go_rx);
    let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
    auth_incidents::set_before_publish_hook(Some(Arc::new({
        let first = Arc::clone(&first);
        move || {
            if first.swap(false, Ordering::SeqCst) {
                let _ = at_publish_tx.send(());
                let _ = go_rx
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .recv();
            }
        }
    })));
    let host_for_thread = host.clone();
    let writer = thread::spawn(move || {
        auth_incidents::enable_after_load_hook_on_this_thread();
        auth_incidents::record_incident(&host_for_thread, AgentKind::Codex, None, TURN_AUTH_AT)
    });
    at_publish_rx
        .recv()
        .expect("writer reached the publish window while holding the lock");

    struct Unlock {
        go: Option<mpsc::Sender<()>>,
    }
    impl Drop for Unlock {
        fn drop(&mut self) {
            if let Some(go) = self.go.take() {
                let _ = go.send(());
            }
            auth_incidents::set_before_publish_hook(None);
        }
    }
    let unlock = Unlock {
        go: Some(go_tx.clone()),
    };

    let mut child = Command::new(std::env::current_exe().unwrap())
        .env(AUTH_INCIDENT_PEER_HOST, &host)
        .args([
            "--exact",
            "before_publish_lock_keeps_a_peer_process_from_erasing_an_incident",
            "--nocapture",
            "--test-threads=1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let marker_dir = host.parent().expect("host has a parent");
    let started = marker_dir.join("peer-started");
    let done = marker_dir.join("peer-done");
    let wait_started = Instant::now();
    while !started.exists() {
        assert!(
            wait_started.elapsed() < Duration::from_secs(10),
            "peer process did not start"
        );
        assert!(
            child.try_wait().unwrap().is_none(),
            "peer process exited before taking the lock"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let hold = Instant::now();
    while hold.elapsed() < Duration::from_millis(400) {
        assert!(
            child.try_wait().unwrap().is_none(),
            "peer process must wait on the incident lock through the exchange window"
        );
        assert!(
            !done.exists(),
            "peer process published while the lock was still held"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let stored = fs::read_to_string(host.join(auth_incidents::AUTH_INCIDENTS_FILE)).unwrap();
    assert!(stored.contains("claude"), "{stored}");
    assert!(!stored.contains("cursor"), "{stored}");

    go_tx.send(()).unwrap();
    drop(unlock);
    writer.join().expect("parent record").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "peer stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        done.exists(),
        "peer process must finish after the lock is released"
    );

    let facts = facts_with_incidents(&host);
    let reason = turn_auth_failure_reason(TURN_AUTH_AT).unwrap();
    assert_eq!(
        agent_named(&facts, "codex").auth,
        AgentAuth::UnknownWithReason(reason)
    );
    assert_eq!(
        agent_named(&facts, "cursor").auth_by_profile,
        vec![("agents".into(), AgentAuth::UnknownWithReason(reason))]
    );
}
