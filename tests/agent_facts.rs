#[path = "support/fake_herdr.rs"]
mod fake_herdr;

use std::{
    ffi::OsString,
    os::unix::process::ExitStatusExt,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant},
};

use fake_herdr::{FakeHerdr, Reply};
use mac_worker::{
    agent_facts::{
        AgentAuth, AgentFacts, AgentProbe, EnvProfile, FACTS_TTL, HerdrFactState, HerdrFacts,
        PROBE_DEADLINE, ProfileProbe, collect_agent_facts_at, collect_agent_facts_at_with_timing,
    },
    config::{Config, WorkerEntry},
    error::{ProcessError, WorkerError},
    lease::SlotState,
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

        if program == "zsh" && args == ["-lc", "git config --get user.name"] {
            return Ok(success(b"Submitter\n"));
        }
        if program == "zsh" && args == ["-lc", "git config --get user.email"] {
            return Ok(success(b"submitter@example.test\n"));
        }

        if program == "/bin/zsh" && args.first().map(String::as_str) == Some("-lc") {
            assert!(request.isolate_parent_environment);
            let shell = args.last().map(String::as_str).unwrap_or_default();
            // The herdr lookups extend PATH with `~/.local/bin` before the
            // command proper, which follows the last `; `.
            let (prefix, shell) = match shell.rsplit_once("; ") {
                Some((prefix, command)) => (Some(prefix), command),
                None => (None, shell),
            };
            if shell.starts_with("command -v ") {
                if self.scenario == Scenario::NoBinariesResolve {
                    return Ok(failure(b"command not found\n"));
                }
                let binary = shell.strip_prefix("command -v ").unwrap_or_default();
                if binary == "herdr" {
                    assert!(
                        prefix.is_some_and(|prefix| prefix.contains("$HOME/.local/bin")),
                        "the herdr lookup must search ~/.local/bin: {prefix:?}"
                    );
                    return Ok(match self.herdr {
                        HerdrBinary::Missing => failure(b"herdr: command not found\n"),
                        HerdrBinary::Installed | HerdrBinary::VersionFails => {
                            success(b"/Users/worker/.local/bin/herdr\n")
                        }
                    });
                }
                if self.scenario == Scenario::KeychainOrdering && binary != "claude" {
                    return Ok(failure(b"command not found\n"));
                }
                if binary == "opencode" && self.scenario == Scenario::MissingOpenCode {
                    return Ok(failure(b"opencode: command not found\n"));
                }
                return Ok(success(format!("/opt/tools/{binary}\n").as_bytes()));
            }
            if shell == "exec 'herdr' '--version'" {
                assert!(
                    prefix.is_some_and(|prefix| prefix.contains("$HOME/.local/bin")),
                    "the herdr version probe must search ~/.local/bin: {prefix:?}"
                );
                return Ok(match self.herdr {
                    HerdrBinary::Installed => {
                        success(format!("herdr {HERDR_VERSION}\n").as_bytes())
                    }
                    HerdrBinary::VersionFails => failure(b"herdr: unrecognized option\n"),
                    HerdrBinary::Missing => panic!("herdr --version ran without a herdr binary"),
                });
            }
            if shell.starts_with("exec ") {
                return self.exec_command(request, shell);
            }
        }

        panic!("unexpected process request: {request:?}");
    }
}

impl FakeProcessRunner {
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
    assert!(
        shell_command(&lookups[0]).contains("$HOME/.local/bin"),
        "the lookup must reach herdr's install directory: {:?}",
        shell_command(&lookups[0])
    );
    assert!(
        requests_containing(&runner, "'herdr' '--version'").is_empty(),
        "no version probe runs without a binary"
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
        names.push(format!("timing agent={agent} profile=- step=locate"));
        names.push(format!("timing agent={agent} profile=- step=version"));
        names.push(format!(
            "timing agent={agent} profile=- step=auth result={auth}"
        ));
        names.push(format!("timing agent={agent} profile=agents step=locate"));
        names.push(format!(
            "timing agent={agent} profile=agents step=auth result=authenticated"
        ));
    }
    names.push("timing agent=herdr profile=- step=locate".into());
    names.push("timing step=total".into());
    names
}

fn expected_available_herdr_timing_names() -> Vec<String> {
    vec![
        "timing agent=herdr profile=- step=locate".into(),
        "timing agent=herdr profile=- step=version".into(),
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
