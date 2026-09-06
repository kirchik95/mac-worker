use std::{ffi::OsString, os::unix::process::ExitStatusExt, sync::Mutex, time::Duration};

use mac_worker::{
    agent_facts::{
        AgentAuth, AgentFacts, AgentProbe, EnvProfile, FACTS_TTL, ProfileProbe,
        collect_agent_facts_at,
    },
    error::WorkerError,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
};

const COLLECTED_AT: u64 = 100_000;
const SECRET: &str = "PLANTED_PROFILE_SECRET";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    AllAgents,
    MissingOpenCode,
    LockedClaude,
    ProfileError,
    NetworkCodex,
    AmbiguousOpenCode,
    InvalidUtf8Cursor,
    LoginPathUnavailable,
    KeychainUnlockFailure,
}

const LOGIN_PATH: &str = "/opt/tools:/usr/local/bin:/usr/bin:/bin";

struct FakeProcessRunner {
    scenario: Scenario,
    requests: Mutex<Vec<ProcessRequest>>,
}

impl FakeProcessRunner {
    fn new(scenario: Scenario) -> Self {
        Self {
            scenario,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.requests
            .lock()
            .expect("request mutex poisoned")
            .clone()
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
            assert_eq!(self.scenario, Scenario::KeychainUnlockFailure);
            assert_eq!(request.environment, Vec::<(OsString, OsString)>::new());
            assert_eq!(request.stdin, Some(b"profile-password\n".to_vec()));
            return Ok(failure(
                b"security: profile-password /tmp/profile.keychain-db\n",
            ));
        }

        if program == "zsh" && args == ["-lc", "printf %s \"$PATH\""] {
            assert!(request.environment.is_empty());
            return match self.scenario {
                Scenario::LoginPathUnavailable => Ok(failure(b"zsh: login shell failed\n")),
                _ => Ok(success(LOGIN_PATH.as_bytes())),
            };
        }
        if program == "zsh" && args == ["-lc", "command -v codex"] {
            return Ok(success(b"/opt/tools/codex\n"));
        }
        if program == "zsh" && args == ["-lc", "command -v claude"] {
            return Ok(success(b"/opt/tools/claude\n"));
        }
        if program == "zsh" && args == ["-lc", "command -v cursor-agent"] {
            return Ok(success(b"/opt/tools/cursor-agent\n"));
        }
        if program == "zsh" && args == ["-lc", "command -v opencode"] {
            return match self.scenario {
                Scenario::MissingOpenCode => Ok(failure(b"opencode: command not found\n")),
                _ => Ok(success(b"/opt/tools/opencode\n")),
            };
        }

        if program == "zsh" && args == ["-lc", "git config --get user.name"] {
            return Ok(success(b"Submitter\n"));
        }
        if program == "zsh" && args == ["-lc", "git config --get user.email"] {
            return Ok(success(b"submitter@example.test\n"));
        }

        if program.ends_with("/codex") && args == ["--version"] {
            return Ok(success(b"codex-cli 0.152.1\n"));
        }
        if program.ends_with("/codex") && args == ["login", "status"] {
            if self.scenario == Scenario::NetworkCodex {
                return Ok(success(b"Logged in\nnetwork error\n"));
            }
            return Ok(success(b"Logged in using ChatGPT\n"));
        }

        if program.ends_with("/claude") && args == ["--version"] {
            return Ok(success(b"2.1.252 (Claude Code)\n"));
        }
        if program.ends_with("/claude") && args == ["auth", "status"] {
            if self.scenario == Scenario::LockedClaude {
                return Ok(success(b"Error: Your macOS login keychain is locked.\n"));
            }
            if self.scenario == Scenario::ProfileError
                && request.environment.iter().any(|(_, value)| value == SECRET)
            {
                return Err(WorkerError::Protocol(format!("provider rejected {SECRET}")));
            }
            return if has_environment(request, "CLAUDE_CODE_OAUTH_TOKEN") {
                Ok(success(br#"{"loggedIn":true}"#))
            } else {
                Ok(success(br#"{"loggedIn":false}"#))
            };
        }

        if program.ends_with("/cursor-agent") && args == ["--version"] {
            return Ok(success(b"cursor-agent 1.3.0\n"));
        }
        if program.ends_with("/cursor-agent") && args == ["status"] {
            if self.scenario == Scenario::InvalidUtf8Cursor {
                return Ok(success(&[0xff, b'\n']));
            }
            return if has_environment(request, "CURSOR_API_KEY") {
                Ok(success(b"Authenticated as test-user\n"))
            } else {
                Ok(success(b"Not authenticated\n"))
            };
        }

        if program.ends_with("/opencode") && args == ["--version"] {
            return Ok(success(b"opencode 1.0.0\n"));
        }
        if program.ends_with("/opencode") && args == ["auth", "list"] {
            if self.scenario == Scenario::AmbiguousOpenCode {
                return Ok(success(b"unexpected output\n"));
            }
            return if has_environment(request, "OPENAI_API_KEY") {
                Ok(success(br#"[{"provider":"openai"}]"#))
            } else {
                Ok(success(b"[]"))
            };
        }

        panic!("unexpected process request: {request:?}");
    }
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
    let facts = collect_agent_facts_at(&runner, &profiles(), COLLECTED_AT);

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
    let _ = collect_agent_facts_at(&runner, &profiles(), COLLECTED_AT);
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
            request.program.to_string_lossy().ends_with("/claude")
                && request.args == [OsString::from("auth"), OsString::from("status")]
        })
        .collect::<Vec<_>>();
    assert_eq!(claude_auth.len(), 2);
    assert_eq!(
        claude_auth[0].environment,
        vec![(OsString::from("PATH"), OsString::from(LOGIN_PATH))]
    );
    assert_eq!(
        claude_auth[1]
            .environment
            .iter()
            .find(|(key, _)| key == &OsString::from("CLAUDE_CODE_OAUTH_TOKEN"))
            .map(|(_, value)| value.to_string_lossy().into_owned()),
        Some(SECRET.into())
    );
    assert!(!requests.iter().any(|request| {
        request
            .environment
            .iter()
            .any(|(_, value)| value == "unsafe-value")
    }));
    let codex_auth = requests
        .iter()
        .filter(|request| {
            request.program.to_string_lossy().ends_with("/codex")
                && request.args == [OsString::from("login"), OsString::from("status")]
        })
        .collect::<Vec<_>>();
    assert_eq!(codex_auth.len(), 2);
    assert!(
        codex_auth[1].environment.iter().any(|(key, value)| key
            == &OsString::from("CLAUDE_CODE_OAUTH_TOKEN")
            && value == SECRET)
    );
}

#[test]
fn missing_binaries_are_omitted_and_auth_errors_are_unknown_without_leaking_values() {
    let runner = FakeProcessRunner::new(Scenario::MissingOpenCode);
    let facts = collect_agent_facts_at(&runner, &profiles(), COLLECTED_AT);
    assert!(!facts.agents.iter().any(|agent| agent.name == "opencode"));

    let runner = FakeProcessRunner::new(Scenario::LockedClaude);
    let facts = collect_agent_facts_at(&runner, &profiles(), COLLECTED_AT);
    let claude = facts
        .agents
        .iter()
        .find(|agent| agent.name == "claude")
        .unwrap();
    assert_eq!(claude.auth, AgentAuth::Unknown);

    let runner = FakeProcessRunner::new(Scenario::ProfileError);
    let facts = collect_agent_facts_at(&runner, &profiles(), COLLECTED_AT);
    let bytes = facts.canonical_bytes().unwrap();
    assert!(!String::from_utf8(bytes).unwrap().contains(SECRET));
    assert!(!format!("{:?}", profiles()[0]).contains(SECRET));
}

#[test]
fn ambiguous_network_and_non_utf8_auth_outputs_are_unknown() {
    let runner = FakeProcessRunner::new(Scenario::NetworkCodex);
    let facts = collect_agent_facts_at(&runner, &profiles(), COLLECTED_AT);
    let codex = facts
        .agents
        .iter()
        .find(|agent| agent.name == "codex")
        .unwrap();
    assert_eq!(codex.auth, AgentAuth::Unknown);
    assert_eq!(codex.auth_by_profile[0].1, AgentAuth::Unknown);

    let runner = FakeProcessRunner::new(Scenario::AmbiguousOpenCode);
    let facts = collect_agent_facts_at(&runner, &profiles(), COLLECTED_AT);
    let opencode = facts
        .agents
        .iter()
        .find(|agent| agent.name == "opencode")
        .unwrap();
    assert_eq!(opencode.auth, AgentAuth::Unknown);
    assert_eq!(opencode.auth_by_profile[0].1, AgentAuth::Unknown);

    let runner = FakeProcessRunner::new(Scenario::InvalidUtf8Cursor);
    let facts = collect_agent_facts_at(&runner, &profiles(), COLLECTED_AT);
    let cursor = facts
        .agents
        .iter()
        .find(|agent| agent.name == "cursor")
        .unwrap();
    assert_eq!(cursor.auth, AgentAuth::Unknown);
    assert_eq!(cursor.auth_by_profile[0].1, AgentAuth::Unknown);
}

#[test]
fn keychain_unlock_failures_are_unknown_with_a_safe_reason() {
    let runner = FakeProcessRunner::new(Scenario::KeychainUnlockFailure);
    let facts = collect_agent_facts_at(&runner, &keychain_profiles(), COLLECTED_AT);

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
    assert_eq!(parsed.canonical_bytes().unwrap(), facts.canonical_bytes().unwrap());
}

#[test]
fn facts_are_stale_only_after_the_ttl_and_age_subtraction_is_saturating() {
    let facts = AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: false,
        collected_at_millis: COLLECTED_AT,
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
    let _ = collect_agent_facts_at(&runner, &[profile], COLLECTED_AT);
    assert!(
        runner
            .requests()
            .iter()
            .any(|request| { request.environment.iter().any(|(_, value)| value == SECRET) })
    );
}

#[test]
fn version_and_auth_probes_run_with_the_login_shell_path_and_profiles_win() {
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    let facts = collect_agent_facts_at(&runner, &profiles(), COLLECTED_AT);
    assert_eq!(facts.agents.len(), 4);

    let requests = runner.requests();
    let launches = requests
        .iter()
        .filter(|request| request.program.to_string_lossy().starts_with("/opt/tools/"))
        .collect::<Vec<_>>();
    assert!(!launches.is_empty());
    for request in &launches {
        assert_eq!(
            request.environment.first(),
            Some(&(OsString::from("PATH"), OsString::from(LOGIN_PATH))),
            "{request:?}"
        );
    }

    // A profile that sets PATH is applied after the login PATH, so it wins.
    let path_profile = EnvProfile {
        name: "path-override".into(),
        secure: true,
        entries: vec![(OsString::from("PATH"), OsString::from("/profile/bin"))],
    };
    let runner = FakeProcessRunner::new(Scenario::AllAgents);
    collect_agent_facts_at(&runner, &[path_profile], COLLECTED_AT);
    let overridden = runner
        .requests()
        .into_iter()
        .filter(|request| {
            request.program.to_string_lossy().ends_with("/codex")
                && request.args == [OsString::from("login"), OsString::from("status")]
        })
        .collect::<Vec<_>>();
    assert_eq!(overridden.len(), 2);
    assert_eq!(
        overridden[1].environment,
        vec![
            (OsString::from("PATH"), OsString::from(LOGIN_PATH)),
            (OsString::from("PATH"), OsString::from("/profile/bin")),
        ]
    );
}

#[test]
fn an_unavailable_login_shell_path_falls_back_to_the_helper_environment() {
    let runner = FakeProcessRunner::new(Scenario::LoginPathUnavailable);
    let facts = collect_agent_facts_at(&runner, &profiles(), COLLECTED_AT);
    assert_eq!(facts.agents.len(), 4);
    assert!(
        runner
            .requests()
            .iter()
            .filter(|request| request.program.to_string_lossy().starts_with("/opt/tools/"))
            .all(|request| !has_environment(request, "PATH"))
    );
}
