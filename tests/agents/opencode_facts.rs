//! Facts collection for the two OpenCode generations.
//!
//! OpenCode 2 sends `auth list` to a shared background service unless the
//! command carries `--standalone`; OpenCode 1 rejects that flag. A facts
//! refresh must never reach the service, so the probe takes its form from the
//! version the same refresh has just observed.
//!
//! Every process here is scripted. No OpenCode runs.

use std::{
    ffi::OsString, os::unix::process::ExitStatusExt, path::Path, process::ExitStatus, sync::Mutex,
};

use mac_worker::test_support::{
    agents::agent_facts::{
        AgentAuth, AgentProbe, EnvProfile, collect_agent_facts_at,
        collect_agent_facts_at_with_timing,
    },
    core::error::WorkerError,
    host::process::{ProcessRequest, ProcessResult, ProcessRunner},
};

const COLLECTED_AT: u64 = 100_000;
const V1_LISTING: &[u8] = b"\xe2\x94\x8c  Credentials \x1b[90m~/.local/share/opencode/auth.json\n\xe2\x94\x82\n\xe2\x97\x8f  GitHub Copilot \x1b[90moauth\n\xe2\x94\x82\n\xe2\x94\x94  1 credentials\n";
const V1_USAGE: &[u8] =
    b"opencode auth list\n\nlist providers and credentials\n\nOptions:\n  -h, --help  show help\n";
const V2_TABLE: &[u8] = b"OpenCode Go     API key                     stored\nGitHub Copilot  OAuth                       stored\n";

/// One scripted OpenCode install.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Install {
    generation: Generation,
    /// What `--version` prints, with its exit status.
    version: (&'static [u8], i32),
    /// What v2 prints for `auth list --standalone`.
    table: &'static [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Generation {
    V1,
    V2,
}

const V1: Install = Install {
    generation: Generation::V1,
    version: (b"1.18.32\n", 0),
    table: b"",
};
const V2: Install = Install {
    generation: Generation::V2,
    version: (b"opencode v2.0.18\n", 0),
    table: V2_TABLE,
};

/// Answers the account login shell like a host with OpenCode only. `profile`
/// is the install a profile that overrides `PATH` resolves instead.
struct Host {
    default: Install,
    profile: Option<Install>,
    /// `<which install> <opencode arguments>` for every OpenCode command.
    commands: Mutex<Vec<String>>,
}

impl Host {
    fn new(default: Install) -> Self {
        Self {
            default,
            profile: None,
            commands: Mutex::new(Vec::new()),
        }
    }

    fn with_profile_install(mut self, install: Install) -> Self {
        self.profile = Some(install);
        self
    }

    fn commands(&self) -> Vec<String> {
        self.commands.lock().unwrap().clone()
    }

    /// Commands that would have reached the v2 background service: anything
    /// but `--version` sent to a v2 install without `--standalone`.
    fn background_service_commands(&self) -> Vec<String> {
        self.commands()
            .into_iter()
            .filter(|command| {
                let mut fields = command.splitn(3, ' ');
                let (_which, generation, arguments) = (
                    fields.next().unwrap(),
                    fields.next().unwrap(),
                    fields.next().unwrap(),
                );
                generation == "v2"
                    && arguments != "--version"
                    && !arguments.ends_with("--standalone")
            })
            .collect()
    }

    fn install_for(&self, request: &ProcessRequest) -> (Install, &'static str) {
        let overrides_path = request
            .environment
            .iter()
            .any(|(name, _)| name == &OsString::from("PATH"));
        match (overrides_path, self.profile) {
            (true, Some(install)) => (install, "profile"),
            _ => (self.default, "default"),
        }
    }

    fn opencode(&self, request: &ProcessRequest, arguments: &[&str]) -> ProcessResult {
        let (install, which) = self.install_for(request);
        let generation = match install.generation {
            Generation::V1 => "v1",
            Generation::V2 => "v2",
        };
        self.commands
            .lock()
            .unwrap()
            .push(format!("{which} {generation} {}", arguments.join(" ")));
        match (install.generation, arguments) {
            (_, ["--version"]) => output(install.version.1, install.version.0, b""),
            (Generation::V1, ["auth", "list"]) => output(0, V1_LISTING, b"\x1b[0m"),
            // v1 rejects the flag it does not know.
            (Generation::V1, ["auth", "list", "--standalone"]) => output(1, b"", V1_USAGE),
            (Generation::V2, ["auth", "list", "--standalone"]) => output(0, install.table, b""),
            // Recorded as a background-service command; the answer is what
            // the service would print.
            (Generation::V2, ["auth", "list"]) => output(0, V2_TABLE, b""),
            other => panic!("unexpected OpenCode command: {other:?}"),
        }
    }
}

impl ProcessRunner for Host {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == "/usr/bin/git" {
            return Ok(output(1, b"", b""));
        }
        assert_eq!(request.program, "/bin/zsh", "{request:?}");
        assert_eq!(request.args[0], "-lc");
        let shell = request.args[1].to_str().unwrap();
        if shell.starts_with("git config --global --get user.") {
            return Ok(output(0, b"Fixture User\n", b""));
        }
        if shell.contains("command -v ") {
            if !shell.starts_with("command -v opencode ") {
                // Only OpenCode is installed on this host.
                return Ok(output(1, b"", b"command not found\n"));
            }
            assert!(shell.ends_with("&& exec 'opencode' '--version'"), "{shell}");
            let version = self.opencode(request, &["--version"]);
            let mut stdout = b"/opt/tools/opencode\nMAC_WORKER_FACTS_VERSION\n".to_vec();
            stdout.extend_from_slice(&version.stdout);
            return Ok(ProcessResult {
                status: version.status,
                stdout,
                stderr: version.stderr,
            });
        }
        let arguments = shell
            .strip_prefix("exec 'opencode' ")
            .unwrap_or_else(|| panic!("unexpected login shell command: {shell}"))
            .split(' ')
            .map(|argument| argument.trim_matches('\''))
            .collect::<Vec<_>>();
        Ok(self.opencode(request, &arguments))
    }
}

fn output(status: i32, stdout: &[u8], stderr: &[u8]) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(status << 8),
        stdout: stdout.to_vec(),
        stderr: stderr.to_vec(),
    }
}

fn account_home() -> &'static Path {
    Path::new("/Users/worker")
}

fn collect(host: &Host, profiles: &[EnvProfile]) -> AgentProbe {
    let facts = collect_agent_facts_at(host, account_home(), profiles, COLLECTED_AT);
    assert_eq!(
        facts
            .agents
            .iter()
            .map(|agent| agent.name.as_str())
            .collect::<Vec<_>>(),
        ["opencode"]
    );
    facts.agents.into_iter().next().unwrap()
}

/// A secure profile that carries a token only: the login shell resolves the
/// same binary as without it.
fn token_profile() -> EnvProfile {
    EnvProfile::new(
        "agents",
        true,
        vec![("OPENAI_API_KEY".into(), "profile-key".into())],
    )
}

/// A secure profile that moves `PATH`, so its login shell can resolve
/// another OpenCode.
fn path_profile() -> EnvProfile {
    EnvProfile::new(
        "other-bin",
        true,
        vec![("PATH".into(), "/profile/bin:/usr/bin:/bin".into())],
    )
}

#[test]
fn a_v2_host_probes_auth_with_standalone_and_never_reaches_the_service() {
    let host = Host::new(V2);
    let opencode = collect(&host, &[token_profile()]);
    assert_eq!(opencode.version.as_deref(), Some("2.0.18"));
    assert_eq!(opencode.auth, AgentAuth::Authenticated);
    assert_eq!(
        opencode.auth_by_profile,
        [("agents".to_owned(), AgentAuth::Authenticated)]
    );
    assert_eq!(
        host.commands(),
        [
            "default v2 --version",
            "default v2 auth list --standalone",
            // The profile cannot change the binary: it reuses the version.
            "default v2 auth list --standalone",
        ]
    );
    assert_eq!(host.background_service_commands(), Vec::<String>::new());
}

#[test]
fn a_v2_host_without_stored_credentials_is_unauthenticated_and_junk_is_unknown() {
    for (table, expected) in [
        (&b""[..], AgentAuth::Unauthenticated),
        (b"\n", AgentAuth::Unauthenticated),
        (b"unexpected output\n", AgentAuth::Unknown),
        (b"Anthropic  OAuth  expired\n", AgentAuth::Unknown),
    ] {
        let host = Host::new(Install { table, ..V2 });
        let opencode = collect(&host, &[]);
        assert_eq!(
            opencode.auth,
            expected,
            "{:?}",
            String::from_utf8_lossy(table)
        );
        assert_eq!(opencode.version.as_deref(), Some("2.0.18"));
        assert_eq!(host.background_service_commands(), Vec::<String>::new());
    }
}

#[test]
fn a_v1_host_keeps_the_plain_auth_probe() {
    let host = Host::new(V1);
    let opencode = collect(&host, &[token_profile()]);
    assert_eq!(opencode.version.as_deref(), Some("1.18.32"));
    assert_eq!(opencode.auth, AgentAuth::Authenticated);
    assert_eq!(
        opencode.auth_by_profile,
        [("agents".to_owned(), AgentAuth::Authenticated)]
    );
    assert_eq!(
        host.commands(),
        [
            "default v1 --version",
            "default v1 auth list",
            "default v1 auth list",
        ]
    );
}

#[test]
fn an_unobserved_version_is_probed_with_standalone_on_either_generation() {
    for version in [
        (&b"development build\n"[..], 0),
        (b"", 0),
        (b"opencode: internal error\n", 3),
    ] {
        // A v2 whose version cannot be read must still not reach the service.
        let host = Host::new(Install { version, ..V2 });
        let opencode = collect(&host, &[]);
        assert_eq!(opencode.version, None);
        assert_eq!(opencode.auth, AgentAuth::Authenticated);
        assert_eq!(
            host.commands(),
            ["default v2 --version", "default v2 auth list --standalone"]
        );
        assert_eq!(host.background_service_commands(), Vec::<String>::new());

        // A v1 in the same state rejects the flag: its login stays unknown
        // rather than being probed in the form that would be unsafe on v2.
        let host = Host::new(Install { version, ..V1 });
        let opencode = collect(&host, &[]);
        assert_eq!(opencode.version, None);
        assert_eq!(opencode.auth, AgentAuth::Unknown);
        assert_eq!(
            host.commands(),
            ["default v1 --version", "default v1 auth list --standalone"]
        );
    }
}

#[test]
fn a_profile_that_moves_path_is_probed_in_the_form_of_the_binary_it_resolves() {
    // The default login shell resolves v1 and the profile's PATH resolves v2.
    let host = Host::new(V1).with_profile_install(V2);
    let opencode = collect(&host, &[path_profile()]);
    // The recorded version stays the default shell's.
    assert_eq!(opencode.version.as_deref(), Some("1.18.32"));
    assert_eq!(opencode.auth, AgentAuth::Authenticated);
    assert_eq!(
        opencode.auth_by_profile,
        [("other-bin".to_owned(), AgentAuth::Authenticated)]
    );
    assert_eq!(
        host.commands(),
        [
            "default v1 --version",
            "default v1 auth list",
            "profile v2 --version",
            "profile v2 auth list --standalone",
        ]
    );
    assert_eq!(host.background_service_commands(), Vec::<String>::new());

    // And the other way round.
    let host = Host::new(V2).with_profile_install(V1);
    let opencode = collect(&host, &[path_profile()]);
    assert_eq!(opencode.version.as_deref(), Some("2.0.18"));
    assert_eq!(
        host.commands(),
        [
            "default v2 --version",
            "default v2 auth list --standalone",
            "profile v1 --version",
            "profile v1 auth list",
        ]
    );
    assert_eq!(host.background_service_commands(), Vec::<String>::new());
}

#[test]
fn the_facts_timing_steps_are_the_same_for_both_generations() {
    for install in [V1, V2] {
        let host = Host::new(install);
        let (_, timing) = collect_agent_facts_at_with_timing(
            &host,
            account_home(),
            &[token_profile()],
            COLLECTED_AT,
        );
        let steps = timing
            .lines()
            .iter()
            .filter(|line| line.contains("agent=opencode"))
            .map(|line| {
                line.split_whitespace()
                    .filter(|field| {
                        field.starts_with("profile=")
                            || field.starts_with("step=")
                            || field.starts_with("result=")
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            steps,
            [
                "profile=- step=locate+version",
                "profile=- step=auth result=authenticated",
                "profile=agents step=auth result=authenticated",
            ],
            "{install:?}"
        );
        // The form of the command never reaches the timing lines.
        assert!(
            !timing
                .lines()
                .iter()
                .any(|line| line.contains("standalone"))
        );
    }
}

#[test]
fn the_fixture_would_catch_a_plain_auth_probe_on_v2() {
    // Guards the guard: a plain `auth list` against a v2 install is what
    // `background_service_commands` reports.
    let host = Host::new(V2);
    let request = ProcessRequest {
        program: "/bin/zsh".into(),
        args: vec!["-lc".into(), "exec 'opencode' 'auth' 'list'".into()],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: mac_worker::test_support::host::process::ProcessPolicy {
            stdout_limit: 4096,
            stderr_limit: 4096,
            // Only the recorded command matters here; a loaded login shell and
            // a fixture's first exec can take seconds to start.
            deadline: std::time::Duration::from_secs(30),
        },
        isolate_parent_environment: true,
    };
    host.run(&request).unwrap();
    assert_eq!(host.background_service_commands(), ["default v2 auth list"]);
}
