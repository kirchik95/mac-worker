use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::{PermissionsExt, symlink},
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    config::Config,
    error::WorkerError,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    run_with_io_in_context,
};
use std::{
    collections::VecDeque, os::unix::process::ExitStatusExt, process::ExitStatus, sync::Mutex,
};

struct Host {
    replies: Mutex<VecDeque<ProcessResult>>,
    requests: Mutex<Vec<ProcessRequest>>,
}

impl Host {
    fn with(replies: Vec<ProcessResult>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl ProcessRunner for Host {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected process call"))
    }
}

fn reply(code: i32, text: &str) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(code << 8),
        stdout: text.as_bytes().to_vec(),
        stderr: b"private remote diagnostic".to_vec(),
    }
}

fn probe(agents: serde_json::Value, profiles: serde_json::Value) -> String {
    serde_json::json!({
        "protocol_version": mac_worker::protocol::PROTOCOL_VERSION,
        "supervision_version": mac_worker::protocol::SUPERVISION_VERSION,
        "hostname": "mini.local", "arch": "arm64", "os_version": "26.2",
        "free_disk_bytes": 100_000_000_000_u64, "total_disk_bytes": 200_000_000_000_u64,
        "memory_pressure": "normal", "swap_used_bytes": 0,
        "available_memory_bytes": 12_000_000_000_u64,
        "cpu_counters": { "user_ticks": 1, "system_ticks": 1, "idle_ticks": 100, "nice_ticks": 0 },
        "slot_state": "idle", "active_lease": null,
        "capabilities": ["darwin-arm64"], "facts_age_millis": 0,
        "agent_facts": {"agents": agents, "env_profiles": profiles, "git_identity": true, "collected_at_millis": 1}
    }).to_string()
}

fn codex(auth: &str) -> serde_json::Value {
    serde_json::json!([{"name": "codex", "version": "1.0", "auth": auth, "auth_by_profile": []}])
}

fn installed_host(agent_probe: &str) -> Host {
    // SSH, platform, Git; existing helper, fresh agent facts, final readiness probe.
    Host::with(vec![
        reply(0, ""),
        reply(0, "Darwin\narm64\n"),
        reply(0, "git version 2.50\n"),
        reply(0, agent_probe),
        reply(0, ""),
        reply(0, agent_probe),
    ])
}

struct Fixture {
    root: tempfile::TempDir,
    config: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("config/mac-worker/config.toml");
        Self { root, config }
    }
    fn run(&self, host: &Host, options: &[&str]) -> (u8, String, String) {
        let mut args = vec![
            "worker",
            "--json",
            "--config",
            self.config.to_str().unwrap(),
            "init",
        ];
        args.extend_from_slice(options);
        let cli = Cli::try_parse_from(args).expect("init is a public command");
        let runtime = RuntimeContext::isolated(
            BTreeMap::new(),
            self.root.path().into(),
            self.root.path().into(),
        );
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = run_with_io_in_context(cli, host, &runtime, &mut out, &mut err);
        (
            code,
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        )
    }
    fn write(&self, config: &str) {
        fs::create_dir_all(self.config.parent().unwrap()).unwrap();
        fs::write(&self.config, config).unwrap();
    }
}

#[test]
fn init_creates_inventory_without_a_project_and_reports_a_ready_agent() {
    let fixture = Fixture::new();
    let host = installed_host(&probe(codex("authenticated"), serde_json::json!([])));
    let (code, out, err) = fixture.run(&host, &["alice@mini.local"]);
    assert_eq!(code, 0, "{out}\n{err}");
    let report: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(report["kind"], "init");
    assert_eq!(report["ready"], true);
    let config = Config::load(&fixture.config).unwrap();
    assert_eq!(config.workers.len(), 1);
    assert_eq!(config.workers[0].ssh, "alice@mini.local");
    assert_eq!(config.workers[0].name, "mini");
    assert_eq!(
        fs::metadata(&fixture.config).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(out.contains("worker task submit"));
    assert!(!out.contains("private remote diagnostic"));
    assert!(err.is_empty());
    let requests = host.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .all(|r| r.args.contains(&"BatchMode=yes".into()))
    );
    assert!(
        requests
            .iter()
            .all(|r| r.args.contains(&"ForwardAgent=no".into()))
    );
    assert!(
        requests.iter().all(|r| r.stdin.is_none()),
        "healthy helper must not be uploaded again"
    );
}

#[test]
fn init_retry_preserves_inventory_comments_and_existing_worker_name() {
    let fixture = Fixture::new();
    let original = "# My fleet\nversion = 1\n\n[[workers]] # primary\nname = \"old-mini\"\nssh = \"alice@mini.local\"\nslots = 1\n";
    fixture.write(original);
    for _ in 0..2 {
        let (code, out, err) = fixture.run(
            &installed_host(&probe(codex("authenticated"), serde_json::json!([]))),
            &["alice@mini.local"],
        );
        assert_eq!(code, 0, "{out}{err}");
        assert!(out.contains("old-mini"));
    }
    assert_eq!(fs::read_to_string(&fixture.config).unwrap(), original);
}

#[test]
fn init_appends_a_second_worker_without_rewriting_the_first() {
    let fixture = Fixture::new();
    let original =
        "# Keep comments\nversion = 1\n[[workers]]\nname = \"one\"\nssh = \"mac1\"\nslots = 1\n";
    fixture.write(original);
    let (code, out, err) = fixture.run(
        &installed_host(&probe(codex("authenticated"), serde_json::json!([]))),
        &["bob@mini.local", "--name", "two"],
    );
    assert_eq!(code, 0, "{out}{err}");
    assert!(
        fs::read_to_string(&fixture.config)
            .unwrap()
            .starts_with(original)
    );
    let config = Config::load(&fixture.config).unwrap();
    assert_eq!(
        config
            .workers
            .iter()
            .map(|w| w.name.as_str())
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
}

#[test]
fn init_rejects_a_conflicting_name_before_connecting() {
    let fixture = Fixture::new();
    let original = "version = 1\n[[workers]]\nname = \"mini\"\nssh = \"someone-else\"\nslots = 1\n";
    fixture.write(original);
    let host = Host::with(vec![]);
    let (code, out, err) = fixture.run(&host, &["alice@mini.local"]);
    assert_ne!(code, 0, "{out}{err}");
    assert_eq!(fs::read_to_string(&fixture.config).unwrap(), original);
    assert!(host.requests.lock().unwrap().is_empty());
}

#[test]
fn init_ssh_failure_explains_the_next_step_without_creating_inventory_or_echoing_stderr() {
    let fixture = Fixture::new();
    let (code, out, err) = fixture.run(&Host::with(vec![reply(255, "")]), &["alice@mini.local"]);
    assert_eq!(code, 69, "{out}{err}");
    assert!(out.contains("SSH_UNAVAILABLE"));
    assert!(out.contains("ssh"));
    assert!(!out.contains("private remote diagnostic"));
    assert!(!fixture.config.exists());
}

#[test]
fn init_rejects_incompatible_hosts_before_saving_or_uploading() {
    for platform in ["Linux\naarch64\n", "Darwin\nx86_64\n", "garbage\n"] {
        let fixture = Fixture::new();
        let host = Host::with(vec![reply(0, ""), reply(0, platform)]);
        let (code, out, err) = fixture.run(&host, &["alice@mini.local"]);
        assert_ne!(code, 0, "{out}{err}");
        assert!(out.contains("PLATFORM_UNSUPPORTED"));
        assert!(!fixture.config.exists());
    }
}

#[test]
fn init_keeps_inventory_but_blocks_until_selected_agent_is_authenticated() {
    for agents in [
        serde_json::json!([]),
        codex("unauthenticated"),
        codex("unknown"),
    ] {
        let fixture = Fixture::new();
        let (code, out, err) = fixture.run(
            &installed_host(&probe(agents, serde_json::json!([]))),
            &["alice@mini.local"],
        );
        assert_eq!(code, 69, "{out}{err}");
        let report: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(report["ready"], false);
        assert!(out.contains("codex"));
        assert!(out.contains("worker init"));
        assert_eq!(Config::load(&fixture.config).unwrap().workers.len(), 1);
    }
}

#[test]
fn init_requires_the_requested_profile_to_be_secure_and_authenticated() {
    for secure in [true, false] {
        let fixture = Fixture::new();
        let agents = serde_json::json!([{"name": "cursor", "version": "1.0", "auth": "unauthenticated", "auth_by_profile": [["agents", "authenticated"]]}]);
        let profiles = serde_json::json!([{"name": "agents", "secure": secure}]);
        let (code, out, err) = fixture.run(
            &installed_host(&probe(agents, profiles)),
            &[
                "alice@mini.local",
                "--agent",
                "cursor",
                "--env-profile",
                "agents",
            ],
        );
        assert_eq!(code, if secure { 0 } else { 69 }, "{out}{err}");
        if secure {
            assert!(out.contains("--env-profile=agents"));
        }
    }
}

#[test]
fn init_refuses_invalid_or_symlinked_inventory_without_connecting() {
    let fixture = Fixture::new();
    fixture.write("this is not TOML");
    let host = Host::with(vec![]);
    assert_ne!(fixture.run(&host, &["alice@mini.local"]).0, 0);
    let target = fixture.root.path().join("untouched");
    fs::write(&target, "version = 1\n").unwrap();
    fs::remove_file(&fixture.config).unwrap();
    symlink(&target, &fixture.config).unwrap();
    assert_ne!(fixture.run(&host, &["alice@mini.local"]).0, 0);
    assert_eq!(fs::read_to_string(&target).unwrap(), "version = 1\n");
    assert!(host.requests.lock().unwrap().is_empty());
}

#[test]
fn init_rejects_options_that_could_become_shell_instructions() {
    for args in [
        vec!["worker", "init", "alice@mini;touch"],
        vec!["worker", "init", "mac1", "--agent", "arbitrary"],
        vec!["worker", "init", "mac1", "--env-profile", "../secret"],
    ] {
        assert!(Cli::try_parse_from(args).is_err());
    }
}

#[test]
fn init_installs_a_missing_helper_through_the_transactional_installer() {
    let fixture = Fixture::new();
    let ready = probe(codex("authenticated"), serde_json::json!([]));
    let host = Host::with(vec![
        reply(0, ""),
        reply(0, "Darwin\narm64\n"),
        reply(0, "git version 2.50\n"),
        reply(127, ""),   // Helper not installed.
        reply(0, &ready), // Local candidate probe.
        reply(0, ""),
        reply(0, ""),
        reply(0, "match\n"),
        reply(0, ""),
        reply(0, ""),
        reply(0, "promoted\n"),
        reply(0, "worker 0.1.0\n"), // Gatekeeper warm-up (--version) before verification.
        reply(0, ""), // Standalone host refresh-facts (migrate-layout + refresh-facts) before verification.
        reply(0, &ready), // Verification probe.
        reply(0, ""), // Verified install, scoped cleanup.
        reply(0, ""),
        reply(0, &ready), // Refresh and readiness.
    ]);
    let (code, out, err) = fixture.run(&host, &["alice@mini.local"]);
    assert_eq!(code, 0, "{out}{err}");
    let requests = host.requests.lock().unwrap();
    assert_eq!(requests.iter().filter(|r| r.stdin.is_some()).count(), 1);
    let remote_commands: Vec<String> = requests
        .iter()
        .filter_map(|request| {
            request
                .args
                .last()
                .map(|argument| argument.to_string_lossy().into_owned())
        })
        .collect();
    let warmup = remote_commands
        .iter()
        .position(|command| command.contains("\"$worker_path\" --version"))
        .expect("installer must warm the promoted helper");
    let facts_refresh = remote_commands
        .iter()
        .enumerate()
        .find(|(index, command)| {
            *index > warmup && command.contains("\"$worker_path\" host refresh-facts")
        })
        .map(|(index, _)| index)
        .expect("installer must refresh facts separately from verification");
    let verification = remote_commands
        .iter()
        .enumerate()
        .find(|(index, command)| *index > facts_refresh && command.contains("host probe"))
        .map(|(index, _)| index)
        .expect("installer must verify with a host probe");
    assert!(warmup < facts_refresh, "warmup then facts-refresh");
    assert!(
        facts_refresh < verification,
        "facts-refresh then verification"
    );
    assert!(
        !remote_commands[facts_refresh].contains("host probe"),
        "facts-refresh must not run the verification probe"
    );
    assert!(host.replies.lock().unwrap().is_empty());
    assert_eq!(Config::load(&fixture.config).unwrap().workers.len(), 1);
}

#[test]
fn init_reports_a_retained_installer_lock_without_claiming_readiness() {
    let fixture = Fixture::new();
    let ready = probe(codex("authenticated"), serde_json::json!([]));
    let host = Host::with(vec![
        reply(0, ""),
        reply(0, "Darwin\narm64\n"),
        reply(0, "git version 2.50\n"),
        reply(127, ""),
        reply(0, &ready),
        reply(75, ""),
    ]);
    let (code, out, err) = fixture.run(&host, &["alice@mini.local"]);
    assert_eq!(code, 69, "{out}{err}");
    assert!(out.contains("INSTALL_LOCKED"));
    assert!(!out.contains("private remote diagnostic"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&out).unwrap()["ready"],
        false
    );
    assert!(
        host.requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.stdin.is_none())
    );
    assert!(Config::load(&fixture.config).is_ok());
}

#[test]
fn init_requires_git_before_registering_a_worker() {
    let fixture = Fixture::new();
    let host = Host::with(vec![
        reply(0, ""),
        reply(0, "Darwin\narm64\n"),
        reply(1, ""),
    ]);
    let (code, out, err) = fixture.run(&host, &["alice@mini.local"]);
    assert_eq!(code, 69, "{out}{err}");
    assert!(out.contains("GIT_MISSING"));
    assert!(!fixture.config.exists());
}

#[test]
fn init_does_not_use_cached_auth_if_refresh_fails_or_facts_are_stale() {
    let fixture = Fixture::new();
    let ready = probe(codex("authenticated"), serde_json::json!([]));
    let host = Host::with(vec![
        reply(0, ""),
        reply(0, "Darwin\narm64\n"),
        reply(0, "git version 2.50\n"),
        reply(0, &ready),
        reply(1, ""),
    ]);
    let (code, out, err) = fixture.run(&host, &["alice@mini.local"]);
    assert_eq!(code, 69, "{out}{err}");
    assert!(out.contains("REFRESH_FACTS_FAILED"));
    let mut stale: serde_json::Value = serde_json::from_str(&ready).unwrap();
    stale["facts_age_millis"] = serde_json::json!(900_001);
    let (code, out, err) = fixture.run(&installed_host(&stale.to_string()), &["alice@mini.local"]);
    assert_eq!(code, 69, "{out}{err}");
    assert!(out.contains("AGENT_FACTS_UNAVAILABLE"));
}

#[test]
fn init_generated_commands_preserve_a_custom_config_path_with_shell_metacharacters() {
    let mut fixture = Fixture::new();
    fixture.config = fixture.root.path().join("alice's $(config).toml");
    let host = installed_host(&probe(codex("authenticated"), serde_json::json!([])));
    let (code, out, err) = fixture.run(&host, &["alice@mini.local"]);
    assert_eq!(code, 0, "{out}{err}");
    let report: serde_json::Value = serde_json::from_str(&out).unwrap();
    let command = report["next_steps"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .find(|s| s.starts_with("worker task submit"))
        .unwrap();
    // Ask the shell to parse the generated argv, replacing worker with a harmless argument printer.
    let script = format!("worker() {{ printf '%s\\n' \"$@\"; }}\n{command}");
    let parsed = std::process::Command::new("/bin/sh")
        .args(["-c", &script])
        .output()
        .unwrap();
    assert!(parsed.status.success());
    let args = String::from_utf8(parsed.stdout).unwrap();
    assert!(
        args.lines()
            .any(|arg| arg == fixture.config.to_str().unwrap())
    );
    assert!(parsed.stderr.is_empty());
}

#[test]
fn init_invalid_inventory_does_not_print_values_from_toml_diagnostics() {
    for contents in [
        "version = 1\napi_key = \"PRIVATE_CREDENTIAL_SENTINEL\"\n",
        "version = 1\n[[workers]]\nname = \"mini\"\nssh = \"PRIVATE_CREDENTIAL_SENTINEL;\"\nslots = 1\n",
    ] {
        let fixture = Fixture::new();
        fixture.write(contents);
        let host = Host::with(vec![]);
        let (code, out, err) = fixture.run(&host, &["alice@mini.local"]);
        assert_ne!(code, 0);
        assert!(!format!("{out}{err}").contains("PRIVATE_CREDENTIAL_SENTINEL"));
        assert!(host.requests.lock().unwrap().is_empty());
    }
}

#[test]
fn init_commands_can_be_parsed_for_all_previously_valid_worker_names() {
    for name in ["work@home", "-mini", "a.b_c", ".", &"w".repeat(129)] {
        let fixture = Fixture::new();
        fixture.write(&format!(
            "version = 1\n[[workers]]\nname = \"{name}\"\nssh = \"alice@mini.local\"\nslots = 1\n"
        ));
        for auth in ["authenticated", "unauthenticated"] {
            let (_, out, _) = fixture.run(
                &installed_host(&probe(codex(auth), serde_json::json!([]))),
                &["alice@mini.local"],
            );
            let report: serde_json::Value = serde_json::from_str(&out).unwrap();
            for command in report["next_steps"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|v| v.as_str())
                .filter(|s| s.starts_with("worker init ") || s.starts_with("worker task submit "))
            {
                let script = format!("worker() {{ printf '%s\\n' \"$@\"; }}\n{command}");
                let parsed = std::process::Command::new("/bin/sh")
                    .args(["-c", &script])
                    .output()
                    .unwrap();
                assert!(parsed.status.success());
                let args = String::from_utf8(parsed.stdout).unwrap();
                assert!(
                    Cli::try_parse_from(std::iter::once("worker").chain(args.lines())).is_ok(),
                    "generated command must accept inventory name {name:?}: {command}"
                );
            }
        }
    }
}
