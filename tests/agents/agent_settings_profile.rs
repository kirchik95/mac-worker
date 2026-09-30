use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::Cursor,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    process::ExitStatus,
    sync::Mutex,
    time::Duration,
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    agent_settings::{AgentSettingsGetRequest, AgentSettingsSaveRequest, NativeAgentSettingsStore},
    cli::Cli,
    error::WorkerError,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    run_with_stdio_in_context,
};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

// Bound simultaneous login shells on shared test hosts. Each endpoint still
// exercises all three catalog discoveries concurrently inside this guard.
static HOST_SETTINGS_LOCK: Mutex<()> = Mutex::new(());

struct KeychainRunner {
    requests: Mutex<Vec<ProcessRequest>>,
    succeed: bool,
}

impl KeychainRunner {
    fn new(succeed: bool) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            succeed,
        }
    }
}

impl ProcessRunner for KeychainRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == "/bin/zsh" {
            // Discovery may only execute inside this test's fixture HOME.
            let home = request
                .environment
                .iter()
                .find(|(key, _)| key == "HOME")
                .unwrap();
            for binary in ["codex", "opencode", "cursor-agent"] {
                assert!(
                    std::path::Path::new(&home.1)
                        .join("bin")
                        .join(binary)
                        .is_file()
                );
            }
            return SystemProcessRunner.run(request);
        }
        assert_eq!(request.program, "/usr/bin/security");
        self.requests.lock().unwrap().push(request.clone());
        Ok(ProcessResult {
            status: ExitStatus::from_raw(if self.succeed { 0 } else { 1 << 8 }),
            stdout: Vec::new(),
            stderr: b"fixture-unlock-password selected-key unrelated-key".to_vec(),
        })
    }
}

fn fixture(expected_key: &str, catalogue: bool) -> TempDir {
    let home = tempdir().unwrap();
    fs::write(home.path().join(".zshenv"), "unsetopt GLOBAL_RCS\n").unwrap();
    fs::create_dir_all(home.path().join("bin")).unwrap();
    fs::create_dir_all(home.path().join(".cursor")).unwrap();
    fs::create_dir_all(home.path().join(".config/mac-worker/env")).unwrap();
    fs::write(
        home.path().join(".zprofile"),
        "export PATH=\"$HOME/bin:/usr/bin:/bin\"\nexport CURSOR_API_KEY=login-key\n",
    )
    .unwrap();
    fs::write(home.path().join(".cursor/cli-config.json"),
        r#"{"model":{"modelId":"remembered-model"},"selectedModel":{"modelId":"remembered-model","parameters":[]},"unrelated":true}"#).unwrap();
    let binary = home.path().join("bin/cursor-agent");
    fs::write(&binary, format!(r#"#!/bin/sh
[ "${{CURSOR_API_KEY-}}" = '{expected_key}' ] || exit 21
[ -z "${{OPENAI_API_KEY-}}" ] || exit 22
[ -z "${{MAC_WORKER_KEYCHAIN_PASSWORD-}}" ] || exit 23
[ -z "${{MAC_WORKER_KEYCHAIN_PATH-}}" ] || exit 24
[ -z "${{MAC_WORKER_CURSOR_CATALOG_API_KEY-}}" ] || exit 25
printf called > "$HOME/catalogue-called"
{}
IFS= read -r initialize || exit 26
printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{"protocolVersion":1}}}}'
IFS= read -r catalogue || exit 27
printf '%s\n' '{{"jsonrpc":"2.0","id":2,"result":{{"models":[{{"value":"catalogue-model","name":"Catalogue Model","configOptions":[]}}]}}}}'
sleep 60
"#, if catalogue { "" } else { "exit 1" })).unwrap();
    fs::set_permissions(binary, fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["codex", "opencode"] {
        let binary = home.path().join("bin").join(name);
        fs::write(&binary, "#!/bin/sh\nexit 1\n").unwrap();
        fs::set_permissions(binary, fs::Permissions::from_mode(0o700)).unwrap();
    }
    home
}

fn profile(home: &TempDir, contents: &str, mode: u32) {
    let path = home.path().join(".config/mac-worker/env/agents.env");
    fs::write(&path, contents).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

fn host(home: &TempDir, runner: &KeychainRunner, operation: &str, body: &[u8]) -> (u8, Value) {
    let _serial = HOST_SETTINGS_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let runtime = RuntimeContext::isolated(
        BTreeMap::from([
            (
                OsString::from("CURSOR_API_KEY"),
                OsString::from("runtime-key"),
            ),
            (
                OsString::from("MAC_WORKER_KEYCHAIN_PASSWORD"),
                OsString::from("runtime-password"),
            ),
            (
                OsString::from("MAC_WORKER_KEYCHAIN_PATH"),
                OsString::from("runtime-keychain"),
            ),
        ]),
        home.path().to_owned(),
        home.path().to_owned(),
    );
    let cli = Cli::try_parse_from(["worker", "host", operation]).unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        runner,
        &runtime,
        &mut Cursor::new(body),
        &mut stdout,
        &mut stderr,
    );
    assert!(stderr.is_empty());
    (exit, serde_json::from_slice(&stdout).unwrap())
}

fn cursor(response: &Value) -> &Value {
    response["agents"]
        .as_array()
        .expect("settings list")
        .iter()
        .find(|entry| entry["agent"] == "cursor")
        .unwrap()
}

#[test]
fn optional_profile_wire_preserves_old_canonical_requests() {
    for payload in [json!({}), json!({"env_profile": null})] {
        let request: AgentSettingsGetRequest = serde_json::from_value(payload).unwrap();
        assert_eq!(serde_json::to_string(&request).unwrap(), "{}");
    }
    let get: AgentSettingsGetRequest =
        serde_json::from_value(json!({"env_profile": "agents"})).unwrap();
    assert_eq!(
        serde_json::to_value(get).unwrap(),
        json!({"env_profile": "agents"})
    );

    let old = r#"{"agent":"cursor","model":null,"effort":null,"fast":null,"revision":"aa"}"#;
    for payload in [
        old.to_owned(),
        old.replace(
            "\"revision\":\"aa\"",
            "\"revision\":\"aa\",\"env_profile\":null",
        ),
    ] {
        let request: AgentSettingsSaveRequest = serde_json::from_str(&payload).unwrap();
        assert_eq!(serde_json::to_string(&request).unwrap(), old);
    }
    let selected = old.replace(
        "\"revision\":\"aa\"",
        "\"revision\":\"aa\",\"env_profile\":\"agents\"",
    );
    let request: AgentSettingsSaveRequest = serde_json::from_str(&selected).unwrap();
    assert_eq!(serde_json::to_string(&request).unwrap(), selected);
}

#[test]
fn selected_profile_authenticates_catalogue_after_login_without_exporting_other_secrets() {
    let home = fixture("selected-key", true);
    profile(
        &home,
        "CURSOR_API_KEY=selected-key\nOPENAI_API_KEY=unrelated-key\nMAC_WORKER_KEYCHAIN_PASSWORD=fixture-unlock-password\n",
        0o600,
    );
    let before = fs::read(home.path().join(".cursor/cli-config.json")).unwrap();
    let runner = KeychainRunner::new(true);
    let (exit, response) = host(
        &home,
        &runner,
        "agent-settings-get",
        br#"{"env_profile":"agents"}"#,
    );
    assert_eq!(exit, 0, "{response}");
    let entry = cursor(&response);
    assert_eq!(entry["model_catalog_source"], "live");
    assert_eq!(entry["model_catalog_profile"], "agents");
    assert!(
        entry["model_options"]
            .as_array()
            .unwrap()
            .iter()
            .any(|option| option["id"] == "catalogue-model")
    );
    assert!(
        response["agents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["agent"] != "cursor")
            .all(|entry| entry.get("model_catalog_profile").is_none())
    );
    assert_eq!(
        fs::read(home.path().join(".cursor/cli-config.json")).unwrap(),
        before
    );
    #[cfg(target_os = "macos")]
    {
        let calls = runner.requests.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].stdin.as_deref(),
            Some(b"fixture-unlock-password\n".as_slice())
        );
        assert_eq!(calls[0].policy.deadline, Duration::from_secs(10));
        assert!(calls[0].args.iter().all(|argument| {
            !argument
                .to_string_lossy()
                .contains("fixture-unlock-password")
        }));
    }
    let wire = response.to_string();
    for secret in [
        "selected-key",
        "unrelated-key",
        "fixture-unlock-password",
        "runtime-password",
    ] {
        assert!(!wire.contains(secret));
    }
}

#[test]
fn selected_keychain_profile_does_not_inherit_another_api_key() {
    let home = fixture("", true);
    profile(
        &home,
        "MAC_WORKER_KEYCHAIN_PASSWORD=fixture-unlock-password\n",
        0o600,
    );
    let (exit, response) = host(
        &home,
        &KeychainRunner::new(true),
        "agent-settings-get",
        br#"{"env_profile":"agents"}"#,
    );
    assert_eq!(exit, 0, "{response}");
    assert_eq!(cursor(&response)["model_catalog_source"], "live");
}

#[test]
fn ordinary_catalogue_failure_keeps_the_selected_profile_on_remembered_models() {
    let home = fixture("selected-key", false);
    profile(&home, "CURSOR_API_KEY=selected-key\n", 0o600);
    let (exit, response) = host(
        &home,
        &KeychainRunner::new(true),
        "agent-settings-get",
        br#"{"env_profile":"agents"}"#,
    );
    assert_eq!(exit, 0, "{response}");
    assert_eq!(cursor(&response)["model_catalog_source"], "remembered");
    assert_eq!(cursor(&response)["model_catalog_profile"], "agents");
    assert_eq!(cursor(&response)["model"], "remembered-model");
    assert!(home.path().join("catalogue-called").exists());
}

#[test]
fn unavailable_selected_profile_fails_before_catalogue_or_unlock() {
    for (contents, mode) in [
        (None, 0o600),
        (Some("CURSOR_API_KEY=selected-key\n"), 0o644),
        (Some("OPENAI_API_KEY=unrelated-key\n"), 0o600),
        (Some("CURSOR_API_KEY=\n"), 0o600),
        (Some("MAC_WORKER_KEYCHAIN_PASSWORD=\n"), 0o600),
        (Some("invalid-secret-line"), 0o600),
    ] {
        let home = fixture("selected-key", true);
        if let Some(contents) = contents {
            profile(&home, contents, mode);
        }
        let runner = KeychainRunner::new(true);
        let (exit, response) = host(
            &home,
            &runner,
            "agent-settings-get",
            br#"{"env_profile":"agents"}"#,
        );
        assert_ne!(exit, 0);
        assert_eq!(
            response["error"]["code"], "SETTINGS_UNAVAILABLE",
            "{response}"
        );
        assert!(!response.to_string().contains("invalid-secret-line"));
        assert!(!response.to_string().contains("agents.env"));
        assert!(!home.path().join("catalogue-called").exists());
        assert!(runner.requests.lock().unwrap().is_empty());
    }
}

#[test]
fn invalid_profile_names_are_rejected_on_both_host_operations() {
    let home = fixture("selected-key", true);
    profile(
        &home,
        "MAC_WORKER_KEYCHAIN_PASSWORD=fixture-unlock-password\n",
        0o600,
    );
    let runner = KeychainRunner::new(true);
    for name in [
        "",
        ".",
        "..",
        "../agents",
        "dir/agents",
        "dir\\agents",
        "agents\n",
        &"a".repeat(129),
    ] {
        let encoded = serde_json::to_string(name).unwrap();
        let get = format!("{{\"env_profile\":{encoded}}}");
        let save = format!(
            "{{\"agent\":\"cursor\",\"model\":null,\"effort\":null,\"fast\":null,\"revision\":\"aa\",\"env_profile\":{encoded}}}"
        );
        for (operation, body) in [("agent-settings-get", get), ("agent-settings-set", save)] {
            let (exit, response) = host(&home, &runner, operation, body.as_bytes());
            assert_ne!(exit, 0);
            assert_eq!(response["error"]["code"], "SETTINGS_INVALID");
        }
    }
    assert!(!home.path().join("catalogue-called").exists());
    assert!(runner.requests.lock().unwrap().is_empty());
}

#[test]
fn cursor_save_uses_and_returns_the_selected_catalogue_profile() {
    let home = fixture("selected-key", true);
    profile(&home, "CURSOR_API_KEY=selected-key\n", 0o600);
    let revision = NativeAgentSettingsStore::new(home.path())
        .read("cursor")
        .unwrap()
        .revision
        .unwrap();
    let body = format!(
        "{{\"agent\":\"cursor\",\"model\":\"catalogue-model\",\"effort\":null,\"fast\":null,\"revision\":\"{revision}\",\"env_profile\":\"agents\"}}"
    );
    let (exit, response) = host(
        &home,
        &KeychainRunner::new(true),
        "agent-settings-set",
        body.as_bytes(),
    );
    assert_eq!(exit, 0, "{response}");
    assert_eq!(response["model"], "catalogue-model");
    assert_eq!(response["model_catalog_profile"], "agents");
    assert_eq!(response["model_catalog_source"], "live");
    assert_eq!(
        NativeAgentSettingsStore::new(home.path())
            .read("cursor")
            .unwrap()
            .model
            .as_deref(),
        Some("catalogue-model")
    );
}

#[test]
fn other_agent_save_does_not_load_or_unlock_the_cursor_profile() {
    let home = fixture("selected-key", true);
    let runner = KeychainRunner::new(false);
    let revision = NativeAgentSettingsStore::new(home.path())
        .read("claude")
        .unwrap()
        .revision
        .unwrap();
    let body = format!(
        "{{\"agent\":\"claude\",\"model\":\"claude-fixture\",\"effort\":null,\"fast\":null,\"revision\":\"{revision}\",\"env_profile\":\"missing\"}}"
    );
    let (exit, response) = host(&home, &runner, "agent-settings-set", body.as_bytes());
    assert_eq!(exit, 0, "{response}");
    assert_eq!(response["model"], "claude-fixture");
    assert!(response.get("model_catalog_profile").is_none());
    assert!(runner.requests.lock().unwrap().is_empty());
    assert!(!home.path().join("catalogue-called").exists());
}

#[cfg(target_os = "macos")]
#[test]
fn failed_selected_keychain_unlock_does_not_discover_or_save() {
    let home = fixture("selected-key", true);
    profile(
        &home,
        "CURSOR_API_KEY=selected-key\nMAC_WORKER_KEYCHAIN_PASSWORD=fixture-unlock-password\n",
        0o600,
    );
    let before = fs::read(home.path().join(".cursor/cli-config.json")).unwrap();
    let revision = NativeAgentSettingsStore::new(home.path())
        .read("cursor")
        .unwrap()
        .revision
        .unwrap();
    let body = format!(
        "{{\"agent\":\"cursor\",\"model\":\"catalogue-model\",\"effort\":null,\"fast\":null,\"revision\":\"{revision}\",\"env_profile\":\"agents\"}}"
    );
    let runner = KeychainRunner::new(false);
    let (exit, response) = host(&home, &runner, "agent-settings-set", body.as_bytes());
    assert_ne!(exit, 0);
    assert_eq!(response["error"]["code"], "SETTINGS_UNAVAILABLE");
    assert_eq!(runner.requests.lock().unwrap().len(), 1);
    for secret in ["selected-key", "fixture-unlock-password", "unrelated-key"] {
        assert!(!response.to_string().contains(secret));
    }
    assert_eq!(
        fs::read(home.path().join(".cursor/cli-config.json")).unwrap(),
        before
    );
    assert!(!home.path().join("catalogue-called").exists());
}

/// Fixture line that answers `--version` like OpenCode 1. Settings asks
/// OpenCode for its version before it lists models, and lists them only on
/// v1; Codex is never asked, so the line is inert in its fixture.
const OPENCODE_V1_VERSION: &str = "[ \"$*\" = --version ] && { printf '1.18.32\\n'; exit 0; }";

fn install_binary(home: &TempDir, name: &str, script: &str) {
    let path = home.path().join("bin").join(name);
    fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

#[test]
fn settings_get_discovers_all_three_catalogs_concurrently_without_profile_leaks() {
    let home = fixture("selected-key", true);
    profile(
        &home,
        "CURSOR_API_KEY=selected-key\nOPENAI_API_KEY=unrelated-key\n",
        0o600,
    );
    // Every catalog must start before any one returns. A sequential endpoint
    // exhausts the fixture barrier and fails the live-source assertions.
    fs::write(home.path().join("catalogue-barrier.sh"), r#"
attempt=0
while [ ! -f "$HOME/catalogue-called" ] || [ ! -f "$HOME/codex-called" ] || [ ! -f "$HOME/opencode-called" ]; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 500 ] || exit 31
    sleep 0.01
done
"#).unwrap();
    let cursor_path = home.path().join("bin/cursor-agent");
    let cursor_script = fs::read_to_string(&cursor_path).unwrap().replace(
        "printf called > \"$HOME/catalogue-called\"",
        "printf called > \"$HOME/catalogue-called\"\n. \"$HOME/catalogue-barrier.sh\"",
    );
    fs::write(cursor_path, cursor_script).unwrap();
    for (name, arguments, output) in [
        (
            "codex",
            "debug models",
            r#"{"models":[{"slug":"fresh-codex","visibility":"list","additional_speed_tiers":["fast"]}]}"#,
        ),
        ("opencode", "models", "opencode-go/fresh\nzai/fresh"),
    ] {
        install_binary(
            &home,
            name,
            &format!(
                r#"
[ -z "${{OPENAI_API_KEY-}}" ] || exit 22
[ -z "${{MAC_WORKER_KEYCHAIN_PASSWORD-}}" ] || exit 23
[ -z "${{MAC_WORKER_KEYCHAIN_PATH-}}" ] || exit 24
{OPENCODE_V1_VERSION}
[ "$*" = '{arguments}' ] || exit 21
printf called > "$HOME/{name}-called"
. "$HOME/catalogue-barrier.sh"
printf '%s\n' '{output}'
"#
            ),
        );
    }
    let (exit, response) = host(
        &home,
        &KeychainRunner::new(true),
        "agent-settings-get",
        br#"{"env_profile":"agents"}"#,
    );
    assert_eq!(exit, 0, "{response}");
    for (agent, expected) in [
        ("cursor", "catalogue-model"),
        ("codex", "fresh-codex"),
        ("opencode", "opencode-go/fresh"),
    ] {
        let setting = response["agents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["agent"] == agent)
            .unwrap();
        assert_eq!(setting["model_catalog_source"], "live", "{setting}");
        assert_eq!(setting["model_options"][0]["id"], expected);
        if agent != "cursor" {
            assert!(setting.get("model_catalog_profile").is_none());
        }
    }
}

#[test]
fn codex_save_revalidates_fast_against_live_catalog_without_loading_cursor_profile() {
    let home = fixture("selected-key", true);
    install_binary(
        &home,
        "codex",
        r#"
[ "$*" = 'debug models' ] || exit 21
[ -z "${OPENAI_API_KEY-}" ] || exit 22
[ -z "${MAC_WORKER_KEYCHAIN_PASSWORD-}" ] || exit 23
[ -z "${MAC_WORKER_KEYCHAIN_PATH-}" ] || exit 24
printf called > "$HOME/codex-called"
printf '%s\n' '{"models":[{"slug":"fresh-codex","visibility":"list","additional_speed_tiers":["fast"]}]}'
"#,
    );
    install_binary(
        &home,
        "opencode",
        "printf unexpected > \"$HOME/opencode-called\"\nexit 1",
    );
    let revision = NativeAgentSettingsStore::new(home.path())
        .read("codex")
        .unwrap()
        .revision
        .unwrap();
    let request = AgentSettingsSaveRequest {
        agent: "codex".into(),
        model: Some("fresh-codex".into()),
        effort: None,
        fast: Some(true),
        revision,
        env_profile: Some("missing".into()),
    };
    let runner = KeychainRunner::new(false);
    let (exit, response) = host(
        &home,
        &runner,
        "agent-settings-set",
        &serde_json::to_vec(&request).unwrap(),
    );
    assert_eq!(exit, 0, "{response}");
    assert_eq!(response["model"], "fresh-codex");
    assert_eq!(response["fast"], true);
    assert_eq!(response["model_catalog_source"], "live");
    assert!(response.get("model_catalog_profile").is_none());
    assert!(runner.requests.lock().unwrap().is_empty());
    assert!(home.path().join("codex-called").exists());
    assert!(!home.path().join("catalogue-called").exists());
    assert!(!home.path().join("opencode-called").exists());
    assert!(
        fs::read_to_string(home.path().join(".codex/config.toml"))
            .unwrap()
            .contains("service_tier = \"fast\"")
    );
}

#[test]
fn opencode_save_does_not_discover_or_load_a_profile() {
    let home = fixture("selected-key", true);
    for name in ["codex", "opencode"] {
        install_binary(
            &home,
            name,
            &format!("printf unexpected > \"$HOME/{name}-called\"\nexit 1"),
        );
    }
    let revision = NativeAgentSettingsStore::new(home.path())
        .read("opencode")
        .unwrap()
        .revision
        .unwrap();
    let request = AgentSettingsSaveRequest {
        agent: "opencode".into(),
        model: Some("provider/custom-model".into()),
        effort: None,
        fast: None,
        revision,
        env_profile: Some("missing".into()),
    };
    let runner = KeychainRunner::new(false);
    let (exit, response) = host(
        &home,
        &runner,
        "agent-settings-set",
        &serde_json::to_vec(&request).unwrap(),
    );
    assert_eq!(exit, 0, "{response}");
    assert_eq!(response["model"], "provider/custom-model");
    assert_eq!(response["model_catalog_source"], "remembered");
    assert!(runner.requests.lock().unwrap().is_empty());
    for marker in ["codex-called", "opencode-called", "catalogue-called"] {
        assert!(!home.path().join(marker).exists());
    }
}

#[test]
fn settings_get_runs_catalogs_outside_the_callers_project_directory() {
    let _serial = HOST_SETTINGS_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let home = fixture("login-key", true);
    let project = home.path().join("project");
    fs::create_dir_all(project.join(".codex")).unwrap();
    fs::write(project.join("opencode.json"), r#"{"model":"project-only"}"#).unwrap();
    for (name, output) in [
        (
            "codex",
            r#"{"models":[{"slug":"account-codex","visibility":"list"}]}"#,
        ),
        ("opencode", "provider/account-model"),
    ] {
        install_binary(
            &home,
            name,
            &format!(
                r#"
[ ! -e opencode.json ] && [ ! -e .codex ] || exit 21
[ -z "$(ls -A)" ] || exit 22
{OPENCODE_V1_VERSION}
printf '%s' "$PWD" > "$HOME/{name}-cwd"
printf '%s\n' '{output}'
"#
            ),
        );
    }
    let output = assert_cmd::Command::new(env!("CARGO_BIN_EXE_worker"))
        .args(["host", "agent-settings-get"])
        .current_dir(&project)
        .env_clear()
        .env("HOME", home.path())
        .env("PATH", "/usr/bin:/bin")
        .write_stdin("{}")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    for agent in ["codex", "opencode"] {
        let settings = response["agents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["agent"] == agent)
            .unwrap();
        assert_eq!(settings["model_catalog_source"], "live", "{settings}");
        let cwd = fs::read_to_string(home.path().join(format!("{agent}-cwd"))).unwrap();
        assert_ne!(std::path::Path::new(&cwd), project);
        assert!(!std::path::Path::new(&cwd).exists());
    }
    assert_eq!(
        fs::read_to_string(project.join("opencode.json")).unwrap(),
        r#"{"model":"project-only"}"#
    );
}
