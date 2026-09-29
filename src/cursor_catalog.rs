//! Read the worker account's Cursor model catalogue without creating a session.

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::{fs::DirBuilderExt, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::{Value, json};

use crate::{
    agent_settings::{AgentSettingsError, validate_env_profile},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    turn::EnvProfile,
};

const CURSOR_AUTH_ENV: &str = "CURSOR_API_KEY";
const STAGED_AUTH_ENV: &str = "MAC_WORKER_CURSOR_CATALOG_API_KEY";

pub(crate) fn discover_for_profile(
    home: &Path,
    environment: &BTreeMap<OsString, OsString>,
    profile_name: Option<&str>,
    runner: &dyn ProcessRunner,
) -> Result<Option<Value>, AgentSettingsError> {
    validate_env_profile(profile_name)?;
    let Some(name) = profile_name else {
        return Ok(discover(home, environment));
    };
    let unavailable =
        || AgentSettingsError::Unavailable("selected environment profile is unavailable");
    let profile = EnvProfile::load_for_home(
        &home
            .join(".config/mac-worker/env")
            .join(format!("{name}.env")),
        home,
    )
    .map_err(|_| unavailable())?;
    // Cursor's adapter allows only this authentication variable. Other agent
    // credentials and reserved host Keychain values stay out of the child.
    let api_key = profile
        .entries()
        .iter()
        .find_map(|(key, value)| (key == CURSOR_AUTH_ENV && !value.is_empty()).then_some(value));
    if api_key.is_none()
        && !profile
            .keychain()
            .is_some_and(|config| !config.password().is_empty())
    {
        return Err(unavailable());
    }
    if let Some(config) = profile.keychain() {
        crate::keychain::unlock_keychain_if_supported(
            runner,
            config,
            &profile.redaction_boundary(home),
        )
        .map_err(|_| unavailable())?;
    }
    let mut environment = environment.clone();
    environment.remove(std::ffi::OsStr::new(CURSOR_AUTH_ENV));
    if let Some(value) = api_key {
        environment.insert(CURSOR_AUTH_ENV.into(), value.clone());
    }
    Ok(discover_with_environment(home, &environment, true))
}

/// Uses the account's existing authentication; never initiates login or inference.
/// Unsupported CLI versions and unavailable catalogues have no fallback snapshot.
pub fn discover(home: &Path, environment: &BTreeMap<OsString, OsString>) -> Option<Value> {
    discover_with_environment(home, environment, false)
}

fn discover_with_environment(
    home: &Path,
    environment: &BTreeMap<OsString, OsString>,
    selected_profile: bool,
) -> Option<Value> {
    let scratch = DiscoveryDirectory::create()?;
    let scratch_text = scratch.0.to_str()?;
    // Apply isolation after login-shell startup, which can otherwise overwrite
    // environment entries. HOME still identifies the worker's Keychain account.
    let argv = vec![
        "/usr/bin/env".to_owned(),
        format!("CURSOR_CONFIG_DIR={scratch_text}"),
        format!("CURSOR_DATA_DIR={scratch_text}"),
        "NO_OPEN_BROWSER=1".to_owned(),
        "cursor-agent".to_owned(),
        "acp".to_owned(),
    ];
    let entries = environment
        .iter()
        .filter(|(key, _)| !crate::keychain::is_reserved_env_name(&key.to_string_lossy()))
        .filter(|(key, _)| *key != STAGED_AUTH_ENV)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let mut request = crate::agent::prebind_login_request(&argv, home, &entries).ok()?;
    // Login startup can replace CURSOR_API_KEY. Stage the chosen value in the
    // environment and restore it via a shell variable, never literal argv.
    let mut prefix = "unset MAC_WORKER_KEYCHAIN_PASSWORD MAC_WORKER_KEYCHAIN_PATH; ".to_owned();
    if let Some(api_key) = environment.get(std::ffi::OsStr::new(CURSOR_AUTH_ENV)) {
        request
            .environment
            .push((STAGED_AUTH_ENV.into(), api_key.clone()));
        prefix.push_str(&format!(
            "export {CURSOR_AUTH_ENV}=\"${STAGED_AUTH_ENV}\"; "
        ));
    } else if selected_profile {
        // A Keychain-only selection must not use a different account's API key.
        prefix.push_str(&format!("unset {CURSOR_AUTH_ENV}; "));
    }
    prefix.push_str(&format!("unset {STAGED_AUTH_ENV}; "));
    prefix.push_str(request.args.get(1)?.to_str()?);
    request.args[1] = prefix.into();
    request.policy = ProcessPolicy {
        stdout_limit: 1024 * 1024,
        stderr_limit: 64 * 1024,
        // mini-3 takes 8.5–9.3 s warm / 10.4 s cold; outer SSH deadline is 30 s.
        deadline: Duration::from_secs(20),
    };
    exchange(&request, &scratch.0)
}

struct DiscoveryDirectory(PathBuf);

impl DiscoveryDirectory {
    fn create() -> Option<Self> {
        let path = std::env::temp_dir().join(format!(
            "mac-worker-cursor-catalog-{}",
            uuid::Uuid::new_v4()
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).ok()?;
        Some(Self(path))
    }
}

impl Drop for DiscoveryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct CatalogProcess(Child);

impl Drop for CatalogProcess {
    fn drop(&mut self) {
        // Do not reap before signalling: retaining the owned leader prevents
        // its PID (and saved process-group ID) from being reused during cleanup.
        // Keep stdin open until after signalling; EOF can abort a pending ACP reply.
        unsafe {
            libc::killpg(self.0.id() as libc::pid_t, libc::SIGKILL);
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn nonblocking(pipe: &impl AsRawFd) -> Option<()> {
    let fd = pipe.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return None;
    }
    Some(())
}

/// Drains only immediately available bytes. Total limits also bound an
/// unterminated JSON line, without reader threads that descendants can strand.
fn drain(
    pipe: &mut impl Read,
    mut destination: Option<&mut Vec<u8>>,
    total: &mut usize,
    limit: usize,
    deadline: Instant,
) -> Option<bool> {
    let mut buffer = [0_u8; 8192];
    loop {
        if Instant::now() >= deadline {
            return None;
        }
        match pipe.read(&mut buffer) {
            Ok(0) => return Some(true),
            Ok(length) => {
                *total = total.checked_add(length)?;
                if *total > limit {
                    return None;
                }
                if let Some(bytes) = destination.as_deref_mut() {
                    bytes.extend_from_slice(&buffer[..length]);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Some(false),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return None,
        }
    }
}

fn request_line(id: u8, method: &str, params: Value) -> Vec<u8> {
    let mut bytes = json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params})
        .to_string()
        .into_bytes();
    bytes.push(b'\n');
    bytes
}

fn exchange(request: &ProcessRequest, cwd: &Path) -> Option<Value> {
    let deadline = Instant::now().checked_add(request.policy.deadline)?;
    let mut command = Command::new(&request.program);
    if request.isolate_parent_environment {
        command.env_clear();
    }
    command
        .args(&request.args)
        .envs(request.environment.iter().map(|(key, value)| (key, value)));
    for key in &request.environment_remove {
        command.env_remove(key);
    }
    command
        .current_dir(cwd)
        .process_group(0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut process = CatalogProcess(command.spawn().ok()?);
    nonblocking(process.0.stdin.as_ref()?)?;
    nonblocking(process.0.stdout.as_ref()?)?;
    nonblocking(process.0.stderr.as_ref()?)?;
    let mut pending = request_line(
        1,
        "initialize",
        json!({
            "protocolVersion":1,
            "clientCapabilities":{"fs":{"readTextFile":false,"writeTextFile":false},"terminal":false},
            "clientInfo":{"name":"mac-worker-model-catalog","version":env!("CARGO_PKG_VERSION")}
        }),
    );
    let mut written = 0;
    let mut expected_id = 1_u8;
    let mut output = Vec::new();
    let mut stdout_total = 0;
    let mut stderr_total = 0;
    loop {
        if Instant::now() >= deadline {
            return None;
        }
        if written < pending.len() {
            match process.0.stdin.as_mut()?.write(&pending[written..]) {
                Ok(0) => return None,
                Ok(length) => written += length,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) => {}
                Err(_) => return None,
            }
        }
        drain(
            process.0.stderr.as_mut()?,
            None,
            &mut stderr_total,
            request.policy.stderr_limit,
            deadline,
        )?;
        let stdout_closed = drain(
            process.0.stdout.as_mut()?,
            Some(&mut output),
            &mut stdout_total,
            request.policy.stdout_limit,
            deadline,
        )?;
        while let Some(end) = output.iter().position(|byte| *byte == b'\n') {
            if Instant::now() >= deadline {
                return None;
            }
            let message: Value = serde_json::from_slice(&output[..end]).ok()?;
            output.drain(..=end);
            if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
                return None;
            }
            if message.get("id").and_then(Value::as_u64) != Some(u64::from(expected_id)) {
                continue;
            }
            if written != pending.len() || message.get("error").is_some() {
                return None;
            }
            let result = message.get("result")?.as_object()?;
            if expected_id == 1 {
                if result.get("protocolVersion").and_then(Value::as_u64) != Some(1) {
                    return None;
                }
                pending = request_line(2, "cursor/list_available_models", json!({}));
                written = 0;
                expected_id = 2;
            } else {
                result.get("models")?.as_array()?;
                return Some(Value::Object(result.clone()));
            }
        }
        if stdout_closed {
            return None;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::ProcessPolicy;
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        time::{Duration, Instant},
    };

    const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}"#;
    const CATALOG: &str = r#"{"jsonrpc":"2.0","id":2,"result":{"models":[{"value":"native-model","name":"Native Model","configOptions":[{"id":"reasoning_effort","type":"select","currentValue":"high","options":[{"value":"low","name":"Low"},{"value":"high","name":"High"}]}]}]}}"#;

    fn fixture(script: &str, policy: ProcessPolicy) -> (tempfile::TempDir, ProcessRequest) {
        let temp = tempfile::tempdir().unwrap();
        let request = ProcessRequest {
            program: OsString::from("/bin/sh"),
            args: vec![OsString::from("-c"), OsString::from(script)],
            environment: vec![(OsString::from("PATH"), OsString::from("/bin:/usr/bin"))],
            environment_remove: vec![],
            stdin: None,
            policy,
            isolate_parent_environment: true,
        };
        (temp, request)
    }

    fn policy() -> ProcessPolicy {
        ProcessPolicy {
            stdout_limit: 16 * 1024,
            stderr_limit: 4096,
            deadline: Duration::from_secs(2),
        }
    }

    fn handshake(tail: &str) -> String {
        format!(
            "IFS= read -r first || exit 11\ncase \"$first\" in *'\"method\":\"initialize\"'*) ;; *) exit 12 ;; esac\nprintf '%s\\n' '{INITIALIZE}'\nIFS= read -r second || exit 13\ncase \"$second\" in *'\"method\":\"cursor/list_available_models\"'*) ;; *) exit 14 ;; esac\n{tail}"
        )
    }

    fn assert_reaped(temp: &tempfile::TempDir) {
        let pid: i32 = fs::read_to_string(temp.path().join("leader.pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "discovery child {pid} remains alive"
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) },
            -1
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
    }

    #[test]
    fn sends_only_initialize_then_catalog_and_keeps_stdin_open_until_response() {
        let tail = format!(
            "printf '%s\\n' '{CATALOG}'\nIFS= read -r unexpected\nprintf '%s' \"$unexpected\" > unexpected.txt\nexit 15"
        );
        let script = format!("printf '%s' $$ > leader.pid\n{}", handshake(&tail));
        let (temp, request) = fixture(&script, policy());
        let result = exchange(&request, temp.path()).expect("catalogue response");
        assert_eq!(result["models"][0]["value"], "native-model");
        assert_eq!(
            result["models"][0]["configOptions"][0]["id"],
            "reasoning_effort"
        );
        assert_eq!(
            result["models"][0]["configOptions"][0]["currentValue"],
            "high"
        );
        assert!(
            !temp.path().join("unexpected.txt").exists(),
            "stdin closed before the child was terminated"
        );
        assert_reaped(&temp);
    }

    #[test]
    fn accepts_catalogue_from_a_process_that_exits_cleanly() {
        let script = format!(
            "printf '%s' $$ > leader.pid\n{}",
            handshake(&format!("printf '%s\\n' '{CATALOG}'\nexit 0"))
        );
        let (temp, request) = fixture(&script, policy());
        assert!(exchange(&request, temp.path()).is_some());
        assert_reaped(&temp);
    }

    #[test]
    fn waits_for_initialize_response_before_requesting_the_catalogue() {
        let script = format!(
            "IFS= read -r first || exit 11\nif IFS= read -r -t 0.05 early; then exit 12; fi\nprintf '%s\\n' '{INITIALIZE}'\nIFS= read -r second || exit 13\ncase \"$second\" in *'\"method\":\"cursor/list_available_models\"'*) ;; *) exit 14 ;; esac\nprintf '%s\\n' '{CATALOG}'\n"
        );
        let (temp, mut request) = fixture(&script, policy());
        request.program = OsString::from("/bin/zsh");
        assert!(exchange(&request, temp.path()).is_some());
    }

    #[test]
    fn preserves_catalogue_json_larger_than_a_pipe_read() {
        let response = json!({"jsonrpc":"2.0", "id":2, "result":{"models":[{"value":"large-model", "name":"x".repeat(20_000)}]}});
        let script = handshake(&format!("printf '%s\\n' '{response}'\n"));
        let (temp, request) = fixture(
            &script,
            ProcessPolicy {
                stdout_limit: 32 * 1024,
                ..policy()
            },
        );
        let result = exchange(&request, temp.path()).expect("complete multi-read JSON");
        assert_eq!(result["models"][0]["value"], "large-model");
        assert_eq!(result["models"][0]["name"].as_str().unwrap().len(), 20_000);
    }

    #[test]
    fn eof_before_catalogue_returns_without_waiting_for_deadline() {
        let script = format!("printf '%s' $$ > leader.pid\n{}", handshake("exit 0"));
        let (temp, request) = fixture(&script, policy());
        let started = Instant::now();
        assert!(exchange(&request, temp.path()).is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_reaped(&temp);
    }

    #[test]
    fn times_out_and_reaps_the_process() {
        let script = format!("printf '%s' $$ > leader.pid\n{}", handshake("sleep 60"));
        let (temp, request) = fixture(
            &script,
            ProcessPolicy {
                deadline: Duration::from_millis(100),
                ..policy()
            },
        );
        let started = Instant::now();
        assert!(exchange(&request, temp.path()).is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_reaped(&temp);
    }

    #[test]
    fn rejects_malformed_error_and_non_catalogue_responses() {
        for response in [
            "not-json",
            r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"unsupported"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"result":{"models":"wrong"}}"#,
        ] {
            let script = format!(
                "printf '%s' $$ > leader.pid\n{}",
                handshake(&format!("printf '%s\\n' '{response}'\nsleep 60"))
            );
            let (temp, request) = fixture(&script, policy());
            assert!(exchange(&request, temp.path()).is_none());
            assert_reaped(&temp);
        }
    }

    #[test]
    fn rejects_oversized_stdout_without_a_newline_and_stderr() {
        for stream in ["", " >&2"] {
            let script = format!(
                "printf '%s' $$ > leader.pid\n{}",
                handshake(&format!(
                    "while :; do printf 'xxxxxxxxxxxxxxxx'{stream}; done"
                ))
            );
            let (temp, request) = fixture(
                &script,
                ProcessPolicy {
                    stdout_limit: 1024,
                    stderr_limit: 128,
                    ..policy()
                },
            );
            let started = Instant::now();
            assert!(exchange(&request, temp.path()).is_none());
            assert!(started.elapsed() < Duration::from_secs(1));
            assert_reaped(&temp);
        }
    }

    #[test]
    fn discover_isolates_cursor_files_but_preserves_worker_environment() {
        let home = tempfile::tempdir().unwrap();
        let native = home.path().join(".cursor");
        let bin = home.path().join("bin");
        fs::create_dir(&native).unwrap();
        fs::create_dir(&bin).unwrap();
        fs::write(native.join("cli-config.json"), "native-config").unwrap();
        fs::write(
            home.path().join(".zprofile"),
            "export PATH=\"$FIXTURE_BIN:/usr/bin:/bin\"\n",
        )
        .unwrap();
        let script = format!(
            "#!/bin/sh\n[ \"$HOME\" = \"$EXPECTED_HOME\" ] || exit 21\n[ \"$NO_OPEN_BROWSER\" = 1 ] || exit 22\nprintf '%s' \"$CURSOR_CONFIG_DIR\" > \"$DISCOVERY_RECORD\"\nprintf isolated > \"$CURSOR_CONFIG_DIR/cli-config.json\"\nprintf isolated > \"$CURSOR_DATA_DIR/probe.txt\"\n{}",
            handshake(&format!("printf '%s\\n' '{CATALOG}'\nsleep 60"))
        );
        let binary = bin.join("cursor-agent");
        fs::write(&binary, script).unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();
        let record = home.path().join("scratch-path");
        let environment = BTreeMap::from([
            (
                OsString::from("EXPECTED_HOME"),
                home.path().as_os_str().to_owned(),
            ),
            (OsString::from("FIXTURE_BIN"), bin.as_os_str().to_owned()),
            (
                OsString::from("DISCOVERY_RECORD"),
                record.as_os_str().to_owned(),
            ),
            (
                OsString::from("CURSOR_CONFIG_DIR"),
                native.as_os_str().to_owned(),
            ),
            (
                OsString::from("CURSOR_DATA_DIR"),
                native.as_os_str().to_owned(),
            ),
        ]);
        assert!(discover(home.path(), &environment).is_some());
        assert_eq!(
            fs::read_to_string(native.join("cli-config.json")).unwrap(),
            "native-config"
        );
        assert!(!native.join("probe.txt").exists());
        let scratch = fs::read_to_string(record).unwrap();
        assert!(
            !Path::new(&scratch).exists(),
            "temporary Cursor configuration was retained"
        );
    }

    #[test]
    fn deadline_terminates_descendants_that_hold_output_pipes() {
        let script = format!(
            "printf '%s' $$ > leader.pid\n{}",
            handshake("(sleep 0.4; printf leaked > descendant-ran) &\nwait")
        );
        let (temp, request) = fixture(
            &script,
            ProcessPolicy {
                deadline: Duration::from_millis(100),
                ..policy()
            },
        );
        assert!(exchange(&request, temp.path()).is_none());
        assert_reaped(&temp);
        std::thread::sleep(Duration::from_millis(450));
        assert!(
            !temp.path().join("descendant-ran").exists(),
            "catalogue descendant survived deadline cleanup"
        );
    }
}
