//! LaunchAgent lifecycle for the persistent controller.

use std::{
    ffi::OsString,
    io,
    path::{Component, Path},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    controller::leader::lock_exclusive,
    error::WorkerError,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    rooted_fs::{PrivateEntryIdentity, RootedDir},
};

pub const LABEL: &str = "com.mac-worker.controller";
const PLIST_NAME: &str = "com.mac-worker.controller.plist";
const LOG_NAME: &str = "controller.log";
const MAX_PLIST_BYTES: u64 = 64 * 1024;
// launchctl's missing-service code. Other failures (including an unavailable
// GUI domain) are unknown observations and must not authorize unload/reinstall.
const SERVICE_NOT_FOUND: i32 = 113;
const LAUNCHCTL_POLICY: ProcessPolicy = ProcessPolicy {
    stdout_limit: 1024 * 1024,
    stderr_limit: 64 * 1024,
    deadline: Duration::from_secs(30),
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceAction {
    Install,
    Restart,
    Uninstall,
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub label: String,
    pub domain: String,
    pub installed: bool,
    pub loaded: bool,
}

struct InstalledPlist {
    bytes: Vec<u8>,
    identity: PrivateEntryIdentity,
}

/// Manage only the caller's LaunchAgent. The host command supplies its effective
/// uid and actual home; no client-provided executable or launchctl arguments are
/// accepted. `loaded` is a launchd observation, not a claim that the leader is
/// healthy; controller health supplies that independent observation.
pub fn manage(
    home: &Path,
    uid: u32,
    runner: &dyn ProcessRunner,
    action: ServiceAction,
) -> Result<ServiceStatus, WorkerError> {
    validate_home(home)?;
    let create = action == ServiceAction::Install;
    let agents = open_directory_chain(home, &["Library", "LaunchAgents"], create)?;
    // Serialize cooperating installers while keeping status entirely read-only.
    let _lock = if action != ServiceAction::Status {
        agents
            .as_ref()
            .map(|agents| {
                let file = agents.open_private_lock(".mac-worker-controller.lock")?;
                lock_exclusive(&file)?;
                Ok::<_, WorkerError>(file)
            })
            .transpose()?
    } else {
        None
    };
    let previous = agents.as_ref().map(read_plist).transpose()?.flatten();
    let mut status = ServiceStatus {
        label: LABEL.to_owned(),
        domain: format!("gui/{uid}"),
        installed: previous.is_some(),
        loaded: is_loaded(runner, uid)?,
    };
    let target = format!("{}/{LABEL}", status.domain);
    match action {
        ServiceAction::Status => Ok(status),
        ServiceAction::Uninstall => {
            if status.loaded {
                run_checked(runner, &["bootout".into(), target.into()])?;
                if is_loaded(runner, uid)? {
                    return Err(service_error("service remained loaded after bootout"));
                }
            }
            if let (Some(agents), Some(previous)) = (agents.as_ref(), previous) {
                remove_plist(agents, previous.identity)?;
            }
            status.installed = false;
            status.loaded = false;
            Ok(status)
        }
        ServiceAction::Install | ServiceAction::Restart => {
            let agents = agents.ok_or_else(|| {
                service_error("service is not installed; run controller init first")
            })?;
            if action == ServiceAction::Restart && !status.installed {
                return Err(service_error(
                    "service is not installed; run controller init first",
                ));
            }
            let _logs = prepare_log(home)?;
            let desired = launchd_plist(home, None)?;
            let changed = previous.as_ref().map(|previous| previous.bytes.as_slice())
                != Some(desired.as_bytes());
            if changed {
                match previous {
                    Some(previous) => agents.replace_private_regular_exact(
                        PLIST_NAME,
                        &previous.bytes,
                        desired.as_bytes(),
                    )?,
                    None => {
                        agents.write_private_atomic_no_replace(PLIST_NAME, desired.as_bytes())?
                    }
                }
                if status.loaded {
                    run_checked(runner, &["bootout".into(), target.clone().into()])?;
                    status.loaded = false;
                }
            }
            if !status.loaded {
                agents.verify_bound()?;
                run_checked(
                    runner,
                    &[
                        "bootstrap".into(),
                        status.domain.clone().into(),
                        home.join("Library/LaunchAgents")
                            .join(PLIST_NAME)
                            .into_os_string(),
                    ],
                )?;
            }
            if action == ServiceAction::Restart || !status.loaded {
                run_checked(runner, &["kickstart".into(), "-k".into(), target.into()])?;
                if !is_loaded(runner, uid)? {
                    return Err(service_error("service did not remain loaded after start"));
                }
            }
            status.installed = true;
            status.loaded = true;
            Ok(status)
        }
    }
}

/// Called by `controller run --supervised` before leader acquisition on every
/// launchd start. Truncate the existing inode: replacing it would leave launchd
/// writing into its old open descriptor. A single run's log is retained until
/// its next start, including starts rejected because another leader owns the lock.
pub fn truncate_log(home: &Path) -> Result<(), WorkerError> {
    validate_home(home)?;
    let logs = prepare_log(home)?;
    let file = logs.open_private_append(LOG_NAME)?;
    logs.validate_private_append_binding(LOG_NAME, &file)?;
    file.set_len(0)?;
    file.sync_all()?;
    logs.validate_private_append_binding(LOG_NAME, &file)?;
    Ok(())
}

/// Render instructions only. The caller prints these for an operator to review
/// and run on the controller host; this function never launches any process.
pub fn launchdaemon_commands(home: &Path, username: &str, uid: u32) -> Result<String, WorkerError> {
    validate_home(home)?;
    if username.is_empty()
        || username.len() > 255
        || !username
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(service_error("account name is invalid"));
    }
    let plist = launchd_plist(home, Some(username))?;
    let agent = shell_quote(
        home.join("Library/LaunchAgents")
            .join(PLIST_NAME)
            .to_str()
            .ok_or_else(|| service_error("account home must be UTF-8"))?,
    );
    let daemon = format!("'/Library/LaunchDaemons/{PLIST_NAME}'");
    Ok(format!(
        "sudo /bin/launchctl bootout 'gui/{uid}/{LABEL}'\n\
         sudo /bin/rm -f {agent}\n\
         sudo /usr/bin/tee {daemon} >/dev/null <<'MAC_WORKER_CONTROLLER_PLIST'\n\
         {plist}MAC_WORKER_CONTROLLER_PLIST\n\
         sudo /usr/sbin/chown root:wheel {daemon}\n\
         sudo /bin/chmod 0644 {daemon}\n\
         sudo /bin/launchctl bootstrap system {daemon}\n\
         sudo /bin/launchctl kickstart -k 'system/{LABEL}'\n"
    ))
}

fn launchd_plist(home: &Path, username: Option<&str>) -> Result<String, WorkerError> {
    let home = xml_escape(validate_home(home)?);
    let username = username
        .map(|username| {
            format!(
                "  <key>UserName</key>\n  <string>{}</string>\n",
                xml_escape(username)
            )
        })
        .unwrap_or_default();
    // Same escaped XML representation as outbox's launchd_plist. This job has
    // separate lifecycle, throttling and log-retention requirements.
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LABEL}</string>
{username}  <key>ProgramArguments</key>
  <array>
    <string>{home}/.local/bin/worker</string>
    <string>controller</string>
    <string>run</string>
    <string>--supervised</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>HOME</key>
    <string>{home}</string>
  </dict>
  <key>WorkingDirectory</key>
  <string>{home}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>30</integer>
  <key>StandardOutPath</key>
  <string>{home}/Library/Logs/mac-worker/controller.log</string>
  <key>StandardErrorPath</key>
  <string>{home}/Library/Logs/mac-worker/controller.log</string>
</dict>
</plist>
"#
    ))
}

fn validate_home(home: &Path) -> Result<&str, WorkerError> {
    let value = home
        .to_str()
        .ok_or_else(|| service_error("account home must be UTF-8"))?;
    if !home.is_absolute()
        || home.parent().is_none()
        || value.chars().any(char::is_control)
        || home
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(service_error(
            "account home must be an absolute directory without traversal",
        ));
    }
    Ok(value.trim_end_matches('/'))
}

fn open_directory(path: &Path, create: bool) -> Result<Option<RootedDir>, WorkerError> {
    let directory = if create {
        RootedDir::open_or_create_anchored_absolute(path)
    } else {
        RootedDir::open_anchored_absolute(path)
    };
    let directory = match directory {
        Ok(directory) => directory,
        Err(error) if !create && error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = directory.root_metadata()?;
    // ~/Library and its standard children often are 0755. Preserve their modes
    // while refusing any directory writable by another account.
    if metadata.st_uid != unsafe { libc::geteuid() } || metadata.st_mode & 0o022 != 0 {
        return Err(service_error(
            "service directory ownership or permissions are unsafe",
        ));
    }
    Ok(Some(directory))
}

fn open_directory_chain(
    home: &Path,
    parts: &[&str],
    create: bool,
) -> Result<Option<RootedDir>, WorkerError> {
    let mut path = home.to_path_buf();
    let mut parent =
        open_directory(home, false)?.ok_or_else(|| service_error("account home is missing"))?;
    for part in parts {
        parent.verify_bound()?;
        path.push(part);
        let next = open_directory(&path, create)?;
        parent.verify_bound()?;
        let Some(next) = next else { return Ok(None) };
        parent = next;
    }
    Ok(Some(parent))
}

fn read_plist(agents: &RootedDir) -> Result<Option<InstalledPlist>, WorkerError> {
    if !agents.entry_exists(PLIST_NAME)? {
        return Ok(None);
    }
    let identity = agents.private_entry_identity(PLIST_NAME)?;
    let bytes = agents.read_private_regular(PLIST_NAME, MAX_PLIST_BYTES)?;
    let file = agents.open_private_regular_handle(PLIST_NAME)?;
    agents.validate_private_regular_binding(PLIST_NAME, &file, identity)?;
    Ok(Some(InstalledPlist { bytes, identity }))
}

fn prepare_log(home: &Path) -> Result<RootedDir, WorkerError> {
    let logs = open_directory_chain(home, &["Library", "Logs", "mac-worker"], true)?
        .ok_or_else(|| service_error("service log directory is missing"))?;
    if logs.entry_exists(LOG_NAME)? {
        logs.open_private_append(LOG_NAME)?;
    } else {
        logs.write_private_atomic_no_replace(LOG_NAME, b"")?;
    }
    Ok(logs)
}

fn remove_plist(agents: &RootedDir, identity: PrivateEntryIdentity) -> Result<(), WorkerError> {
    // The shared LaunchAgents directory may be 0755, so the private-tree cleanup
    // API (which requires a 0700 parent) is deliberately not used. Unlink only
    // this fixed entry through the retained directory; never follow a symlink.
    let file = agents.open_private_regular_handle(PLIST_NAME)?;
    agents.validate_private_regular_binding(PLIST_NAME, &file, identity)?;
    let result = unsafe {
        libc::unlinkat(
            agents.raw_directory_fd(),
            c"com.mac-worker.controller.plist".as_ptr(),
            0,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error().into());
    }
    agents.sync_root()?;
    agents.verify_bound()?;
    Ok(())
}

fn is_loaded(runner: &dyn ProcessRunner, uid: u32) -> Result<bool, WorkerError> {
    let result = run(
        runner,
        &["print".into(), format!("gui/{uid}/{LABEL}").into()],
    )?;
    if result.status.success() {
        return Ok(true);
    }
    if result.status.code() == Some(SERVICE_NOT_FOUND) {
        return Ok(false);
    }
    Err(service_error(
        "launchctl could not determine service state; verify a logged-in GUI session",
    ))
}

fn run_checked(runner: &dyn ProcessRunner, args: &[OsString]) -> Result<(), WorkerError> {
    if run(runner, args)?.status.success() {
        return Ok(());
    }
    Err(service_error(
        "launchctl service operation failed; verify a logged-in GUI session",
    ))
}

fn run(runner: &dyn ProcessRunner, args: &[OsString]) -> Result<ProcessResult, WorkerError> {
    runner
        .run(&ProcessRequest {
            program: "/bin/launchctl".into(),
            args: args.to_vec(),
            environment: vec![("LC_ALL".into(), "C".into())],
            environment_remove: Vec::new(),
            stdin: None,
            policy: LAUNCHCTL_POLICY,
            isolate_parent_environment: true,
        })
        .map_err(|_| service_error("launchctl could not be executed"))
}

fn service_error(message: &str) -> WorkerError {
    WorkerError::Unavailable(format!("CONTROLLER_SERVICE: {message}"))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}
