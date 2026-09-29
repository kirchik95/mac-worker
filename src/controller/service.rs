//! LaunchAgent lifecycle for the persistent controller.

use std::{
    ffi::{CStr, CString, OsString},
    io,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    controller::leader::lock_exclusive,
    error::{ExitKind, WorkerError},
    paths::PathLayout,
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
// A bootout waits for the leader to exit, and a kickstart soon after a start
// waits out ThrottleInterval (30 s) before launchd respawns the job.
const LAUNCHCTL_POLICY: ProcessPolicy = ProcessPolicy {
    stdout_limit: 1024 * 1024,
    stderr_limit: 64 * 1024,
    deadline: Duration::from_secs(75),
};
/// The laptop's wait for a service change must outlast the launchctl calls
/// inside it, including a throttled respawn.
pub(crate) const SERVICE_CHANGE_DEADLINE: Duration = Duration::from_secs(90);

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit_status: Option<i32>,
    /// Host clock sampled immediately before this request's kickstart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_started_at_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paths: Option<ServicePaths>,
}

impl ServiceStatus {
    pub(crate) fn for_wire(mut self, include_details: bool) -> Self {
        // Host replies are checked by typed byte-canonical reserialization.
        // Even additive response fields therefore require an explicit opt-in.
        if !include_details {
            self.pid = None;
            self.running = None;
            self.last_exit_status = None;
            self.restart_started_at_millis = None;
            self.paths = None;
        }
        self
    }
}

/// Resolved helper paths, also used by the printed LaunchDaemon variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServicePaths {
    pub config: PathBuf,
    pub state: PathBuf,
    pub cache: PathBuf,
    pub data: PathBuf,
}

impl ServicePaths {
    pub fn from_layout(paths: &PathLayout) -> Result<Self, WorkerError> {
        Ok(Self {
            config: std::path::absolute(&paths.config)?,
            state: std::path::absolute(&paths.state)?,
            cache: std::path::absolute(&paths.cache)?,
            data: std::path::absolute(&paths.data)?,
        })
    }
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
    paths: &PathLayout,
    config_home: &Path,
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
    let observation = observe(runner, uid)?;
    let mut status = ServiceStatus {
        label: LABEL.to_owned(),
        domain: format!("gui/{uid}"),
        installed: previous.is_some(),
        loaded: observation.loaded,
        pid: observation.pid,
        running: observation.running,
        last_exit_status: observation.last_exit_status,
        restart_started_at_millis: None,
        paths: Some(ServicePaths::from_layout(paths)?),
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
            status.pid = None;
            status.running = Some(false);
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
            let desired = launchd_plist(
                home,
                status.paths.as_ref().expect("resolved paths"),
                config_home,
                None,
            )?;
            let changed = previous.as_ref().map(|previous| previous.bytes.as_slice())
                != Some(desired.as_bytes());
            if changed {
                match previous {
                    Some(previous) => replace_plist(&agents, &previous, desired.as_bytes())?,
                    None => {
                        agents.write_private_atomic_no_replace(PLIST_NAME, desired.as_bytes())?
                    }
                }
                if status.loaded {
                    run_checked(runner, &["bootout".into(), target.clone().into()])?;
                    status.loaded = false;
                }
            }
            // RunAtLoad starts the job on bootstrap. A kickstart right after it
            // would kill that fresh instance, and launchd would hold the
            // respawn for ThrottleInterval. Only restart a job that was
            // already loaded. Either way a new leader starts after this time.
            let started_by_bootstrap = !status.loaded;
            let starts = action == ServiceAction::Restart || started_by_bootstrap;
            if starts {
                status.restart_started_at_millis = Some(super::leader::now_millis()?);
            }
            if started_by_bootstrap {
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
            } else if action == ServiceAction::Restart {
                run_checked(runner, &["kickstart".into(), "-k".into(), target.into()])?;
            }
            if starts {
                let observed = observe(runner, uid)?;
                status.pid = observed.pid;
                status.running = observed.running;
                status.last_exit_status = observed.last_exit_status;
                if !observed.loaded {
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
pub fn launchdaemon_commands(
    home: &Path,
    paths: &ServicePaths,
    username: &str,
    uid: u32,
) -> Result<String, WorkerError> {
    validate_home(home)?;
    if username.is_empty()
        || username.len() > 255
        || !username
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(service_error("account name is invalid"));
    }
    // Provisioning writes the helper's default <config_home>/mac-worker/config.toml.
    // Reuse that verified root without adding fields to canonical wire replies.
    let config_home = paths
        .config
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| service_error("missing config home"))?;
    let plist = launchd_plist(home, paths, config_home, Some(username))?;
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

fn launchd_plist(
    home: &Path,
    paths: &ServicePaths,
    config_home: &Path,
    username: Option<&str>,
) -> Result<String, WorkerError> {
    let config = xml_escape(validate_home(&paths.config)?);
    let config_home = xml_escape(validate_home(config_home)?);
    let xdg_root = |path: &Path| -> Result<String, WorkerError> {
        Ok(xml_escape(validate_home(
            path.parent()
                .ok_or_else(|| service_error("missing XDG root"))?,
        )?))
    };
    let state = xdg_root(&paths.state)?;
    let cache = xdg_root(&paths.cache)?;
    let data = xdg_root(&paths.data)?;
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
    <string>--config</string>
    <string>{config}</string>
    <string>controller</string>
    <string>run</string>
    <string>--supervised</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>HOME</key>
    <string>{home}</string>
    <key>XDG_CONFIG_HOME</key>
    <string>{config_home}</string>
    <key>XDG_STATE_HOME</key>
    <string>{state}</string>
    <key>XDG_CACHE_HOME</key>
    <string>{cache}</string>
    <key>XDG_DATA_HOME</key>
    <string>{data}</string>
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

fn replace_plist(
    agents: &RootedDir,
    previous: &InstalledPlist,
    desired: &[u8],
) -> Result<(), WorkerError> {
    // RootedDir's general replacement cleanup requires a private directory.
    // LaunchAgents is shared with other jobs and normally 0755: retain both
    // inodes across an atomic exchange and unlink only the displaced plist.
    let original = agents.open_private_regular_handle(PLIST_NAME)?;
    agents.validate_private_regular_binding(PLIST_NAME, &original, previous.identity)?;
    let temporary = format!(".controller-plist-{}", uuid::Uuid::new_v4().simple());
    agents.write_private_atomic_no_replace(&temporary, desired)?;
    let replacement = agents.open_private_regular_handle(&temporary)?;
    let replacement_identity = agents.private_entry_identity(&temporary)?;
    let temporary_name = CString::new(temporary.as_str()).expect("generated name has no NUL");
    let exchange = (|| -> Result<(), WorkerError> {
        agents.validate_private_regular_binding(&temporary, &replacement, replacement_identity)?;
        if agents.read_private_regular(PLIST_NAME, MAX_PLIST_BYTES)? != previous.bytes {
            return Err(io::Error::from_raw_os_error(libc::ESTALE).into());
        }
        agents.validate_private_regular_binding(PLIST_NAME, &original, previous.identity)?;
        exchange_plist(agents.raw_directory_fd(), &temporary_name)?;
        Ok(())
    })();
    if let Err(error) = exchange {
        // If an uncooperative writer replaced the staged file, preserve it and
        // return the original failure rather than unlinking an unknown inode.
        let _ = unlink_exact(agents, &temporary_name, &replacement, replacement_identity);
        return Err(error);
    }
    agents.validate_private_regular_binding(PLIST_NAME, &replacement, replacement_identity)?;
    agents.validate_private_regular_binding(&temporary, &original, previous.identity)?;
    if agents.read_private_regular(&temporary, MAX_PLIST_BYTES)? != previous.bytes {
        return Err(io::Error::from_raw_os_error(libc::ESTALE).into());
    }
    agents.sync_root()?;
    unlink_exact(agents, &temporary_name, &original, previous.identity)?;
    agents.validate_private_regular_binding(PLIST_NAME, &replacement, replacement_identity)?;
    Ok(())
}

fn exchange_plist(directory: std::os::fd::RawFd, temporary: &CStr) -> io::Result<()> {
    // SAFETY: the retained directory and both fixed/generated NUL-terminated
    // components remain valid for this non-retaining atomic exchange.
    #[cfg(target_vendor = "apple")]
    let result = unsafe {
        libc::renameatx_np(
            directory,
            temporary.as_ptr(),
            directory,
            c"com.mac-worker.controller.plist".as_ptr(),
            libc::RENAME_SWAP,
        )
    };
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let result = unsafe {
        libc::renameat2(
            directory,
            temporary.as_ptr(),
            directory,
            c"com.mac-worker.controller.plist".as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    #[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
    let result = {
        let _ = (directory, temporary);
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic plist exchange is unavailable",
        ));
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
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
    unlink_exact(agents, c"com.mac-worker.controller.plist", &file, identity)
}

fn unlink_exact(
    agents: &RootedDir,
    name: &CStr,
    file: &std::fs::File,
    identity: PrivateEntryIdentity,
) -> Result<(), WorkerError> {
    let component = name.to_str().expect("fixed/generated name is UTF-8");
    agents.validate_private_regular_binding(component, file, identity)?;
    // SAFETY: the retained descriptor and validated name are live. unlinkat
    // removes the entry itself, without following a symbolic link.
    let result = unsafe { libc::unlinkat(agents.raw_directory_fd(), name.as_ptr(), 0) };
    if result != 0 {
        return Err(io::Error::last_os_error().into());
    }
    agents.sync_root()?;
    agents.verify_bound()?;
    Ok(())
}

#[derive(Default)]
struct LaunchdObservation {
    loaded: bool,
    pid: Option<u32>,
    running: Option<bool>,
    last_exit_status: Option<i32>,
}

fn is_loaded(runner: &dyn ProcessRunner, uid: u32) -> Result<bool, WorkerError> {
    Ok(observe(runner, uid)?.loaded)
}

fn observe(runner: &dyn ProcessRunner, uid: u32) -> Result<LaunchdObservation, WorkerError> {
    let target = format!("gui/{uid}/{LABEL}");
    let result = run(runner, &["print".into(), target.clone().into()])?;
    if result.status.success() {
        return Ok(parse_observation(&result.stdout, &target));
    }
    if result.status.code() == Some(SERVICE_NOT_FOUND) {
        return Ok(LaunchdObservation {
            running: Some(false),
            ..Default::default()
        });
    }
    Err(service_error(
        "launchctl could not determine service state; verify a logged-in GUI session",
    ))
}

fn parse_observation(bytes: &[u8], target: &str) -> LaunchdObservation {
    let unknown = || LaunchdObservation {
        loaded: true,
        ..Default::default()
    };
    let Ok(output) = std::str::from_utf8(bytes) else {
        return unknown();
    };
    let mut lines = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    if lines.next() != Some(format!("{target} = {{").as_str()) {
        return unknown();
    }
    let mut depth = 1usize;
    let mut fields = std::collections::BTreeMap::new();
    for line in lines {
        if line == "}" {
            let Some(next) = depth.checked_sub(1) else {
                return unknown();
            };
            depth = next;
        } else if line.ends_with('{') {
            depth += 1;
        } else if depth == 1 {
            if let Some((key, value)) = line.split_once(" = ") {
                // Duplicate fields are ambiguous, including duplicate equal values.
                fields
                    .entry(key)
                    .and_modify(|value| *value = None)
                    .or_insert(Some(value));
            }
        } else if depth == 0 {
            return unknown();
        }
    }
    if depth != 0 {
        return unknown();
    }
    let value = |key| fields.get(key).copied().flatten();
    let running = match value("state") {
        Some("running") => Some(true),
        Some("not running" | "exited" | "waiting") => Some(false),
        _ => None,
    };
    LaunchdObservation {
        loaded: true,
        running,
        pid: if running == Some(true) {
            value("pid")
                .and_then(|v| v.parse().ok())
                .filter(|pid| *pid > 0)
        } else {
            None
        },
        last_exit_status: value("last exit code").and_then(|v| v.parse().ok()),
    }
}

/// Init and setup use the same bounded proof. launchctl registration alone is
/// never readiness; all observations must identify the newly supervised build.
pub fn verify_restart(
    runner: &dyn ProcessRunner,
    controller: &crate::config::ControllerConfig,
    restarted: ServiceStatus,
    expected_sha: &str,
    expected_config: &Path,
    wait: &dyn Fn(Duration),
) -> Result<(), WorkerError> {
    let restart_time = restarted.restart_started_at_millis.ok_or_else(|| {
        service_error("restart has no start-time proof; upgrade the controller helper")
    })?;
    let expected_paths = restarted.paths.clone().ok_or_else(|| {
        service_error("restart has no resolved paths; upgrade the controller helper")
    })?;
    if expected_paths.config != expected_config || !expected_config.is_absolute() {
        return Err(service_error(
            "service config path differs from the configured inventory",
        ));
    }
    let mut status = restarted;
    let mut reason = String::new();
    for attempt in 0..=20 {
        if status.label != LABEL || !status.installed {
            return Err(service_error("controller LaunchAgent is not installed"));
        }
        let observed = super::health_read::fetch_controller_health(runner, controller);
        use super::health_read::HealthReason;
        if matches!(
            observed.reason,
            HealthReason::LeaderUnverifiable
                | HealthReason::ReadFailed
                | HealthReason::HealthUnsupported
        ) {
            return Err(service_error(
                "leader health or process identity could not be verified",
            ));
        }
        if observed.leader_running == Some(true) {
            if let Some(health) = observed.health {
                let pid = health.leader.pid();
                if status.pid.is_some_and(|service_pid| service_pid != pid)
                    || (status.pid.is_none()
                        && status.last_exit_status == Some(ExitKind::Infrastructure as i32))
                {
                    return Err(WorkerError::Unavailable(format!(
                        "CONTROLLER_FOREIGN_LEADER: process {pid} holds the controller leader lock outside the restarted LaunchAgent; stop that process, then rerun worker controller init"
                    )));
                }
                if status.pid.is_none() && attempt == 20 {
                    return Err(WorkerError::Unavailable(
                        "CONTROLLER_SERVICE_UNVERIFIED: restart verification could not identify the LaunchAgent process (launchd exposed no pid)".into(),
                    ));
                }
                if !health.supervised {
                    return Err(WorkerError::Unavailable(
                        "CONTROLLER_SERVICE_UNVERIFIED: the live controller leader does not report a supervised startup".into(),
                    ));
                }
                if !status.loaded || status.running != Some(true) || status.pid != Some(pid) {
                    reason = format!(
                        "LaunchAgent is not running (last exit status {:?})",
                        status.last_exit_status
                    );
                } else if health.started_at_millis <= restart_time {
                    reason = "leader predates this restart".into();
                } else if health.binary_sha256.as_deref() != Some(expected_sha)
                    || expected_sha.is_empty()
                {
                    reason = "leader binary digest differs from the installed helper".into();
                } else if health.config_path.as_deref() != Some(expected_config) {
                    reason = "leader config path differs from the configured inventory".into();
                } else if health.paths.as_deref() != Some(&expected_paths) {
                    reason = "leader state/cache/data roots differ from the service roots".into();
                } else {
                    return Ok(());
                }
            } else {
                reason = "live leader health has no process identity".into();
            }
        } else {
            reason = format!(
                "no live leader; LaunchAgent running={:?}, last exit status={:?}",
                status.running, status.last_exit_status
            );
        }
        if attempt == 20 {
            break;
        }
        wait(Duration::from_millis(250));
        status = fetch_status(runner, controller).map_err(|error| {
            service_error(&format!(
                "service status could not be verified [{}]",
                error.public_code()
            ))
        })?;
        if status.paths.as_ref() != Some(&expected_paths) {
            return Err(service_error(
                "service paths changed during restart verification",
            ));
        }
    }
    Err(service_error(&format!(
        "restart verification timed out: {reason}"
    )))
}

pub fn fetch_status(
    runner: &dyn ProcessRunner,
    controller: &crate::config::ControllerConfig,
) -> Result<ServiceStatus, WorkerError> {
    let host = super::controller_worker_entry(controller)?;
    let request = |include_details| {
        crate::transfer::controller_host_request(
            runner,
            &host,
            crate::transfer::HostOperation::ControllerService,
            &crate::protocol::ControllerServiceRequest {
                action: ServiceAction::Status,
                include_details,
            },
        )
    };
    match request(true) {
        // Old helpers strictly reject unknown request fields. Retrying this
        // read-only operation preserves status access without claiming proof.
        Err(error) if error.public_code() == "INVALID_REQUEST" => request(false),
        result => result,
    }
}

/// Install, restart or uninstall the controller service through its host
/// helper, allowing for launchd's waits (see [`SERVICE_CHANGE_DEADLINE`]).
pub(crate) fn service_change_request(
    runner: &dyn ProcessRunner,
    host: &crate::config::WorkerEntry,
    request: &crate::protocol::ControllerServiceRequest,
) -> Result<ServiceStatus, WorkerError> {
    crate::transfer::SshJsonTransport::new(runner).request(
        host,
        crate::transfer::HostOperation::ControllerService,
        request,
        ProcessPolicy {
            deadline: SERVICE_CHANGE_DEADLINE,
            ..super::provision::PROCESS_POLICY
        },
    )
}

pub fn restart_and_verify(
    runner: &dyn ProcessRunner,
    controller: &crate::config::ControllerConfig,
    expected_sha: &str,
    wait: &dyn Fn(Duration),
) -> Result<(), WorkerError> {
    let host = super::controller_worker_entry(controller)?;
    let restarted = service_change_request(
        runner,
        &host,
        &crate::protocol::ControllerServiceRequest {
            action: ServiceAction::Restart,
            include_details: true,
        },
    )
    .map_err(|error| service_error(&format!("restart request failed [{}]", error.public_code())))?;
    let config_path = restarted
        .paths
        .as_ref()
        .map(|paths| paths.config.clone())
        .ok_or_else(|| service_error("restart has no resolved config path"))?;
    verify_restart(
        runner,
        controller,
        restarted,
        expected_sha,
        &config_path,
        wait,
    )
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
