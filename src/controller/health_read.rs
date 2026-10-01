//! Local and additive RPC health reads. No task/request stores are opened here.
use std::{io, os::fd::AsRawFd, path::Path};

use serde::{Deserialize, Serialize};

use crate::{
    config::ControllerConfig,
    error::{WorkerError, is_stable_public_code},
    job::ProcessIdentity,
    process::ProcessRunner,
    supervisor::{ProcessInspector, ProcessObservation, SystemProcessInspector},
};

use super::{
    ControllerReadIdentity, ControllerReadReply, ControllerRequest,
    health::{ControllerHealth, HealthStore, STALE_AFTER_MILLIS},
    leader::{now_millis, open_existing_controller_root},
    read::invalid_controller_reply,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Healthy,
    Running,
    Degraded,
    Stale,
    Unknown,
    Unsupported,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthReason {
    Missing,
    Stopped,
    LeaderNotRunning,
    LeaderUnverifiable,
    TickOverdue,
    TickRunning,
    TickFailed,
    TickSucceeded,
    HealthUnsupported,
    ReadFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerHealthStatus {
    /// Features of the binary serving this read, not of the leader process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<Vec<String>>,
    pub state: HealthState,
    pub reason: HealthReason,
    #[serde(default)]
    pub leader_running: Option<bool>,
    #[serde(default)]
    pub record_age_millis: Option<u64>,
    #[serde(default)]
    pub health: Option<ControllerHealth>,
    #[serde(default)]
    pub error_code: Option<String>,
}

impl ControllerHealthStatus {
    pub fn summary(&self) -> String {
        let description = match self.reason {
            HealthReason::Missing => "stale: no health record; start or upgrade the controller",
            HealthReason::Stopped => "stale: controller leader stopped",
            HealthReason::LeaderNotRunning => "stale: controller leader is not running",
            HealthReason::LeaderUnverifiable => {
                "unknown: controller leader identity cannot be verified"
            }
            HealthReason::TickOverdue => "stale: no tick completed within the health deadline",
            HealthReason::TickRunning => "running: controller tick in progress",
            HealthReason::TickFailed => "degraded: the last controller tick reported failures",
            HealthReason::TickSucceeded => "healthy: the last controller tick succeeded",
            HealthReason::HealthUnsupported => {
                "health unavailable: older controller; upgrade the controller binary and restart worker controller run"
            }
            HealthReason::ReadFailed => "health unavailable: controller health could not be read",
        };
        let mut line = format!("controller: {description}");
        if let Some(health) = &self.health {
            line.push_str(&format!("; active={} oldest_pending_ms={} last_success_ms={} last_progress_ms={} truncated={} cursor_stale={}",
                health.active_count,
                optional_number(health.oldest_pending_age_millis),
                optional_number(health.last_success_millis),
                optional_number(health.last_progress_millis),
                health.truncated, health.cursor_stale));
        }
        line
    }

    pub fn unavailable(error: &WorkerError) -> Self {
        Self {
            features: None,
            state: HealthState::Unavailable,
            reason: HealthReason::ReadFailed,
            leader_running: None,
            record_age_millis: None,
            health: None,
            error_code: Some(error.public_code()),
        }
    }
}

fn optional_number(value: Option<u64>) -> String {
    value.map_or_else(|| "unknown".into(), |n| n.to_string())
}

pub fn assess_health(
    health: Option<ControllerHealth>,
    observation: ProcessObservation,
    now: u64,
) -> ControllerHealthStatus {
    let mut status = ControllerHealthStatus {
        features: None,
        state: HealthState::Stale,
        reason: HealthReason::Missing,
        leader_running: None,
        record_age_millis: None,
        health,
        error_code: None,
    };
    let Some(record) = &status.health else {
        return status;
    };
    let updated = record
        .last_tick_end_millis
        .or(record.last_tick_start_millis)
        .unwrap_or(record.started_at_millis);
    status.record_age_millis = Some(now.saturating_sub(updated));
    status.leader_running = match observation {
        ProcessObservation::Matching { .. } => Some(true),
        ProcessObservation::Absent | ProcessObservation::Reused => Some(false),
        ProcessObservation::Ambiguous => None,
    };
    (status.state, status.reason) = if record.stopped_at_millis.is_some() {
        (HealthState::Stale, HealthReason::Stopped)
    } else if status.leader_running == Some(false) {
        (HealthState::Stale, HealthReason::LeaderNotRunning)
    } else if status.leader_running.is_none() {
        (HealthState::Unknown, HealthReason::LeaderUnverifiable)
    } else if now < updated || now - updated > STALE_AFTER_MILLIS {
        (HealthState::Stale, HealthReason::TickOverdue)
    } else if record.last_tick_end_millis.is_none() {
        (HealthState::Running, HealthReason::TickRunning)
    } else if !record.last_tick_failures.is_empty() || record.pending_age_incomplete {
        (HealthState::Degraded, HealthReason::TickFailed)
    } else {
        (HealthState::Healthy, HealthReason::TickSucceeded)
    };
    status
}

pub fn read_health_status(path: &Path) -> Result<ControllerHealthStatus, WorkerError> {
    let home = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_default();
    let paths = crate::paths::PathLayout {
        state: path.parent().unwrap_or(path).join("mac-worker"),
        config: home.join(".config/mac-worker/config.toml"),
        cache: home.join(".cache/mac-worker"),
        data: home.join(".local/share/mac-worker"),
    };
    let features = if paths.controller_state_root() == path {
        serving_features(&paths, &home)
    } else {
        crate::features::CONTROLLER_FEATURES
            .iter()
            .map(|feature| (*feature).to_owned())
            .collect()
    };
    read_health_status_with_features(path, features)
}

fn read_health_status_with_features(
    path: &Path,
    features: Vec<String>,
) -> Result<ControllerHealthStatus, WorkerError> {
    let health = HealthStore::read_existing(path)?;
    let observation = match &health {
        Some(record) => observe_leader(path, record.leader)?,
        None => ProcessObservation::Ambiguous,
    };
    let mut status = assess_health(health, observation, now_millis()?);
    status.features = Some(features);
    Ok(status)
}

fn serving_features(paths: &crate::paths::PathLayout, home: &Path) -> Vec<String> {
    use super::channel::{
        codec::SessionCodec,
        contracts::{ChannelRuntime, ClientContext, SETUP_GUARD},
        identity::read_live_service,
    };
    let runtime = super::runtime::SystemChannelRuntime::default();
    let ctx = ClientContext {
        runtime: &runtime,
        deadline: runtime.now().saturating_add(SETUP_GUARD),
        should_stop: &|| false,
    };
    let mut features: Vec<_> = crate::features::CONTROLLER_FEATURES
        .iter()
        .map(|feature| (*feature).to_owned())
        .collect();
    if read_live_service(paths, home, &SessionCodec::new(), &ctx)
        .ok()
        .flatten()
        .is_some()
    {
        features.push(crate::features::CONTROLLER_SOCKET.to_owned());
        features.sort();
    }
    features
}

pub(crate) fn observe_leader(
    path: &Path,
    expected: ProcessIdentity,
) -> Result<ProcessObservation, WorkerError> {
    observe_leader_with_hook(path, expected, || {})
}

fn observe_leader_with_hook(
    path: &Path,
    expected: ProcessIdentity,
    mut after_open: impl FnMut(),
) -> Result<ProcessObservation, WorkerError> {
    let Some(root) = open_existing_controller_root(path)? else {
        return Ok(ProcessObservation::Absent);
    };
    let mut attempts = 0;
    let bytes = loop {
        match root.read_private_regular_with_hook("leader.json", 4096, &mut after_open) {
            Ok(bytes) => break bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(ProcessObservation::Absent);
            }
            Err(error) if error.raw_os_error() == Some(libc::ESTALE) && attempts < 3 => {
                // Leader handoff can replace this identity while it is read.
                // Keep the original directory and compare the new identity to
                // the health record's expected leader below.
                root.verify_bound()?;
                attempts += 1;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let current: ProcessIdentity =
        serde_json::from_slice(&bytes).map_err(|_| invalid_controller_reply())?;
    current.validate()?;
    if current != expected {
        return Ok(ProcessObservation::Reused);
    }
    // PID liveness alone is insufficient: a stopped leader can still be inside
    // a long-lived embedding process. Inspect the existing lock without creating it.
    let lock = match root.open_existing_private_lock("controller.lock") {
        Ok(lock) => lock,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ProcessObservation::Absent);
        }
        Err(error) => return Err(error.into()),
    };
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(ProcessObservation::Absent); // descriptor drop releases our probe lock
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() != Some(libc::EWOULDBLOCK) && error.raw_os_error() != Some(libc::EAGAIN)
    {
        return Ok(ProcessObservation::Ambiguous);
    }
    Ok(SystemProcessInspector.observe(expected))
}

// Older hosts route unknown commands through the durable request kernel. Use
// an additive selector on an existing read for discovery: an older task.list
// rejects the unknown key without publishing an orphan request receipt.
pub fn is_health_read(request: &ControllerRequest) -> bool {
    request.command() == "controller.health"
        || (request.command() == "task.list" && request.body().get("controller_health").is_some())
}

fn valid_health_request(request: &ControllerRequest) -> bool {
    (request.command() == "controller.health" && request.body() == &serde_json::json!({}))
        || (request.command() == "task.list"
            && request.body() == &serde_json::json!({"controller_health": true}))
}

pub fn serve_health_read(request: &ControllerRequest, path: &Path) -> Result<Vec<u8>, WorkerError> {
    if !valid_health_request(request) {
        return Err(WorkerError::Protocol(
            "INVALID_REQUEST: invalid controller health read".into(),
        ));
    }
    // Return an additive typed diagnostic even for unreadable health. The old
    // host's unsupported-command code stays distinguishable from record damage.
    let mut status = read_health_status(path)
        .unwrap_or_else(|error| ControllerHealthStatus::unavailable(&error));
    // read_health_status includes only positively proven dynamic features.
    if status.features.is_none() {
        status.features = Some(
            crate::features::CONTROLLER_FEATURES
                .iter()
                .map(|s| (*s).to_owned())
                .collect(),
        );
    }
    super::encode_json_frame(&ControllerReadReply::from_request(request, status))
}

pub fn serve_health_read_with_paths(
    request: &ControllerRequest,
    paths: &crate::paths::PathLayout,
    home: &Path,
) -> Result<Vec<u8>, WorkerError> {
    if !valid_health_request(request) {
        return Err(WorkerError::Protocol(
            "INVALID_REQUEST: invalid controller health read".into(),
        ));
    }
    let features = serving_features(paths, home);
    let mut status =
        read_health_status_with_features(&paths.controller_state_root(), features.clone())
            .unwrap_or_else(|error| ControllerHealthStatus::unavailable(&error));
    status.features = Some(features);
    super::encode_json_frame(&ControllerReadReply::from_request(request, status))
}

impl ControllerReadIdentity for ControllerHealthStatus {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        if !valid_health_request(request)
            || self
                .error_code
                .as_ref()
                .is_some_and(|code| !is_stable_public_code(code))
        {
            return Err(invalid_controller_reply());
        }
        if let Some(health) = &self.health {
            health.validate().map_err(|_| invalid_controller_reply())?;
        }
        Ok(())
    }
}

pub fn fetch_controller_health(
    runner: &dyn ProcessRunner,
    config: &ControllerConfig,
) -> ControllerHealthStatus {
    let request = super::parse_request(
        &serde_json::to_vec(&serde_json::json!({
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "request_id": uuid::Uuid::new_v4().simple().to_string(),
            "command": "task.list", "body": {"controller_health": true},
        }))
        .expect("health request contains only JSON primitives"),
    )
    .expect("valid health request");
    match super::send_controller_read::<ControllerHealthStatus>(runner, config, &request) {
        Ok(reply) => reply.into_result(),
        Err(error)
            if matches!(
                error.public_code().as_str(),
                "CONTROLLER_TRANSPORT" | "INVALID_REQUEST"
            ) =>
        {
            ControllerHealthStatus {
                features: None,
                state: HealthState::Unsupported,
                reason: HealthReason::HealthUnsupported,
                leader_running: None,
                record_age_millis: None,
                health: None,
                error_code: None,
            }
        }
        Err(error) => ControllerHealthStatus::unavailable(&error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt};

    fn identity(number: u32) -> ProcessIdentity {
        ProcessIdentity::new(crate::fixture_pid::fixture_pid(number), 1).unwrap()
    }

    #[test]
    fn leader_read_rechecks_a_new_leader_after_open() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("controller");
        let root = super::super::leader::open_controller_root(&path).unwrap();
        let original = serde_json::to_vec(&identity(1)).unwrap();
        let replacement = serde_json::to_vec(&identity(2)).unwrap();
        root.write_private_atomic_no_replace("leader.json", &original)
            .unwrap();
        let mut replace = true;
        let observed = observe_leader_with_hook(&path, identity(1), || {
            if std::mem::take(&mut replace) {
                root.replace_private_regular_exact("leader.json", &original, &replacement)
                    .unwrap();
            }
        })
        .unwrap();

        assert_eq!(observed, ProcessObservation::Reused);
        assert_eq!(fs::read(path.join("leader.json")).unwrap(), replacement);
    }

    #[test]
    fn leader_read_recheck_preserves_invalid_records_and_directory_bindings() {
        for kind in ["record", "permissions", "directory"] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("controller");
            let root = super::super::leader::open_controller_root(&path).unwrap();
            let original = serde_json::to_vec(&identity(1)).unwrap();
            root.write_private_atomic_no_replace("leader.json", &original)
                .unwrap();
            let mut replace = true;
            let error = observe_leader_with_hook(&path, identity(1), || {
                if !std::mem::take(&mut replace) {
                    return;
                }
                let file = path.join("leader.json");
                match kind {
                    "record" => root
                        .replace_private_regular_exact("leader.json", &original, b"{}")
                        .unwrap(),
                    "permissions" => {
                        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap()
                    }
                    "directory" => {
                        fs::rename(&path, temp.path().join("detached")).unwrap();
                        fs::create_dir(&path).unwrap();
                        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
                        fs::write(&file, serde_json::to_vec(&identity(2)).unwrap()).unwrap();
                        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
                    }
                    _ => unreachable!(),
                }
            })
            .unwrap_err();

            match kind {
                "record" => assert_eq!(error.public_code(), "CONTROLLER_UNAVAILABLE"),
                "permissions" => assert!(matches!(error, WorkerError::Io(error)
                    if error.kind() == io::ErrorKind::PermissionDenied)),
                "directory" => assert!(matches!(error, WorkerError::Io(error)
                    if error.raw_os_error() == Some(libc::ESTALE))),
                _ => unreachable!(),
            }
        }
    }
}
