//! Bounded, code-only controller health. This is an observation, never ownership
//! authority: request locks, runner identities and durable rows remain authoritative.
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    error::{WorkerError, is_stable_public_code},
    job::ProcessIdentity,
    rooted_fs::RootedDir,
    task_client::ReconcileReport,
};

use super::{
    ActiveBootstrapReport, ActiveResumeReport, ControllerCommandHandler, ControllerStore,
    leader::{open_controller_root, open_existing_controller_root},
    tick_controller_leader,
};

const HEALTH_FILE: &str = "health.json";
pub const MAX_HEALTH_BYTES: u64 = 16 * 1024;
const MAX_FAILURE_CODES: usize = 32;
const LOG_INTERVAL_MILLIS: u64 = 30_000;
pub const TICK_INTERVAL_MILLIS: u64 = 2_000;
pub const STALE_AFTER_MILLIS: u64 = 5 * TICK_INTERVAL_MILLIS;

#[derive(Debug)]
pub struct ControllerRequestTickReport {
    pub bootstrap: Result<ActiveBootstrapReport, WorkerError>,
    pub resume: Result<ActiveResumeReport, WorkerError>,
}

#[derive(Debug)]
pub struct ControllerTickReport {
    pub requests: ControllerRequestTickReport,
    pub recovery: Result<ReconcileReport, WorkerError>,
    pub pending: Result<PendingHealth, WorkerError>,
}

impl ControllerTickReport {
    pub fn collect(
        store: &ControllerStore,
        handler: &dyn ControllerCommandHandler,
        recovery: impl FnOnce() -> Result<ReconcileReport, WorkerError>,
        clock: impl FnOnce() -> Result<u64, WorkerError>,
    ) -> Self {
        Self {
            requests: tick_controller_leader(store, handler),
            recovery: recovery(),
            pending: clock().and_then(|now| store.pending_health(now)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PendingHealth {
    pub active_count: u64,
    pub oldest_pending_age_millis: Option<u64>,
    pub age_incomplete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailureCount {
    pub count: u64,
    pub last_seen_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerHealth {
    pub version: u32,
    pub leader: ProcessIdentity,
    pub binary_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_path: Option<PathBuf>,
    #[serde(default)]
    pub supervised: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paths: Option<Box<super::service::ServicePaths>>,
    pub started_at_millis: u64,
    #[serde(default)]
    pub last_tick_start_millis: Option<u64>,
    #[serde(default)]
    pub last_tick_end_millis: Option<u64>,
    #[serde(default)]
    pub last_tick_duration_millis: Option<u64>,
    #[serde(default)]
    pub last_success_millis: Option<u64>,
    #[serde(default)]
    pub last_progress_millis: Option<u64>,
    #[serde(default)]
    pub failures: BTreeMap<String, FailureCount>,
    #[serde(default)]
    pub last_tick_failures: BTreeMap<String, FailureCount>,
    #[serde(default)]
    pub oldest_pending_age_millis: Option<u64>,
    #[serde(default)]
    pub active_count: u64,
    #[serde(default)]
    pub pending_age_incomplete: bool,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub cursor_stale: bool,
    #[serde(default)]
    pub stopped_at_millis: Option<u64>,
}

impl ControllerHealth {
    pub fn new(leader: ProcessIdentity, started_at_millis: u64) -> Self {
        Self {
            version: 1,
            leader,
            binary_version: env!("CARGO_PKG_VERSION").into(),
            config_path: None,
            supervised: false,
            build_id: None,
            binary_sha256: None,
            paths: None,
            started_at_millis,
            last_tick_start_millis: None,
            last_tick_end_millis: None,
            last_tick_duration_millis: None,
            last_success_millis: None,
            last_progress_millis: None,
            failures: BTreeMap::new(),
            last_tick_failures: BTreeMap::new(),
            oldest_pending_age_millis: None,
            active_count: 0,
            pending_age_incomplete: false,
            truncated: false,
            cursor_stale: false,
            stopped_at_millis: None,
        }
    }

    pub fn begin_tick(&mut self, now: u64) {
        self.last_tick_start_millis = Some(now);
        self.last_tick_end_millis = None;
        self.last_tick_duration_millis = None;
        self.last_tick_failures.clear();
    }

    pub fn finish_tick(&mut self, now: u64, duration_millis: u64, report: &ControllerTickReport) {
        self.last_tick_end_millis = Some(now);
        self.last_tick_duration_millis = Some(duration_millis);
        let mut progressed = false;
        match &report.requests.bootstrap {
            Ok(bootstrap) => {
                progressed |= !bootstrap.already_bootstrapped && !bootstrap.rebuilt.is_empty();
                // Bootstrap corruption is persisted evidence. Keep reporting it
                // on later ticks until repaired; never copy its filenames/text.
                for _ in &bootstrap.corrupt {
                    self.record_failure("CONTROLLER_TRANSPORT", now);
                }
            }
            Err(error) => self.record_failure(&error.public_code(), now),
        }
        self.truncated = false;
        self.cursor_stale = false;
        match &report.requests.resume {
            Ok(resume) => {
                progressed |= !resume.completed.is_empty();
                for (_, failure) in &resume.failed {
                    let code = failure
                        .split_once(": ")
                        .map_or(failure.as_str(), |(code, _)| code);
                    self.record_failure(code, now);
                }
                for _ in &resume.orphan_receipts {
                    self.record_failure("CONTROLLER_TRANSPORT", now);
                }
                self.truncated = resume.truncated;
                self.cursor_stale = resume.cursor_stale;
                if resume.cursor_stale {
                    self.record_failure("CONTROLLER_REQUEST_CONFLICT", now);
                }
            }
            Err(error) => self.record_failure(&error.public_code(), now),
        }
        match &report.recovery {
            Ok(recovery) => {
                progressed |= recovery.started_runners() > 0
                    || recovery.replaced_runners() > 0
                    || recovery.repaired_rows() > 0;
                for _ in 0..recovery.unverifiable_rows() {
                    self.record_failure("PROCESS_AMBIGUOUS", now);
                }
            }
            Err(error) => self.record_failure(&error.public_code(), now),
        }
        match &report.pending {
            Ok(pending) => {
                self.active_count = pending.active_count;
                self.oldest_pending_age_millis = pending.oldest_pending_age_millis;
                self.pending_age_incomplete = pending.age_incomplete;
            }
            Err(error) => {
                self.pending_age_incomplete = true;
                self.record_failure(&error.public_code(), now);
            }
        }
        if self.last_tick_failures.is_empty() {
            self.last_success_millis = Some(now);
        }
        if progressed {
            self.last_progress_millis = Some(now);
        }
    }

    pub fn record_failure(&mut self, code: &str, now: u64) {
        increment_failure(&mut self.failures, code, now);
        increment_failure(&mut self.last_tick_failures, code, now);
    }

    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        self.leader.validate()?;
        if self.version != 1
            || self.binary_version.len() > 64
            || !self
                .binary_version
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".-+".contains(&b))
            || [&self.failures, &self.last_tick_failures]
                .iter()
                .any(|map| {
                    map.len() > MAX_FAILURE_CODES
                        || map.keys().any(|key| !is_stable_public_code(key))
                })
        {
            return Err(invalid_health());
        }
        Ok(())
    }
}

fn increment_failure(map: &mut BTreeMap<String, FailureCount>, code: &str, now: u64) {
    // Reserve one bucket for overflow/invalid codes: neither memory nor disk
    // grows with a stream of distinct failures. No raw message can enter it.
    let code = if !is_stable_public_code(code)
        || (!map.contains_key(code) && map.len() >= MAX_FAILURE_CODES - 1)
    {
        "PROTOCOL"
    } else {
        code
    };
    let failure = map.entry(code.to_owned()).or_insert(FailureCount {
        count: 0,
        last_seen_millis: now,
    });
    failure.count = failure.count.saturating_add(1);
    failure.last_seen_millis = now;
}

#[derive(Default)]
pub struct HealthLogger {
    last_logged_millis: Option<u64>,
}

impl HealthLogger {
    pub fn failure_line(&mut self, health: &ControllerHealth, now: u64) -> Option<String> {
        if health.last_tick_failures.is_empty()
            || self
                .last_logged_millis
                .is_some_and(|last| now.saturating_sub(last) < LOG_INTERVAL_MILLIS)
        {
            return None;
        }
        self.last_logged_millis = Some(now);
        let counts = health
            .failures
            .iter()
            .filter(|(code, _)| is_stable_public_code(code))
            .take(MAX_FAILURE_CODES)
            .map(|(code, count)| format!("{code}={}", count.count))
            .collect::<Vec<_>>()
            .join(" ");
        Some(format!(
            "controller tick failed: {counts}; active={} truncated={} cursor_stale={}",
            health.active_count, health.truncated, health.cursor_stale
        ))
    }
}

pub struct HealthStore {
    root: RootedDir,
}

impl HealthStore {
    pub fn open(path: &Path) -> Result<Self, WorkerError> {
        Ok(Self {
            root: open_controller_root(path)?,
        })
    }

    pub fn read_existing(path: &Path) -> Result<Option<ControllerHealth>, WorkerError> {
        let Some(root) = open_existing_controller_root(path)? else {
            return Ok(None);
        };
        Self { root }.read()
    }

    pub fn read(&self) -> Result<Option<ControllerHealth>, WorkerError> {
        self.read_with_hook(|| {})
    }

    fn read_with_hook(
        &self,
        mut after_open: impl FnMut(),
    ) -> Result<Option<ControllerHealth>, WorkerError> {
        let mut attempts = 0;
        let bytes = loop {
            match self.root.read_private_regular_with_hook(
                HEALTH_FILE,
                MAX_HEALTH_BYTES,
                &mut after_open,
            ) {
                Ok(bytes) => break bytes,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) if error.raw_os_error() == Some(libc::ESTALE) && attempts < 3 => {
                    // Tick publication replaces this observation atomically.
                    // Recheck only within the root retained by this store.
                    self.root.verify_bound()?;
                    attempts += 1;
                }
                Err(error) => return Err(error.into()),
            }
        };
        let health: ControllerHealth =
            serde_json::from_slice(&bytes).map_err(|_| invalid_health())?;
        health.validate()?;
        Ok(Some(health))
    }

    pub fn write(&self, health: &ControllerHealth) -> Result<(), WorkerError> {
        health.validate()?;
        let bytes = serde_json::to_vec(health).map_err(|_| invalid_health())?;
        if bytes.len() as u64 > MAX_HEALTH_BYTES {
            return Err(invalid_health());
        }
        match self
            .root
            .read_private_regular(HEALTH_FILE, MAX_HEALTH_BYTES)
        {
            Ok(previous) => {
                self.root
                    .replace_private_regular_exact(HEALTH_FILE, &previous, &bytes)?
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.root
                    .write_private_atomic_no_replace(HEALTH_FILE, &bytes)?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

fn invalid_health() -> WorkerError {
    WorkerError::Protocol("CONTROLLER_TRANSPORT: controller health record is invalid".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt};

    fn health() -> ControllerHealth {
        ControllerHealth::new(
            ProcessIdentity::new(crate::fixture_pid::fixture_pid(1), 1).unwrap(),
            1,
        )
    }

    #[test]
    fn health_read_rechecks_a_tick_replacement_after_open() {
        let temp = tempfile::tempdir().unwrap();
        let store = HealthStore::open(&temp.path().join("controller")).unwrap();
        let initial = health();
        store.write(&initial).unwrap();
        let mut next = initial.clone();
        next.begin_tick(2);
        let mut replace = true;

        let observed = store
            .read_with_hook(|| {
                if std::mem::take(&mut replace) {
                    store.write(&next).unwrap();
                }
            })
            .unwrap()
            .unwrap();

        assert_eq!(observed, next);
    }

    #[test]
    fn health_read_recheck_preserves_invalid_records_and_directory_bindings() {
        for kind in ["record", "permissions", "directory"] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("controller");
            let store = HealthStore::open(&path).unwrap();
            store.write(&health()).unwrap();
            let mut replace = true;
            let error = store
                .read_with_hook(|| {
                    if !std::mem::take(&mut replace) {
                        return;
                    }
                    let file = path.join(HEALTH_FILE);
                    match kind {
                        "record" => {
                            let previous = fs::read(&file).unwrap();
                            let mut invalid = health();
                            invalid.version = 2;
                            store
                                .root
                                .replace_private_regular_exact(
                                    HEALTH_FILE,
                                    &previous,
                                    &serde_json::to_vec(&invalid).unwrap(),
                                )
                                .unwrap();
                        }
                        "permissions" => {
                            fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap()
                        }
                        "directory" => {
                            let bytes = fs::read(&file).unwrap();
                            fs::rename(&path, temp.path().join("detached")).unwrap();
                            fs::create_dir(&path).unwrap();
                            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
                            fs::write(&file, bytes).unwrap();
                            fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
                        }
                        _ => unreachable!(),
                    }
                })
                .unwrap_err();

            match kind {
                "record" => assert_eq!(error.public_code(), "CONTROLLER_TRANSPORT"),
                "permissions" => assert!(matches!(error, WorkerError::Io(error)
                    if error.kind() == io::ErrorKind::PermissionDenied)),
                "directory" => assert!(matches!(error, WorkerError::Io(error)
                    if error.raw_os_error() == Some(libc::ESTALE))),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn health_read_recheck_is_bounded_during_continuous_ticks() {
        let temp = tempfile::tempdir().unwrap();
        let store = HealthStore::open(&temp.path().join("controller")).unwrap();
        let mut next = health();
        store.write(&next).unwrap();
        let mut ticks = 0;
        let error = store
            .read_with_hook(|| {
                ticks += 1;
                assert!(ticks <= 16, "health snapshot must stop retrying");
                next.begin_tick(ticks + 1);
                store.write(&next).unwrap();
            })
            .unwrap_err();

        assert!(ticks > 1, "a raced health snapshot must be rechecked");
        assert!(matches!(error, WorkerError::Io(error)
            if error.raw_os_error() == Some(libc::ESTALE)));
    }
}
