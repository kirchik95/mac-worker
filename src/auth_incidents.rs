//! Private auth-incident records beside `facts.json`.
//!
//! A finished turn that matches an adapter's authentication-failure
//! signatures writes one record: agent kind, env profile name or none, a
//! fixed reason, and a timestamp. Nothing from the agent's output is stored.
//! [`merge_into_facts`] overlays a current incident onto collected facts so
//! `codex login status` still printing "Logged in" cannot keep the scheduler
//! advertising the agent. The host probe hot path applies the same overlay
//! read-only onto cached `facts.json` so a fresh authenticated cache cannot keep
//! advertising an agent after a turn already recorded a failure.
//!
//! An incident expires after [`AUTH_INCIDENT_TTL_MILLIS`] and is cleared
//! earlier when (a) a later turn of the same agent and profile succeeds,
//! (b) the operator passes `--clear-auth-incidents`, or (c) for Codex,
//! `~/.codex/auth.json` is newer than the incident (a re-login happened).
//! Successes use the same 24 h retention so the 64 KiB file cannot fill
//! with stale rows.
//!
//! Load-transform-publish is serialized by a rooted exclusive flock on
//! [`AUTH_INCIDENTS_LOCK_FILE`] plus an in-process mutex. `replace_private_regular_exact`
//! is not a linearizable CAS for concurrent writers (it can publish before
//! returning ESTALE), so a facts refresh and a turn must not overlap that
//! window. Unchanged bytes skip replacement.

use std::{
    cell::Cell,
    fs::File,
    io,
    os::fd::AsRawFd,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::UNIX_EPOCH,
};

use serde::{Deserialize, Serialize};

use crate::{
    agent::AgentKind,
    agent_facts::{AgentAuth, AgentFacts, turn_auth_failure_reason},
    error::WorkerError,
    rooted_fs::{PrivateEntryIdentity, RootedDir},
};

/// Sibling of `facts.json` under the host state root.
pub const AUTH_INCIDENTS_FILE: &str = "auth-incidents.json";
/// Exclusive flock covering one load-transform-publish of [`AUTH_INCIDENTS_FILE`].
pub const AUTH_INCIDENTS_LOCK_FILE: &str = "auth-incidents.lock";
/// Fixed reason stored on the incident. Never a log excerpt.
pub const AUTH_INCIDENT_REASON: &str = "auth failed in a turn";
/// Public task-outcome text for an authentication failure observed in a turn.
pub const AGENT_AUTHENTICATION_FAILED: &str = "agent authentication failed";
/// Overlay when the private incident file cannot be read. Interned, bounded.
pub const AUTH_INCIDENTS_UNREADABLE_REASON: &str = "auth incidents unreadable";
/// Incidents and successes older than this are ignored and pruned.
pub const AUTH_INCIDENT_TTL_MILLIS: u64 = 24 * 60 * 60 * 1000;

const MAX_INCIDENTS_BYTES: u64 = 64 * 1024;
const INCIDENTS_WRITE_RETRIES: usize = 8;
const MAX_PROFILE_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct AuthIncidentStore {
    incidents: Vec<AuthIncidentRecord>,
    successes: Vec<AuthSuccessRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthIncidentRecord {
    agent: String,
    profile: Option<String>,
    reason: String,
    at_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthSuccessRecord {
    agent: String,
    profile: Option<String>,
    at_millis: u64,
}

fn protocol(message: impl Into<String>) -> WorkerError {
    WorkerError::Protocol(message.into())
}

fn agent_name(kind: AgentKind) -> &'static str {
    kind.as_str()
}

fn same_key(agent: &str, profile: Option<&str>, kind: AgentKind, wanted: Option<&str>) -> bool {
    agent == agent_name(kind) && profile == wanted
}

fn valid_profile(profile: Option<&str>) -> bool {
    match profile {
        None => true,
        Some(name) => {
            !name.is_empty()
                && name.len() <= MAX_PROFILE_BYTES
                && !name.contains('/')
                && !name.contains('\\')
                && name != "."
                && name != ".."
                && !name.chars().any(char::is_control)
        }
    }
}

fn load_with_bytes(host_state_root: &Path) -> Result<(Vec<u8>, AuthIncidentStore), WorkerError> {
    let root = RootedDir::open(host_state_root).map_err(WorkerError::Io)?;
    if !root.entry_exists(AUTH_INCIDENTS_FILE)? {
        return Ok((Vec::new(), AuthIncidentStore::default()));
    }
    let bytes = root
        .read_private_regular(AUTH_INCIDENTS_FILE, MAX_INCIDENTS_BYTES)
        .map_err(WorkerError::Io)?;
    let store: AuthIncidentStore = serde_json::from_slice(&bytes)
        .map_err(|_| protocol("cached auth incidents are invalid"))?;
    store.validate()?;
    let canonical =
        serde_json::to_vec(&store).map_err(|_| protocol("cached auth incidents are invalid"))?;
    if canonical != bytes {
        return Err(protocol("cached auth incidents are not canonical"));
    }
    Ok((bytes, store))
}

enum Commit {
    Done,
    Retry,
}

fn commit(
    host_state_root: &Path,
    expected: &[u8],
    state: &AuthIncidentStore,
) -> Result<Commit, WorkerError> {
    state.validate()?;
    let bytes =
        serde_json::to_vec(state).map_err(|_| protocol("failed to serialize auth incidents"))?;
    if bytes.len() as u64 > MAX_INCIDENTS_BYTES {
        return Err(protocol("auth incidents exceed 64 KiB"));
    }
    if bytes == expected {
        return Ok(Commit::Done);
    }
    let root = RootedDir::open(host_state_root).map_err(WorkerError::Io)?;
    if expected.is_empty() {
        if !root.entry_exists(AUTH_INCIDENTS_FILE)?
            && state.incidents.is_empty()
            && state.successes.is_empty()
        {
            return Ok(Commit::Done);
        }
        match root.write_private_atomic_no_replace(AUTH_INCIDENTS_FILE, &bytes) {
            Ok(()) => Ok(Commit::Done),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(Commit::Retry),
            Err(error) => Err(WorkerError::Io(error)),
        }
    } else {
        match root.replace_private_regular_exact(AUTH_INCIDENTS_FILE, expected, &bytes) {
            Ok(()) => Ok(Commit::Done),
            Err(error) if error.raw_os_error() == Some(libc::ESTALE) => Ok(Commit::Retry),
            Err(error) => Err(WorkerError::Io(error)),
        }
    }
}

fn mutate(
    host_state_root: &Path,
    mut transform: impl FnMut(&mut AuthIncidentStore) -> Result<(), WorkerError>,
) -> Result<(), WorkerError> {
    run_before_lock_hook();
    let _threads = INCIDENT_THREADS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let lock = IncidentLock::acquire(host_state_root)?;
    for _ in 0..INCIDENTS_WRITE_RETRIES {
        let (expected, mut state) = load_with_bytes(host_state_root)?;
        run_after_load_hook();
        transform(&mut state)?;
        state.canonicalize();
        run_before_publish_hook();
        match commit(host_state_root, &expected, &state)? {
            Commit::Done => {
                lock.validate()?;
                return Ok(());
            }
            Commit::Retry => continue,
        }
    }
    Err(WorkerError::Io(io::Error::from_raw_os_error(libc::EAGAIN)))
}

static INCIDENT_THREADS: Mutex<()> = Mutex::new(());

/// Rooted exclusive flock for the incident file. Same-process threads also
/// take [`INCIDENT_THREADS`] because Darwin `flock` is owned by the process.
struct IncidentLock {
    root: RootedDir,
    file: File,
    identity: PrivateEntryIdentity,
    #[cfg(test)]
    _fork_exclusion: crate::test_sync::HeldFlock,
}

impl IncidentLock {
    fn acquire(host_state_root: &Path) -> Result<Self, WorkerError> {
        let root = RootedDir::open(host_state_root).map_err(WorkerError::Io)?;
        for _ in 0..INCIDENTS_WRITE_RETRIES {
            let (file, created) = root
                .open_private_lock_with_created(AUTH_INCIDENTS_LOCK_FILE)
                .map_err(WorkerError::Io)?;
            if created {
                root.sync_root().map_err(WorkerError::Io)?;
            }
            let identity = root
                .private_entry_identity(AUTH_INCIDENTS_LOCK_FILE)
                .map_err(WorkerError::Io)?;
            root.validate_private_regular_binding(AUTH_INCIDENTS_LOCK_FILE, &file, identity)
                .map_err(WorkerError::Io)?;
            #[cfg(test)]
            let fork_exclusion = crate::test_sync::HeldFlock::acquire();
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
            if result != 0 {
                return Err(WorkerError::Io(io::Error::last_os_error()));
            }
            match root.validate_private_regular_binding(AUTH_INCIDENTS_LOCK_FILE, &file, identity) {
                Ok(()) => {
                    root.verify_bound().map_err(WorkerError::Io)?;
                    return Ok(Self {
                        root,
                        file,
                        identity,
                        #[cfg(test)]
                        _fork_exclusion: fork_exclusion,
                    });
                }
                Err(error) if error.raw_os_error() == Some(libc::ESTALE) => {
                    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
                    continue;
                }
                Err(error) => return Err(WorkerError::Io(error)),
            }
        }
        Err(WorkerError::Io(io::Error::from_raw_os_error(libc::EAGAIN)))
    }

    fn validate(&self) -> Result<(), WorkerError> {
        self.root.verify_bound().map_err(WorkerError::Io)?;
        self.root
            .validate_private_regular_binding(AUTH_INCIDENTS_LOCK_FILE, &self.file, self.identity)
            .map_err(WorkerError::Io)?;
        Ok(())
    }
}

impl Drop for IncidentLock {
    fn drop(&mut self) {
        let _ = self.validate();
        unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

impl AuthIncidentStore {
    fn validate(&self) -> Result<(), WorkerError> {
        for incident in &self.incidents {
            if AgentKind::from_name(&incident.agent).is_none() {
                return Err(protocol("auth incident agent is unknown"));
            }
            if !valid_profile(incident.profile.as_deref()) {
                return Err(protocol("auth incident profile is invalid"));
            }
            if incident.reason != AUTH_INCIDENT_REASON {
                return Err(protocol("auth incident reason is not the fixed value"));
            }
        }
        for success in &self.successes {
            if AgentKind::from_name(&success.agent).is_none() {
                return Err(protocol("auth success agent is unknown"));
            }
            if !valid_profile(success.profile.as_deref()) {
                return Err(protocol("auth success profile is invalid"));
            }
        }
        Ok(())
    }

    fn canonicalize(&mut self) {
        self.incidents.sort_by(|left, right| {
            left.agent
                .cmp(&right.agent)
                .then_with(|| left.profile.cmp(&right.profile))
        });
        self.successes.sort_by(|left, right| {
            left.agent
                .cmp(&right.agent)
                .then_with(|| left.profile.cmp(&right.profile))
        });
    }

    fn upsert_incident(&mut self, kind: AgentKind, profile: Option<&str>, at_millis: u64) {
        let agent = agent_name(kind).to_owned();
        let profile = profile.map(str::to_owned);
        if let Some(existing) = self.incidents.iter_mut().find(|incident| {
            same_key(
                &incident.agent,
                incident.profile.as_deref(),
                kind,
                profile.as_deref(),
            )
        }) {
            existing.at_millis = existing.at_millis.max(at_millis);
            existing.reason = AUTH_INCIDENT_REASON.to_owned();
            return;
        }
        self.incidents.push(AuthIncidentRecord {
            agent,
            profile,
            reason: AUTH_INCIDENT_REASON.to_owned(),
            at_millis,
        });
    }

    fn upsert_success(&mut self, kind: AgentKind, profile: Option<&str>, at_millis: u64) {
        let agent = agent_name(kind).to_owned();
        let profile_owned = profile.map(str::to_owned);
        self.incidents.retain(|incident| {
            !same_key(&incident.agent, incident.profile.as_deref(), kind, profile)
                || incident.at_millis > at_millis
        });
        if let Some(existing) = self
            .successes
            .iter_mut()
            .find(|success| same_key(&success.agent, success.profile.as_deref(), kind, profile))
        {
            existing.at_millis = existing.at_millis.max(at_millis);
            return;
        }
        self.successes.push(AuthSuccessRecord {
            agent,
            profile: profile_owned,
            at_millis,
        });
    }

    fn prune_expired(&mut self, account_home: Option<&Path>, now_millis: u64) {
        self.successes.retain(|success| {
            now_millis.saturating_sub(success.at_millis) < AUTH_INCIDENT_TTL_MILLIS
        });
        self.incidents.retain(|incident| {
            incident_is_current(incident, &self.successes, account_home, now_millis)
        });
    }
}

fn incident_is_current(
    incident: &AuthIncidentRecord,
    successes: &[AuthSuccessRecord],
    account_home: Option<&Path>,
    now_millis: u64,
) -> bool {
    if now_millis.saturating_sub(incident.at_millis) >= AUTH_INCIDENT_TTL_MILLIS {
        return false;
    }
    if successes.iter().any(|success| {
        success.agent == incident.agent
            && success.profile == incident.profile
            && success.at_millis >= incident.at_millis
    }) {
        return false;
    }
    if incident.agent == AgentKind::Codex.as_str()
        && let Some(home) = account_home
        && let Some(mtime) = codex_auth_mtime_millis(home)
        && mtime > incident.at_millis
    {
        return false;
    }
    true
}

fn codex_auth_mtime_millis(account_home: &Path) -> Option<u64> {
    let metadata = std::fs::metadata(account_home.join(".codex").join("auth.json")).ok()?;
    let modified = metadata.modified().ok()?;
    let duration = modified.duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(duration.as_millis()).ok()
}

/// Records an authentication failure observed in a finished turn.
///
/// Only the agent, profile name, the fixed reason, and the time are stored.
pub fn record_incident(
    host_state_root: &Path,
    kind: AgentKind,
    profile: Option<&str>,
    at_millis: u64,
) -> Result<(), WorkerError> {
    if !valid_profile(profile) {
        return Err(protocol("auth incident profile is invalid"));
    }
    mutate(host_state_root, |state| {
        state.upsert_incident(kind, profile, at_millis);
        state.prune_expired(None, at_millis);
        Ok(())
    })
}

/// Records that a later turn of the same agent and profile succeeded.
///
/// A late success older than an already-recorded incident does not hide that
/// incident. The success timestamp is kept as a max so a later success still
/// clears a matching older failure.
pub fn record_success(
    host_state_root: &Path,
    kind: AgentKind,
    profile: Option<&str>,
    at_millis: u64,
) -> Result<(), WorkerError> {
    if !valid_profile(profile) {
        return Err(protocol("auth success profile is invalid"));
    }
    mutate(host_state_root, |state| {
        state.upsert_success(kind, profile, at_millis);
        state.prune_expired(None, at_millis);
        Ok(())
    })
}

/// Operator-requested clearance used by `refresh-facts --clear-auth-incidents`.
pub fn clear_all(host_state_root: &Path) -> Result<(), WorkerError> {
    mutate(host_state_root, |state| {
        *state = AuthIncidentStore::default();
        Ok(())
    })
}

/// Overlays current incidents onto collected facts and prunes the store.
///
/// For each remaining incident the matching `auth` or `auth_by_profile`
/// slot becomes [`AgentAuth::UnknownWithReason`] with an interned
/// `auth failed in a turn at <ISO minute>`. Expired incidents, incidents
/// older than a later success, and Codex incidents whose `auth.json` is
/// newer are dropped from the private file.
pub fn merge_into_facts(
    facts: &mut AgentFacts,
    host_state_root: &Path,
    account_home: &Path,
    now_millis: u64,
) -> Result<(), WorkerError> {
    let mut overlaid = None;
    mutate(host_state_root, |state| {
        state.prune_expired(Some(account_home), now_millis);
        overlaid = Some(state.clone());
        Ok(())
    })?;
    if let Some(state) = overlaid.as_ref() {
        overlay_facts(facts, state);
    }
    Ok(())
}

/// Read-only overlay of current incidents onto already-collected facts.
///
/// Used by the host probe hot path so a turn-recorded failure is visible
/// before the next facts refresh. Does not prune, lock-write, or change
/// [`AgentFacts::collected_at_millis`]. Unreadable stores get
/// [`apply_unreadable_overlay`]. Codex `auth.json` re-login is left to
/// [`merge_into_facts`] on refresh.
pub fn overlay_current_incidents(
    facts: &mut AgentFacts,
    host_state_root: &Path,
    now_millis: u64,
) -> Result<(), WorkerError> {
    match load_with_bytes(host_state_root) {
        Ok((_, mut state)) => {
            state.incidents.retain(|incident| {
                incident_is_current(incident, &state.successes, None, now_millis)
            });
            overlay_facts(facts, &state);
            Ok(())
        }
        Err(error) => {
            apply_unreadable_overlay(facts);
            Err(error)
        }
    }
}

/// Replaces advertised authentication with [`AUTH_INCIDENTS_UNREADABLE_REASON`].
///
/// Used when the incident store cannot be applied: facts stay conservative
/// (no `agent:<kind>` capability) and the reason is visible to operators.
pub fn apply_unreadable_overlay(facts: &mut AgentFacts) {
    for agent in &mut facts.agents {
        if !matches!(agent.auth, AgentAuth::UnknownWithReason(_)) {
            agent.auth = AgentAuth::UnknownWithReason(AUTH_INCIDENTS_UNREADABLE_REASON);
        }
        for (_, auth) in &mut agent.auth_by_profile {
            if !matches!(auth, AgentAuth::UnknownWithReason(_)) {
                *auth = AgentAuth::UnknownWithReason(AUTH_INCIDENTS_UNREADABLE_REASON);
            }
        }
    }
}

fn overlay_facts(facts: &mut AgentFacts, state: &AuthIncidentStore) {
    for incident in &state.incidents {
        let Some(reason) = turn_auth_failure_reason(incident.at_millis) else {
            continue;
        };
        let Some(agent) = facts
            .agents
            .iter_mut()
            .find(|agent| agent.name == incident.agent)
        else {
            continue;
        };
        match incident.profile.as_deref() {
            None => agent.auth = AgentAuth::UnknownWithReason(reason),
            Some(profile) => {
                if let Some((_, auth)) = agent
                    .auth_by_profile
                    .iter_mut()
                    .find(|(name, _)| name == profile)
                {
                    *auth = AgentAuth::UnknownWithReason(reason);
                }
            }
        }
    }
}

static AFTER_LOAD_HOOK: Mutex<Option<Arc<dyn Fn() + Send + Sync>>> = Mutex::new(None);
static AFTER_LOAD_GENERATION: AtomicUsize = AtomicUsize::new(0);
static BEFORE_LOCK_HOOK: Mutex<Option<Arc<dyn Fn() + Send + Sync>>> = Mutex::new(None);
static BEFORE_LOCK_GENERATION: AtomicUsize = AtomicUsize::new(0);
static BEFORE_PUBLISH_HOOK: Mutex<Option<Arc<dyn Fn() + Send + Sync>>> = Mutex::new(None);
static BEFORE_PUBLISH_GENERATION: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static AFTER_LOAD_SEEN_GENERATION: Cell<usize> = const { Cell::new(0) };
    static BEFORE_LOCK_SEEN_GENERATION: Cell<usize> = const { Cell::new(0) };
    static BEFORE_PUBLISH_SEEN_GENERATION: Cell<usize> = const { Cell::new(0) };
    static MUTATE_HOOKS_ENABLED: Cell<bool> = const { Cell::new(false) };
}

/// Test hook fired after each first load of a mutate generation (retries skip).
#[doc(hidden)]
pub fn set_after_load_hook(hook: Option<Arc<dyn Fn() + Send + Sync>>) {
    AFTER_LOAD_GENERATION.fetch_add(1, Ordering::SeqCst);
    *AFTER_LOAD_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = hook;
}

/// Test hook fired once per mutate generation before the in-process mutex
/// and rooted flock. Start barriers belong here, not under the lock.
#[doc(hidden)]
pub fn set_before_lock_hook(hook: Option<Arc<dyn Fn() + Send + Sync>>) {
    BEFORE_LOCK_GENERATION.fetch_add(1, Ordering::SeqCst);
    *BEFORE_LOCK_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = hook;
}

/// Test hook fired after the transform and before publishing, while the
/// rooted lock is still held. Exercises the replace_exact exchange window.
#[doc(hidden)]
pub fn set_before_publish_hook(hook: Option<Arc<dyn Fn() + Send + Sync>>) {
    BEFORE_PUBLISH_GENERATION.fetch_add(1, Ordering::SeqCst);
    *BEFORE_PUBLISH_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = hook;
}

/// Opt this thread into mutate test hooks. Other tests must not pause.
#[doc(hidden)]
pub fn enable_after_load_hook_on_this_thread() {
    MUTATE_HOOKS_ENABLED.set(true);
}

fn run_after_load_hook() {
    run_generation_hook(
        &AFTER_LOAD_HOOK,
        &AFTER_LOAD_GENERATION,
        &AFTER_LOAD_SEEN_GENERATION,
    );
}

fn run_before_lock_hook() {
    run_generation_hook(
        &BEFORE_LOCK_HOOK,
        &BEFORE_LOCK_GENERATION,
        &BEFORE_LOCK_SEEN_GENERATION,
    );
}

fn run_before_publish_hook() {
    run_generation_hook(
        &BEFORE_PUBLISH_HOOK,
        &BEFORE_PUBLISH_GENERATION,
        &BEFORE_PUBLISH_SEEN_GENERATION,
    );
}

fn run_generation_hook(
    hook: &Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    generation: &AtomicUsize,
    seen: &'static std::thread::LocalKey<Cell<usize>>,
) {
    if !MUTATE_HOOKS_ENABLED.get() {
        return;
    }
    let generation = generation.load(Ordering::SeqCst);
    let hook = hook
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let Some(hook) = hook else {
        return;
    };
    if seen.get() == generation {
        return;
    }
    seen.set(generation);
    hook();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    use crate::host_store::HostStore;

    fn host_root() -> (tempfile::TempDir, std::path::PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("host");
        HostStore::open(&root).unwrap();
        (temp, root)
    }

    #[test]
    fn a_late_success_does_not_hide_a_newer_incident() {
        let (_temp, host) = host_root();
        record_incident(&host, AgentKind::Codex, None, 200).unwrap();
        record_success(&host, AgentKind::Codex, None, 100).unwrap();
        let stored = std::fs::read_to_string(host.join(AUTH_INCIDENTS_FILE)).unwrap();
        assert!(stored.contains(r#""at_millis":200"#), "{stored}");
        assert!(stored.contains("incidents"), "{stored}");
    }

    #[test]
    fn a_later_success_clears_an_older_incident() {
        let (_temp, host) = host_root();
        record_incident(&host, AgentKind::Codex, None, 100).unwrap();
        record_success(&host, AgentKind::Codex, None, 200).unwrap();
        let stored = std::fs::read_to_string(host.join(AUTH_INCIDENTS_FILE)).unwrap();
        assert!(!stored.contains(r#""at_millis":100"#), "{stored}");
        assert!(stored.contains(r#""at_millis":200"#), "{stored}");
    }

    #[test]
    fn successes_older_than_the_ttl_are_pruned() {
        let (_temp, host) = host_root();
        record_success(&host, AgentKind::Codex, None, 1).unwrap();
        record_incident(&host, AgentKind::Cursor, None, 1 + AUTH_INCIDENT_TTL_MILLIS).unwrap();
        let stored = std::fs::read_to_string(host.join(AUTH_INCIDENTS_FILE)).unwrap();
        assert!(!stored.contains("codex"), "{stored}");
        assert!(stored.contains("cursor"), "{stored}");
    }

    #[test]
    fn incident_lock_is_private_and_created_with_the_store() {
        let (_temp, host) = host_root();
        record_incident(&host, AgentKind::Codex, None, 1).unwrap();
        let metadata = std::fs::metadata(host.join(AUTH_INCIDENTS_LOCK_FILE)).unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }
}
