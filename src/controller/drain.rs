//! Persistent controller admission valve. Requests and running turns keep
//! progressing while drained; only new runner handoffs are deferred.
//!
//! The stable lock is shared by launchers and exclusive for flag updates.
//! A launcher retains its permit through spawn and ownership publication,
//! so a successful `set_drained(true)` fences every later handoff.

use std::{
    fs::File,
    io,
    os::{fd::AsRawFd, unix::fs::FileExt},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::{error::WorkerError, rooted_fs::RootedDir};

/// Optional sink is supplied only after opening an initialized host journal.
pub fn set_drained_with_event_sink(
    state_root: &Path,
    drained: bool,
    sink: Option<std::sync::Arc<dyn crate::controller::events::EventSink>>,
) -> Result<(), WorkerError> {
    if let Some(warning) = set_drained_with_warning(state_root, drained, sink)? {
        warning.print();
    }
    Ok(())
}

pub(crate) fn set_drained_with_warning(
    state_root: &Path,
    drained: bool,
    sink: Option<std::sync::Arc<dyn crate::controller::events::EventSink>>,
) -> Result<Option<GateWarning>, WorkerError> {
    set_gate_at(
        state_root,
        Some(drained),
        crate::integration::contracts::IntegrationPauseReason::ControllerDrained,
        super::leader::now_millis()?,
        sink,
    )
}

pub(crate) fn close_for_disable(state_root: &Path) -> Result<Option<GateWarning>, WorkerError> {
    set_gate_at(
        state_root,
        None,
        crate::integration::contracts::IntegrationPauseReason::ControllerDisabled,
        super::leader::now_millis()?,
        None,
    )
}

fn set_gate_at(
    state_root: &Path,
    drained: Option<bool>,
    reason: crate::integration::contracts::IntegrationPauseReason,
    now: u64,
    sink: Option<std::sync::Arc<dyn crate::controller::events::EventSink>>,
) -> Result<Option<GateWarning>, WorkerError> {
    let hints = sink.map(crate::client_state::events::DeferredHints::begin);
    let root = open_controller_root(state_root)?;
    let lock = root.open_private_lock(DRAIN_LOCK).map_err(store_io)?;
    lock_exclusive(&lock)?;
    validate_lock(&root, &lock)?;
    // Ordinary dispatch is independent of optional integration metadata.
    // Commit its operator flag first, under the same admission lock.
    if let Some(drained) = drained {
        let next = state_bytes(drained)?;
        let changed = if root.entry_exists(DRAIN_FILE).map_err(store_io)? {
            let (previous, state) = read_state(&root)?;
            if state.drained != drained {
                root.replace_private_regular_exact(DRAIN_FILE, &previous, &next)
                    .map_err(store_io)?;
                true
            } else {
                false
            }
        } else {
            root.write_private_atomic_no_replace(DRAIN_FILE, &next)
                .map_err(store_io)?;
            true
        };
        if changed && let Some(hints) = &hints {
            hints.capture(crate::controller::events::NewEvent::ControllerDrainChanged { drained });
        }
        mark_initialized(&root, &lock)?;
    }
    let warning = match update_integration_gate(&root, drained.unwrap_or(true), reason, now) {
        Ok(warning) => warning.map(|error| GateWarning::new(&error, true, drained.is_none())),
        Err(error) => Some(GateWarning::new(&error, false, drained.is_none())),
    };
    // On both success and error, reverse local drop order releases the lock first.
    Ok(warning)
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct GateWarning {
    code: String,
    reset: bool,
    disabled: bool,
}

impl GateWarning {
    pub(crate) fn new(error: &WorkerError, reset: bool, disabled: bool) -> Self {
        Self {
            code: error.public_code(),
            reset,
            disabled,
        }
    }
    fn line(&self) -> String {
        let detail = if self.reset {
            "pause history was reset"
        } else {
            "pause could not be persisted"
        };
        let resume = if self.disabled && !self.reset {
            "; integration may resume after re-enable"
        } else {
            ""
        };
        format!(
            "warning: integration gate [{}]; {detail}{resume}",
            self.code
        )
    }
    pub(crate) fn print(&self) {
        use std::io::Write;
        let _ = writeln!(std::io::stderr(), "{}", self.line());
    }
}

/// Only public gate diagnostics cross the operator transport. Ordinary result
/// DTOs and arbitrary remote stderr stay unchanged and unexposed.
pub(crate) struct OperatorRunner<'a> {
    runner: &'a dyn crate::process::ProcessRunner,
    warnings: std::sync::Mutex<Vec<GateWarning>>,
}
impl<'a> OperatorRunner<'a> {
    pub(crate) fn new(runner: &'a dyn crate::process::ProcessRunner) -> Self {
        Self {
            runner,
            warnings: std::sync::Mutex::new(vec![]),
        }
    }
    pub(crate) fn record(&self, warning: GateWarning) {
        let mut warnings = self.warnings.lock().unwrap_or_else(|e| e.into_inner());
        if !warnings
            .iter()
            .any(|existing| existing.line() == warning.line())
        {
            warnings.push(warning);
        }
    }
    fn capture(&self, result: &crate::process::ProcessResult) {
        // Match complete known lines rather than forwarding untrusted stderr.
        for code in ["IO", "CONTROLLER_TRANSPORT"] {
            for reset in [false, true] {
                for disabled in [false, true] {
                    let warning = GateWarning {
                        code: code.into(),
                        reset,
                        disabled,
                    };
                    if result
                        .stderr
                        .split(|b| *b == b'\n')
                        .take(16)
                        .any(|line| line == warning.line().as_bytes())
                    {
                        self.record(warning);
                    }
                }
            }
        }
    }
    pub(crate) fn emit(&self, stderr: &mut dyn std::io::Write) {
        for warning in self
            .warnings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            let _ = writeln!(stderr, "{}", warning.line());
        }
    }
}
impl crate::process::ProcessRunner for OperatorRunner<'_> {
    fn run(
        &self,
        request: &crate::process::ProcessRequest,
    ) -> Result<crate::process::ProcessResult, WorkerError> {
        let result = self.runner.run(request)?;
        self.capture(&result);
        Ok(result)
    }
    fn run_in_new_session(
        &self,
        request: &crate::process::ProcessRequest,
    ) -> Result<crate::process::ProcessResult, WorkerError> {
        let result = self.runner.run_in_new_session(request)?;
        self.capture(&result);
        Ok(result)
    }
    fn run_interruptible(
        &self,
        request: &crate::process::ProcessRequest,
        stop: &dyn Fn() -> bool,
    ) -> Result<crate::process::ProcessResult, WorkerError> {
        let result = self.runner.run_interruptible(request, stop)?;
        self.capture(&result);
        Ok(result)
    }
}

use super::leader::{
    lock_exclusive, open_controller_root, open_existing_controller_root, store_io,
};

const DRAIN_LOCK: &str = "drain.lock";
const DRAIN_FILE: &str = "drain.json";
const MAX_DRAIN_BYTES: u64 = 4096;
const INTEGRATION_GATE: &str = "integration-gate.json";
// Admission consults ten active minutes and backoff at most thirty seconds.
// Keep a wide margin, measured in active time rather than wall time.
const PAUSE_HISTORY_ACTIVE_MILLIS: u64 = 60 * 60 * 1000;
const MAX_PAUSE_WINDOWS: usize = 256;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IntegrationGate {
    version: u32,
    windows: Vec<PauseWindow>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PauseWindow {
    reason: crate::integration::contracts::IntegrationPauseReason,
    effective_at_millis: u64,
    resumed_at_millis: Option<u64>,
}

fn read_integration_gate(
    root: &RootedDir,
) -> Result<Option<(Vec<u8>, IntegrationGate)>, WorkerError> {
    let bytes = match root.read_private_regular(
        INTEGRATION_GATE,
        crate::integration::contracts::MAX_PRIVATE_RECORD_BYTES as u64,
    ) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(store_io(e)),
    };
    let gate = decode_integration_gate(&bytes)?;
    Ok(Some((bytes, gate)))
}

fn decode_integration_gate(bytes: &[u8]) -> Result<IntegrationGate, WorkerError> {
    let gate: IntegrationGate = serde_json::from_slice(bytes).map_err(|_| invalid_state())?;
    if gate.version != 1
        || gate.windows.iter().enumerate().any(|(i, window)| {
            !matches!(
                window.reason,
                crate::integration::contracts::IntegrationPauseReason::ControllerDrained
                    | crate::integration::contracts::IntegrationPauseReason::ControllerDisabled
            ) || window
                .resumed_at_millis
                .is_some_and(|end| end < window.effective_at_millis)
                || (i + 1 < gate.windows.len() && window.resumed_at_millis.is_none())
                || (i > 0
                    && gate.windows[i - 1]
                        .resumed_at_millis
                        .is_none_or(|end| end > window.effective_at_millis))
        })
    {
        return Err(invalid_state());
    }
    Ok(gate)
}
fn update_integration_gate(
    root: &RootedDir,
    drained: bool,
    reason: crate::integration::contracts::IntegrationPauseReason,
    now: u64,
) -> Result<Option<WorkerError>, WorkerError> {
    let old = match root.read_private_regular(
        INTEGRATION_GATE,
        crate::integration::contracts::MAX_PRIVATE_RECORD_BYTES as u64,
    ) {
        Ok(bytes) => Some(bytes),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(store_io(e)),
    };
    let mut warning = None;
    let previous = match old.as_ref() {
        None => None,
        Some(bytes) => match decode_integration_gate(bytes) {
            Ok(gate) => Some(gate),
            Err(error) => {
                // Quarantine only a bounded, private file that was read
                // successfully. I/O/safety failures never enter this branch.
                const CORRUPT: &str = "integration-gate.json.corrupt";
                if root.entry_exists(CORRUPT).map_err(store_io)? {
                    let saved = root
                        .read_private_regular(
                            CORRUPT,
                            crate::integration::contracts::MAX_PRIVATE_RECORD_BYTES as u64,
                        )
                        .map_err(store_io)?;
                    root.replace_private_regular_exact(CORRUPT, &saved, bytes)
                        .map_err(store_io)?;
                } else {
                    root.write_private_atomic_no_replace(CORRUPT, bytes)
                        .map_err(store_io)?;
                }
                warning = Some(error);
                None
            }
        },
    };
    if old.is_none() && !drained {
        return Ok(None);
    }
    let mut gate = previous.unwrap_or(IntegrationGate {
        version: 1,
        windows: vec![],
    });
    if drained {
        if gate
            .windows
            .last()
            .is_none_or(|w| w.resumed_at_millis.is_some())
        {
            let now = now.max(
                gate.windows
                    .last()
                    .and_then(|w| w.resumed_at_millis)
                    .unwrap_or(0),
            );
            gate.windows.push(PauseWindow {
                reason,
                effective_at_millis: now,
                resumed_at_millis: None,
            });
        } else if reason
            == crate::integration::contracts::IntegrationPauseReason::ControllerDisabled
        {
            gate.windows.last_mut().expect("open window").reason = reason;
        }
    } else if let Some(window) = gate.windows.last_mut()
        && window.resumed_at_millis.is_none()
    {
        window.resumed_at_millis = Some(now.max(window.effective_at_millis));
    }
    prune_pause_history(&mut gate, now);
    let bytes = serde_json::to_vec(&gate).map_err(|_| invalid_state())?;
    if bytes.len() > crate::integration::contracts::MAX_PRIVATE_RECORD_BYTES {
        return Err(invalid_state());
    }
    match old {
        Some(old) if old == bytes => {}
        Some(old) => root
            .replace_private_regular_exact(INTEGRATION_GATE, &old, &bytes)
            .map_err(store_io)?,
        None => root
            .write_private_atomic_no_replace(INTEGRATION_GATE, &bytes)
            .map_err(store_io)?,
    }
    Ok(warning)
}

fn prune_pause_history(gate: &mut IntegrationGate, now: u64) {
    let mut cursor = now.max(
        gate.windows
            .last()
            .map_or(0, |w| w.resumed_at_millis.unwrap_or(w.effective_at_millis)),
    );
    let mut active = 0u64;
    let mut first_retained = 0;
    for (index, window) in gate.windows.iter().enumerate().rev() {
        if let Some(end) = window.resumed_at_millis {
            active = active.saturating_add(cursor.saturating_sub(end));
            if active > PAUSE_HISTORY_ACTIVE_MILLIS {
                first_retained = index + 1;
                break;
            }
        }
        cursor = window.effective_at_millis;
    }
    gate.windows.drain(..first_retained);
    // Rapid cycling can exceed the cap inside a live budget. Losing the
    // oldest closed pauses then charges more active time, never less: an
    // auxiliary may expire early with a re-drivable queue timeout. The open
    // window is last and is never dropped. Below the cap, live accounting
    // stays exact because only windows beyond the active horizon are removed.
    let excess = gate.windows.len().saturating_sub(MAX_PAUSE_WINDOWS);
    gate.windows.drain(..excess);
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DrainState {
    version: u32,
    drained: bool,
}

/// Held only for the new runner's spawn/handoff, never for a running turn.
pub(crate) struct DrainLaunchPermit {
    _lock: Option<File>,
    _hints: crate::client_state::events::DeferredHints,
}

/// Establish the lock before a controller store can execute any requests.
/// Existing flags, including `true`, must survive every store/leader reopen.
pub(crate) fn initialize(root: &RootedDir) -> Result<(), WorkerError> {
    let lock = root.open_private_lock(DRAIN_LOCK).map_err(store_io)?;
    lock_exclusive(&lock)?;
    let initialized = validate_lock(root, &lock)?;
    if root.entry_exists(DRAIN_FILE).map_err(store_io)? {
        read_state(root)?;
    } else if !initialized {
        root.write_private_atomic_no_replace(DRAIN_FILE, &state_bytes(false)?)
            .map_err(store_io)?;
    } else {
        // A prior controller already established this gate. Losing its
        // state cannot silently undo an operator's persisted drain.
        return Err(invalid_state());
    }
    mark_initialized(root, &lock)?;
    Ok(())
}

/// Persist the valve under the same lock used for runner admission. This
/// does not stop a running turn or prevent durable request publication.
#[cfg(any(test, feature = "test-support"))]
pub fn set_drained(state_root: &Path, drained: bool) -> Result<(), WorkerError> {
    set_drained_with_event_sink(state_root, drained, None)
}

#[cfg(test)]
pub(crate) fn set_drained_at(
    state_root: &Path,
    drained: bool,
    now: u64,
) -> Result<(), WorkerError> {
    if let Some(warning) = set_gate_at(
        state_root,
        Some(drained),
        crate::integration::contracts::IntegrationPauseReason::ControllerDrained,
        now,
        None,
    )? {
        warning.print();
    }
    Ok(())
}

/// Read-only observation. A missing controller store is ordinary local
/// mode; an initialized store with unreadable state fails closed.
pub fn is_drained(state_root: &Path) -> Result<bool, WorkerError> {
    let Some(root) = open_existing_controller_root(state_root)? else {
        return Ok(false);
    };
    let Some(lock) = existing_lock(&root)? else {
        return Ok(false);
    };
    lock_shared(&lock)?;
    read_drain_flag(&root, &lock)
}

fn read_drain_flag(root: &RootedDir, lock: &File) -> Result<bool, WorkerError> {
    let initialized = validate_lock(root, lock)?;
    // Disable may establish only an integration window. No ordinary flag
    // was committed until a controller store opens or an operator drains.
    if !initialized && !root.entry_exists(DRAIN_FILE).map_err(store_io)? {
        return Ok(false);
    }
    Ok(read_state(root)?.1.drained)
}

pub(crate) fn integration_pause(
    state_root: &Path,
) -> Result<Option<crate::integration::contracts::IntegrationPauseEvidence>, WorkerError> {
    let Some(root) = open_existing_controller_root(state_root)? else {
        return Ok(None);
    };
    let Some(lock) = existing_lock(&root)? else {
        return Ok(None);
    };
    lock_shared(&lock)?;
    validate_lock(&root, &lock)?;
    Ok(read_integration_gate(&root)?.and_then(|(_, gate)| {
        gate.windows
            .last()
            .filter(|w| w.resumed_at_millis.is_none())
            .map(
                |w| crate::integration::contracts::IntegrationPauseEvidence {
                    reason: w.reason,
                    effective_at_millis: w.effective_at_millis,
                },
            )
    }))
}

/// None means drained; errors also prohibit launch. No files are created
/// on this path, so clients without a controller store retain local mode.
pub(crate) fn launch_permit(
    state_root: &Path,
    deadline: crate::client_state::WaitDeadline,
) -> Result<Option<DrainLaunchPermit>, WorkerError> {
    let hints = crate::client_state::events::DeferredHints::fence();
    deadline.remaining()?;
    let Some(root) = open_existing_controller_root(state_root)? else {
        return Ok(Some(DrainLaunchPermit {
            _lock: None,
            _hints: hints,
        }));
    };
    let Some(lock) = existing_lock(&root)? else {
        return Ok(Some(DrainLaunchPermit {
            _lock: None,
            _hints: hints,
        }));
    };
    deadline.lock(lock.as_raw_fd(), libc::LOCK_SH)?;
    if read_drain_flag(&root, &lock)? {
        return Ok(None);
    }
    Ok(Some(DrainLaunchPermit {
        _lock: Some(lock),
        _hints: hints,
    }))
}

/// Only the file guard crosses the Send integration phase contract. Queue
/// launchers retain their existing thread-local deferred-hint fence above.
pub(crate) struct IntegrationDrainPermit {
    _lock: Option<File>,
}

/// Gate state and effective time are observed together under the admission lock.
pub(crate) fn integration_admission(
    state_root: &Path,
    deadline: crate::client_state::WaitDeadline,
    now: u64,
) -> Result<
    Result<IntegrationDrainPermit, crate::integration::contracts::IntegrationPauseEvidence>,
    WorkerError,
> {
    deadline.remaining()?;
    let Some(root) = open_existing_controller_root(state_root)? else {
        return Ok(Ok(IntegrationDrainPermit { _lock: None }));
    };
    let Some(lock) = existing_lock(&root)? else {
        return Ok(Ok(IntegrationDrainPermit { _lock: None }));
    };
    deadline.lock(lock.as_raw_fd(), libc::LOCK_SH)?;
    validate_lock(&root, &lock)?;
    let metadata = read_integration_gate(&root)?;
    if read_drain_flag(&root, &lock)?
        || metadata.as_ref().is_some_and(|(_, gate)| {
            gate.windows
                .last()
                .is_some_and(|w| w.resumed_at_millis.is_none())
        })
    {
        let window = metadata.as_ref().and_then(|(_, gate)| gate.windows.last());
        return Ok(Err(
            crate::integration::contracts::IntegrationPauseEvidence {
                reason: window.map_or(
                    crate::integration::contracts::IntegrationPauseReason::ControllerDrained,
                    |w| w.reason,
                ),
                effective_at_millis: window.map_or(now, |w| w.effective_at_millis),
            },
        ));
    }
    Ok(Ok(IntegrationDrainPermit { _lock: Some(lock) }))
}

/// Called while the caller holds the shared drain permit. The record's durable
/// updated time makes extending active deadlines idempotent across a crash.
pub(crate) fn elapsed_pause_time(
    state_root: &Path,
    since: u64,
    through: u64,
) -> Result<u64, WorkerError> {
    let Some(root) = open_existing_controller_root(state_root)? else {
        return Ok(0);
    };
    let Some((_, gate)) = read_integration_gate(&root)? else {
        return Ok(0);
    };
    Ok(gate
        .windows
        .iter()
        .filter_map(|w| {
            w.resumed_at_millis.map(|end| {
                end.min(through)
                    .saturating_sub(w.effective_at_millis.max(since))
            })
        })
        .fold(0u64, u64::saturating_add))
}
pub(crate) fn resumed_at(state_root: &Path, effective_at: u64) -> Result<Option<u64>, WorkerError> {
    let Some(root) = open_existing_controller_root(state_root)? else {
        return Ok(Some(effective_at));
    };
    let Some((_, gate)) = read_integration_gate(&root)? else {
        return Ok(Some(effective_at));
    };
    Ok(
        match gate
            .windows
            .iter()
            .find(|w| w.effective_at_millis == effective_at)
        {
            Some(window) => window.resumed_at_millis,
            // A pruned or reset window is forgotten pause time. Charge from its
            // original anchor; using recovery time would renew an old budget.
            None => Some(effective_at),
        },
    )
}

fn existing_lock(root: &RootedDir) -> Result<Option<File>, WorkerError> {
    match root.open_existing_private_lock(DRAIN_LOCK) {
        Ok(lock) => Ok(Some(lock)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if root.entry_exists(DRAIN_FILE).map_err(store_io)? {
                Err(invalid_state())
            } else {
                Ok(None)
            }
        }
        Err(error) => Err(store_io(error)),
    }
}

fn validate_lock(root: &RootedDir, lock: &File) -> Result<bool, WorkerError> {
    let identity = root.private_entry_identity(DRAIN_LOCK).map_err(store_io)?;
    root.validate_private_regular_binding(DRAIN_LOCK, lock, identity)
        .map_err(store_io)?;
    match lock.metadata().map_err(store_io)?.len() {
        0 => Ok(false),
        1 => {
            let mut marker = [0];
            lock.read_exact_at(&mut marker, 0).map_err(store_io)?;
            if marker == *b"1" {
                Ok(true)
            } else {
                Err(invalid_state())
            }
        }
        _ => Err(invalid_state()),
    }
}

fn mark_initialized(root: &RootedDir, lock: &File) -> Result<(), WorkerError> {
    // Keep the lock inode stable. This distinguishes interruption before
    // the first flag publication from disappearance of a committed flag.
    if !validate_lock(root, lock)? {
        lock.write_all_at(b"1", 0).map_err(store_io)?;
        lock.sync_all().map_err(store_io)?;
        root.sync_root().map_err(store_io)?;
    }
    Ok(())
}

fn lock_shared(lock: &File) -> Result<(), WorkerError> {
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_SH) } == 0 {
        Ok(())
    } else {
        Err(store_io(io::Error::last_os_error()))
    }
}

fn read_state(root: &RootedDir) -> Result<(Vec<u8>, DrainState), WorkerError> {
    let bytes = root
        .read_private_regular(DRAIN_FILE, MAX_DRAIN_BYTES)
        .map_err(store_io)?;
    let state: DrainState = serde_json::from_slice(&bytes).map_err(|_| invalid_state())?;
    if state.version != 1 {
        return Err(invalid_state());
    }
    Ok((bytes, state))
}

fn state_bytes(drained: bool) -> Result<Vec<u8>, WorkerError> {
    serde_json::to_vec(&DrainState {
        version: 1,
        drained,
    })
    .map_err(|_| invalid_state())
}

fn invalid_state() -> WorkerError {
    WorkerError::Protocol("CONTROLLER_TRANSPORT: controller drain state is invalid".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn history(path: &Path) -> IntegrationGate {
        read_integration_gate(&open_controller_root(path).unwrap())
            .unwrap()
            .unwrap()
            .1
    }

    mod integration {
        use super::*;

        #[test]
        fn pause_history_thousand_updates_and_locked_cap_crossing_are_bounded() {
            let mut gate = IntegrationGate {
                version: 1,
                windows: vec![],
            };
            for cycle in 0..1000 {
                let start = 1000 + cycle * 2000;
                gate.windows.push(PauseWindow {
                    reason:
                        crate::integration::contracts::IntegrationPauseReason::ControllerDrained,
                    effective_at_millis: start,
                    resumed_at_millis: None,
                });
                prune_pause_history(&mut gate, start);
                assert!(gate.windows.len() <= MAX_PAUSE_WINDOWS);
                assert_eq!(gate.windows.last().unwrap().effective_at_millis, start);
                assert!(gate.windows.last().unwrap().resumed_at_millis.is_none());
                gate.windows.last_mut().unwrap().resumed_at_millis = Some(start + 1500);
                prune_pause_history(&mut gate, start + 1500);
                let retained: u64 = gate
                    .windows
                    .iter()
                    .map(|window| window.resumed_at_millis.unwrap() - window.effective_at_millis)
                    .sum();
                let exact = (cycle + 1) * 1500;
                if cycle < 256 {
                    assert_eq!(retained, exact)
                } else {
                    assert!(retained <= exact)
                }
                assert!(
                    serde_json::to_vec(&gate).unwrap().len()
                        <= crate::integration::contracts::MAX_PRIVATE_RECORD_BYTES
                );
            }
            // A short real locked-writer run crosses the same cap, keeping the
            // gate regression fast while nightly retains all fsynced cycles.
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().canonicalize().unwrap().join("controller");
            seed_history(&path, 254);
            for cycle in 254..262 {
                let start = 1000 + cycle * 2000;
                set_drained_at(&path, true, start).unwrap();
                assert!(is_drained(&path).unwrap());
                let open = history(&path);
                assert!(open.windows.len() <= MAX_PAUSE_WINDOWS);
                assert_eq!(open.windows.last().unwrap().effective_at_millis, start);
                assert!(open.windows.last().unwrap().resumed_at_millis.is_none());
                set_drained_at(&path, false, start + 1500).unwrap();
                assert!(!is_drained(&path).unwrap());
                let retained = elapsed_pause_time(&path, 1000, start + 1500).unwrap();
                let exact = (cycle + 1) * 1500;
                if cycle < 256 {
                    assert_eq!(retained, exact)
                } else {
                    assert!(retained <= exact)
                }
                assert!(history(&path).windows.len() <= MAX_PAUSE_WINDOWS);
            }
        }
    }

    #[test]
    fn operator_prunes_an_existing_history_to_the_hard_cap() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().canonicalize().unwrap().join("controller");
        seed_history(&path, 300);
        set_drained_at(&path, true, 601000).unwrap();
        let gate = history(&path);
        assert_eq!(gate.windows.len(), 256);
        assert!(gate.windows.last().unwrap().resumed_at_millis.is_none());
    }

    fn seed_history(path: &Path, count: u64) {
        set_drained_at(path, false, 1000).unwrap();
        let gate = IntegrationGate {
            version: 1,
            windows: (0..count)
                .map(|cycle| PauseWindow {
                    reason:
                        crate::integration::contracts::IntegrationPauseReason::ControllerDrained,
                    effective_at_millis: 1000 + cycle * 2000,
                    resumed_at_millis: Some(2500 + cycle * 2000),
                })
                .collect(),
        };
        open_controller_root(path)
            .unwrap()
            .write_private_atomic_no_replace(INTEGRATION_GATE, &serde_json::to_vec(&gate).unwrap())
            .unwrap();
    }

    #[test]
    fn pause_history_horizon_counts_active_time_and_keeps_the_open_window() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().canonicalize().unwrap().join("controller");
        let day = 86400000;
        set_drained_at(&path, true, 1000).unwrap();
        set_drained_at(&path, false, day + 1000).unwrap();
        set_drained_at(&path, true, day + 1001001).unwrap();
        set_drained_at(&path, false, 2 * day + 1001001).unwrap();
        assert_eq!(
            history(&path).windows.len(),
            2,
            "paused days are not active time"
        );
        set_drained_at(&path, true, 2 * day + 4001001).unwrap();
        let gate = history(&path);
        assert_eq!(gate.windows.len(), 2);
        assert_eq!(gate.windows[0].effective_at_millis, day + 1001001);
        assert!(gate.windows[1].resumed_at_millis.is_none());
    }

    #[test]
    fn pruned_pause_recovery_charges_missing_history_instead_of_renewing_time() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().canonicalize().unwrap().join("controller");
        seed_history(&path, 257);
        set_drained_at(&path, false, 515000).unwrap();
        assert_eq!(resumed_at(&path, 1000).unwrap(), Some(1000));
        let old_resume = resumed_at(&path, 1000).unwrap().unwrap();
        let pause = elapsed_pause_time(&path, old_resume, 515000).unwrap();
        let remaining = (old_resume + pause + 600000).saturating_sub(515000);
        assert!(
            remaining <= 471500,
            "pruned recovery extended a live budget"
        );
    }

    #[test]
    fn undrain_can_leave_an_empty_expired_history_that_admission_accepts() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().canonicalize().unwrap().join("controller");
        set_drained_at(&path, true, 1000).unwrap();
        set_drained_at(&path, false, 2000).unwrap();
        set_drained_at(&path, false, 4000000).unwrap();
        assert!(history(&path).windows.is_empty());
        assert!(
            integration_admission(&path, crate::client_state::WaitDeadline::new(None), 4000000)
                .unwrap()
                .is_ok()
        );
    }

    #[test]
    fn corrupt_gate_refuses_integration_until_an_operator_quarantines_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().canonicalize().unwrap().join("controller");
        set_drained_at(&path, false, 1001).unwrap();
        let root = open_controller_root(&path).unwrap();
        root.write_private_atomic_no_replace(INTEGRATION_GATE, b"invalid gate")
            .unwrap();
        assert!(
            integration_admission(&path, crate::client_state::WaitDeadline::new(None), 2001)
                .is_err()
        );
        set_drained_at(&path, true, 2001).expect("corrupt gate blocked ordinary drain");
        assert!(is_drained(&path).unwrap());
        assert!(
            integration_admission(&path, crate::client_state::WaitDeadline::new(None), 2001)
                .unwrap()
                .is_err()
        );
        assert_eq!(
            std::fs::read(path.join("integration-gate.json.corrupt")).unwrap(),
            b"invalid gate"
        );
        set_drained_at(&path, false, 3001).unwrap();
        assert!(
            integration_admission(&path, crate::client_state::WaitDeadline::new(None), 3001)
                .unwrap()
                .is_ok()
        );
    }
}
