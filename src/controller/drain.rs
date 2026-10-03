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
    let hints = sink.map(crate::client_state::events::DeferredHints::begin);
    let root = open_controller_root(state_root)?;
    let lock = root.open_private_lock(DRAIN_LOCK).map_err(store_io)?;
    lock_exclusive(&lock)?;
    validate_lock(&root, &lock)?;
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
    // On both success and error, reverse local drop order releases the lock first.
    Ok(())
}

use super::leader::{
    lock_exclusive, open_controller_root, open_existing_controller_root, store_io,
};

const DRAIN_LOCK: &str = "drain.lock";
const DRAIN_FILE: &str = "drain.json";
const MAX_DRAIN_BYTES: u64 = 4096;

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
    validate_lock(&root, &lock)?;
    Ok(read_state(&root)?.1.drained)
}

/// None means drained; errors also prohibit launch. No files are created
/// on this path, so clients without a controller store retain local mode.
pub(crate) fn launch_permit(
    state_root: &Path,
    deadline: crate::client_state::WaitDeadline,
) -> Result<Option<DrainLaunchPermit>, WorkerError> {
    let hints = crate::client_state::events::DeferredHints::fence();
    Ok(
        integration_permit(state_root, deadline)?.map(|permit| DrainLaunchPermit {
            _lock: permit.0,
            _hints: hints,
        }),
    )
}

/// Only the file guard crosses the Send integration phase contract. Queue
/// launchers retain their existing thread-local deferred-hint fence above.
pub(crate) struct IntegrationDrainPermit(Option<File>);
pub(crate) fn integration_permit(
    state_root: &Path,
    deadline: crate::client_state::WaitDeadline,
) -> Result<Option<IntegrationDrainPermit>, WorkerError> {
    deadline.remaining()?;
    let Some(root) = open_existing_controller_root(state_root)? else {
        return Ok(Some(IntegrationDrainPermit(None)));
    };
    let Some(lock) = existing_lock(&root)? else {
        return Ok(Some(IntegrationDrainPermit(None)));
    };
    deadline.lock(lock.as_raw_fd(), libc::LOCK_SH)?;
    validate_lock(&root, &lock)?;
    if read_state(&root)?.1.drained {
        return Ok(None);
    }
    Ok(Some(IntegrationDrainPermit(Some(lock))))
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
