//! Process-wide exclusion between same-process `fork` and held `flock` FDs.
//!
//! Several supervisor paths `fork` without `exec` (or `fork` and only later
//! close inherited descriptors). A child that exists while another test holds
//! an `flock` keeps a copy of that locked FD until it exits or closes it, so a
//! later `LOCK_EX | LOCK_NB` in the owner test returns `WouldBlock`.
//! `FD_CLOEXEC` does not help without `exec`. Closing extra FDs in
//! `Command::pre_exec` also closes libstd's exec-error pipe and aborts.
//! `closefrom` does not link at this crate's macOS 11 minimum. A bounded
//! `WouldBlock` retry would hide a leaked lock FD.
//!
//! Counts live in a mutex that is never held across `fork`. Same-thread nested
//! flock opens (a live writer probing a busy journal) only bump a refcount.
//! The child of `fork` must `_exit` or `exec`, never drop `HeldFork`.

use std::sync::{Condvar, Mutex, OnceLock};

struct State {
    flocks: u32,
    forks: u32,
}

static LOCK: OnceLock<(Mutex<State>, Condvar)> = OnceLock::new();

fn lock_and_cv() -> &'static (Mutex<State>, Condvar) {
    LOCK.get_or_init(|| {
        (
            Mutex::new(State {
                flocks: 0,
                forks: 0,
            }),
            Condvar::new(),
        )
    })
}

fn recover<T>(result: std::sync::LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Held for the lifetime of a test-process `flock` FD. Same-process forks wait.
pub(crate) struct HeldFlock;

impl HeldFlock {
    pub(crate) fn acquire() -> Self {
        let (lock, cv) = lock_and_cv();
        let mut state = recover(lock.lock());
        while state.forks > 0 {
            state = recover(cv.wait(state));
        }
        state.flocks += 1;
        Self
    }
}

impl Drop for HeldFlock {
    fn drop(&mut self) {
        let (lock, cv) = lock_and_cv();
        let mut state = recover(lock.lock());
        state.flocks -= 1;
        cv.notify_all();
    }
}

/// Held while a same-process child may still inherit this process's FD table.
pub(crate) struct HeldFork;

impl HeldFork {
    pub(crate) fn acquire() -> Self {
        let (lock, cv) = lock_and_cv();
        let mut state = recover(lock.lock());
        while state.flocks > 0 {
            state = recover(cv.wait(state));
        }
        state.forks += 1;
        Self
    }
}

impl Drop for HeldFork {
    fn drop(&mut self) {
        let (lock, cv) = lock_and_cv();
        let mut state = recover(lock.lock());
        state.forks -= 1;
        cv.notify_all();
    }
}
