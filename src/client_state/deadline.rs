use std::{
    cell::Cell,
    io,
    os::fd::RawFd,
    time::{Duration, Instant},
};

use crate::error::WorkerError;

/// One monotonic budget shared by a wait and all work in its local polls.
#[derive(Clone, Copy, Default)]
pub(crate) struct WaitDeadline {
    expires: Option<Instant>,
}

thread_local! {
    // Filesystem cleanup and Git retries are synchronous and shared with
    // non-wait callers. Scope the same budget through those lower helpers
    // without changing their ownership or recovery APIs.
    static CURRENT: Cell<WaitDeadline> = const { Cell::new(WaitDeadline { expires: None }) };
}

struct RestoreDeadline(WaitDeadline);

impl Drop for RestoreDeadline {
    fn drop(&mut self) {
        CURRENT.set(self.0);
    }
}

impl WaitDeadline {
    pub(crate) fn current() -> Self {
        CURRENT.get()
    }

    pub(crate) fn in_scope<T>(self, action: impl FnOnce() -> T) -> T {
        let previous = Self::current();
        let expires = match (self.expires, previous.expires) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        let _restore = RestoreDeadline(previous);
        CURRENT.set(Self::until(expires));
        action()
    }

    pub(crate) fn new(timeout: Option<Duration>) -> Self {
        Self::until(timeout.and_then(|timeout| Instant::now().checked_add(timeout)))
    }

    pub(crate) fn until(expires: Option<Instant>) -> Self {
        Self { expires }
    }

    pub(crate) fn expires(self) -> Option<Instant> {
        self.expires
    }

    pub(crate) fn remaining_at(self, now: Instant) -> Result<Option<Duration>, WorkerError> {
        match self.expires {
            Some(expires) if now >= expires => Err(WorkerError::task(
                "WAIT_TIMEOUT",
                "task wait timed out without cancelling the task",
            )),
            Some(expires) => Ok(Some(expires - now)),
            None => Ok(None),
        }
    }

    pub(crate) fn remaining(self) -> Result<Option<Duration>, WorkerError> {
        self.remaining_at(Instant::now())
    }

    pub(crate) fn remaining_io(self) -> io::Result<Option<Duration>> {
        self.remaining().map_err(Self::io_error)
    }

    pub(crate) fn lock_io(self, fd: RawFd, operation: libc::c_int) -> io::Result<()> {
        self.lock(fd, operation).map_err(Self::io_error)
    }

    fn io_error(error: WorkerError) -> io::Error {
        match error {
            WorkerError::Io(error) => error,
            error => io::Error::new(io::ErrorKind::TimedOut, error),
        }
    }

    pub(crate) fn cap(self, maximum: Duration) -> Result<Duration, WorkerError> {
        Ok(self
            .remaining()?
            .map_or(maximum, |remaining| remaining.min(maximum)))
    }

    pub(crate) fn poll_delay(self) -> Result<Duration, WorkerError> {
        self.cap(Duration::from_millis(100))
    }

    /// Blocking flock is retained for callers without a wait budget. Timed
    /// callers retry nonblocking acquisition and recheck before using the lock.
    pub(crate) fn lock(self, fd: RawFd, operation: libc::c_int) -> Result<(), WorkerError> {
        if self.expires().is_none() || operation & libc::LOCK_NB != 0 {
            self.remaining()?;
            return super::cvt(unsafe { libc::flock(fd, operation) }).map_err(WorkerError::Io);
        }
        loop {
            self.remaining()?;
            match super::cvt(unsafe { libc::flock(fd, operation | libc::LOCK_NB) }) {
                Ok(()) => {
                    if let Err(error) = self.remaining() {
                        // In particular, namespace descriptors can outlive a
                        // failed acquisition; never leave an expired lock held.
                        let _ = super::cvt(unsafe { libc::flock(fd, libc::LOCK_UN) });
                        return Err(error);
                    }
                    return Ok(());
                }
                Err(error) if super::lock_would_block(&error) => {
                    std::thread::sleep(self.cap(Duration::from_millis(5))?);
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(WorkerError::Io(error)),
            }
        }
    }
}
