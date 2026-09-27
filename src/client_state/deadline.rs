use std::{
    os::fd::RawFd,
    time::{Duration, Instant},
};

use crate::error::WorkerError;

/// One monotonic budget shared by a wait and all work in its local polls.
#[derive(Clone, Copy, Default)]
pub(crate) struct WaitDeadline {
    expires: Option<Instant>,
}

impl WaitDeadline {
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
                    self.remaining()?;
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
