use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use tokio::sync::oneshot;

use crate::controller::channel::contracts::{ChannelFailure, ChannelReason};

/// Try-only admission for blocking channel metadata work.
#[derive(Default)]
pub struct NativeControl {
    occupied: Arc<AtomicBool>,
}

impl NativeControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn try_run<T: Send + 'static>(
        &self,
        job: Box<dyn FnOnce() -> T + Send>,
    ) -> Result<oneshot::Receiver<T>, ChannelFailure> {
        self.occupied
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| ChannelFailure::Unavailable(ChannelReason::Busy))?;
        let occupied = Arc::clone(&self.occupied);
        let (sender, receiver) = oneshot::channel();
        if thread::Builder::new()
            .name("controller-channel-control".into())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(job));
                // Job exit is known, even on unwind. Publish only after release.
                occupied.store(false, Ordering::Release);
                if let Ok(value) = result {
                    let _ = sender.send(value);
                }
            })
            .is_err()
        {
            self.occupied.store(false, Ordering::Release);
            return Err(ChannelFailure::Unavailable(
                ChannelReason::ServiceUnavailable,
            ));
        }
        Ok(receiver)
    }
}
