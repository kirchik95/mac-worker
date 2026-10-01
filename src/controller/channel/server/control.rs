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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[tokio::test(flavor = "current_thread")]
    async fn gated_control_work_does_not_block_runtime_or_queue_a_replacement() {
        let control = NativeControl::new();
        let (entered, entry) = oneshot::channel();
        let (release, gate) = mpsc::channel();
        let done = control
            .try_run(Box::new(move || {
                entered.send(()).unwrap();
                gate.recv().unwrap();
                37
            }))
            .expect("first metadata job is admitted");
        entry.await.unwrap();
        assert!(matches!(
            control.try_run(Box::new(|| 99)),
            Err(ChannelFailure::Unavailable(ChannelReason::Busy))
        ));
        // A signal/control task can still run on this current-thread runtime.
        assert_eq!(tokio::spawn(async { 11 }).await.unwrap(), 11);
        release.send(()).unwrap();
        assert_eq!(done.await.unwrap(), 37);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completed_native_work_releases_the_control_slot() {
        let control = NativeControl::new();
        for expected in 0..12 {
            let result = control.try_run(Box::new(move || expected)).unwrap();
            assert_eq!(result.await.unwrap(), expected);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panicked_native_work_closes_its_result_and_releases_the_slot() {
        let control = NativeControl::new();
        let result = control
            .try_run::<()>(Box::new(|| panic!("fixture metadata panic")))
            .unwrap();
        assert!(result.await.is_err());
        assert_eq!(control.try_run(Box::new(|| 17)).unwrap().await.unwrap(), 17);
    }
}
