//! One bounded worker per process; the event facade supplies the typed batch.
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, SyncSender, TryRecvError},
    },
    time::Duration,
};

pub(super) const QUEUE_BATCHES: usize = 128;
pub(super) const QUEUE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum EnqueueResult {
    Queued,
    Full,
    Stopping,
    Unavailable,
}

#[derive(Default)]
struct Shared {
    accepting: AtomicBool,
    stop_now: AtomicBool,
    unavailable: AtomicBool,
    done: AtomicBool,
    queued_bytes: AtomicUsize,
    full_drops: AtomicU64,
    stopping_drops: AtomicU64,
    unavailable_drops: AtomicU64,
    write_failures: AtomicU64,
    panics: AtomicU64,
}

pub(super) struct Queue<T> {
    sender: SyncSender<Option<Queued<T>>>,
    size: Arc<dyn Fn(&T) -> usize + Send + Sync>,
    shared: Arc<Shared>,
}

struct Queued<T> {
    batch: Option<T>,
    bytes: usize,
    shared: Arc<Shared>,
}

impl<T> Drop for Queued<T> {
    fn drop(&mut self) {
        self.shared
            .queued_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

pub(super) trait GraceClock {
    fn now(&self) -> Duration;
    fn sleep(&self, duration: Duration);
    fn cancelled(&self) -> bool;
}

pub(super) struct WorkerHandle {
    shared: Arc<Shared>,
    wake: Box<dyn Fn() + Send + Sync>,
}

pub(super) fn start<T: Send + 'static>(
    write: impl Fn(T) -> Result<(), ()> + Send + 'static,
    size: impl Fn(&T) -> usize + Send + Sync + 'static,
) -> (Arc<Queue<T>>, WorkerHandle) {
    let (sender, receiver) = mpsc::sync_channel::<Option<Queued<T>>>(QUEUE_BATCHES);
    let shared = Arc::new(Shared::default());
    shared.accepting.store(true, Ordering::Release);
    let sink = Arc::new(Queue {
        sender: sender.clone(),
        size: Arc::new(size),
        shared: shared.clone(),
    });
    let state = shared.clone();
    // Dropping the JoinHandle detaches the worker. Shutdown never joins disk I/O.
    let spawned = std::thread::Builder::new()
        .name("controller-events".into())
        .spawn(move || {
            loop {
                if state.stop_now.load(Ordering::Acquire) {
                    break;
                }
                let message = if state.accepting.load(Ordering::Acquire) {
                    receiver.recv().ok()
                } else {
                    match receiver.try_recv() {
                        Ok(message) => Some(message),
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => None,
                    }
                };
                let Some(Some(mut queued)) = message else {
                    break;
                };
                let batch = queued.batch.take().expect("queued batch is present");
                drop(queued);
                match catch_unwind(AssertUnwindSafe(|| write(batch))) {
                    Ok(Ok(())) => {}
                    Ok(Err(())) => {
                        state.write_failures.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        state.panics.fetch_add(1, Ordering::Relaxed);
                        state.unavailable.store(true, Ordering::Release);
                        break;
                    }
                }
            }
            state.accepting.store(false, Ordering::Release);
            // Drop queued reservations before announcing completion.
            drop(receiver);
            state.done.store(true, Ordering::Release);
        });
    if spawned.is_err() {
        shared.accepting.store(false, Ordering::Release);
        shared.unavailable.store(true, Ordering::Release);
        shared.done.store(true, Ordering::Release);
    }
    let handle = WorkerHandle {
        shared,
        wake: Box::new(move || {
            let _ = sender.try_send(None);
        }),
    };
    (sink, handle)
}

impl<T> Queue<T> {
    pub(super) fn try_enqueue(&self, batch: T) -> EnqueueResult {
        if self.shared.unavailable.load(Ordering::Acquire) {
            self.shared
                .unavailable_drops
                .fetch_add(1, Ordering::Relaxed);
            return EnqueueResult::Unavailable;
        }
        if !self.shared.accepting.load(Ordering::Acquire) {
            self.shared.stopping_drops.fetch_add(1, Ordering::Relaxed);
            return EnqueueResult::Stopping;
        }
        let bytes = (self.size)(&batch);
        if self
            .shared
            .queued_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|total| *total <= QUEUE_BYTES)
            })
            .is_err()
        {
            self.shared.full_drops.fetch_add(1, Ordering::Relaxed);
            return EnqueueResult::Full;
        }
        let queued = Queued {
            batch: Some(batch),
            bytes,
            shared: self.shared.clone(),
        };
        match self.sender.try_send(Some(queued)) {
            Ok(()) => EnqueueResult::Queued,
            Err(mpsc::TrySendError::Full(_)) => {
                self.shared.full_drops.fetch_add(1, Ordering::Relaxed);
                EnqueueResult::Full
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.shared
                    .unavailable_drops
                    .fetch_add(1, Ordering::Relaxed);
                EnqueueResult::Unavailable
            }
        }
    }
}

impl WorkerHandle {
    pub(super) fn stop_without_join(&self) {
        self.shared.accepting.store(false, Ordering::Release);
        self.shared.stop_now.store(true, Ordering::Release);
        (self.wake)();
    }

    pub(super) fn finish_with_grace(&self, grace: Duration, clock: &dyn GraceClock) {
        self.shared.accepting.store(false, Ordering::Release);
        (self.wake)();
        let deadline = clock
            .now()
            .saturating_add(grace.min(Duration::from_millis(50)));
        while !self.shared.done.load(Ordering::Acquire) && !clock.cancelled() {
            let remaining = deadline.saturating_sub(clock.now());
            if remaining.is_zero() {
                break;
            }
            clock.sleep(remaining.min(Duration::from_millis(1)));
        }
        self.stop_without_join();
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.stop_without_join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    struct ManualClock(AtomicU64);
    impl GraceClock for ManualClock {
        fn now(&self) -> Duration {
            Duration::from_millis(self.0.load(Ordering::Acquire))
        }
        fn sleep(&self, duration: Duration) {
            self.0
                .fetch_add(duration.as_millis() as u64, Ordering::AcqRel);
        }
        fn cancelled(&self) -> bool {
            false
        }
    }

    #[test]
    fn exit_grace_is_capped_while_writer_remains_blocked() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (sink, handle) = start(
            move |_: Vec<u8>| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            },
            Vec::len,
        );
        assert_eq!(sink.try_enqueue(vec![1]), EnqueueResult::Queued);
        entered_rx.recv().unwrap();
        let clock = ManualClock(AtomicU64::new(0));
        handle.finish_with_grace(Duration::from_secs(30), &clock);
        assert_eq!(clock.now(), Duration::from_millis(50));
        assert_eq!(sink.try_enqueue(vec![2]), EnqueueResult::Stopping);
        release_tx.send(()).unwrap();
    }

    #[test]
    fn blocked_writer_does_not_block_admission_or_stop() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let (sink, handle) = start(
            move |_: Vec<u8>| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                finished_tx.send(()).unwrap();
                Ok(())
            },
            Vec::len,
        );
        assert_eq!(sink.try_enqueue(vec![1]), EnqueueResult::Queued);
        entered_rx.recv().unwrap();
        for _ in 0..128 {
            assert_eq!(sink.try_enqueue(vec![2]), EnqueueResult::Queued);
        }
        assert_eq!(sink.try_enqueue(vec![3]), EnqueueResult::Full);
        handle.stop_without_join();
        assert_eq!(sink.try_enqueue(vec![4]), EnqueueResult::Stopping);
        release_tx.send(()).unwrap();
        finished_rx.recv().unwrap();
    }

    #[test]
    fn byte_budget_is_reserved_before_queueing() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (sink, handle) = start(
            move |_: usize| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            },
            |size| *size,
        );
        assert_eq!(sink.try_enqueue(1), EnqueueResult::Queued);
        entered_rx.recv().unwrap();
        assert_eq!(sink.try_enqueue(4 * 1024 * 1024), EnqueueResult::Queued);
        assert_eq!(sink.try_enqueue(1), EnqueueResult::Full);
        handle.stop_without_join();
        release_tx.send(()).unwrap();
    }
}
