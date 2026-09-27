//! Signal-responsive controller loop. The leader guard outlives the scoped
//! worker, so no tick can mutate state after leadership is relinquished.
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use crate::{
    error::{ProcessError, WorkerError},
    process::{ProcessRequest, ProcessResult, ProcessRunner},
};

pub struct ControllerProcessRunner<'a> {
    inner: &'a dyn ProcessRunner,
    shutdown: &'a AtomicBool,
}

impl<'a> ControllerProcessRunner<'a> {
    pub fn new(inner: &'a dyn ProcessRunner, shutdown: &'a AtomicBool) -> Self {
        Self { inner, shutdown }
    }

    fn stopping(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

impl ProcessRunner for ControllerProcessRunner<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.inner.run_interruptible(request, &|| self.stopping())
    }

    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        self.inner
            .run_interruptible(request, &|| self.stopping() || should_stop())
    }

    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if self.stopping() {
            return Err(ProcessError::Cancelled.into());
        }
        // Preserve session semantics for callers that require them. Controller
        // Git/SSH operations use run/run_interruptible above.
        self.inner.run_in_new_session(request)
    }
}

pub fn run_tick_loop(
    runtime: &tokio::runtime::Runtime,
    shutdown_flag: &AtomicBool,
    mut tick: impl FnMut() -> Result<Option<String>, WorkerError> + Send,
    shutdown: impl Future<Output = ()>,
    mut emit: impl FnMut(&str) -> Result<(), WorkerError>,
) -> Result<(), WorkerError> {
    let (wake_tx, wake_rx) = mpsc::channel();
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
    std::thread::scope(|scope| {
        let worker = scope.spawn(move || {
            while !shutdown_flag.load(Ordering::Acquire) {
                let result = tick();
                let failed = result.is_err();
                if event_tx.blocking_send(result).is_err() || failed {
                    break;
                }
                if wake_rx
                    .recv_timeout(Duration::from_millis(super::health::TICK_INTERVAL_MILLIS))
                    .is_ok()
                {
                    break;
                }
            }
        });
        let result = runtime.block_on(async {
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => break Ok(()),
                    event = event_rx.recv() => match event {
                        Some(Ok(Some(line))) => emit(&line)?,
                        Some(Ok(None)) => {},
                        Some(Err(error)) => break Err(error),
                        None => break Ok(()),
                    },
                }
            }
        });
        shutdown_flag.store(true, Ordering::Release);
        let _ = wake_tx.send(());
        // Also release a worker blocked on its bounded diagnostic channel.
        drop(event_rx);
        let joined = worker.join().map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: controller tick thread failed".into())
        });
        result.and(joined)
    })
}
