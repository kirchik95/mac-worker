//! Monotonic foreground runtime and common command configuration.

use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use super::{EventRuntime, JOURNAL_CHECK_INTERVAL};
use crate::{
    RuntimeContext,
    cli::Cli,
    config::Config,
    error::WorkerError,
    paths::PathLayout,
    transfer::{ResolutionRuntime, SystemResolutionRuntime},
};

/// Channel setup and reads share the foreground event clock and cancellation.
pub struct EventChannelRuntime(pub Arc<dyn EventRuntime>);
impl crate::controller::channel::ChannelRuntime for EventChannelRuntime {
    fn now(&self) -> Duration {
        self.0.now()
    }
    fn cancelled(&self) -> bool {
        self.0.cancelled()
    }
}

pub(crate) struct ForegroundRuntime {
    cancelled: Arc<AtomicBool>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}
impl ForegroundRuntime {
    pub(crate) fn install() -> Result<Arc<Self>, WorkerError> {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        // Register before admitting any RPC so an immediate Ctrl-C is retained.
        let mut interrupt = {
            let _entered = executor.enter();
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let signal_flag = cancelled.clone();
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name("controller-event-signal".into())
            .spawn(move || {
                executor.block_on(async {
                    tokio::select! {
                        _ = interrupt.recv() => { signal_flag.store(true, Ordering::Release); }
                        _ = shutdown => {}
                    }
                });
            })?;
        Ok(Arc::new(Self {
            cancelled,
            stop: Some(stop),
        }))
    }
}
impl EventRuntime for ForegroundRuntime {
    fn now(&self) -> Duration {
        SystemResolutionRuntime.monotonic_now()
    }
    fn sleep(&self, duration: Duration) {
        let until = self.now().saturating_add(duration);
        while !self.cancelled() && self.now() < until {
            SystemResolutionRuntime
                .sleep(until.saturating_sub(self.now()).min(JOURNAL_CHECK_INTERVAL));
        }
    }
    fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}
impl Drop for ForegroundRuntime {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        // No join: neither signal handling nor outstanding system I/O may
        // hold foreground cancellation open indefinitely.
    }
}

pub(crate) fn event_client(
    raw: Arc<dyn crate::process::ProcessRunner>,
    scope: crate::controller::channel::ReadLoopScope,
    paths: &PathLayout,
    config: &Config,
    runtime: Arc<dyn EventRuntime>,
    context: &RuntimeContext,
) -> super::client::ControllerEventClient {
    match crate::controller_read_channel_dependencies(
        paths,
        config,
        Arc::new(EventChannelRuntime(runtime.clone())),
        context,
    ) {
        Some(dependencies) => super::client::ControllerEventClient::for_read_loop(
            raw,
            scope,
            paths,
            config,
            runtime,
            dependencies,
        ),
        None => super::client::ControllerEventClient::new(raw, config.controller.clone(), runtime),
    }
}

pub(crate) fn configuration(
    cli: &Cli,
    runtime: &RuntimeContext,
) -> Result<(PathLayout, Config), WorkerError> {
    let paths = PathLayout::discover(cli.config.clone(), runtime.environment(), runtime.home())?;
    let config = Config::load(&paths.config)?;
    if !config.controller.enabled {
        return Err(WorkerError::Config(
            "controller mode is required for events and notify".into(),
        ));
    }
    Ok((paths, config))
}
pub(crate) fn report(error: WorkerError, stderr: &mut dyn Write) -> u8 {
    let _ = writeln!(stderr, "{}", crate::error::operator_diagnostic(&error));
    error.exit_code()
}
