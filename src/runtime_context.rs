use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use crate::{
    client_state::{self, ClientStateStore},
    controller,
    error::WorkerError,
};

#[doc(hidden)]
#[derive(Clone)]
pub struct RuntimeContext {
    pub(crate) environment: BTreeMap<OsString, OsString>,
    pub(crate) home: PathBuf,
    current_dir: RuntimeCurrentDir,
    pub(crate) controller_channel: Option<std::sync::Arc<controller::channel::ClientDeps>>,
}

impl std::fmt::Debug for RuntimeContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeContext")
            .field("environment", &self.environment)
            .field("home", &self.home)
            .field("current_dir", &self.current_dir)
            .field("controller_channel", &self.controller_channel.is_some())
            .finish()
    }
}

#[derive(Debug, Clone)]
enum RuntimeCurrentDir {
    Process,
    Fixed(PathBuf),
}

impl RuntimeContext {
    pub(crate) fn capture() -> Self {
        let environment = std::env::vars_os().collect::<BTreeMap<_, _>>();
        let home = environment
            .get(&OsString::from("HOME"))
            .map(PathBuf::from)
            .unwrap_or_default();
        Self {
            environment,
            home,
            current_dir: RuntimeCurrentDir::Process,
            controller_channel: None,
        }
    }

    #[doc(hidden)]
    pub fn isolated(
        environment: BTreeMap<OsString, OsString>,
        home: PathBuf,
        current_dir: PathBuf,
    ) -> Self {
        Self {
            environment,
            home,
            current_dir: RuntimeCurrentDir::Fixed(current_dir),
            controller_channel: None,
        }
    }

    /// Supply the foreground read channel's dependencies without changing raw RPCs.
    #[doc(hidden)]
    pub fn with_controller_channel_dependencies(
        mut self,
        dependencies: controller::channel::ClientDeps,
    ) -> Self {
        self.controller_channel = Some(std::sync::Arc::new(dependencies));
        self
    }

    pub(crate) fn current_dir(&self) -> Result<PathBuf, WorkerError> {
        match &self.current_dir {
            RuntimeCurrentDir::Process => std::env::current_dir().map_err(WorkerError::Io),
            RuntimeCurrentDir::Fixed(current_dir) => Ok(current_dir.clone()),
        }
    }

    pub(crate) fn home(&self) -> &std::path::Path {
        &self.home
    }

    pub(crate) fn environment(&self) -> &BTreeMap<OsString, OsString> {
        &self.environment
    }
}

/// Monotonic event timing with process-scoped shutdown cancellation.
pub struct ControllerEventRuntime {
    clock: std::sync::Arc<dyn controller::events::EventRuntime>,
    stopped: std::sync::atomic::AtomicBool,
}

struct SystemControllerEventClock;

impl controller::events::EventRuntime for SystemControllerEventClock {
    fn now(&self) -> std::time::Duration {
        crate::transfer::ResolutionRuntime::monotonic_now(&crate::transfer::SystemResolutionRuntime)
    }
    fn sleep(&self, duration: std::time::Duration) {
        crate::transfer::ResolutionRuntime::sleep(
            &crate::transfer::SystemResolutionRuntime,
            duration,
        );
    }
    fn cancelled(&self) -> bool {
        false
    }
}

impl ControllerEventRuntime {
    pub fn new(clock: std::sync::Arc<dyn controller::events::EventRuntime>) -> Self {
        Self {
            clock,
            stopped: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn system() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new(std::sync::Arc::new(SystemControllerEventClock)))
    }

    pub fn cancel(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl controller::events::EventRuntime for ControllerEventRuntime {
    fn now(&self) -> std::time::Duration {
        self.clock.now()
    }
    fn sleep(&self, duration: std::time::Duration) {
        if !self.cancelled() {
            self.clock.sleep(duration);
        }
    }
    fn cancelled(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::Acquire) || self.clock.cancelled()
    }
}

/// Retain exactly one publisher for the command's lifetime. Store clones and
/// deadline reopens share its try-only sink; no producer owns journal I/O.
pub struct ControllerEventPublisher {
    journal: std::sync::Arc<controller::events::journal::ControllerJournal>,
    sink: std::sync::Arc<dyn controller::events::EventSink>,
    publisher: controller::events::journal::PublisherHandle,
    runtime: std::sync::Arc<ControllerEventRuntime>,
}

impl ControllerEventPublisher {
    pub(crate) fn start(
        journal: std::sync::Arc<controller::events::journal::ControllerJournal>,
        runtime: std::sync::Arc<ControllerEventRuntime>,
    ) -> Self {
        let (sink, publisher) =
            controller::events::journal::BoundedPublisher::start(journal.clone(), runtime.clone());
        Self {
            journal,
            sink,
            publisher,
            runtime,
        }
    }

    /// Attach a journal already initialized by a leader or opened existing-only.
    pub fn attach(
        store: ClientStateStore,
        journal: std::sync::Arc<controller::events::journal::ControllerJournal>,
        runtime: std::sync::Arc<ControllerEventRuntime>,
    ) -> (ClientStateStore, Self) {
        let publisher = Self::start(journal, runtime);
        (store.with_event_sink(publisher.sink()), publisher)
    }

    pub fn sink(&self) -> std::sync::Arc<dyn controller::events::EventSink> {
        self.sink.clone()
    }
    pub fn journal(&self) -> std::sync::Arc<controller::events::journal::ControllerJournal> {
        self.journal.clone()
    }
    pub fn runtime(&self) -> std::sync::Arc<ControllerEventRuntime> {
        self.runtime.clone()
    }
    pub fn diagnostics(&self) -> Vec<controller::events::journal::PublisherDiagnostic> {
        self.publisher.diagnostics()
    }
}

impl Drop for ControllerEventPublisher {
    fn drop(&mut self) {
        // Every entry-point guard outlives its authoritative work and fences.
        // Grace is bounded; a stalled fsync is never joined.
        self.publisher
            .finish_with_grace(controller::events::PUBLISHER_EXIT_GRACE);
        self.runtime.cancel();
        let count = client_state::events::dropped_hint_count();
        if count != 0 {
            let diagnostics = self
                .publisher
                .diagnostics()
                .iter()
                .map(|item| format!("{}={}", item.code, item.count))
                .collect::<Vec<_>>()
                .join(",");
            eprintln!("CONTROLLER_EVENT_HINTS_DROPPED count={count} publisher=[{diagnostics}]");
        }
    }
}
