//! Signal-responsive controller loop. The leader guard outlives the scoped
//! worker, so no tick can mutate state after leadership is relinquished.
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

/// The same monotonic clock used by existing RPC/event budgets, with owned
/// leader cancellation. Channel retirement is independent of this flag.
#[derive(Default)]
pub struct SystemChannelRuntime {
    shutdown: Arc<AtomicBool>,
}
impl SystemChannelRuntime {
    pub fn new(shutdown: Arc<AtomicBool>) -> Self {
        Self { shutdown }
    }
}
impl super::channel::ChannelRuntime for SystemChannelRuntime {
    fn now(&self) -> Duration {
        crate::transfer::ResolutionRuntime::monotonic_now(&crate::transfer::SystemResolutionRuntime)
    }
    fn cancelled(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

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

pub fn run_tick_loop_with_shutdown(
    runtime: &tokio::runtime::Runtime,
    shutdown_flag: &AtomicBool,
    mut tick: impl FnMut() -> Result<Option<String>, WorkerError> + Send,
    shutdown: impl Future<Output = ()>,
    mut emit: impl FnMut(&str) -> Result<(), WorkerError>,
    before_unpoll: impl Future<Output = ()>,
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
            let result = loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => break Ok(()),
                    event = event_rx.recv() => match event {
                        Some(Ok(Some(line))) => if let Err(error) = emit(&line) { break Err(error); },
                        Some(Ok(None)) => {},
                        Some(Err(error)) => break Err(error),
                        None => break Ok(()),
                    },
                }
            };
            // Every exit shares this path while listener/session tasks are
            // still polled. Preserve the original diagnostic/tick result.
            shutdown_flag.store(true, Ordering::Release);
            before_unpoll.await;
            result
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

#[cfg(any(test, feature = "test-support"))]
pub fn run_tick_loop(
    runtime: &tokio::runtime::Runtime,
    shutdown_flag: &AtomicBool,
    tick: impl FnMut() -> Result<Option<String>, WorkerError> + Send,
    shutdown: impl Future<Output = ()>,
    emit: impl FnMut(&str) -> Result<(), WorkerError>,
) -> Result<(), WorkerError> {
    run_tick_loop_with_shutdown(runtime, shutdown_flag, tick, shutdown, emit, async {})
}

use super::channel::{
    codec::SessionCodec,
    contracts::{
        ChannelRuntime, ChildRpcSpec, ControllerAccount, ForwardDisposition, RunningImageSource,
        SETUP_GUARD, ServiceIdentity, UuidString,
    },
    files::{LeaderSocketLease, bind_leader},
    server::{ChildRpcExecutor, NativeControl, ServerDeps, ShutdownEvidence, SocketService},
};
use crate::{job::ClientId, paths::PathLayout, process::TrackedProcessRunner};
use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};
use tokio::sync::{Notify, oneshot};

pub struct LeaderChannelConfig {
    pub paths: PathLayout,
    pub home: PathBuf,
    pub environment: BTreeMap<OsString, OsString>,
    pub client_id: ClientId,
    /// The existing leader initialization result; unavailable journals are
    /// never opened or initialized again to make this transport available.
    pub journal: Option<Arc<super::events::journal::ControllerJournal>>,
}

pub struct LeaderChannelDeps {
    pub image: Arc<dyn RunningImageSource>,
    pub runner: Arc<dyn TrackedProcessRunner>,
    pub runtime: Arc<dyn ChannelRuntime>,
    pub control: Arc<NativeControl>,
}

#[derive(Clone, Copy, Debug)]
pub struct LeaderChannelShutdown {
    #[cfg(any(test, feature = "test-support"))]
    pub rpc: ShutdownEvidence,
    #[cfg(any(test, feature = "test-support"))]
    pub files: ForwardDisposition,
}

struct ChannelStop {
    stopped: AtomicBool,
    leader_shutdown: Arc<AtomicBool>,
    changed: Notify,
}
impl ChannelStop {
    fn stopping(&self) -> bool {
        self.stopped.load(Ordering::Acquire) || self.leader_shutdown.load(Ordering::Acquire)
    }
    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.changed.notify_one();
    }
    async fn wait(&self) {
        while !self.stopping() {
            tokio::select! {
                _ = self.changed.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
    }
}

/// One optional generation. Its retirement never stops the controller tick.
/// The task's shutdown is explicitly awaited before the runtime is unpolled.
pub struct LeaderChannel {
    stop: Arc<ChannelStop>,
    #[cfg(any(test, feature = "test-support"))]
    ready: Arc<AtomicBool>,
    #[cfg(any(test, feature = "test-support"))]
    retired: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<LeaderChannelShutdown>,
}
impl LeaderChannel {
    /// Call only after registering the leader's existing signals and entering
    /// its runtime. This method does no filesystem or image work.
    pub fn start(
        config: LeaderChannelConfig,
        leader: Arc<super::ControllerLeader>,
        deps: LeaderChannelDeps,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        let stop = Arc::new(ChannelStop {
            stopped: AtomicBool::new(false),
            leader_shutdown: shutdown,
            changed: Notify::new(),
        });
        #[cfg(any(test, feature = "test-support"))]
        let ready = Arc::new(AtomicBool::new(false));
        #[cfg(any(test, feature = "test-support"))]
        let retired = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(drive_generation(
            config,
            leader,
            deps,
            stop.clone(),
            #[cfg(any(test, feature = "test-support"))]
            ready.clone(),
            #[cfg(any(test, feature = "test-support"))]
            retired.clone(),
        ));
        Self {
            stop,
            #[cfg(any(test, feature = "test-support"))]
            ready,
            #[cfg(any(test, feature = "test-support"))]
            retired,
            task,
        }
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire) && !self.stop.stopping()
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }
    pub async fn shutdown(self) -> LeaderChannelShutdown {
        self.stop.stop();
        self.task.await.unwrap_or(LeaderChannelShutdown {
            #[cfg(any(test, feature = "test-support"))]
            rpc: ShutdownEvidence {
                completed: 0,
                unknown: 1,
            },
            #[cfg(any(test, feature = "test-support"))]
            files: ForwardDisposition::Retained,
        })
    }
}

struct Generation {
    lease: LeaderSocketLease,
    service: ServiceIdentity,
    child: ChildRpcSpec,
    // A stalled native job cannot mutate files after relinquishing leadership.
    _leader: Arc<super::ControllerLeader>,
    withdrawn: Option<ForwardDisposition>,
}

fn prepare_generation(
    config: LeaderChannelConfig,
    leader: Arc<super::ControllerLeader>,
    image: &dyn RunningImageSource,
    stop: &ChannelStop,
) -> Result<Generation, WorkerError> {
    let unavailable =
        || WorkerError::Unavailable("CONTROLLER_CHANNEL: generation unavailable".into());
    if stop.stopping() {
        return Err(unavailable());
    }
    let image = image.capture().map_err(|_| unavailable())?;
    if stop.stopping() {
        return Err(unavailable());
    }
    let generation = UuidString::new_v4();
    let mut lease = bind_leader(&config.paths, &leader, &image, &generation)?;
    let prepare = || -> Result<(ServiceIdentity, ChildRpcSpec), WorkerError> {
        let host = super::init::host_identity(&config.home)?;
        let journal_id = config.journal.as_ref().and_then(|journal| {
            UuidString::parse(&journal.initialized_journal_id().to_string()).ok()
        });
        let mut features: Vec<_> = crate::features::CONTROLLER_FEATURES
            .iter()
            .map(|feature| (*feature).to_owned())
            .collect();
        features.push(crate::features::CONTROLLER_SOCKET.to_owned());
        features.sort();
        let service = ServiceIdentity {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            channel_version: super::channel::CHANNEL_VERSION,
            controller_client_id: config.client_id,
            account: ControllerAccount {
                uid: host.uid,
                username: host.username,
                home: host.home,
            },
            leader: leader.identity(),
            service_generation: generation,
            socket_path: config.paths.controller_state_root().join("rpc/s"),
            features,
            journal_id,
        };
        service.validate().map_err(|_| unavailable())?;
        let mut environment = vec![
            ("HOME".into(), config.home.clone().into_os_string()),
            (
                "XDG_CONFIG_HOME".into(),
                PathLayout::config_home(&config.environment, &config.home).into_os_string(),
            ),
        ];
        for (name, path) in [
            ("XDG_STATE_HOME", &config.paths.state),
            ("XDG_CACHE_HOME", &config.paths.cache),
            ("XDG_DATA_HOME", &config.paths.data),
        ] {
            environment.push((
                name.into(),
                path.parent()
                    .ok_or_else(unavailable)?
                    .as_os_str()
                    .to_owned(),
            ));
        }
        if let Some(path) = config.environment.get(std::ffi::OsStr::new("PATH")) {
            environment.push(("PATH".into(), path.clone()));
        }
        let child = ChildRpcSpec {
            executable: lease.executable(),
            detached_runner_executable: lease.detached_runner_executable().to_owned(),
            config: std::path::absolute(&config.paths.config)?,
            environment,
        };
        Ok((service, child))
    };
    match prepare() {
        Ok((service, child)) if !stop.stopping() => Ok(Generation {
            lease,
            service,
            child,
            _leader: leader,
            withdrawn: None,
        }),
        result => {
            // No runtime ever received this listener, so no RPC child exists.
            lease.withdraw(true);
            Err(result.err().unwrap_or_else(unavailable))
        }
    }
}

async fn guard_elapsed(runtime: &dyn ChannelRuntime, deadline: Duration) {
    while runtime.now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn control_result<T>(
    rx: &mut oneshot::Receiver<T>,
    runtime: &dyn ChannelRuntime,
    deadline: Duration,
) -> Option<T> {
    tokio::select! {
        value = rx => value.ok(),
        _ = guard_elapsed(runtime, deadline) => None,
    }
}

type Publication = oneshot::Receiver<(Generation, bool)>;

async fn finish_generation(
    service: Option<&SocketService>,
    generation: Option<Generation>,
    pending: Option<Publication>,
    deps: &LeaderChannelDeps,
    stop: &ChannelStop,
    rpc_exits_proven: &AtomicBool,
) -> LeaderChannelShutdown {
    stop.stop();
    let deadline = deps.runtime.now().saturating_add(SETUP_GUARD);
    let rpc = match service {
        Some(service) => {
            service
                .shutdown(&super::channel::ServerContext {
                    runtime: deps.runtime.clone(),
                    deadline,
                    cancelled: Arc::new(AtomicBool::new(true)),
                })
                .await
        }
        None => ShutdownEvidence::default(),
    };
    rpc_exits_proven.store(rpc.unknown == 0, Ordering::Release);
    let generation = match (generation, pending) {
        (Some(generation), _) => Some(generation),
        (None, Some(mut pending)) => control_result(&mut pending, deps.runtime.as_ref(), deadline)
            .await
            .map(|(generation, _)| generation),
        _ => None,
    };
    let _files = if let Some(generation) = generation {
        if generation.withdrawn == Some(ForwardDisposition::Cleaned) {
            ForwardDisposition::Cleaned
        } else {
            let proven = rpc.unknown == 0;
            match deps.control.try_run(Box::new(move || {
                let mut generation = generation;
                let disposition = generation.lease.withdraw(proven);
                drop(generation);
                disposition
            })) {
                Ok(mut rx) => control_result(&mut rx, deps.runtime.as_ref(), deadline)
                    .await
                    .unwrap_or(ForwardDisposition::Retained),
                Err(_) => ForwardDisposition::Retained,
            }
        }
    } else {
        ForwardDisposition::Retained
    };
    LeaderChannelShutdown {
        #[cfg(any(test, feature = "test-support"))]
        rpc,
        #[cfg(any(test, feature = "test-support"))]
        files: _files,
    }
}

#[cfg(any(test, feature = "test-support"))]
struct GenerationStatus {
    ready: Arc<AtomicBool>,
    retired: Arc<AtomicBool>,
}
#[cfg(any(test, feature = "test-support"))]
impl Drop for GenerationStatus {
    fn drop(&mut self) {
        self.ready.store(false, Ordering::Release);
        self.retired.store(true, Ordering::Release);
    }
}

async fn drive_generation(
    config: LeaderChannelConfig,
    leader: Arc<super::ControllerLeader>,
    deps: LeaderChannelDeps,
    stop: Arc<ChannelStop>,
    #[cfg(any(test, feature = "test-support"))] ready: Arc<AtomicBool>,
    #[cfg(any(test, feature = "test-support"))] retired: Arc<AtomicBool>,
) -> LeaderChannelShutdown {
    #[cfg(any(test, feature = "test-support"))]
    let _status = GenerationStatus {
        ready: ready.clone(),
        retired,
    };
    let retained = || LeaderChannelShutdown {
        #[cfg(any(test, feature = "test-support"))]
        rpc: ShutdownEvidence::default(),
        #[cfg(any(test, feature = "test-support"))]
        files: ForwardDisposition::Retained,
    };
    let (image, prepare_stop) = (deps.image.clone(), stop.clone());
    let Ok(mut startup) = deps.control.try_run(Box::new(move || {
        prepare_generation(config, leader, image.as_ref(), &prepare_stop)
    })) else {
        return retained();
    };
    let deadline = deps.runtime.now().saturating_add(SETUP_GUARD);
    let generation = tokio::select! {
        value = control_result(&mut startup, deps.runtime.as_ref(), deadline) => value,
        _ = stop.wait() => None,
    };
    let Some(Ok(mut generation)) = generation else {
        stop.stop();
        return retained();
    };
    let rpc_exits_proven = Arc::new(AtomicBool::new(false));
    let service = match generation
        .lease
        .take_listener()
        .ok_or_else(|| WorkerError::Unavailable("CONTROLLER_CHANNEL: listener missing".into()))
        .and_then(|listener| {
            SocketService::start(
                listener,
                generation.service.clone(),
                ServerDeps {
                    codec: Arc::new(SessionCodec::new()),
                    executor: Arc::new(ChildRpcExecutor::new(
                        deps.runner.clone(),
                        generation.child.clone(),
                    )),
                    runtime: deps.runtime.clone(),
                },
                stop.leader_shutdown.clone(),
            )
        }) {
        Ok(service) => service,
        Err(_) => {
            return finish_generation(
                None,
                Some(generation),
                None,
                &deps,
                &stop,
                &rpc_exits_proven,
            )
            .await;
        }
    };
    let ready_result = tokio::select! {
        result = service.wait_ready() => result.is_ok(),
        _ = stop.wait() => false,
    };
    if !ready_result {
        return finish_generation(
            Some(&service),
            Some(generation),
            None,
            &deps,
            &stop,
            &rpc_exits_proven,
        )
        .await;
    }
    let (publish_stop, publish_proven) = (stop.clone(), rpc_exits_proven.clone());
    let Ok(mut publication) = deps.control.try_run(Box::new(move || {
        let published =
            !publish_stop.stopping() && generation.lease.publish(&generation.service).is_ok();
        if publish_stop.stopping() {
            generation.withdrawn = Some(
                generation
                    .lease
                    .withdraw(publish_proven.load(Ordering::Acquire)),
            );
        }
        (generation, published)
    })) else {
        return finish_generation(Some(&service), None, None, &deps, &stop, &rpc_exits_proven)
            .await;
    };
    let published = tokio::select! {
        result = &mut publication => result.ok(),
        _ = stop.wait() => return finish_generation(Some(&service), None, Some(publication), &deps, &stop, &rpc_exits_proven).await,
        _ = guard_elapsed(deps.runtime.as_ref(), deadline) => return finish_generation(Some(&service), None, Some(publication), &deps, &stop, &rpc_exits_proven).await,
    };
    let Some((generation, published)) = published else {
        return finish_generation(Some(&service), None, None, &deps, &stop, &rpc_exits_proven)
            .await;
    };
    if published && service.ready() && !stop.stopping() {
        #[cfg(any(test, feature = "test-support"))]
        ready.store(true, Ordering::Release);
        // Retirement (including eight uncertain supervisors) does not set the
        // leader's cancellation flag or replenish capacity for this generation.
        while service.ready() && !stop.stopping() {
            tokio::select! {
                _ = stop.wait() => {},
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
    }
    #[cfg(any(test, feature = "test-support"))]
    ready.store(false, Ordering::Release);
    finish_generation(
        Some(&service),
        Some(generation),
        None,
        &deps,
        &stop,
        &rpc_exits_proven,
    )
    .await
}
