//! Monotonic foreground runtime and common command configuration.

use std::{
    ffi::OsString,
    io::{self, Write},
    os::unix::process::CommandExt,
    path::PathBuf,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    time::Duration,
};

use super::{
    EventRuntime, JOURNAL_CHECK_INTERVAL, NOTICE_CHANNEL_BUDGET, NOTIFY_COALESCE_COUNT,
    REPAIR_INTERVAL, RPC_BUDGET,
};
use crate::{
    RuntimeContext,
    cli::Cli,
    config::Config,
    controller::channel::SETUP_GUARD,
    error::{ExitKind, WorkerError},
    paths::PathLayout,
    process::{PROCESS_GROUP_KILL_BUDGET, TERMINATED_DRAIN_GRACE},
    transfer::{ResolutionRuntime, SystemResolutionRuntime},
};

/// Channel setup and reads share the foreground event clock and cancellation.
pub(crate) struct EventChannelRuntime(pub(crate) Arc<dyn EventRuntime>);
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

/// One watchdog tick: about a second of the watchdog thread's own awake time.
const FOLLOW_WATCHDOG_TICK: Duration = Duration::from_secs(1);

/// Longest legitimate follow pass, from the real budgets: 106 s today. Every
/// request in a pass shares one RPC budget. The request in flight when it
/// ends can overrun it by a forward-cancel guard plus two process kills and
/// drains. A plan can then deliver one summary and the coalescing limit of
/// notices to both channels, macOS through an osascript that may need its own
/// kill and drain. At most one pause follows; the longest is the repair
/// interval.
const FOLLOW_PASS_BOUND: Duration = {
    let kill_and_drain = PROCESS_GROUP_KILL_BUDGET.as_secs() + TERMINATED_DRAIN_GRACE.as_secs();
    let overrun = SETUP_GUARD.as_secs() + 2 * kill_and_drain;
    let notices = NOTIFY_COALESCE_COUNT as u64 + 1;
    let delivery = notices * (2 * NOTICE_CHANNEL_BUDGET.as_secs() + kill_and_drain);
    Duration::from_secs(RPC_BUDGET.as_secs() + overrun + delivery + REPAIR_INTERVAL.as_secs())
};

/// Ticks without a new pass before a follow loop counts as stalled: 150 s of
/// the watchdog's awake time, at least a third above the longest legitimate
/// pass. The build fails if a budget change erodes that margin.
pub(crate) const FOLLOW_STALL_TICKS: u32 = 150;
const _: () = assert!(
    FOLLOW_STALL_TICKS as u64 * FOLLOW_WATCHDOG_TICK.as_secs()
        >= FOLLOW_PASS_BOUND.as_secs() * 4 / 3
);

/// Advanced once per pass by a foreground follow loop; its stall watchdog
/// reads it from another thread.
#[derive(Debug, Default)]
pub(crate) struct FollowHeartbeat(AtomicU64);

impl FollowHeartbeat {
    pub(crate) fn beat(&self) {
        self.0.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn passes(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

/// Counts watchdog ticks since the watched loop last started a pass. It
/// counts ticks, not elapsed time: a tick that spans a system sleep counts
/// once, so after every wake the loop again has `limit` awake ticks to move.
pub(crate) struct StallWatch<'a> {
    heartbeat: &'a FollowHeartbeat,
    seen: u64,
    idle: u32,
    limit: u32,
}

impl<'a> StallWatch<'a> {
    pub(crate) fn new(heartbeat: &'a FollowHeartbeat, limit: u32) -> Self {
        Self {
            heartbeat,
            seen: heartbeat.passes(),
            idle: 0,
            limit,
        }
    }

    /// Consumes `ticks` until they end or `limit` ticks in a row pass without
    /// a new pass. A stall runs `on_stall` once and returns true.
    pub(crate) fn watch(
        &mut self,
        ticks: impl IntoIterator<Item = ()>,
        on_stall: impl FnOnce(),
    ) -> bool {
        for () in ticks {
            let passes = self.heartbeat.passes();
            if passes != self.seen {
                self.seen = passes;
                self.idle = 0;
                continue;
            }
            self.idle = self.idle.saturating_add(1);
            if self.idle >= self.limit {
                on_stall();
                return true;
            }
        }
        false
    }
}

/// Keeps a follow watchdog armed; dropping it stops the watchdog thread.
pub(crate) struct FollowWatchdog {
    _stop: mpsc::Sender<()>,
}

impl ForegroundRuntime {
    /// Arms a watchdog for a follow loop that beats `heartbeat` once per
    /// pass. Its thread counts its own one-second ticks, independent of the
    /// loop's thread and timers, so a loop parked on a timer that never fires
    /// cannot hold it. After `FOLLOW_STALL_TICKS` ticks without a pass it
    /// reports `stalled` and restarts the command; see `restart_stalled_follow`.
    pub(crate) fn arm_follow_watchdog(
        &self,
        heartbeat: Arc<FollowHeartbeat>,
        stalled: &'static str,
    ) -> Result<FollowWatchdog, WorkerError> {
        let cancelled = self.cancelled.clone();
        let restart = FollowRestart::capture();
        let (stop, stopped) = mpsc::channel::<()>();
        std::thread::Builder::new()
            .name("controller-follow-watchdog".into())
            .spawn(move || {
                let ticks = std::iter::from_fn(|| {
                    matches!(
                        stopped.recv_timeout(FOLLOW_WATCHDOG_TICK),
                        Err(mpsc::RecvTimeoutError::Timeout)
                    )
                    .then_some(())
                });
                StallWatch::new(&heartbeat, FOLLOW_STALL_TICKS).watch(ticks, || {
                    restart_stalled_follow(stalled, cancelled.load(Ordering::Acquire), &restart)
                });
            })?;
        Ok(FollowWatchdog { _stop: stop })
    }
}

/// The executable and argv to restart, captured when the watchdog is armed.
struct FollowRestart {
    executable: Option<PathBuf>,
    argv: Vec<OsString>,
}

impl FollowRestart {
    fn capture() -> Self {
        Self {
            executable: std::env::current_exe().ok(),
            argv: std::env::args_os().collect(),
        }
    }

    fn command(&self) -> Option<Command> {
        let mut command = Command::new(self.executable.as_ref()?);
        let (arg0, args) = self.argv.split_first()?;
        command.arg0(arg0).args(args);
        Some(command)
    }
}

/// Replaces a stalled follow command with a fresh copy of itself: same pid,
/// stdio and argv. The stuck thread cannot run its own cleanup, and
/// close-on-exec descriptors, the notifier cache lock among them, close with
/// the old image. A restart repeats no banner: notify saves each decision
/// before it delivers a banner (`commit_then_deliver`) and admits candidates
/// before reconciliation, so the new process resumes from the saved cursor.
/// After Ctrl-C it exits 0 instead, as the command promises; if exec fails
/// it exits 69 rather than leave a stuck follower running.
fn restart_stalled_follow(stalled: &str, cancelled: bool, restart: &FollowRestart) -> ! {
    if cancelled {
        write_stderr_unlocked(&format!("{stalled}; exiting after Ctrl-C\n"));
        // SAFETY: ends the process without the std cleanup a stuck thread
        // could block; nothing buffered remains to flush.
        unsafe { libc::_exit(0) }
    }
    write_stderr_unlocked(&format!("{stalled}; restarting\n"));
    let failure = match restart.command() {
        Some(mut command) => command.exec(),
        None => io::Error::from(io::ErrorKind::NotFound),
    };
    write_stderr_unlocked(&format!(
        "{stalled}; restart failed ({}); exiting\n",
        failure.kind()
    ));
    // SAFETY: as above.
    unsafe { libc::_exit(ExitKind::Unavailable as i32) }
}

/// Writes to descriptor 2 directly. `main` holds the std stderr lock for the
/// whole command, and that lock stays held while its loop thread is stuck.
fn write_stderr_unlocked(text: &str) {
    let mut bytes = text.as_bytes();
    while !bytes.is_empty() {
        // SAFETY: `bytes` is valid for its length; a closed descriptor only
        // fails the write.
        let written =
            unsafe { libc::write(libc::STDERR_FILENO, bytes.as_ptr().cast(), bytes.len()) };
        if written > 0 {
            bytes = &bytes[written as usize..];
        } else if written == 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::{ProcessPolicy, ProcessRequest, ProcessRunner, SystemProcessRunner};

    const FIXTURE_TEST: &str = "controller::events::foreground::tests::stalled_follow_fixture";
    const FIXTURE_MODE: &str = "MAC_WORKER_STALLED_FOLLOW_FIXTURE";

    fn restart(executable: Option<&str>, argv: &[&str]) -> FollowRestart {
        FollowRestart {
            executable: executable.map(PathBuf::from),
            argv: argv.iter().map(OsString::from).collect(),
        }
    }

    #[test]
    fn restart_reuses_the_captured_executable_and_argv() {
        let command = restart(Some("/opt/worker"), &["worker", "notify", "--follow"])
            .command()
            .unwrap();
        assert_eq!(command.get_program(), "/opt/worker");
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["notify", "--follow"]
        );
        assert!(restart(None, &["worker"]).command().is_none());
        assert!(restart(Some("/opt/worker"), &[]).command().is_none());
    }

    #[test]
    #[ignore = "subprocess body: run only by stalled_follow_acts_while_its_stuck_thread_holds_stderr"]
    fn stalled_follow_fixture() {
        let Some(mode) = std::env::var_os(FIXTURE_MODE) else {
            return;
        };
        // Like `main`, hold the std stderr lock for the whole command.
        let _held = io::stderr().lock();
        let target = if mode == "missing" {
            restart(None, &["sh"])
        } else {
            restart(Some("/bin/sh"), &["sh", "-c", "printf restarted"])
        };
        let cancelled = mode == "cancelled";
        std::thread::spawn(move || {
            restart_stalled_follow("fixture follow stalled", cancelled, &target)
        });
        // The follow loop that never comes back.
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn stalled_follow_acts_while_its_stuck_thread_holds_stderr() {
        for (mode, code, stdout, stderr) in [
            (
                "restart",
                0,
                "restarted",
                "fixture follow stalled; restarting\n",
            ),
            (
                "cancelled",
                0,
                "",
                "fixture follow stalled; exiting after Ctrl-C\n",
            ),
            (
                "missing",
                69,
                "",
                "fixture follow stalled; restarting\n\
                 fixture follow stalled; restart failed (entity not found); exiting\n",
            ),
        ] {
            let request = ProcessRequest {
                program: std::env::current_exe().unwrap().into(),
                args: ["--ignored", "--exact", FIXTURE_TEST, "--nocapture"]
                    .into_iter()
                    .map(OsString::from)
                    .collect(),
                environment: vec![(FIXTURE_MODE.into(), mode.into())],
                environment_remove: Vec::new(),
                stdin: None,
                policy: ProcessPolicy {
                    stdout_limit: 64 * 1024,
                    stderr_limit: 64 * 1024,
                    // A hang guard only: every mode ends the fixture itself.
                    deadline: crate::test_support::HANDSHAKE_TIMEOUT,
                },
                isolate_parent_environment: false,
            };
            let result = SystemProcessRunner.run(&request).unwrap();
            let printed = String::from_utf8_lossy(&result.stdout);
            let diagnostics = String::from_utf8_lossy(&result.stderr);
            assert_eq!(result.status.code(), Some(code), "{mode}: {diagnostics}");
            assert!(printed.ends_with(stdout), "{mode}: {printed}");
            assert_eq!(diagnostics, stderr, "{mode}");
        }
    }
}
