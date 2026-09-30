use std::{
    ffi::OsStr,
    io::{self, Read, Write},
    os::fd::AsRawFd,
    os::unix::process::CommandExt,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use crate::{
    RuntimeContext,
    config::Config,
    controller::controller_dashboard_ssh_request,
    dashboard::command::{BrowserOpener, SystemBrowserOpener, validate_dashboard_url},
    error::WorkerError,
    process::ProcessRequest,
};

const REAP_GRACE: Duration = Duration::from_secs(2);
/// Stdout EOF can arrive before ssh has been reaped. Poll for the real exit
/// status this long before SIGTERM, so a finished remote command is not
/// mistaken for an unknown death.
const EXIT_STATUS_GRACE: Duration = Duration::from_millis(500);
const WAIT_SLICE: Duration = Duration::from_millis(50);
const STDOUT_LIMIT: usize = 64 * 1024;
const SNAPSHOT_LIMIT: usize = 64 * 1024;

/// Durations for the controller dashboard tunnel. Production values are the
/// defaults. Debug builds may shrink them with `MAC_WORKER_TEST_TUNNEL_*_MS`
/// and `MAC_WORKER_TEST_VIEWER_HEARTBEAT_MS` so tests stay deterministic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DashboardTunnelTimings {
    pub readiness_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub viewer_heartbeat_timeout: Duration,
    pub backoff_initial: Duration,
    pub backoff_cap: Duration,
    pub port_rotation_after: Duration,
}

impl DashboardTunnelTimings {
    pub(crate) fn production() -> Self {
        Self {
            readiness_timeout: Duration::from_secs(8),
            heartbeat_interval: Duration::from_secs(5),
            viewer_heartbeat_timeout: Duration::from_secs(30),
            backoff_initial: Duration::from_secs(1),
            backoff_cap: Duration::from_secs(30),
            port_rotation_after: Duration::from_secs(60),
        }
    }

    pub(crate) fn resolved() -> Self {
        let mut timings = Self::production();
        #[cfg(debug_assertions)]
        apply_debug_timing_overrides(&mut timings);
        timings
    }
}

#[cfg(debug_assertions)]
fn apply_debug_timing_overrides(timings: &mut DashboardTunnelTimings) {
    override_millis(
        "MAC_WORKER_TEST_TUNNEL_READINESS_MS",
        &mut timings.readiness_timeout,
    );
    override_millis(
        "MAC_WORKER_TEST_TUNNEL_HEARTBEAT_MS",
        &mut timings.heartbeat_interval,
    );
    override_millis(
        "MAC_WORKER_TEST_VIEWER_HEARTBEAT_MS",
        &mut timings.viewer_heartbeat_timeout,
    );
    override_millis(
        "MAC_WORKER_TEST_TUNNEL_BACKOFF_INITIAL_MS",
        &mut timings.backoff_initial,
    );
    override_millis(
        "MAC_WORKER_TEST_TUNNEL_BACKOFF_CAP_MS",
        &mut timings.backoff_cap,
    );
    override_millis(
        "MAC_WORKER_TEST_TUNNEL_PORT_ROTATION_MS",
        &mut timings.port_rotation_after,
    );
}

#[cfg(debug_assertions)]
fn override_millis(name: &str, slot: &mut Duration) {
    if let Ok(value) = std::env::var(name)
        && let Some(parsed) = parse_millis(&value)
    {
        *slot = parsed;
    }
}

#[cfg(debug_assertions)]
fn parse_millis(value: &str) -> Option<Duration> {
    value.parse::<u64>().ok().map(Duration::from_millis)
}

pub async fn run_controller_dashboard_tunnel(
    config: &Config,
    runtime: &RuntimeContext,
    port: Option<u16>,
    no_open: bool,
    no_facts_refresh: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), WorkerError> {
    run_controller_dashboard_tunnel_with_timings(
        config,
        runtime,
        port,
        no_open,
        no_facts_refresh,
        DashboardTunnelTimings::resolved(),
        stdout,
        stderr,
    )
    .await
}

#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn run_controller_dashboard_tunnel_with_readiness_timeout(
    config: &Config,
    runtime: &RuntimeContext,
    port: Option<u16>,
    no_open: bool,
    no_facts_refresh: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    readiness_timeout: Duration,
) -> Result<(), WorkerError> {
    let mut timings = DashboardTunnelTimings::resolved();
    timings.readiness_timeout = readiness_timeout;
    run_controller_dashboard_tunnel_with_timings(
        config,
        runtime,
        port,
        no_open,
        no_facts_refresh,
        timings,
        stdout,
        stderr,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_controller_dashboard_tunnel_with_timings(
    config: &Config,
    runtime: &RuntimeContext,
    requested_port: Option<u16>,
    no_open: bool,
    no_facts_refresh: bool,
    timings: DashboardTunnelTimings,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), WorkerError> {
    // Register SIGINT/HUP/TERM now. unix::signal() installs the handler
    // immediately; tokio::signal::ctrl_c() would wait until the select arm is polled,
    // leaving a default-terminate window after spawn.
    let mut signals = arm_tunnel_signals();
    let mut port = allocate_loopback_port(requested_port)?;
    let mut ever_ready = false;
    let mut announced = false;
    let mut port_problem_since: Option<Instant> = None;
    let mut port_problem_attempts = 0u32;
    let mut nominal_backoff = timings.backoff_initial;
    let mut published_port: Option<u16> = None;

    loop {
        if ever_ready {
            let failing_for = port_problem_since.map(|start| start.elapsed());
            if should_rotate_port(
                port_problem_attempts,
                failing_for,
                timings.port_rotation_after,
            ) {
                port = allocate_fresh_port(port)?;
                port_problem_attempts = 0;
                port_problem_since = None;
            }
            if interruptible_sleep(&mut signals, equal_jitter(nominal_backoff, random_u64())).await
            {
                return Ok(());
            }
            nominal_backoff = next_backoff(nominal_backoff, timings.backoff_cap);
            if local_port_is_busy(port) {
                note_port_problem(
                    &mut announced,
                    &mut port_problem_since,
                    &mut port_problem_attempts,
                    stderr,
                );
                continue;
            }
        }

        let mut request =
            controller_dashboard_ssh_request(&config.controller, port, no_facts_refresh)?;
        apply_labelled_fake_ssh(&mut request, runtime);
        let mut child = match spawn_held_stdin_ssh(&request) {
            Ok(child) => child,
            Err(error) => {
                if !ever_ready {
                    return Err(error);
                }
                note_connection_failure(
                    &mut announced,
                    &mut port_problem_since,
                    &mut port_problem_attempts,
                    stderr,
                );
                continue;
            }
        };
        let mut stdin = Some(child.stdin.take().ok_or_else(controller_unavailable)?);
        let Some(child_stdout) = child.stdout.take() else {
            let exit_code = reap_failed_attempt(&mut child, stdin.take());
            if !ever_ready {
                return Err(controller_unavailable());
            }
            record_reconnect_failure(
                exit_code,
                &mut announced,
                &mut port_problem_since,
                &mut port_problem_attempts,
                stderr,
            );
            continue;
        };

        let ready = tokio::select! {
            ready = wait_until_ready(port, &mut child, child_stdout, timings.readiness_timeout) => ready,
            _ = recv_signal(&mut signals.interrupt) => {
                reap_with_escalation(&mut child, stdin.take());
                return Ok(());
            }
            _ = recv_signal(&mut signals.hangup) => {
                reap_with_escalation(&mut child, stdin.take());
                return Ok(());
            }
            _ = recv_signal(&mut signals.terminate) => {
                reap_with_escalation(&mut child, stdin.take());
                return Ok(());
            }
        };

        match ready {
            Ok(url) => {
                if published_port != Some(port) {
                    if let Err(error) = write_url(stdout, &url) {
                        reap_with_escalation(&mut child, stdin.take());
                        return Err(error);
                    }
                    if !no_open {
                        open_dashboard(&url, stderr);
                    }
                    published_port = Some(port);
                }
                if ever_ready {
                    announce_tunnel_restored(stderr);
                }
                ever_ready = true;
                announced = false;
                port_problem_since = None;
                port_problem_attempts = 0;
                nominal_backoff = timings.backoff_initial;
                match supervise_ready_tunnel(
                    &mut child,
                    &mut stdin,
                    &mut signals,
                    timings.heartbeat_interval,
                )
                .await
                {
                    SteadyStop::Signal => return Ok(()),
                    SteadyStop::Lost => {
                        announce_tunnel_lost(stderr, &mut announced);
                    }
                }
            }
            Err(error) => {
                let exit_code = reap_failed_attempt(&mut child, stdin.take());
                if !ever_ready {
                    return Err(error);
                }
                record_reconnect_failure(
                    exit_code,
                    &mut announced,
                    &mut port_problem_since,
                    &mut port_problem_attempts,
                    stderr,
                );
            }
        }
    }
}

struct TunnelSignals {
    interrupt: Option<tokio::signal::unix::Signal>,
    hangup: Option<tokio::signal::unix::Signal>,
    terminate: Option<tokio::signal::unix::Signal>,
}

fn arm_tunnel_signals() -> TunnelSignals {
    TunnelSignals {
        interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok(),
        hangup: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).ok(),
        terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok(),
    }
}

fn should_rotate_port(
    failed_attempts: u32,
    failing_for: Option<Duration>,
    threshold: Duration,
) -> bool {
    failed_attempts > 0 && failing_for.is_some_and(|elapsed| elapsed >= threshold)
}

fn next_backoff(current: Duration, cap: Duration) -> Duration {
    match current.checked_mul(2) {
        Some(doubled) if doubled < cap => doubled,
        Some(_) => cap,
        None => cap,
    }
}

/// Equal jitter: a delay in `[d/2, d]`, inclusive on both ends for millisecond values.
fn equal_jitter(base: Duration, roll: u64) -> Duration {
    let Ok(high) = u64::try_from(base.as_millis()) else {
        return base;
    };
    if high == 0 {
        return Duration::ZERO;
    }
    let low = high / 2;
    let span = high - low + 1;
    Duration::from_millis(low + roll % span)
}

fn random_u64() -> u64 {
    let mut bytes = [0u8; 8];
    if let Ok(mut file) = std::fs::File::open("/dev/urandom")
        && file.read_exact(&mut bytes).is_ok()
    {
        return u64::from_ne_bytes(bytes);
    }
    0xA5A5_A5A5_A5A5_A5A5
}

fn allocate_fresh_port(avoid: u16) -> Result<u16, WorkerError> {
    for _ in 0..16 {
        let port = allocate_loopback_port(None)?;
        if port != avoid {
            return Ok(port);
        }
    }
    Err(bind_failed())
}

/// A reconnect attempt moves the tunnel off its published port only when that
/// port is the problem: the remote command exited with a status other than
/// ssh's connection failure (255), or `127.0.0.1:port` is already bound here.
fn counts_toward_port_rotation(exit_code: Option<i32>, local_port_busy: bool) -> bool {
    local_port_busy || matches!(exit_code, Some(code) if code != 255)
}

fn local_port_is_busy(port: u16) -> bool {
    match std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)) {
        Ok(listener) => {
            drop(listener);
            false
        }
        Err(error) => error.kind() == io::ErrorKind::AddrInUse,
    }
}

fn record_reconnect_failure(
    exit_code: Option<i32>,
    announced: &mut bool,
    port_problem_since: &mut Option<Instant>,
    port_problem_attempts: &mut u32,
    stderr: &mut dyn Write,
) {
    // An unknown status (the child was still running, or it died from a signal)
    // is neutral: it must not count as a stuck port and must not clear a window
    // that earlier remote failures already started. Only an explicit 255 resets.
    if exit_code.is_none() {
        announce_tunnel_lost(stderr, announced);
        return;
    }
    if counts_toward_port_rotation(exit_code, false) {
        note_port_problem(announced, port_problem_since, port_problem_attempts, stderr);
    } else {
        note_connection_failure(announced, port_problem_since, port_problem_attempts, stderr);
    }
}

fn note_port_problem(
    announced: &mut bool,
    port_problem_since: &mut Option<Instant>,
    port_problem_attempts: &mut u32,
    stderr: &mut dyn Write,
) {
    announce_tunnel_lost(stderr, announced);
    if port_problem_since.is_none() {
        *port_problem_since = Some(Instant::now());
    }
    *port_problem_attempts = port_problem_attempts.saturating_add(1);
}

fn note_connection_failure(
    announced: &mut bool,
    port_problem_since: &mut Option<Instant>,
    port_problem_attempts: &mut u32,
    stderr: &mut dyn Write,
) {
    announce_tunnel_lost(stderr, announced);
    *port_problem_since = None;
    *port_problem_attempts = 0;
}

fn reap_failed_attempt(child: &mut Child, stdin: Option<std::process::ChildStdin>) -> Option<i32> {
    let deadline = Instant::now() + EXIT_STATUS_GRACE;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                drop(stdin);
                return status.code();
            }
            Ok(None) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    reap_with_escalation(child, stdin);
                    return None;
                }
                std::thread::sleep(remaining.min(Duration::from_millis(10)));
            }
            Err(_) => {
                drop(stdin);
                return None;
            }
        }
    }
}

fn announce_tunnel_lost(stderr: &mut dyn Write, announced: &mut bool) {
    if *announced {
        return;
    }
    *announced = true;
    let _ = writeln!(stderr, "dashboard: controller tunnel lost; reconnecting…");
    let _ = stderr.flush();
}

fn announce_tunnel_restored(stderr: &mut dyn Write) {
    let _ = writeln!(stderr, "dashboard: controller tunnel restored");
    let _ = stderr.flush();
}

fn open_dashboard(url: &str, stderr: &mut dyn Write) {
    let opener = SystemBrowserOpener;
    if opener.open(url).is_err() {
        let _ = writeln!(
            stderr,
            "DASHBOARD_BROWSER_OPEN_FAILED: dashboard browser could not be opened"
        );
        let _ = stderr.flush();
    }
}

enum SteadyStop {
    Signal,
    Lost,
}

async fn supervise_ready_tunnel(
    child: &mut Child,
    stdin: &mut Option<std::process::ChildStdin>,
    signals: &mut TunnelSignals,
    heartbeat_interval: Duration,
) -> SteadyStop {
    let mut next_heartbeat = Instant::now();
    loop {
        let until_heartbeat = next_heartbeat.saturating_duration_since(Instant::now());
        tokio::select! {
            _ = recv_signal(&mut signals.interrupt) => {
                reap_with_escalation(child, stdin.take());
                return SteadyStop::Signal;
            }
            _ = recv_signal(&mut signals.hangup) => {
                reap_with_escalation(child, stdin.take());
                return SteadyStop::Signal;
            }
            _ = recv_signal(&mut signals.terminate) => {
                reap_with_escalation(child, stdin.take());
                return SteadyStop::Signal;
            }
            _ = tokio::time::sleep(until_heartbeat) => {
                let wrote = stdin.as_mut().is_some_and(write_heartbeat);
                if !wrote {
                    finish_lost_child(child, stdin);
                    return SteadyStop::Lost;
                }
                next_heartbeat = Instant::now() + heartbeat_interval;
            }
            _ = tokio::time::sleep(WAIT_SLICE) => {
                match child.try_wait() {
                    Ok(None) => {}
                    Ok(Some(_)) => {
                        drop(stdin.take());
                        return SteadyStop::Lost;
                    }
                    Err(_) => {
                        finish_lost_child(child, stdin);
                        return SteadyStop::Lost;
                    }
                }
            }
        }
    }
}

fn finish_lost_child(child: &mut Child, stdin: &mut Option<std::process::ChildStdin>) {
    match child.try_wait() {
        Ok(Some(_)) => {
            drop(stdin.take());
        }
        _ => reap_with_escalation(child, stdin.take()),
    }
}

fn write_heartbeat(stdin: &mut std::process::ChildStdin) -> bool {
    if set_nonblocking(stdin.as_raw_fd()).is_err() {
        return false;
    }
    let mut bytes = &b"\n"[..];
    while !bytes.is_empty() {
        match stdin.write(bytes) {
            Ok(0) => return false,
            Ok(n) => bytes = &bytes[n..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return true,
            Err(_) => return false,
        }
    }
    true
}

async fn interruptible_sleep(signals: &mut TunnelSignals, delay: Duration) -> bool {
    if delay.is_zero() {
        return false;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        _ = recv_signal(&mut signals.interrupt) => true,
        _ = recv_signal(&mut signals.hangup) => true,
        _ = recv_signal(&mut signals.terminate) => true,
    }
}

fn allocate_loopback_port(requested: Option<u16>) -> Result<u16, WorkerError> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", requested.unwrap_or(0)))
        .map_err(|_| bind_failed())?;
    let port = listener.local_addr().map_err(|_| bind_failed())?.port();
    drop(listener);
    if port == 0 {
        return Err(bind_failed());
    }
    Ok(port)
}

fn apply_labelled_fake_ssh(request: &mut ProcessRequest, runtime: &RuntimeContext) {
    let Some(path) = runtime.environment().get(OsStr::new("MAC_WORKER_FAKE_SSH")) else {
        return;
    };
    let path = Path::new(path);
    if path.is_absolute() {
        request.program = path.as_os_str().to_os_string();
    }
}

fn spawn_held_stdin_ssh(request: &ProcessRequest) -> Result<Child, WorkerError> {
    let mut command = Command::new(&request.program);
    command
        .args(&request.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .envs(
            request
                .environment
                .iter()
                .map(|(key, value)| (key.as_os_str(), value.as_os_str())),
        );
    for key in &request.environment_remove {
        command.env_remove(key);
    }
    // Separate process group so graceful laptop SIGINT/HUP/TERM can killpg the
    // ssh child without signalling the CLI. Parent-death teardown is the held
    // stdin pipe (EOF), not group membership. A new session (setsid) is not
    // used; that is the outbox/runner detach path and is unnecessary for killpg.
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    command.spawn().map_err(|_| controller_unavailable())
}

async fn wait_until_ready(
    port: u16,
    child: &mut Child,
    stdout: std::process::ChildStdout,
    readiness_timeout: Duration,
) -> Result<String, WorkerError> {
    let deadline = Instant::now() + readiness_timeout;
    let url = read_first_line_bounded(stdout, STDOUT_LIMIT, deadline).await?;
    let trimmed = url.trim();
    validate_dashboard_url(trimmed)?;
    let parsed = url::Url::parse(trimmed).map_err(|_| controller_unavailable())?;
    if parsed.port() != Some(port) {
        return Err(controller_unavailable());
    }
    ensure_child_live(child)?;
    get_snapshot_bounded(port, deadline).await?;
    ensure_child_live(child)?;
    Ok(trimmed.to_owned())
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

async fn read_first_line_bounded(
    mut stdout: std::process::ChildStdout,
    limit: usize,
    deadline: Instant,
) -> Result<String, WorkerError> {
    if remaining(deadline).is_zero() {
        return Err(controller_unavailable());
    }
    let fd = stdout.as_raw_fd();
    set_nonblocking(fd)?;
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if remaining(deadline).is_zero() {
            return Err(controller_unavailable());
        }
        if poll_readable(fd, Instant::now())? {
            match stdout.read(&mut byte) {
                Ok(0) => return Err(controller_unavailable()),
                Ok(_) => {
                    if byte[0] == b'\n' {
                        return String::from_utf8(line).map_err(|_| controller_unavailable());
                    }
                    if line.len() >= limit {
                        return Err(controller_unavailable());
                    }
                    line.push(byte[0]);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    wait_slice(deadline).await;
                }
                Err(_) => return Err(controller_unavailable()),
            }
            continue;
        }
        wait_slice(deadline).await;
    }
}

fn poll_readable(fd: i32, deadline: Instant) -> Result<bool, WorkerError> {
    let mut fds = [libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    }];
    loop {
        let millis = i32::try_from(remaining(deadline).as_millis().min(i32::MAX as u128))
            .unwrap_or(i32::MAX);
        let n = unsafe { libc::poll(fds.as_mut_ptr(), 1, millis) };
        if n < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(controller_unavailable());
        }
        return Ok(n > 0);
    }
}

fn set_nonblocking(fd: i32) -> Result<(), WorkerError> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
    if flags < 0 {
        return Err(controller_unavailable());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(controller_unavailable());
    }
    Ok(())
}

async fn wait_slice(deadline: Instant) {
    let wait = remaining(deadline).min(WAIT_SLICE);
    if !wait.is_zero() {
        tokio::time::sleep(wait).await;
    }
}

fn ensure_child_live(child: &mut Child) -> Result<(), WorkerError> {
    match child.try_wait() {
        Ok(None) => Ok(()),
        _ => Err(controller_unavailable()),
    }
}

async fn get_snapshot_bounded(port: u16, deadline: Instant) -> Result<(), WorkerError> {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = loop {
        let wait = remaining(deadline);
        if wait.is_zero() {
            return Err(controller_unavailable());
        }
        match std::net::TcpStream::connect_timeout(&addr, wait.min(WAIT_SLICE)) {
            Ok(stream) => break stream,
            Err(_) => wait_slice(deadline).await,
        }
    };
    stream
        .set_nonblocking(true)
        .map_err(|_| controller_unavailable())?;
    let host = format!("127.0.0.1:{port}");
    let request =
        format!("GET /api/v1/snapshot HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    write_all_deadline(&mut stream, request.as_bytes(), deadline).await?;
    let mut raw = vec![0u8; SNAPSHOT_LIMIT];
    let mut filled = 0usize;
    loop {
        if dashboard_snapshot_response(&raw[..filled]) {
            return Ok(());
        }
        if remaining(deadline).is_zero() {
            return Err(controller_unavailable());
        }
        if filled >= SNAPSHOT_LIMIT {
            return Err(controller_unavailable());
        }
        if poll_readable(stream.as_raw_fd(), Instant::now())? {
            match stream.read(&mut raw[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.kind() == io::ErrorKind::TimedOut =>
                {
                    wait_slice(deadline).await;
                }
                Err(_) => return Err(controller_unavailable()),
            }
            continue;
        }
        wait_slice(deadline).await;
    }
    if dashboard_snapshot_response(&raw[..filled]) {
        Ok(())
    } else {
        Err(controller_unavailable())
    }
}

async fn write_all_deadline(
    stream: &mut std::net::TcpStream,
    mut buf: &[u8],
    deadline: Instant,
) -> Result<(), WorkerError> {
    while !buf.is_empty() {
        if remaining(deadline).is_zero() {
            return Err(controller_unavailable());
        }
        match stream.write(buf) {
            Ok(0) => return Err(controller_unavailable()),
            Ok(n) => buf = &buf[n..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut =>
            {
                wait_slice(deadline).await;
            }
            Err(_) => return Err(controller_unavailable()),
        }
    }
    Ok(())
}

fn dashboard_snapshot_response(raw: &[u8]) -> bool {
    let split = raw.windows(4).position(|window| window == b"\r\n\r\n");
    let Some(split) = split else {
        return false;
    };
    let (head, body) = raw.split_at(split + 4);
    let Ok(head) = std::str::from_utf8(head) else {
        return false;
    };
    let mut lines = head.split("\r\n");
    let status = lines.next().and_then(|line| line.split_whitespace().nth(1));
    let mut json = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-type")
            && value
                .trim()
                .to_ascii_lowercase()
                .starts_with("application/json")
        {
            json = true;
        }
    }
    if !json {
        return false;
    }
    let body = std::str::from_utf8(body).unwrap_or("");
    match status {
        Some("200") => body.contains("\"api_version\""),
        Some("503") => {
            body.contains("DASHBOARD_SNAPSHOT_PENDING")
                || body.contains("DASHBOARD_SNAPSHOT_FAILED")
        }
        _ => false,
    }
}

async fn recv_signal(signal: &mut Option<tokio::signal::unix::Signal>) {
    match signal.as_mut() {
        Some(signal) => {
            signal.recv().await;
        }
        None => std::future::pending::<()>().await,
    }
}

fn reap_with_escalation(child: &mut Child, stdin: Option<std::process::ChildStdin>) {
    let pid = child.id();
    drop(stdin);
    terminate_group(pid);
    let deadline = Instant::now() + REAP_GRACE;
    loop {
        if matches!(child.try_wait(), Ok(Some(_))) {
            return;
        }
        if Instant::now() >= deadline {
            let _ = unsafe { libc::killpg(pid as i32, libc::SIGKILL) };
            let _ = child.wait();
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn terminate_group(pid: u32) {
    let _ = unsafe { libc::killpg(pid as i32, libc::SIGTERM) };
}

fn write_url(stdout: &mut dyn Write, url: &str) -> Result<(), WorkerError> {
    writeln!(stdout, "{url}")?;
    stdout.flush()?;
    Ok(())
}

fn bind_failed() -> WorkerError {
    WorkerError::Protocol("DASHBOARD_BIND_FAILED: dashboard loopback port is unavailable".into())
}

fn controller_unavailable() -> WorkerError {
    WorkerError::Unavailable("CONTROLLER_UNAVAILABLE: controller dashboard tunnel failed".into())
}

#[cfg(test)]
mod tests {
    use super::{
        DashboardTunnelTimings, counts_toward_port_rotation, equal_jitter, next_backoff,
        record_reconnect_failure, should_rotate_port,
    };
    use std::time::{Duration, Instant};

    #[test]
    fn production_timings_match_the_operator_defaults() {
        let timings = DashboardTunnelTimings::production();
        assert_eq!(timings.readiness_timeout, Duration::from_secs(8));
        assert_eq!(timings.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(timings.viewer_heartbeat_timeout, Duration::from_secs(30));
        assert_eq!(timings.backoff_initial, Duration::from_secs(1));
        assert_eq!(timings.backoff_cap, Duration::from_secs(30));
        assert_eq!(timings.port_rotation_after, Duration::from_secs(60));
    }

    #[test]
    fn equal_jitter_stays_inside_half_to_full() {
        let second = Duration::from_secs(1);
        assert_eq!(equal_jitter(second, 0), Duration::from_millis(500));
        assert_eq!(equal_jitter(second, 500), Duration::from_millis(1_000));
        assert_eq!(equal_jitter(second, 501), Duration::from_millis(500));
        let cap = Duration::from_secs(30);
        assert_eq!(equal_jitter(cap, 0), Duration::from_millis(15_000));
        assert_eq!(equal_jitter(cap, 15_000), Duration::from_millis(30_000));
    }

    #[test]
    fn backoff_doubles_until_the_cap() {
        let cap = Duration::from_secs(30);
        let mut delay = Duration::from_secs(1);
        let mut seen = vec![delay];
        for _ in 0..8 {
            delay = next_backoff(delay, cap);
            seen.push(delay);
        }
        assert_eq!(
            seen,
            vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(16),
                Duration::from_secs(30),
                Duration::from_secs(30),
                Duration::from_secs(30),
                Duration::from_secs(30),
            ]
        );
    }

    #[test]
    fn port_rotates_only_after_failed_attempts_cross_the_threshold() {
        let threshold = Duration::from_secs(60);
        assert!(!should_rotate_port(
            0,
            Some(Duration::from_secs(120)),
            threshold
        ));
        assert!(!should_rotate_port(
            2,
            Some(Duration::from_secs(59)),
            threshold
        ));
        assert!(should_rotate_port(
            1,
            Some(Duration::from_secs(60)),
            threshold
        ));
        assert!(!should_rotate_port(4, None, threshold));
    }

    #[test]
    fn only_a_remote_failure_or_a_busy_local_port_counts_toward_rotation() {
        assert!(!counts_toward_port_rotation(Some(255), false));
        assert!(!counts_toward_port_rotation(None, false));
        assert!(counts_toward_port_rotation(Some(1), false));
        assert!(counts_toward_port_rotation(Some(75), false));
        assert!(counts_toward_port_rotation(Some(0), false));
        assert!(counts_toward_port_rotation(Some(255), true));
        assert!(counts_toward_port_rotation(None, true));
    }

    #[test]
    fn unknown_exit_status_leaves_the_port_problem_window_in_place() {
        let started = Instant::now();
        let mut announced = false;
        let mut since = Some(started);
        let mut attempts = 2u32;
        let mut stderr = Vec::new();
        record_reconnect_failure(None, &mut announced, &mut since, &mut attempts, &mut stderr);
        assert_eq!(since, Some(started));
        assert_eq!(attempts, 2);
        assert!(announced);
        let lost = String::from_utf8(stderr).unwrap();
        assert!(lost.contains("reconnecting"));

        record_reconnect_failure(
            Some(255),
            &mut announced,
            &mut since,
            &mut attempts,
            &mut Vec::new(),
        );
        assert_eq!(since, None);
        assert_eq!(attempts, 0);
    }

    #[test]
    fn local_port_is_busy_only_while_a_listener_holds_it() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(super::local_port_is_busy(port));
        drop(listener);
        assert!(!super::local_port_is_busy(port));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn debug_millis_parser_rejects_blank_and_words() {
        assert_eq!(super::parse_millis("40"), Some(Duration::from_millis(40)));
        assert_eq!(super::parse_millis(""), None);
        assert_eq!(super::parse_millis("fast"), None);
    }
}
