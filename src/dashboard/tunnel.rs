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

const READINESS_TIMEOUT: Duration = Duration::from_secs(8);
const REAP_GRACE: Duration = Duration::from_secs(2);
const WAIT_SLICE: Duration = Duration::from_millis(50);
const STDOUT_LIMIT: usize = 64 * 1024;
const SNAPSHOT_LIMIT: usize = 64 * 1024;

pub async fn run_controller_dashboard_tunnel(
    config: &Config,
    runtime: &RuntimeContext,
    port: Option<u16>,
    no_open: bool,
    no_facts_refresh: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), WorkerError> {
    // Register SIGINT/HUP/TERM now. unix::signal() installs the handler
    // immediately; tokio::signal::ctrl_c() would wait until the select arm is polled,
    // leaving a default-terminate window after spawn.
    let mut signals = arm_tunnel_signals();

    let port = allocate_loopback_port(port)?;
    let mut request = controller_dashboard_ssh_request(&config.controller, port, no_facts_refresh)?;
    apply_labelled_fake_ssh(&mut request, runtime);
    let mut child = spawn_held_stdin_ssh(&request)?;
    let mut stdin = Some(child.stdin.take().ok_or_else(controller_unavailable)?);
    let child_stdout = child.stdout.take().ok_or_else(controller_unavailable)?;

    tokio::select! {
        ready = wait_until_ready(port, &mut child, child_stdout) => {
            let ready = match ready {
                Ok(url) => url,
                Err(error) => {
                    reap_with_escalation(&mut child, stdin.take());
                    return Err(error);
                }
            };
            if let Err(error) = write_url(stdout, &ready) {
                reap_with_escalation(&mut child, stdin.take());
                return Err(error);
            }
            if !no_open {
                let opener = SystemBrowserOpener;
                if opener.open(&ready).is_err() {
                    let _ = writeln!(
                        stderr,
                        "DASHBOARD_BROWSER_OPEN_FAILED: dashboard browser could not be opened"
                    );
                    let _ = stderr.flush();
                }
            }
            wait_for_tunnel_exit(child, stdin.take(), signals).await
        }
        _ = recv_signal(&mut signals.interrupt) => {
            reap_with_escalation(&mut child, stdin.take());
            Ok(())
        }
        _ = recv_signal(&mut signals.hangup) => {
            reap_with_escalation(&mut child, stdin.take());
            Ok(())
        }
        _ = recv_signal(&mut signals.terminate) => {
            reap_with_escalation(&mut child, stdin.take());
            Ok(())
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
) -> Result<String, WorkerError> {
    let deadline = Instant::now() + READINESS_TIMEOUT;
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

async fn wait_for_tunnel_exit(
    mut child: Child,
    mut stdin: Option<std::process::ChildStdin>,
    mut signals: TunnelSignals,
) -> Result<(), WorkerError> {
    loop {
        tokio::select! {
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
            _ = tokio::time::sleep(WAIT_SLICE) => {
                match child.try_wait() {
                    Ok(None) => {}
                    Ok(Some(status)) if status.success() => {
                        drop(stdin.take());
                        return Ok(());
                    }
                    _ => {
                        drop(stdin.take());
                        return Err(controller_unavailable());
                    }
                }
            }
        }
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
