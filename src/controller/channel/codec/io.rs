//! Synchronous socket sessions using nonblocking I/O and a borrowed call context.
//!
//! The foreground thread owns the socket. Readiness waits periodically recheck
//! the borrowed predicate; no cancellation closure is sent to a worker thread.

use std::{
    io::{Read, Write},
    net::Shutdown,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::net::UnixStream,
    },
    path::Path,
    sync::Arc,
    time::Duration,
};

use super::super::contracts::{
    ChannelCodec, ChannelFailure, ChannelReason, ClientContext, IDENTITY_BYTES, READ_SCRATCH_BYTES,
    REQUEST_GUARD, SETUP_GUARD, SOCKET_PATH_BYTES, SocketConnector, SocketIdentity, SocketSession,
};
use super::invalid_frame;
use crate::{
    controller::protocol::{ControllerRequest, MAX_FRAME_BYTES, decode_request},
    process::ProcessResult,
};

// A readiness timeout is a cancellation polling interval, never a retry sleep.
const CANCELLATION_POLL: Duration = Duration::from_millis(50);

pub struct FramedSocketConnector {
    codec: Arc<dyn ChannelCodec>,
}

impl FramedSocketConnector {
    pub fn new(codec: Arc<dyn ChannelCodec>) -> Self {
        Self { codec }
    }
}

impl SocketConnector for FramedSocketConnector {
    fn connect(
        &self,
        local: &Path,
        identity: &SocketIdentity,
        ctx: &ClientContext<'_>,
    ) -> Result<Box<dyn SocketSession>, ChannelFailure> {
        let deadline = stage_deadline(ctx, SETUP_GUARD);
        check(ctx, deadline)?;
        let hello = self.codec.encode_hello(identity)?;
        let mut stream = connect_nonblocking(local, ctx, deadline)?;
        write_all(&mut stream, &hello, ctx, deadline)?;
        let ready = read_payload(
            &mut stream,
            self.codec.as_ref(),
            IDENTITY_BYTES,
            ctx,
            deadline,
        )?;
        self.codec.decode_ready(&ready, identity)?;
        check(ctx, deadline)?;
        Ok(Box::new(FramedSocketSession {
            stream: Some(stream),
            codec: self.codec.clone(),
        }))
    }
}

struct FramedSocketSession {
    stream: Option<UnixStream>,
    codec: Arc<dyn ChannelCodec>,
}

impl SocketSession for FramedSocketSession {
    fn exchange(
        &mut self,
        frame: &[u8],
        request: &ControllerRequest,
        ctx: &ClientContext<'_>,
    ) -> Result<ProcessResult, ChannelFailure> {
        let deadline = stage_deadline(ctx, REQUEST_GUARD);
        let outcome = (|| {
            check(ctx, deadline)?;
            let stream = self.stream.as_mut().ok_or_else(forward_lost)?;
            if decode_request(frame).map_err(|_| invalid_frame())? != *request {
                return Err(invalid_frame());
            }
            write_all(stream, frame, ctx, deadline)?;
            let payload =
                read_payload(stream, self.codec.as_ref(), MAX_FRAME_BYTES, ctx, deadline)?;
            let result = self.codec.decode_reply(&payload, request)?;
            check(ctx, deadline)?;
            Ok(result)
        })();
        if outcome.is_err() {
            self.close();
        }
        outcome
    }

    fn close(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

fn stage_deadline(ctx: &ClientContext<'_>, guard: Duration) -> Duration {
    ctx.deadline.min(ctx.runtime.now().saturating_add(guard))
}

fn check(ctx: &ClientContext<'_>, deadline: Duration) -> Result<(), ChannelFailure> {
    ctx.check()?;
    if ctx.runtime.now() >= deadline.min(ctx.deadline) {
        return Err(ChannelFailure::Unavailable(ChannelReason::Timeout));
    }
    Ok(())
}

fn connect_nonblocking(
    path: &Path,
    ctx: &ClientContext<'_>,
    deadline: Duration,
) -> Result<UnixStream, ChannelFailure> {
    let path = path
        .to_str()
        .filter(|s| {
            Path::new(s).is_absolute()
                && s.len() < SOCKET_PATH_BYTES
                && !Path::new(s)
                    .components()
                    .any(|component| component == std::path::Component::ParentDir)
                && !s
                    .chars()
                    .any(|c| c.is_control() || matches!(c, ':' | '%' | '$'))
        })
        .ok_or(ChannelFailure::Unavailable(ChannelReason::UnsafePath))?;
    check(ctx, deadline)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let socket_type = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let socket_type = libc::SOCK_STREAM;
    // SAFETY: socket has no borrowed pointers; the descriptor is owned below.
    let fd = unsafe { libc::socket(libc::AF_UNIX, socket_type, 0) };
    if fd < 0 {
        return Err(forward_lost());
    }
    // SAFETY: fd is a fresh owned Unix socket, handed to exactly one owner.
    let stream = unsafe { UnixStream::from_raw_fd(fd) };
    // SAFETY: set the descriptor flag on this fresh owned socket. Platforms
    // without SOCK_CLOEXEC require this immediately after socket creation.
    if unsafe { libc::fcntl(stream.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return Err(forward_lost());
    }
    stream.set_nonblocking(true).map_err(|_| forward_lost())?;
    suppress_sigpipe(&stream)?;
    // SAFETY: zero is valid for this address; all used fields are filled below.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (slot, byte) in address.sun_path.iter_mut().zip(path.bytes()) {
        *slot = byte as libc::c_char;
    }
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + path.len() + 1;
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    {
        address.sun_len = length as u8;
    }
    check(ctx, deadline)?;
    // SAFETY: the initialized address contains a terminated, bounded Unix path.
    let connected = unsafe {
        libc::connect(
            stream.as_raw_fd(),
            (&address as *const libc::sockaddr_un).cast(),
            length as libc::socklen_t,
        )
    };
    if connected != 0 {
        let error = std::io::Error::last_os_error();
        if !matches!(
            error.raw_os_error(),
            Some(libc::EINPROGRESS | libc::EALREADY)
        ) {
            return Err(forward_lost());
        }
        wait_ready(&stream, libc::POLLOUT, ctx, deadline)?;
        let mut error: libc::c_int = 0;
        let mut size = std::mem::size_of_val(&error) as libc::socklen_t;
        // SAFETY: a live socket and correctly sized writable integer/length.
        let result = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                (&mut error as *mut libc::c_int).cast(),
                &mut size,
            )
        };
        if result != 0 || size as usize != std::mem::size_of_val(&error) || error != 0 {
            return Err(forward_lost());
        }
    }
    check(ctx, deadline)?;
    Ok(stream)
}

fn suppress_sigpipe(stream: &UnixStream) -> Result<(), ChannelFailure> {
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    {
        let enabled: libc::c_int = 1;
        // SAFETY: a live socket and a correctly sized read-only integer option.
        let result = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                (&enabled as *const libc::c_int).cast(),
                std::mem::size_of_val(&enabled) as libc::socklen_t,
            )
        };
        if result != 0 {
            return Err(forward_lost());
        }
    }
    #[cfg(not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    )))]
    let _ = stream;
    Ok(())
}

fn wait_ready(
    stream: &UnixStream,
    events: libc::c_short,
    ctx: &ClientContext<'_>,
    deadline: Duration,
) -> Result<(), ChannelFailure> {
    loop {
        check(ctx, deadline)?;
        let remaining = deadline.min(ctx.deadline).saturating_sub(ctx.runtime.now());
        let timeout = remaining.min(CANCELLATION_POLL).as_millis().max(1) as libc::c_int;
        let mut descriptor = libc::pollfd {
            fd: stream.as_raw_fd(),
            events,
            revents: 0,
        };
        // SAFETY: one initialized pollfd, valid for the duration of the call.
        let ready = unsafe { libc::poll(&mut descriptor, 1, timeout) };
        check(ctx, deadline)?;
        if ready < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(forward_lost());
        }
        if descriptor.revents & libc::POLLNVAL != 0 {
            return Err(forward_lost());
        }
        if ready > 0 && descriptor.revents & (events | libc::POLLERR | libc::POLLHUP) != 0 {
            return Ok(());
        }
    }
}

fn write_all(
    stream: &mut UnixStream,
    frame: &[u8],
    ctx: &ClientContext<'_>,
    deadline: Duration,
) -> Result<(), ChannelFailure> {
    let mut written = 0;
    let mut deadline = deadline;
    while written < frame.len() {
        check(ctx, deadline)?;
        match stream.write(&frame[written..]) {
            Ok(0) => return Err(forward_lost()),
            Ok(count) => {
                if written == 0 {
                    deadline = deadline.min(stage_deadline(ctx, SETUP_GUARD));
                }
                written += count;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                wait_ready(stream, libc::POLLOUT, ctx, deadline)?
            }
            Err(_) => return Err(forward_lost()),
        }
    }
    check(ctx, deadline)
}

fn read_payload(
    stream: &mut UnixStream,
    codec: &dyn ChannelCodec,
    limit: usize,
    ctx: &ClientContext<'_>,
    deadline: Duration,
) -> Result<Vec<u8>, ChannelFailure> {
    let mut decoder = codec.decoder();
    let mut scratch = [0; READ_SCRATCH_BYTES];
    let mut prefix = [0; 4];
    let mut prefix_bytes = 0;
    let mut received = false;
    let mut deadline = deadline;
    loop {
        check(ctx, deadline)?;
        match stream.read(&mut scratch) {
            Ok(0) => return Err(forward_lost()),
            Ok(count) => {
                if !received {
                    deadline = deadline.min(stage_deadline(ctx, SETUP_GUARD));
                    received = true;
                }
                if prefix_bytes < 4 {
                    let take = (4 - prefix_bytes).min(count);
                    prefix[prefix_bytes..prefix_bytes + take].copy_from_slice(&scratch[..take]);
                    prefix_bytes += take;
                    // Enforce the handshake's smaller cap before decoder allocation.
                    if prefix_bytes == 4
                        && (u32::from_be_bytes(prefix) == 0
                            || u32::from_be_bytes(prefix) as usize > limit)
                    {
                        return Err(invalid_frame());
                    }
                }
                let progress = decoder.feed(&scratch[..count])?;
                // A sequential peer cannot append an unsolicited second frame.
                if progress.consumed != count {
                    return Err(invalid_frame());
                }
                if let Some(payload) = progress.payload {
                    check(ctx, deadline)?;
                    return Ok(payload);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                wait_ready(stream, libc::POLLIN, ctx, deadline)?
            }
            Err(_) => return Err(forward_lost()),
        }
    }
}

fn forward_lost() -> ChannelFailure {
    ChannelFailure::Unavailable(ChannelReason::ForwardLost)
}
