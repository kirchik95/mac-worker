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

use super::{
    ChannelCodec, ChannelFailure, ChannelReason, ClientContext, ControllerRequest, IDENTITY_BYTES,
    ProcessResult, READ_SCRATCH_BYTES, REQUEST_GUARD, SETUP_GUARD, SocketConnector, SocketIdentity,
    SocketSession, invalid_frame,
};
use crate::controller::protocol::{MAX_FRAME_BYTES, decode_request};

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
    if (ctx.should_stop)() || ctx.runtime.cancelled() {
        return Err(ChannelFailure::Unavailable(ChannelReason::Cancelled));
    }
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
                && s.len() < 104
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

#[cfg(test)]
mod tests {
    use super::super::{
        SessionCodec,
        tests::{bytes, identity, process, read_payload, request},
    };
    use super::*;
    use crate::controller::channel::contracts::{ChannelRuntime, DecodeProgress, FrameDecoder};
    use crate::controller::protocol::{
        decode_frame, encode_frame, encode_json_frame, parse_request,
    };
    use serde_json::{Value, json};
    use std::{
        cell::Cell,
        io::{Read, Write},
        net::Shutdown,
        os::fd::AsRawFd,
        os::unix::net::UnixListener,
        rc::Rc,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

    const HANG_GUARD: Duration = Duration::from_secs(30);
    #[derive(Default)]
    struct Runtime {
        millis: AtomicU64,
        stopped: AtomicBool,
    }
    impl ChannelRuntime for Runtime {
        fn now(&self) -> Duration {
            Duration::from_millis(self.millis.load(Ordering::SeqCst))
        }
        fn cancelled(&self) -> bool {
            self.stopped.load(Ordering::SeqCst)
        }
    }
    impl Runtime {
        fn advance(&self, duration: Duration) {
            self.millis
                .fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
        }
    }
    fn context<'a>(runtime: &'a Runtime, stop: &'a dyn Fn() -> bool) -> ClientContext<'a> {
        ClientContext {
            runtime,
            deadline: runtime.now() + HANG_GUARD,
            should_stop: stop,
        }
    }
    fn fixture<T: Send + 'static>(
        serve: impl FnOnce(UnixStream) -> T + Send + 'static,
    ) -> (tempfile::TempDir, std::path::PathBuf, thread::JoinHandle<T>) {
        let dir = tempfile::Builder::new()
            .prefix("p3t2-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let path = dir.path().join("s");
        let listener = UnixListener::bind(&path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(HANG_GUARD)).unwrap();
            stream.set_write_timeout(Some(HANG_GUARD)).unwrap();
            serve(stream)
        });
        (dir, path, handle)
    }
    fn read_one(stream: &mut UnixStream) -> Vec<u8> {
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).unwrap();
        let len = u32::from_be_bytes(prefix) as usize;
        assert!((1..=crate::controller::protocol::MAX_FRAME_BYTES).contains(&len));
        let mut payload = vec![0; len];
        stream.read_exact(&mut payload).unwrap();
        payload
    }
    fn accept_hello(stream: &mut UnixStream) {
        let hello: Value = serde_json::from_slice(&read_one(stream)).unwrap();
        assert_eq!(hello["kind"], "hello");
        assert_eq!(hello["channel_version"], 1);
        assert_eq!(
            hello["expected_service"],
            serde_json::to_value(identity()).unwrap()["service"]
        );
        assert_eq!(hello["route_sha256"], "a".repeat(64));
    }
    fn connector() -> FramedSocketConnector {
        FramedSocketConnector::new(Arc::new(SessionCodec::new()))
    }
    fn request_frame(request: &ControllerRequest) -> Vec<u8> {
        encode_json_frame(&json!({"protocol_version":7,"request_id":request.request_id(),"command":request.command(),"body":request.body()})).unwrap()
    }
    fn assert_reason<T>(result: Result<T, ChannelFailure>, reason: ChannelReason) {
        assert!(matches!(result, Err(ChannelFailure::Unavailable(actual)) if actual == reason));
    }

    #[test]
    fn session_sends_hello_before_application_and_reuses_a_sequential_connection() {
        let (_dir, path, server) = fixture(|mut stream| {
            accept_hello(&mut stream);
            let ready = SessionCodec::new().encode_ready(&identity()).unwrap();
            for chunk in ready.chunks(3) {
                stream.write_all(chunk).unwrap();
            }
            for _ in 0..2 {
                let payload = read_one(&mut stream);
                let request = parse_request(&payload).unwrap();
                assert_eq!(request.command(), "task.wait.poll");
                let reply = SessionCodec::new()
                    .encode_reply(&request, &process(&read_payload(&request), 0))
                    .unwrap();
                for chunk in reply.chunks(7) {
                    stream.write_all(chunk).unwrap();
                }
            }
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let runtime = Runtime::default();
        let ctx = context(&runtime, &|| false);
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let request = request();
        let frame = request_frame(&request);
        for _ in 0..2 {
            let result = session.exchange(&frame, &request, &ctx).unwrap();
            assert_eq!(result.status.code(), Some(0));
            assert_eq!(
                serde_json::from_slice::<Value>(decode_frame(&result.stdout).unwrap()).unwrap(),
                read_payload(&request)
            );
        }
        session.close();
        session.close();
        server.join().unwrap();
        assert_reason(
            session.exchange(&frame, &request, &ctx),
            ChannelReason::ForwardLost,
        );
    }

    #[test]
    fn failed_ready_closes_without_sending_application_bytes() {
        let (_dir, path, server) = fixture(|mut stream| {
            accept_hello(&mut stream);
            let mut id = identity();
            id.service.leader = crate::job::ProcessIdentity::new(124, 456).unwrap();
            stream
                .write_all(&SessionCodec::new().encode_ready(&id).unwrap())
                .unwrap();
            let mut app = Vec::new();
            stream.read_to_end(&mut app).unwrap();
            assert!(app.is_empty());
        });
        let runtime = Runtime::default();
        assert!(
            connector()
                .connect(&path, &identity(), &context(&runtime, &|| false))
                .is_err()
        );
        server.join().unwrap();
    }

    #[test]
    fn live_ready_allows_optional_journal_change() {
        let (_dir, path, server) = fixture(|mut stream| {
            accept_hello(&mut stream);
            let mut value = serde_json::to_value(identity()).unwrap();
            value["service"]["journal_id"] = json!("33333333-3333-4333-8333-333333333333");
            let id: SocketIdentity = serde_json::from_value(value).unwrap();
            stream
                .write_all(&SessionCodec::new().encode_ready(&id).unwrap())
                .unwrap();
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let runtime = Runtime::default();
        let mut session = connector()
            .connect(&path, &identity(), &context(&runtime, &|| false))
            .unwrap();
        session.close();
        server.join().unwrap();
    }

    #[test]
    fn handshake_eof_at_partial_prefix_or_payload_never_sends_rpc() {
        for truncated in [vec![], vec![0, 0], vec![0, 0, 0, 8, b'{', b'"']] {
            let (_dir, path, server) = fixture(move |mut stream| {
                accept_hello(&mut stream);
                stream.write_all(&truncated).unwrap();
                stream.shutdown(Shutdown::Write).unwrap();
                let mut app = Vec::new();
                stream.read_to_end(&mut app).unwrap();
                assert!(app.is_empty());
            });
            let runtime = Runtime::default();
            assert!(
                connector()
                    .connect(&path, &identity(), &context(&runtime, &|| false))
                    .is_err()
            );
            server.join().unwrap();
        }
    }

    #[test]
    fn borrowed_non_send_predicate_cancels_an_entered_handshake_without_runtime_cancel() {
        let stopped = Arc::new(AtomicBool::new(false));
        let entered = stopped.clone();
        let (_dir, path, server) = fixture(move |mut stream| {
            accept_hello(&mut stream);
            entered.store(true, Ordering::SeqCst);
            let mut app = Vec::new();
            stream.read_to_end(&mut app).unwrap();
            assert!(app.is_empty());
        });
        let borrowed = Rc::new(Cell::new(0));
        let stop = || {
            borrowed.set(borrowed.get() + 1);
            stopped.load(Ordering::SeqCst)
        };
        let runtime = Runtime::default();
        assert_reason(
            connector().connect(&path, &identity(), &context(&runtime, &stop)),
            ChannelReason::Cancelled,
        );
        assert!(!runtime.cancelled());
        assert!(borrowed.get() > 1);
        server.join().unwrap();
    }

    #[test]
    fn borrowed_predicate_cancels_an_entered_application_read_without_runtime_cancel() {
        let stopped = Arc::new(AtomicBool::new(false));
        let entered = stopped.clone();
        let (_dir, path, server) = fixture(move |mut stream| {
            accept_hello(&mut stream);
            stream
                .write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            let request = parse_request(&read_one(&mut stream)).unwrap();
            assert_eq!(request.command(), "task.wait.poll");
            entered.store(true, Ordering::SeqCst);
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let borrowed = Rc::new(Cell::new(0));
        let stop = || {
            borrowed.set(borrowed.get() + 1);
            stopped.load(Ordering::SeqCst)
        };
        let runtime = Runtime::default();
        let ctx = context(&runtime, &stop);
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let request = request();
        assert_reason(
            session.exchange(&request_frame(&request), &request, &ctx),
            ChannelReason::Cancelled,
        );
        assert!(!runtime.cancelled());
        server.join().unwrap();
    }

    fn small_send_buffer(stream: &UnixStream) {
        let size: libc::c_int = 2048;
        // SAFETY: a live socket descriptor and a correctly sized integer option.
        assert_eq!(
            unsafe {
                libc::setsockopt(
                    stream.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    (&size as *const libc::c_int).cast(),
                    std::mem::size_of_val(&size) as libc::socklen_t,
                )
            },
            0
        );
    }

    #[test]
    fn borrowed_predicate_cancels_a_partial_application_write() {
        let (client, mut peer) = UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        small_send_buffer(&client);
        peer.set_read_timeout(Some(HANG_GUARD)).unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let entered = stopped.clone();
        let (release, released) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut byte = [0];
            peer.read_exact(&mut byte).unwrap();
            entered.store(true, Ordering::SeqCst);
            released.recv_timeout(HANG_GUARD).unwrap();
            let mut rest = Vec::new();
            peer.read_to_end(&mut rest).unwrap();
            rest.len() + 1
        });
        let request = parse_request(&bytes(&json!({"protocol_version":7,"request_id":"55555555555545558555555555555555", "command":"task.wait.poll", "body":{"data":"x".repeat(256*1024)}}))).unwrap();
        let frame = request_frame(&request);
        let runtime = Runtime::default();
        let captured = Rc::new(Cell::new(0));
        let stop = || {
            captured.set(captured.get() + 1);
            stopped.load(Ordering::SeqCst)
        };
        let mut session = FramedSocketSession {
            stream: Some(client),
            codec: Arc::new(SessionCodec::new()),
        };
        let result = session.exchange(&frame, &request, &context(&runtime, &stop));
        session.close();
        release.send(()).unwrap();
        let transmitted = reader.join().unwrap();
        assert_reason(result, ChannelReason::Cancelled);
        assert!(!runtime.cancelled());
        assert!((1..frame.len()).contains(&transmitted));
    }

    #[test]
    fn session_completes_partial_writes_without_truncating_the_request() {
        let (client, mut peer) = UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        small_send_buffer(&client);
        peer.set_read_timeout(Some(HANG_GUARD)).unwrap();
        peer.set_write_timeout(Some(HANG_GUARD)).unwrap();
        let reader = thread::spawn(move || {
            let payload = read_one(&mut peer);
            let request = parse_request(&payload).unwrap();
            assert_eq!(request.body()["data"].as_str().unwrap().len(), 256 * 1024);
            peer.write_all(
                &SessionCodec::new()
                    .encode_reply(&request, &process(&read_payload(&request), 0))
                    .unwrap(),
            )
            .unwrap();
            payload
        });
        let request = parse_request(&bytes(&json!({"protocol_version":7,"request_id":"55555555555545558555555555555555", "command":"task.wait.poll", "body":{"data":"x".repeat(256*1024)}}))).unwrap();
        let frame = request_frame(&request);
        let runtime = Runtime::default();
        let mut session = FramedSocketSession {
            stream: Some(client),
            codec: Arc::new(SessionCodec::new()),
        };
        assert_eq!(
            session
                .exchange(&frame, &request, &context(&runtime, &|| false))
                .unwrap()
                .status
                .code(),
            Some(0)
        );
        session.close();
        assert_eq!(reader.join().unwrap(), decode_frame(&frame).unwrap());
    }

    #[test]
    fn handshake_deadline_uses_the_injected_clock() {
        let runtime = Arc::new(Runtime::default());
        let advance = runtime.clone();
        let (_dir, path, server) = fixture(move |mut stream| {
            accept_hello(&mut stream);
            advance.advance(Duration::from_secs(6));
            let mut app = Vec::new();
            stream.read_to_end(&mut app).unwrap();
            assert!(app.is_empty());
        });
        assert_reason(
            connector().connect(&path, &identity(), &context(&runtime, &|| false)),
            ChannelReason::Timeout,
        );
        server.join().unwrap();
    }

    #[test]
    fn application_deadline_does_not_reset_the_original_call_budget() {
        let runtime = Arc::new(Runtime::default());
        let advance = runtime.clone();
        let (_dir, path, server) = fixture(move |mut stream| {
            accept_hello(&mut stream);
            stream
                .write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            read_one(&mut stream);
            advance.advance(Duration::from_secs(4));
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let ctx = ClientContext {
            runtime: runtime.as_ref(),
            deadline: Duration::from_secs(3),
            should_stop: &|| false,
        };
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let request = request();
        assert_reason(
            session.exchange(&request_frame(&request), &request, &ctx),
            ChannelReason::Timeout,
        );
        server.join().unwrap();
    }

    #[test]
    fn session_eof_at_partial_reply_closes_the_connection() {
        let (_dir, path, server) = fixture(|mut stream| {
            accept_hello(&mut stream);
            stream
                .write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            read_one(&mut stream);
            stream.write_all(&[0, 0, 0, 20, b'{']).unwrap();
            stream.shutdown(Shutdown::Write).unwrap();
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let runtime = Runtime::default();
        let ctx = context(&runtime, &|| false);
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let request = request();
        assert_reason(
            session.exchange(&request_frame(&request), &request, &ctx),
            ChannelReason::ForwardLost,
        );
        assert_reason(
            session.exchange(&request_frame(&request), &request, &ctx),
            ChannelReason::ForwardLost,
        );
        server.join().unwrap();
    }

    #[test]
    fn already_cancelled_borrowed_context_never_opens_a_socket() {
        let runtime = Runtime::default();
        let capture = Rc::new(Cell::new(true));
        let stop = || capture.get();
        assert_reason(
            connector().connect(
                Path::new("/private/tmp/p3t2-missing-socket"),
                &identity(),
                &context(&runtime, &stop),
            ),
            ChannelReason::Cancelled,
        );
        assert!(!runtime.cancelled());
    }

    #[test]
    fn connected_socket_is_nonblocking_and_cannot_leak_through_exec() {
        let (_dir, path, server) = fixture(|mut stream| {
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let runtime = Runtime::default();
        let ctx = context(&runtime, &|| false);
        let stream = connect_nonblocking(&path, &ctx, HANG_GUARD).unwrap();
        // SAFETY: read-only descriptor queries on a live owned socket.
        let descriptor_flags = unsafe { libc::fcntl(stream.as_raw_fd(), libc::F_GETFD) };
        // SAFETY: read-only descriptor query on the same live owned socket.
        let status_flags = unsafe { libc::fcntl(stream.as_raw_fd(), libc::F_GETFL) };
        assert!(descriptor_flags >= 0 && descriptor_flags & libc::FD_CLOEXEC != 0);
        assert!(status_flags >= 0 && status_flags & libc::O_NONBLOCK != 0);
        drop(stream);
        server.join().unwrap();
    }

    struct AdvancingCodec {
        runtime: Arc<Runtime>,
    }
    struct AdvancingDecoder {
        inner: Box<dyn FrameDecoder>,
        runtime: Arc<Runtime>,
    }
    impl FrameDecoder for AdvancingDecoder {
        fn feed(&mut self, input: &[u8]) -> Result<DecodeProgress, ChannelFailure> {
            let progress = self.inner.feed(input)?;
            if progress.payload.is_none() && progress.consumed > 0 {
                self.runtime.advance(Duration::from_secs(6));
            }
            Ok(progress)
        }
        fn retained_bytes(&self) -> usize {
            self.inner.retained_bytes()
        }
    }
    impl ChannelCodec for AdvancingCodec {
        fn decoder(&self) -> Box<dyn FrameDecoder> {
            Box::new(AdvancingDecoder {
                inner: SessionCodec::new().decoder(),
                runtime: self.runtime.clone(),
            })
        }
        fn encode_hello(&self, id: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
            SessionCodec::new().encode_hello(id)
        }
        fn decode_hello(&self, payload: &[u8]) -> Result<SocketIdentity, ChannelFailure> {
            SessionCodec::new().decode_hello(payload)
        }
        fn encode_ready(&self, id: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
            SessionCodec::new().encode_ready(id)
        }
        fn decode_ready(&self, payload: &[u8], id: &SocketIdentity) -> Result<(), ChannelFailure> {
            SessionCodec::new().decode_ready(payload, id)
        }
        fn encode_reply(
            &self,
            request: &ControllerRequest,
            result: &ProcessResult,
        ) -> Result<Vec<u8>, ChannelFailure> {
            SessionCodec::new().encode_reply(request, result)
        }
        fn decode_reply(
            &self,
            payload: &[u8],
            request: &ControllerRequest,
        ) -> Result<ProcessResult, ChannelFailure> {
            SessionCodec::new().decode_reply(payload, request)
        }
    }

    #[test]
    fn partial_reply_guard_uses_injected_time_before_the_application_deadline() {
        let (client, mut peer) = UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        peer.set_read_timeout(Some(HANG_GUARD)).unwrap();
        let reader = thread::spawn(move || {
            read_one(&mut peer);
            peer.write_all(&[0, 0]).unwrap();
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).unwrap(), 0);
        });
        let runtime = Arc::new(Runtime::default());
        let mut session = FramedSocketSession {
            stream: Some(client),
            codec: Arc::new(AdvancingCodec {
                runtime: runtime.clone(),
            }),
        };
        let request = request();
        assert_reason(
            session.exchange(
                &request_frame(&request),
                &request,
                &context(&runtime, &|| false),
            ),
            ChannelReason::Timeout,
        );
        assert_eq!(runtime.now(), Duration::from_secs(6));
        reader.join().unwrap();
    }

    #[test]
    fn session_rejects_mismatched_request_frames_before_sending_bytes() {
        let (client, mut peer) = UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        peer.set_read_timeout(Some(HANG_GUARD)).unwrap();
        let runtime = Runtime::default();
        let mut session = FramedSocketSession {
            stream: Some(client),
            codec: Arc::new(SessionCodec::new()),
        };
        let request = request();
        assert_reason(
            session.exchange(
                &encode_frame(b"{}").unwrap(),
                &request,
                &context(&runtime, &|| false),
            ),
            ChannelReason::InvalidFrame,
        );
        let mut byte = [0];
        assert_eq!(peer.read(&mut byte).unwrap(), 0);
    }
}
