//! Nonblocking controller read service and bounded native jobs.
pub use super::contracts::{ChannelExecutor, ChildRpcSpec, ServerContext};
pub use crate::process::{CleanupState, ProcessCompletion, TrackedProcessRunner};
pub mod child;
pub mod control;
use crate::{
    controller::{
        MAX_FRAME_BYTES,
        channel::contracts::{
            ChannelCodec, ChannelFailure, ChannelReason, ChannelRuntime, IDENTITY_BYTES,
            IDLE_GUARD, MAX_SESSIONS, MAX_SUPERVISORS, READ_SCRATCH_BYTES, REQUEST_GUARD,
            SETUP_GUARD, ServiceIdentity, SocketIdentity, server_eligible_read,
            verify_expected_service,
        },
        decode_frame, encode_frame, parse_request,
    },
    error::WorkerError,
};
pub use child::ChildRpcExecutor;
pub use control::NativeControl;
use std::{
    io,
    os::{fd::AsRawFd, unix::net::UnixListener},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::{
    net::{UnixListener as AsyncListener, UnixStream},
    sync::{Notify, Semaphore, oneshot, watch},
    task::JoinSet,
};

const CLOCK_POLL: Duration = Duration::from_millis(10);

pub struct ServerDeps {
    pub codec: Arc<dyn ChannelCodec>,
    pub executor: Arc<dyn ChannelExecutor>,
    pub runtime: Arc<dyn ChannelRuntime>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownEvidence {
    pub completed: usize,
    pub unknown: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Readiness {
    Starting,
    Ready,
    Stopping,
    Stopped,
}

struct Job {
    cancelled: Arc<AtomicBool>,
    unknown: AtomicBool,
}
#[derive(Default)]
struct Jobs {
    active: Vec<Arc<Job>>,
    completed: usize,
    unknown: usize,
}
struct State {
    sessions: AtomicUsize,
    slots: Arc<Semaphore>,
    jobs: Mutex<Jobs>,
    stop: AtomicBool,
    shutdown: Arc<AtomicBool>,
    runtime: Arc<dyn ChannelRuntime>,
    readiness: watch::Sender<Readiness>,
    changed: Notify,
    #[cfg(test)]
    before_final_write: Mutex<Option<Arc<WriteBarrier>>>,
    #[cfg(test)]
    after_final_write: Mutex<Option<Arc<WriteBarrier>>>,
}
#[cfg(test)]
#[derive(Default)]
struct WriteBarrier {
    entered: Notify,
    release: Notify,
}
impl State {
    fn stopping(&self) -> bool {
        self.stop.load(Ordering::Acquire)
            || self.shutdown.load(Ordering::Acquire)
            || self.runtime.cancelled()
    }
    fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
        let stopped = *self.readiness.borrow() == Readiness::Stopped;
        if !stopped {
            self.readiness.send_replace(Readiness::Stopping);
        }
        for job in &self.jobs.lock().unwrap().active {
            job.cancelled.store(true, Ordering::Release);
        }
        self.changed.notify_waiters();
    }
    fn evidence(&self) -> ShutdownEvidence {
        let jobs = self.jobs.lock().unwrap();
        ShutdownEvidence {
            completed: jobs.completed,
            unknown: jobs.unknown
                + jobs
                    .active
                    .iter()
                    .filter(|job| !job.unknown.load(Ordering::Acquire))
                    .count(),
        }
    }
    fn running(&self) -> bool {
        self.jobs
            .lock()
            .unwrap()
            .active
            .iter()
            .any(|job| !job.unknown.load(Ordering::Acquire))
    }
}

pub struct SocketService {
    state: Arc<State>,
}
impl SocketService {
    pub fn start(
        listener: UnixListener,
        service: ServiceIdentity,
        deps: ServerDeps,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self, WorkerError> {
        tokio::runtime::Handle::try_current().map_err(|_| {
            WorkerError::Unavailable(
                "CONTROLLER_CHANNEL: listener requires the entered runtime".into(),
            )
        })?;
        // Binding and O_NONBLOCK belong to the caller's native control job.
        let flags = unsafe { libc::fcntl(listener.as_raw_fd(), libc::F_GETFL) };
        if flags == -1 || flags & libc::O_NONBLOCK == 0 {
            return Err(WorkerError::Unavailable(
                "CONTROLLER_CHANNEL: listener must be nonblocking".into(),
            ));
        }
        let listener = AsyncListener::from_std(listener)?;
        let (readiness, _) = watch::channel(Readiness::Starting);
        let state = Arc::new(State {
            sessions: AtomicUsize::new(0),
            slots: Arc::new(Semaphore::new(MAX_SUPERVISORS)),
            jobs: Mutex::new(Jobs {
                active: Vec::with_capacity(MAX_SUPERVISORS),
                ..Jobs::default()
            }),
            stop: AtomicBool::new(false),
            shutdown,
            runtime: deps.runtime.clone(),
            readiness,
            changed: Notify::new(),
            #[cfg(test)]
            before_final_write: Mutex::new(None),
            #[cfg(test)]
            after_final_write: Mutex::new(None),
        });
        tokio::spawn(serve(listener, service, Arc::new(deps), state.clone()));
        Ok(Self { state })
    }
    pub fn ready(&self) -> bool {
        !self.state.stopping() && *self.state.readiness.borrow() == Readiness::Ready
    }
    pub async fn wait_ready(&self) -> Result<(), WorkerError> {
        let mut readiness = self.state.readiness.subscribe();
        loop {
            match *readiness.borrow_and_update() {
                Readiness::Ready if !self.state.stopping() => return Ok(()),
                Readiness::Starting => {}
                _ => {
                    return Err(WorkerError::Unavailable(
                        "CONTROLLER_CHANNEL: listener stopped before readiness".into(),
                    ));
                }
            }
            readiness.changed().await.map_err(|_| {
                WorkerError::Unavailable("CONTROLLER_CHANNEL: readiness ended".into())
            })?;
        }
    }
    pub async fn shutdown(&self, ctx: &ServerContext) -> ShutdownEvidence {
        self.state.request_stop();
        // Await stream/listener closure on the still-polled runtime. Native
        // workers have no JoinHandle on this runtime and cannot block it.
        let mut readiness = self.state.readiness.subscribe();
        while *readiness.borrow_and_update() != Readiness::Stopped {
            if readiness.changed().await.is_err() {
                break;
            }
        }
        while self.state.running() && ctx.runtime.now() < ctx.deadline {
            tokio::select! {
                _ = self.state.changed.notified() => {},
                _ = tokio::time::sleep(CLOCK_POLL) => {},
            }
        }
        self.state.evidence()
    }
}
impl Drop for SocketService {
    fn drop(&mut self) {
        self.state.request_stop();
    }
}

struct SessionPermit(Arc<State>);
impl SessionPermit {
    fn acquire(state: &Arc<State>) -> Option<Self> {
        state
            .sessions
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < MAX_SESSIONS).then_some(n + 1)
            })
            .ok()?;
        Some(Self(state.clone()))
    }
}
impl Drop for SessionPermit {
    fn drop(&mut self) {
        self.0.sessions.fetch_sub(1, Ordering::AcqRel);
    }
}
struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

async fn serve(
    listener: AsyncListener,
    service: ServiceIdentity,
    deps: Arc<ServerDeps>,
    state: Arc<State>,
) {
    let mut sessions = JoinSet::new();
    // Register the listener with the runtime before announcing readiness.
    let initial = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(match listener.poll_accept(cx) {
            std::task::Poll::Ready(result) => Some(result),
            std::task::Poll::Pending => None,
        })
    })
    .await;
    if initial.as_ref().is_some_and(Result::is_err) {
        state.request_stop();
    }
    if !state.stopping() {
        state.readiness.send_replace(Readiness::Ready);
    }
    if let Some(Ok((stream, _))) = initial {
        admit(stream, &service, &deps, &state, &mut sessions);
    }
    let mut timer = tokio::time::interval(CLOCK_POLL);
    while !state.stopping() {
        tokio::select! {
            _ = state.changed.notified() => {},
            _ = timer.tick() => {},
            result = listener.accept() => match result {
                Ok((stream, _)) => admit(stream, &service, &deps, &state, &mut sessions),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {},
                Err(_) => break,
            },
            _ = sessions.join_next(), if !sessions.is_empty() => {},
        }
    }
    state.request_stop();
    drop(listener);
    while sessions.join_next().await.is_some() {}
    state.readiness.send_replace(Readiness::Stopped);
    state.changed.notify_waiters();
}

fn admit(
    stream: UnixStream,
    service: &ServiceIdentity,
    deps: &Arc<ServerDeps>,
    state: &Arc<State>,
    sessions: &mut JoinSet<()>,
) {
    if state.stopping() {
        return;
    }
    let Some(permit) = SessionPermit::acquire(state) else {
        return;
    };
    if peer_uid(&stream).ok() != Some(service.account.uid) {
        return;
    }
    let (service, deps, state) = (service.clone(), deps.clone(), state.clone());
    sessions.spawn(async move {
        let _permit = permit;
        let _ = session(stream, service, deps, state).await;
    });
}

fn peer_uid(stream: &UnixStream) -> io::Result<u32> {
    #[cfg(target_os = "macos")]
    {
        let (mut uid, mut gid) = (0, 0);
        if unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(uid)
    }
    #[cfg(target_os = "linux")]
    {
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        if unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                std::ptr::from_mut(&mut cred).cast(),
                &mut len,
            )
        } == -1
            || len as usize != std::mem::size_of::<libc::ucred>()
        {
            return Err(io::Error::last_os_error());
        }
        Ok(cred.uid)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = stream;
        Err(io::ErrorKind::Unsupported.into())
    }
}

fn failure(reason: ChannelReason) -> ChannelFailure {
    ChannelFailure::Unavailable(reason)
}
async fn guard(state: &State, deadline: Duration) {
    loop {
        if state.stopping() || state.runtime.now() >= deadline {
            return;
        }
        tokio::select! {
            _ = state.changed.notified() => {},
            _ = tokio::time::sleep(CLOCK_POLL) => {},
        }
    }
}
async fn read_some(
    stream: &UnixStream,
    bytes: &mut [u8],
    state: &State,
    deadline: Duration,
) -> Result<usize, ChannelFailure> {
    loop {
        if state.stopping() || state.runtime.now() >= deadline {
            return Err(failure(ChannelReason::Timeout));
        }
        match stream.try_read(bytes) {
            Ok(0) => return Err(failure(ChannelReason::ForwardLost)),
            Ok(n) => return Ok(n),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(failure(ChannelReason::ForwardLost)),
        }
        tokio::select! {
            biased;
            _ = guard(state, deadline) => return Err(failure(ChannelReason::Timeout)),
            result = stream.readable() => { result.map_err(|_| failure(ChannelReason::ForwardLost))?; },
        }
    }
}

async fn read_payload(
    stream: &UnixStream,
    codec: &dyn ChannelCodec,
    state: &State,
    cap: usize,
    idle: Duration,
) -> Result<Vec<u8>, ChannelFailure> {
    let mut prefix = [0; 4];
    let (mut offset, mut deadline) = (0, state.runtime.now() + idle);
    while offset < prefix.len() {
        let n = read_some(stream, &mut prefix[offset..], state, deadline).await?;
        if offset == 0 {
            deadline = deadline.min(state.runtime.now() + SETUP_GUARD);
        }
        offset += n;
    }
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > cap {
        return Err(failure(ChannelReason::InvalidFrame));
    }
    // Check the smaller hello cap before the generic decoder can allocate.
    let mut decoder = codec.decoder();
    let progress = decoder.feed(&prefix)?;
    if progress.consumed != 4 || progress.payload.is_some() {
        return Err(failure(ChannelReason::InvalidFrame));
    }
    let mut scratch = [0; READ_SCRATCH_BYTES];
    let mut remaining = length;
    while remaining != 0 {
        let limit = remaining.min(scratch.len());
        let n = read_some(stream, &mut scratch[..limit], state, deadline).await?;
        let progress = decoder.feed(&scratch[..n])?;
        if progress.consumed != n || decoder.retained_bytes() > cap + 4 {
            return Err(failure(ChannelReason::InvalidFrame));
        }
        remaining -= n;
        if let Some(payload) = progress.payload {
            if remaining != 0 || payload.len() != length {
                return Err(failure(ChannelReason::InvalidFrame));
            }
            return Ok(payload);
        }
    }
    Err(failure(ChannelReason::InvalidFrame))
}

fn quiet(stream: &UnixStream) -> Result<(), ChannelFailure> {
    match stream.try_read(&mut [0; 1]) {
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
        _ => return Err(failure(ChannelReason::ForwardLost)),
    }
    // try_read may only consult Tokio's cached readiness. Check the nonblocking
    // descriptor too, so queued early input cannot slip through admission.
    loop {
        let mut byte = [0u8; 1];
        if unsafe {
            libc::recv(
                stream.as_raw_fd(),
                byte.as_mut_ptr().cast(),
                1,
                libc::MSG_PEEK,
            )
        } >= 0
        {
            return Err(failure(ChannelReason::ForwardLost));
        }
        match io::Error::last_os_error().kind() {
            io::ErrorKind::WouldBlock => return Ok(()),
            io::ErrorKind::Interrupted => {}
            _ => return Err(failure(ChannelReason::ForwardLost)),
        }
    }
}
async fn write_reply(
    stream: &UnixStream,
    bytes: &[u8],
    state: &State,
    deadline: Duration,
) -> Result<(), ChannelFailure> {
    quiet(stream)?;
    #[cfg(test)]
    let mut before = state.before_final_write.lock().unwrap().take();
    let mut offset = 0;
    while offset < bytes.len() {
        if state.stopping() || state.runtime.now() >= deadline {
            return Err(failure(ChannelReason::Timeout));
        }
        #[cfg(test)]
        if offset == bytes.len() - 1
            && let Some(barrier) = before.take()
        {
            barrier.entered.notify_one();
            loop {
                tokio::select! {
                    biased;
                    _ = guard(state, deadline) => return Err(failure(ChannelReason::Timeout)),
                    _ = barrier.release.notified() => break,
                    readable = stream.readable() => {
                        readable.map_err(|_| failure(ChannelReason::ForwardLost))?;
                        quiet(stream)?;
                    },
                }
            }
        }
        let end = bytes.len();
        #[cfg(test)]
        let end = if before.is_some() { end - 1 } else { end };
        match stream.try_write(&bytes[offset..end]) {
            Ok(0) => return Err(failure(ChannelReason::ForwardLost)),
            Ok(n) => {
                offset += n;
                // Final write completion hands off to request state before
                // inspecting any simultaneously readable next-request bytes.
                if offset == bytes.len() {
                    #[cfg(test)]
                    {
                        let after = state.after_final_write.lock().unwrap().take();
                        if let Some(barrier) = after {
                            barrier.entered.notify_one();
                            barrier.release.notified().await;
                        }
                    }
                    return Ok(());
                }
                continue;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(failure(ChannelReason::ForwardLost)),
        }
        tokio::select! {
            biased;
            _ = guard(state, deadline) => return Err(failure(ChannelReason::Timeout)),
            result = stream.writable() => { result.map_err(|_| failure(ChannelReason::ForwardLost))?; },
            result = stream.readable() => {
                result.map_err(|_| failure(ChannelReason::ForwardLost))?;
                quiet(stream)?;
            },
        }
    }
    Ok(())
}

fn start_child(
    frame: Vec<u8>,
    ctx: ServerContext,
    deps: &Arc<ServerDeps>,
    state: &Arc<State>,
) -> Result<oneshot::Receiver<ProcessCompletion>, ChannelFailure> {
    let permit = state
        .slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| failure(ChannelReason::Busy))?;
    let job = Arc::new(Job {
        cancelled: ctx.cancelled.clone(),
        unknown: AtomicBool::new(false),
    });
    state.jobs.lock().unwrap().active.push(job.clone());
    let (executor, shared, child_job) = (deps.executor.clone(), state.clone(), job.clone());
    let (sender, receiver) = oneshot::channel();
    if thread::Builder::new()
        .name("controller-channel-child".into())
        .spawn(move || {
            let completion = catch_unwind(AssertUnwindSafe(|| executor.run(&frame, &ctx)))
                .unwrap_or_else(|_| ProcessCompletion {
                    outcome: Err(WorkerError::Unavailable(
                        "CONTROLLER_CHANNEL: child supervisor failed".into(),
                    )),
                    cleanup: CleanupState::Unknown,
                });
            let retire = {
                let mut jobs = shared.jobs.lock().unwrap();
                match completion.cleanup {
                    CleanupState::Completed => {
                        jobs.active
                            .retain(|active| !Arc::ptr_eq(active, &child_job));
                        jobs.completed += 1;
                        drop(permit);
                    }
                    CleanupState::Unknown => {
                        permit.forget();
                        child_job.unknown.store(true, Ordering::Release);
                        jobs.unknown += 1;
                    }
                }
                jobs.unknown == MAX_SUPERVISORS
            };
            if retire {
                shared.request_stop();
            }
            shared.changed.notify_waiters();
            let _ = sender.send(completion);
        })
        .is_err()
    {
        let mut jobs = state.jobs.lock().unwrap();
        jobs.active.retain(|active| !Arc::ptr_eq(active, &job));
        jobs.completed += 1;
        drop(jobs);
        state.changed.notify_waiters();
        return Err(failure(ChannelReason::ServiceUnavailable));
    }
    Ok(receiver)
}
async fn child_result(
    stream: &UnixStream,
    mut result: oneshot::Receiver<ProcessCompletion>,
    state: &State,
    deadline: Duration,
) -> Result<ProcessCompletion, ChannelFailure> {
    loop {
        tokio::select! {
            biased;
            _ = guard(state, deadline) => return Err(failure(ChannelReason::Timeout)),
            readable = stream.readable() => {
                readable.map_err(|_| failure(ChannelReason::ForwardLost))?;
                quiet(stream)?;
            },
            completion = &mut result => return completion.map_err(|_| failure(ChannelReason::ServiceUnavailable)),
        }
    }
}

async fn session(
    stream: UnixStream,
    service: ServiceIdentity,
    deps: Arc<ServerDeps>,
    state: Arc<State>,
) -> Result<(), ChannelFailure> {
    let hello = read_payload(
        &stream,
        deps.codec.as_ref(),
        &state,
        IDENTITY_BYTES,
        SETUP_GUARD,
    )
    .await?;
    let expected = deps.codec.decode_hello(&hello)?;
    let identity = SocketIdentity {
        route_sha256: expected.route_sha256.clone(),
        service,
    };
    verify_expected_service(&expected, &identity)?;
    let ready = deps.codec.encode_ready(&identity)?;
    if decode_frame(&ready)
        .map_err(|_| failure(ChannelReason::InvalidFrame))?
        .len()
        > IDENTITY_BYTES
    {
        return Err(failure(ChannelReason::InvalidFrame));
    }
    write_reply(&stream, &ready, &state, state.runtime.now() + SETUP_GUARD).await?;
    loop {
        let payload = read_payload(
            &stream,
            deps.codec.as_ref(),
            &state,
            MAX_FRAME_BYTES,
            IDLE_GUARD,
        )
        .await?;
        let request = parse_request(&payload).map_err(|_| failure(ChannelReason::InvalidFrame))?;
        if !server_eligible_read(&request) {
            return Err(failure(ChannelReason::Unsupported));
        }
        quiet(&stream)?;
        let frame = encode_frame(&payload).map_err(|_| failure(ChannelReason::InvalidFrame))?;
        drop(payload);
        let deadline = state.runtime.now() + REQUEST_GUARD;
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel_on_exit = CancelOnDrop(cancelled.clone());
        let result = start_child(
            frame,
            ServerContext {
                runtime: state.runtime.clone(),
                deadline,
                cancelled,
            },
            &deps,
            &state,
        )?;
        let completion = child_result(&stream, result, &state, deadline).await?;
        let output = completion
            .outcome
            .map_err(|_| failure(ChannelReason::ForwardLost))?;
        if output
            .status
            .code()
            .is_none_or(|code| !(0..=255).contains(&code))
            || output.stdout.len() > MAX_FRAME_BYTES + 4
            || output.stderr.len() > 256 * 1024
            || decode_frame(&output.stdout).is_err()
        {
            return Err(failure(ChannelReason::InvalidFrame));
        }
        let reply = deps.codec.encode_reply(&request, &output)?;
        decode_frame(&reply).map_err(|_| failure(ChannelReason::InvalidFrame))?;
        write_reply(&stream, &reply, &state, deadline).await?;
    }
}
#[cfg(test)]
#[path = "server/tests.rs"]
mod tests;
