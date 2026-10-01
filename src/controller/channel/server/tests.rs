use super::*;
use crate::{
    controller::channel::contracts::{
        ChannelFailure, ChannelReason, DecodeProgress, FrameDecoder, SocketIdentity,
    },
    controller::{MAX_FRAME_BYTES, decode_frame, decode_request, encode_frame, encode_json_frame},
    process::{CleanupState, ProcessCompletion, ProcessResult},
};
use std::{
    collections::VecDeque,
    io,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{Mutex, atomic::AtomicU64, mpsc},
    time::Duration,
};
use tokio::{net::UnixStream, sync::mpsc as async_mpsc};

#[derive(Default)]
struct Clock {
    millis: AtomicU64,
}
impl Clock {
    fn advance(&self, millis: u64) {
        self.millis.fetch_add(millis, Ordering::AcqRel);
    }
}
impl ChannelRuntime for Clock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.millis.load(Ordering::Acquire))
    }
    fn cancelled(&self) -> bool {
        false
    }
}

// This sibling double exercises the listener, not T2's wire grammar.
struct Codec;
#[derive(Default)]
struct Decoder {
    bytes: Vec<u8>,
}
fn invalid() -> ChannelFailure {
    ChannelFailure::Unavailable(ChannelReason::InvalidFrame)
}
impl FrameDecoder for Decoder {
    fn feed(&mut self, input: &[u8]) -> Result<DecodeProgress, ChannelFailure> {
        let needed = if self.bytes.len() < 4 {
            4 - self.bytes.len()
        } else {
            let length = u32::from_be_bytes(self.bytes[..4].try_into().unwrap()) as usize;
            if length == 0 || length > MAX_FRAME_BYTES {
                return Err(invalid());
            }
            length + 4 - self.bytes.len()
        };
        let consumed = needed.min(input.len());
        self.bytes.extend_from_slice(&input[..consumed]);
        let payload = if self.bytes.len() >= 4 {
            let length = u32::from_be_bytes(self.bytes[..4].try_into().unwrap()) as usize;
            if length == 0 || length > MAX_FRAME_BYTES {
                return Err(invalid());
            }
            (self.bytes.len() == length + 4).then(|| self.bytes[4..].to_vec())
        } else {
            None
        };
        Ok(DecodeProgress { consumed, payload })
    }
    fn retained_bytes(&self) -> usize {
        self.bytes.len()
    }
}
impl ChannelCodec for Codec {
    fn decoder(&self) -> Box<dyn FrameDecoder> {
        Box::new(Decoder::default())
    }
    fn encode_hello(&self, identity: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        encode_json_frame(&serde_json::json!({"identity": identity})).map_err(|_| invalid())
    }
    fn decode_hello(&self, payload: &[u8]) -> Result<SocketIdentity, ChannelFailure> {
        let value: serde_json::Value = serde_json::from_slice(payload).map_err(|_| invalid())?;
        serde_json::from_value(value["identity"].clone()).map_err(|_| invalid())
    }
    fn encode_ready(&self, _: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        encode_frame(b"ready").map_err(|_| invalid())
    }
    fn decode_ready(&self, payload: &[u8], _: &SocketIdentity) -> Result<(), ChannelFailure> {
        if payload == b"ready" {
            Ok(())
        } else {
            Err(invalid())
        }
    }
    fn encode_reply(
        &self,
        request: &crate::controller::ControllerRequest,
        result: &ProcessResult,
    ) -> Result<Vec<u8>, ChannelFailure> {
        let payload = decode_frame(&result.stdout).map_err(|_| invalid())?;
        let reply: crate::controller::ControllerReadReply<serde_json::Value> =
            serde_json::from_slice(payload).map_err(|_| invalid())?;
        reply.verify_envelope(request).map_err(|_| invalid())?;
        encode_frame(payload).map_err(|_| invalid())
    }
    fn decode_reply(
        &self,
        _: &[u8],
        _: &crate::controller::ControllerRequest,
    ) -> Result<ProcessResult, ChannelFailure> {
        Err(invalid())
    }
}

enum Behavior {
    Reply,
    Gate(mpsc::Receiver<()>),
    Unknown,
    Panic,
    Raw(Vec<u8>),
}
struct Executor {
    scripts: Mutex<VecDeque<Behavior>>,
    entered: async_mpsc::UnboundedSender<Arc<AtomicBool>>,
    calls: AtomicUsize,
}
impl ChannelExecutor for Executor {
    fn run(&self, frame: &[u8], ctx: &ServerContext) -> ProcessCompletion {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let _ = self.entered.send(ctx.cancelled.clone());
        let behavior = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Behavior::Reply);
        let raw = match behavior {
            Behavior::Gate(gate) => {
                gate.recv_timeout(Duration::from_secs(30)).unwrap();
                None
            }
            Behavior::Unknown => {
                return ProcessCompletion {
                    outcome: Err(WorkerError::Unavailable("fixture unknown cleanup".into())),
                    cleanup: CleanupState::Unknown,
                };
            }
            Behavior::Panic => panic!("fixture supervisor panic"),
            Behavior::Raw(bytes) => Some(bytes),
            Behavior::Reply => None,
        };
        let request = decode_request(frame).unwrap();
        let stdout = raw.unwrap_or_else(|| encode_json_frame(&serde_json::json!({
            "protocol_version": 7, "command": request.command(), "request_id": request.request_id(), "payload_sha256": request.payload_sha256(), "result": {"ok": true},
        })).unwrap());
        ProcessCompletion {
            outcome: Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout,
                stderr: Vec::new(),
            }),
            cleanup: CleanupState::Completed,
        }
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    identity: SocketIdentity,
    clock: Arc<Clock>,
    executor: Arc<Executor>,
    entry: async_mpsc::UnboundedReceiver<Arc<AtomicBool>>,
    service: SocketService,
}
impl Fixture {
    async fn new(scripts: Vec<Behavior>) -> Self {
        Self::with_uid(scripts, unsafe { libc::geteuid() }).await
    }
    async fn with_uid(scripts: Vec<Behavior>, uid: u32) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("s");
        let identity: SocketIdentity = serde_json::from_value(serde_json::json!({
            "route_sha256": "a".repeat(64), "service": {
                "protocol_version": 7, "channel_version": 1,
                "controller_client_id": "0123456789abcdef0123456789abcdef",
                "account": {"uid": uid, "username": "worker", "home": "/Users/worker"},
                "leader": {"pid": 42, "start_time_micros": 1},
                "service_generation": "a1234567-89ab-4cde-8fab-0123456789ab",
                "socket_path": socket, "features": ["controller.socket"], "journal_id": null,
            },
        }))
        .unwrap();
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        let clock = Arc::new(Clock::default());
        let (entered, entry) = async_mpsc::unbounded_channel();
        let executor = Arc::new(Executor {
            scripts: Mutex::new(scripts.into()),
            entered,
            calls: AtomicUsize::new(0),
        });
        let service = SocketService::start(
            listener,
            identity.service.clone(),
            ServerDeps {
                codec: Arc::new(Codec),
                executor: executor.clone(),
                runtime: clock.clone(),
            },
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        assert!(
            !service.ready(),
            "start returning cannot advertise readiness"
        );
        service.wait_ready().await.unwrap();
        Self {
            _directory: directory,
            identity,
            clock,
            executor,
            entry,
            service,
        }
    }
    async fn raw(&self) -> UnixStream {
        UnixStream::connect(&self.identity.service.socket_path)
            .await
            .unwrap()
    }
    async fn connect(&self) -> UnixStream {
        let stream = self.raw().await;
        send(&stream, &Codec.encode_hello(&self.identity).unwrap()).await;
        assert_eq!(receive(&stream).await.unwrap(), b"ready");
        stream
    }
    fn context(&self) -> ServerContext {
        ServerContext {
            runtime: self.clock.clone(),
            deadline: self.clock.now() + Duration::from_secs(5),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
    async fn close(&self) -> ShutdownEvidence {
        self.service.shutdown(&self.context()).await
    }
}

async fn send(stream: &UnixStream, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        stream.writable().await.unwrap();
        match stream.try_write(bytes) {
            Ok(0) => panic!("fixture write closed"),
            Ok(n) => bytes = &bytes[n..],
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("fixture write: {error}"),
        }
    }
}
async fn exact(stream: &UnixStream, mut bytes: &mut [u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.readable().await?;
        match stream.try_read(bytes) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => bytes = &mut bytes[n..],
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
async fn receive(stream: &UnixStream) -> io::Result<Vec<u8>> {
    let mut prefix = [0; 4];
    exact(stream, &mut prefix).await?;
    let mut payload = vec![0; u32::from_be_bytes(prefix) as usize];
    exact(stream, &mut payload).await?;
    Ok(payload)
}
async fn closed(stream: &UnixStream) {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut scratch = [0; 8192];
        loop {
            stream.readable().await.unwrap();
            match stream.try_read(&mut scratch) {
                Ok(0) => return,
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) if error.kind() == io::ErrorKind::ConnectionReset => return,
                Err(error) => panic!("fixture read: {error}"),
            }
        }
    })
    .await
    .expect("hang guard: service closes the connection");
}
fn request(command: &str, body: serde_json::Value) -> Vec<u8> {
    encode_json_frame(&serde_json::json!({"protocol_version": 7, "request_id": "0123456789abcdef0123456789abcdef", "command": command, "body": body})).unwrap()
}
fn poll() -> Vec<u8> {
    request("task.wait.poll", serde_json::json!({"run": "fixture"}))
}

#[tokio::test(flavor = "current_thread")]
async fn readiness_and_verified_hello_precede_application_admission() {
    let fixture = Fixture::new(vec![]).await;
    assert!(fixture.service.ready());
    let raw = fixture.raw().await;
    send(&raw, &poll()).await;
    closed(&raw).await;
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 0);
    let stream = fixture.connect().await;
    send(&stream, &poll()).await;
    assert!(!receive(&stream).await.unwrap().is_empty());
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 1);
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn peer_uid_and_required_service_identity_are_checked_without_filesystem_probes() {
    let wrong = Fixture::with_uid(vec![], unsafe { libc::geteuid() } + 1).await;
    let stream = wrong.raw().await;
    closed(&stream).await;
    assert_eq!(wrong.executor.calls.load(Ordering::Acquire), 0);
    wrong.close().await;
    let fixture = Fixture::new(vec![]).await;
    let stream = fixture.raw().await;
    let mut changed = fixture.identity.clone();
    changed.service.features.push("unexpected".into());
    send(&stream, &Codec.encode_hello(&changed).unwrap()).await;
    closed(&stream).await;
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 0);
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn journal_hints_do_not_gate_ordinary_wait_traffic() {
    let fixture = Fixture::new(vec![]).await;
    let stream = fixture.raw().await;
    let mut hinted = fixture.identity.clone();
    hinted.service.journal_id =
        serde_json::from_value(serde_json::json!("b1234567-89ab-4cde-8fab-0123456789ab")).unwrap();
    send(&stream, &Codec.encode_hello(&hinted).unwrap()).await;
    assert_eq!(receive(&stream).await.unwrap(), b"ready");
    send(&stream, &poll()).await;
    assert!(!receive(&stream).await.unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn incomplete_handshakes_consume_all_sixteen_session_slots() {
    let fixture = Fixture::new(vec![]).await;
    let mut held = Vec::new();
    for _ in 0..16 {
        held.push(fixture.raw().await);
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while fixture.service.state.sessions.load(Ordering::Acquire) != 16 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let denied = fixture.raw().await;
    closed(&denied).await;
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 0);
    drop(held);
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn handshake_partial_and_idle_guards_use_the_injected_clock() {
    let fixture = Fixture::new(vec![]).await;
    let incomplete = fixture.raw().await;
    send(&incomplete, &[0, 0]).await;
    // Wait for admission before advancing the clock, not for elapsed speed.
    while fixture.service.state.sessions.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    fixture.clock.advance(5_001);
    closed(&incomplete).await;
    let idle = fixture.connect().await;
    fixture.clock.advance(60_001);
    closed(&idle).await;
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn forbidden_commands_and_invalid_bodies_close_without_spawning() {
    let fixture = Fixture::new(vec![]).await;
    for (command, body) in [
        ("task.submit", serde_json::json!({})),
        ("controller.drain", serde_json::json!({"enabled": true})),
        ("task.reconcile", serde_json::json!({"run": "fixture"})),
        ("task.list", serde_json::json!({})),
        (
            "task.list",
            serde_json::json!({"controller_events": {"op": "repair"}, "controller_health": true}),
        ),
        (
            "task.wait.poll",
            serde_json::json!({"run": "fixture", "task_id": "0123456789abcdef0123456789abcdef"}),
        ),
        (
            "task.logs",
            serde_json::json!({"task_id": "0123456789abcdef0123456789abcdef", "wait_ms": "bad"}),
        ),
    ] {
        let stream = fixture.connect().await;
        send(&stream, &request(command, body)).await;
        closed(&stream).await;
    }
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 0);
    let stream = fixture.connect().await;
    send(&stream, &poll()).await;
    receive(&stream).await.unwrap();
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 1);
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn eight_supervisors_are_try_only_and_long_polls_do_not_block_other_sessions() {
    let mut gates = Vec::new();
    let mut scripts = Vec::new();
    for _ in 0..8 {
        let (release, gate) = mpsc::channel();
        gates.push(release);
        scripts.push(Behavior::Gate(gate));
    }
    let mut fixture = Fixture::new(scripts).await;
    let mut held = Vec::new();
    for _ in 0..8 {
        let stream = fixture.connect().await;
        send(&stream, &poll()).await;
        fixture.entry.recv().await.unwrap();
        held.push(stream);
    }
    let denied = fixture.connect().await;
    send(&denied, &poll()).await;
    closed(&denied).await;
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 8);
    gates.remove(0).send(()).unwrap();
    receive(&held.remove(0)).await.unwrap();
    let available = fixture.connect().await;
    send(&available, &poll()).await;
    receive(&available).await.unwrap();
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 9);
    for (gate, stream) in gates.into_iter().zip(held) {
        gate.send(()).unwrap();
        receive(&stream).await.unwrap();
    }
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_empty_and_oversized_child_output_never_reaches_the_socket() {
    let fixture = Fixture::new(vec![
        Behavior::Raw(vec![]),
        Behavior::Raw(encode_frame(b"not JSON").unwrap()),
        Behavior::Raw(vec![0, 0, 0, 0]),
        Behavior::Raw(vec![1; MAX_FRAME_BYTES + 5]),
    ])
    .await;
    for _ in 0..4 {
        let stream = fixture.connect().await;
        send(&stream, &poll()).await;
        let mut prefix = [0; 4];
        assert!(exact(&stream, &mut prefix).await.is_err());
    }
    let stream = fixture.connect().await;
    send(&stream, &poll()).await;
    receive(&stream).await.unwrap();
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 5);
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn completed_requests_keep_capacity_after_more_than_eight_exchanges() {
    let fixture = Fixture::new(vec![]).await;
    let stream = fixture.connect().await;
    for _ in 0..12 {
        send(&stream, &poll()).await;
        receive(&stream).await.unwrap();
    }
    assert!(fixture.service.ready());
    let evidence = fixture.close().await;
    assert_eq!(evidence.completed, 12);
    assert_eq!(evidence.unknown, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn eight_unknown_cleanups_retire_only_the_optional_channel() {
    let fixture = Fixture::new((0..8).map(|_| Behavior::Unknown).collect()).await;
    for _ in 0..8 {
        let stream = fixture.connect().await;
        send(&stream, &poll()).await;
        closed(&stream).await;
    }
    assert!(!fixture.service.ready());
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 8);
    let evidence = fixture.close().await;
    assert_eq!(evidence.unknown, 8);
    assert_eq!(evidence.completed, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn supervisor_panic_retains_its_permit_and_is_contained() {
    let fixture = Fixture::new(vec![Behavior::Panic]).await;
    let stream = fixture.connect().await;
    send(&stream, &poll()).await;
    closed(&stream).await;
    let healthy = fixture.connect().await;
    send(&healthy, &poll()).await;
    receive(&healthy).await.unwrap();
    assert!(fixture.service.ready());
    let evidence = fixture.close().await;
    assert_eq!(evidence.unknown, 1);
    assert_eq!(evidence.completed, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn early_next_request_cancels_the_active_child_without_read_ahead() {
    let (release, gate) = mpsc::channel();
    let mut fixture = Fixture::new(vec![Behavior::Gate(gate)]).await;
    let stream = fixture.connect().await;
    send(&stream, &poll()).await;
    let cancellation = fixture.entry.recv().await.unwrap();
    send(&stream, &poll()).await;
    closed(&stream).await;
    assert!(cancellation.load(Ordering::Acquire));
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 1);
    release.send(()).unwrap();
    let evidence = fixture.close().await;
    assert_eq!(evidence.unknown, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_closes_streams_and_drives_cancellation_before_finishing() {
    let (release, gate) = mpsc::channel();
    let mut fixture = Fixture::new(vec![Behavior::Gate(gate)]).await;
    let stream = fixture.connect().await;
    send(&stream, &poll()).await;
    let cancellation = fixture.entry.recv().await.unwrap();
    let context = fixture.context();
    let shutdown = fixture.service.shutdown(&context);
    let inspect = async {
        closed(&stream).await;
        assert!(cancellation.load(Ordering::Acquire));
        release.send(()).unwrap();
    };
    let (evidence, ()) = tokio::join!(shutdown, inspect);
    assert_eq!(evidence.unknown, 0);
    assert_eq!(evidence.completed, 1);
    assert!(!fixture.service.ready());
}

#[tokio::test(flavor = "current_thread")]
async fn gated_cleanup_has_bounded_unknown_shutdown_without_replenishing_capacity() {
    let (release, gate) = mpsc::channel();
    let mut fixture = Fixture::new(vec![Behavior::Gate(gate)]).await;
    let stream = fixture.connect().await;
    send(&stream, &poll()).await;
    let cancellation = fixture.entry.recv().await.unwrap();
    let mut ctx = fixture.context();
    ctx.deadline = fixture.clock.now();
    let evidence = fixture.service.shutdown(&ctx).await;
    assert_eq!(evidence.unknown, 1);
    assert!(cancellation.load(Ordering::Acquire));
    closed(&stream).await;
    release.send(()).unwrap();
}
