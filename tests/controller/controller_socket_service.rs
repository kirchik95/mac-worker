use mac_worker::test_support::channel::{
    contracts::*,
    server::{ChildRpcExecutor, NativeControl, ServerDeps, ShutdownEvidence, SocketService},
    testing::{
        ManualRuntime, RecordingExecutor, RecordingTrackedRunner, ScriptedImageSource, StubCodec,
        identity_fixture, request_fixture, result_fixture,
    },
};
use mac_worker::test_support::core::error::{ProcessError, WorkerError};
use mac_worker::test_support::{
    channel::contracts::{ChannelFailure, ChannelReason, FrameDecoder, SocketIdentity},
    controller::{MAX_FRAME_BYTES, decode_frame, decode_request, encode_frame, encode_json_frame},
    host::process::{CleanupState, ProcessCompletion, ProcessResult, TrackedProcessRunner},
};
use std::os::unix::net::UnixListener;
use std::{
    collections::VecDeque,
    io,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};
use tokio::{
    net::UnixStream,
    sync::{mpsc as async_mpsc, oneshot},
};

// A local observation wrapper around T1's codec; no wire-grammar claim.
#[derive(Default)]
struct Codec {
    decoders: AtomicUsize,
}
fn invalid() -> ChannelFailure {
    ChannelFailure::Unavailable(ChannelReason::InvalidFrame)
}
impl ChannelCodec for Codec {
    fn decoder(&self) -> Box<dyn FrameDecoder> {
        self.decoders.fetch_add(1, Ordering::AcqRel);
        StubCodec.decoder()
    }
    fn encode_hello(&self, identity: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        StubCodec.encode_hello(identity)
    }
    fn decode_hello(&self, payload: &[u8]) -> Result<SocketIdentity, ChannelFailure> {
        StubCodec.decode_hello(payload)
    }
    fn encode_ready(&self, identity: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        StubCodec.encode_ready(identity)
    }
    fn decode_ready(
        &self,
        payload: &[u8],
        identity: &SocketIdentity,
    ) -> Result<(), ChannelFailure> {
        StubCodec.decode_ready(payload, identity)
    }
    fn encode_reply(
        &self,
        request: &mac_worker::test_support::controller::ControllerRequest,
        result: &ProcessResult,
    ) -> Result<Vec<u8>, ChannelFailure> {
        let payload = decode_frame(&result.stdout).map_err(|_| invalid())?;
        let reply: mac_worker::test_support::controller::ControllerReadReply<serde_json::Value> =
            serde_json::from_slice(payload).map_err(|_| invalid())?;
        reply.verify_envelope(request).map_err(|_| invalid())?;
        StubCodec.encode_reply(request, result)
    }
    fn decode_reply(
        &self,
        payload: &[u8],
        request: &mac_worker::test_support::controller::ControllerRequest,
    ) -> Result<ProcessResult, ChannelFailure> {
        StubCodec.decode_reply(payload, request)
    }
}

enum Behavior {
    Reply,
    Gate(mpsc::Receiver<()>),
    Unknown,
    Panic,
    Raw(Vec<u8>),
    Result(ProcessResult),
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
            Behavior::Result(result) => {
                return ProcessCompletion {
                    outcome: Ok(result),
                    cleanup: CleanupState::Completed,
                };
            }
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
    clock: Arc<ManualRuntime>,
    codec: Arc<Codec>,
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
        let mut identity = identity_fixture();
        identity.service.account.uid = uid;
        identity.service.socket_path = socket.clone();
        identity.service.journal_id = None;
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        let clock = Arc::new(ManualRuntime::default());
        let codec = Arc::new(Codec::default());
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
                codec: codec.clone(),
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
            codec,
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
        send(&stream, &self.codec.encode_hello(&self.identity).unwrap()).await;
        self.codec
            .decode_ready(&receive(&stream).await.unwrap(), &self.identity)
            .unwrap();
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
    send(&stream, &fixture.codec.encode_hello(&changed).unwrap()).await;
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
    send(&stream, &fixture.codec.encode_hello(&hinted).unwrap()).await;
    fixture
        .codec
        .decode_ready(&receive(&stream).await.unwrap(), &fixture.identity)
        .unwrap();
    send(&stream, &poll()).await;
    assert!(!receive(&stream).await.unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn incomplete_handshakes_consume_all_sixteen_session_slots() {
    let fixture = Fixture::new(vec![]).await;
    let mut held = Vec::new();
    for _ in 0..16 {
        let stream = fixture.raw().await;
        send(&stream, &[0, 0, 0, 1]).await;
        held.push(stream);
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while fixture.codec.decoders.load(Ordering::Acquire) != 16 {
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
    send(&incomplete, &[0, 0, 0, 8]).await;
    // Wait for admission before advancing the clock, not for elapsed speed.
    while fixture.codec.decoders.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    fixture.clock.advance(Duration::from_millis(5_001));
    closed(&incomplete).await;
    let idle = fixture.connect().await;
    fixture.clock.advance(Duration::from_millis(60_001));
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
async fn frozen_nullable_wait_selector_is_accepted_without_a_private_grammar() {
    let fixture = Fixture::new(vec![]).await;
    let stream = fixture.connect().await;
    send(
        &stream,
        &request(
            "task.wait.poll",
            serde_json::json!({"run": "fixture", "task_id": null}),
        ),
    )
    .await;
    receive(&stream).await.unwrap();
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 1);
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_frame_lengths_close_before_decoder_allocation_or_child_admission() {
    let fixture = Fixture::new(vec![]).await;
    for length in [0, IDENTITY_BYTES as u32 + 1, u32::MAX] {
        let stream = fixture.raw().await;
        send(&stream, &length.to_be_bytes()).await;
        closed(&stream).await;
        assert_eq!(fixture.codec.decoders.load(Ordering::Acquire), 0);
    }
    for length in [0, MAX_FRAME_BYTES as u32 + 1, u32::MAX] {
        let stream = fixture.connect().await;
        let before = fixture.codec.decoders.load(Ordering::Acquire);
        send(&stream, &length.to_be_bytes()).await;
        closed(&stream).await;
        assert_eq!(fixture.codec.decoders.load(Ordering::Acquire), before);
    }
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 0);
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
    let request = decode_request(&poll()).unwrap();
    let mut signalled = result_fixture(&request, serde_json::json!({"ok": true}), 0);
    signalled.status = ExitStatus::from_raw(libc::SIGTERM);
    let mut huge_stderr = result_fixture(&request, serde_json::json!({"ok": true}), 0);
    huge_stderr.stderr = vec![0; 256 * 1024 + 1];
    let empty = result_fixture(&request, serde_json::json!(""), 0);
    let remaining = MAX_FRAME_BYTES - decode_frame(&empty.stdout).unwrap().len();
    let huge_wrapper = result_fixture(&request, serde_json::json!("x".repeat(remaining)), 0);
    assert_eq!(
        decode_frame(&huge_wrapper.stdout).unwrap().len(),
        MAX_FRAME_BYTES
    );
    assert!(
        StubCodec.encode_reply(&request, &huge_wrapper).is_err(),
        "a maximum-size inner reply cannot fit the wrapper"
    );
    let fixture = Fixture::new(vec![
        Behavior::Raw(vec![]),
        Behavior::Raw(encode_frame(b"not JSON").unwrap()),
        Behavior::Raw(vec![0, 0, 0, 0]),
        Behavior::Raw(vec![1; MAX_FRAME_BYTES + 5]),
        Behavior::Result(signalled),
        Behavior::Result(huge_stderr),
        Behavior::Result(huge_wrapper),
    ])
    .await;
    for _ in 0..7 {
        let stream = fixture.connect().await;
        send(&stream, &poll()).await;
        let mut prefix = [0; 4];
        assert!(exact(&stream, &mut prefix).await.is_err());
    }
    let stream = fixture.connect().await;
    send(&stream, &poll()).await;
    receive(&stream).await.unwrap();
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 8);
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn completed_requests_keep_capacity_after_more_than_eight_exchanges() {
    let request = decode_request(&poll()).unwrap();
    let fixture = Fixture::new(
        (0..12)
            .map(|_| {
                Behavior::Result(result_fixture(
                    &request,
                    serde_json::json!({"ok": true}),
                    75,
                ))
            })
            .collect(),
    )
    .await;
    let stream = fixture.connect().await;
    for _ in 0..12 {
        send(&stream, &poll()).await;
        let result = fixture
            .codec
            .decode_reply(&receive(&stream).await.unwrap(), &request)
            .unwrap();
        assert_eq!(result.status.code(), Some(75));
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

struct RealProcessExecutor {
    script: String,
    completed: async_mpsc::UnboundedSender<(bool, CleanupState)>,
    calls: AtomicUsize,
}
impl ChannelExecutor for RealProcessExecutor {
    fn run(&self, frame: &[u8], ctx: &ServerContext) -> ProcessCompletion {
        self.calls.fetch_add(1, Ordering::AcqRel);
        use mac_worker::test_support::host::process::{
            ProcessPolicy, ProcessRequest, SystemProcessRunner, TrackedProcessRunner,
        };
        let request = ProcessRequest {
            program: "python3".into(),
            args: vec!["-c".into(), self.script.clone().into()],
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: Some(frame.to_vec()),
            policy: ProcessPolicy {
                stdout_limit: MAX_FRAME_BYTES + 4,
                stderr_limit: 256 * 1024,
                deadline: Duration::from_secs(30),
            },
            isolate_parent_environment: false,
        };
        let completion = SystemProcessRunner.run_interruptible_with_cleanup(&request, &|| {
            ctx.cancelled.load(Ordering::Acquire)
                || ctx.runtime.cancelled()
                || ctx.runtime.now() >= ctx.deadline
        });
        let cancelled = matches!(
            &completion.outcome,
            Err(WorkerError::Process(
                mac_worker::test_support::core::error::ProcessError::Cancelled
            ))
        );
        let _ = self.completed.send((cancelled, completion.cleanup));
        completion
    }
}

struct FixtureGroups {
    rpc: i32,
    detached: i32,
}
impl Drop for FixtureGroups {
    fn drop(&mut self) {
        unsafe {
            libc::killpg(self.rpc, libc::SIGKILL);
            libc::killpg(self.detached, libc::SIGKILL);
        }
    }
}

async fn prove_real_group_cancellation(mode: &str) {
    use std::os::unix::net::UnixDatagram;
    let mut fixture = Fixture::new(vec![]).await;
    fixture.close().await;
    let courier_path = fixture._directory.path().join("entry");
    let release_path = fixture._directory.path().join("release");
    let courier = UnixDatagram::bind(&courier_path).unwrap();
    courier
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let courier_json = serde_json::to_string(&courier_path).unwrap();
    let release_json = serde_json::to_string(&release_path).unwrap();
    let script = format!(
        r#"
import os, signal, socket, json
def report(role):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    s.sendto(json.dumps([role, os.getpid(), os.getpgrp()]).encode(), {courier_json})
    s.close()
owned = os.fork()
if owned == 0:
    report('owned')
    while True: signal.pause()
detached = os.fork()
if detached == 0:
    os.setsid()
    for fd in (0, 1, 2): os.close(fd)
    gate = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    gate.bind({release_json})
    report('detached')
    gate.recv(1)
    os._exit(0)
report('rpc')
while True: signal.pause()
"#
    );
    let control = NativeControl::new();
    let entered = control
        .try_run(Box::new(move || {
            let mut roles = std::collections::BTreeMap::new();
            for _ in 0..3 {
                let mut bytes = [0u8; 256];
                let n = courier.recv(&mut bytes).unwrap();
                let (role, pid, group): (String, i32, i32) =
                    serde_json::from_slice(&bytes[..n]).unwrap();
                roles.insert(role, (pid, group));
            }
            roles
        }))
        .unwrap();
    let (done, mut completed) = async_mpsc::unbounded_channel();
    let executor = Arc::new(RealProcessExecutor {
        script,
        completed: done,
        calls: AtomicUsize::new(0),
    });
    fixture.identity.service.socket_path = fixture._directory.path().join("real-s");
    let listener = UnixListener::bind(&fixture.identity.service.socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let service = SocketService::start(
        listener,
        fixture.identity.service.clone(),
        ServerDeps {
            codec: fixture.codec.clone(),
            executor,
            runtime: fixture.clock.clone(),
        },
        shutdown.clone(),
    )
    .unwrap();
    service.wait_ready().await.unwrap();
    let stream = UnixStream::connect(&fixture.identity.service.socket_path)
        .await
        .unwrap();
    send(
        &stream,
        &fixture.codec.encode_hello(&fixture.identity).unwrap(),
    )
    .await;
    fixture
        .codec
        .decode_ready(&receive(&stream).await.unwrap(), &fixture.identity)
        .unwrap();
    send(&stream, &poll()).await;
    let roles = entered.await.unwrap();
    let (rpc, group) = roles["rpc"];
    let (detached, detached_group) = roles["detached"];
    let _cleanup = FixtureGroups { rpc, detached };
    assert_eq!(rpc, group);
    assert_eq!(roles["owned"].1, rpc);
    assert_eq!(detached, detached_group);
    assert_ne!(detached_group, rpc);
    assert_eq!(unsafe { libc::killpg(rpc, 0) }, 0);
    match mode {
        "close" => drop(stream),
        "deadline" => {
            fixture.clock.advance(Duration::from_millis(30_001));
            closed(&stream).await;
        }
        "signal" => {
            shutdown.store(true, Ordering::Release);
            closed(&stream).await;
        }
        _ => {
            let context = fixture.context();
            let (evidence, ()) = tokio::join!(service.shutdown(&context), closed(&stream));
            assert_eq!(evidence.unknown, 0);
            assert_eq!(evidence.completed, 1);
        }
    }
    let (cancelled, cleanup) = tokio::time::timeout(Duration::from_secs(60), completed.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        cancelled,
        "stream/guard shutdown reaches the real runner cancellation path"
    );
    assert_eq!(
        cleanup,
        CleanupState::Completed,
        "tracked runner proves group/capture/stdin cleanup"
    );
    let gone = control
        .try_run(Box::new(move || {
            let hang_guard = std::time::Instant::now();
            loop {
                if unsafe { libc::killpg(rpc, 0) } == -1
                    && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    return true;
                }
                assert!(
                    hang_guard.elapsed() < Duration::from_secs(30),
                    "hang guard: owned group must be gone after cleanup"
                );
                std::thread::yield_now();
            }
        }))
        .unwrap();
    assert!(gone.await.unwrap());
    assert_eq!(
        unsafe { libc::killpg(detached, 0) },
        0,
        "detached task group survives RPC cancellation"
    );
    assert_eq!(unsafe { libc::getpgid(detached) }, detached);
    let release = UnixDatagram::unbound().unwrap();
    release.send_to(b"x", &release_path).unwrap();
    let evidence = service.shutdown(&fixture.context()).await;
    assert_eq!(evidence.unknown, 0);
    assert_eq!(evidence.completed, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn real_process_group_close_cancellation_preserves_the_detached_group() {
    prove_real_group_cancellation("close").await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_process_group_deadline_signal_and_shutdown_drive_actual_termination() {
    for mode in ["deadline", "signal", "shutdown"] {
        prove_real_group_cancellation(mode).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn twelve_real_cleaned_cancellations_keep_the_same_service_available_with_exact_stdin_eof() {
    use std::os::unix::net::UnixDatagram;
    let mut fixture = Fixture::new(vec![]).await;
    fixture.close().await;
    let courier_path = fixture._directory.path().join("entry");
    let courier = UnixDatagram::bind(&courier_path).unwrap();
    courier
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let courier_json = serde_json::to_string(&courier_path).unwrap();
    let input_json = serde_json::to_string(&poll()).unwrap();
    let script = format!(
        r#"
import os, signal, socket, json, sys
assert sys.stdin.buffer.read() == bytes({input_json}), 'exact framed stdin followed by EOF'
s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
s.sendto(json.dumps([os.getpid(), os.getpgrp()]).encode(), {courier_json})
s.close()
while True: signal.pause()
"#
    );
    let (done, mut completed) = async_mpsc::unbounded_channel();
    let executor = Arc::new(RealProcessExecutor {
        script,
        completed: done,
        calls: AtomicUsize::new(0),
    });
    fixture.identity.service.socket_path = fixture._directory.path().join("real-s");
    let listener = UnixListener::bind(&fixture.identity.service.socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let service = SocketService::start(
        listener,
        fixture.identity.service.clone(),
        ServerDeps {
            codec: fixture.codec.clone(),
            executor: executor.clone(),
            runtime: fixture.clock.clone(),
        },
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    service.wait_ready().await.unwrap();
    let control = NativeControl::new();
    for _ in 0..12 {
        let receiver = courier.try_clone().unwrap();
        let entered = control
            .try_run(Box::new(move || {
                let mut bytes = [0; 256];
                let n = receiver.recv(&mut bytes).unwrap();
                serde_json::from_slice::<(i32, i32)>(&bytes[..n]).unwrap()
            }))
            .unwrap();
        let stream = fixture.connect().await;
        send(&stream, &poll()).await;
        let (pid, pgid) = entered.await.unwrap();
        let _cleanup = FixtureGroups {
            rpc: pid,
            detached: pid,
        };
        assert_eq!(pid, pgid);
        drop(stream);
        let (cancelled, cleanup) = tokio::time::timeout(Duration::from_secs(60), completed.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(cancelled);
        assert_eq!(cleanup, CleanupState::Completed);
        assert_eq!(unsafe { libc::killpg(pgid, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
        assert!(
            service.ready(),
            "completed cancellation must not retire this generation"
        );
    }
    let evidence = service.shutdown(&fixture.context()).await;
    assert_eq!(
        evidence,
        ShutdownEvidence {
            completed: 12,
            unknown: 0
        }
    );
    assert_eq!(executor.calls.load(Ordering::Acquire), 12);
}

#[tokio::test(flavor = "current_thread")]
async fn eight_real_abandoned_groups_retire_the_service_without_replacements_after_fixture_cleanup()
{
    use std::os::unix::net::UnixDatagram;
    let mut fixture = Fixture::new(vec![]).await;
    fixture.close().await;
    let courier_path = fixture._directory.path().join("entry");
    let courier = UnixDatagram::bind(&courier_path).unwrap();
    courier
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let courier_json = serde_json::to_string(&courier_path).unwrap();
    let script = format!(
        r#"
import os, signal, socket, json, sys
sys.stdin.buffer.read()
owned = os.fork()
if owned == 0:
    for fd in (0, 1, 2): os.close(fd)
    while True: signal.pause()
s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
s.sendto(json.dumps([os.getpid(), os.getpgrp(), owned]).encode(), {courier_json})
s.close()
# Parent exits and joins all captures, but the owned group is deliberately live.
"#
    );
    let (done, mut completed) = async_mpsc::unbounded_channel();
    let executor = Arc::new(RealProcessExecutor {
        script,
        completed: done,
        calls: AtomicUsize::new(0),
    });
    fixture.identity.service.socket_path = fixture._directory.path().join("real-s");
    let listener = UnixListener::bind(&fixture.identity.service.socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let service = SocketService::start(
        listener,
        fixture.identity.service.clone(),
        ServerDeps {
            codec: fixture.codec.clone(),
            executor: executor.clone(),
            runtime: fixture.clock.clone(),
        },
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    service.wait_ready().await.unwrap();
    let control = NativeControl::new();
    let mut groups = Vec::new();
    for _ in 0..8 {
        let receiver = courier.try_clone().unwrap();
        let entered = control
            .try_run(Box::new(move || {
                let mut bytes = [0; 256];
                let n = receiver.recv(&mut bytes).unwrap();
                serde_json::from_slice::<(i32, i32, i32)>(&bytes[..n]).unwrap()
            }))
            .unwrap();
        let stream = fixture.connect().await;
        send(&stream, &poll()).await;
        let (pid, pgid, owned) = entered.await.unwrap();
        groups.push(FixtureGroups {
            rpc: pid,
            detached: pid,
        });
        assert_eq!(pid, pgid);
        assert_eq!(unsafe { libc::getpgid(owned) }, pgid);
        let (cancelled, cleanup) = tokio::time::timeout(Duration::from_secs(60), completed.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!cancelled);
        assert_eq!(
            cleanup,
            CleanupState::Unknown,
            "live owned descendant prevents positive exit proof"
        );
        closed(&stream).await;
        assert_eq!(unsafe { libc::killpg(pgid, 0) }, 0);
    }
    assert!(!service.ready());
    assert!(service.wait_ready().await.is_err());
    groups.clear(); // Isolated fixture cleanup cannot replenish a retained permit.
    assert_eq!(
        service.shutdown(&fixture.context()).await,
        ShutdownEvidence {
            completed: 0,
            unknown: 8
        }
    );
    assert_eq!(executor.calls.load(Ordering::Acquire), 8);
    assert_eq!(tokio::spawn(async { 19 }).await.unwrap(), 19);
}

mod child_cases {

    use super::*;
    use mac_worker::test_support::{
        channel::contracts::{
            ChannelRuntime, DETACHED_RUNNER_EXECUTABLE_ENV, EntryIdentity, PinnedExecutable,
        },
        controller::{MAX_FRAME_BYTES, encode_json_frame},
        core::error::ProcessError,
        host::process::{ProcessRequest, ProcessResult, ProcessRunner},
    };
    use std::{
        ffi::OsString,
        os::unix::process::ExitStatusExt,
        process::ExitStatus,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    struct Clock;
    impl ChannelRuntime for Clock {
        fn now(&self) -> Duration {
            Duration::from_secs(7)
        }
        fn cancelled(&self) -> bool {
            false
        }
    }

    #[derive(Default)]
    struct Runner {
        calls: Mutex<Vec<ProcessRequest>>,
        stop_during_entry: Option<Arc<AtomicBool>>,
    }
    impl ProcessRunner for Runner {
        fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            panic!("child must use the tracked interruptible seam")
        }
    }
    impl TrackedProcessRunner for Runner {
        fn run_interruptible_with_cleanup(
            &self,
            request: &ProcessRequest,
            stop: &dyn Fn() -> bool,
        ) -> ProcessCompletion {
            self.calls.lock().unwrap().push(request.clone());
            assert!(!stop());
            let outcome = if let Some(flag) = &self.stop_during_entry {
                flag.store(true, Ordering::Release);
                assert!(
                    stop(),
                    "owned cancellation predicate stays live after entry"
                );
                Err(ProcessError::Cancelled.into())
            } else {
                Ok(ProcessResult {
                    status: ExitStatus::from_raw(75 << 8),
                    stdout: vec![1, 2],
                    stderr: vec![3],
                })
            };
            ProcessCompletion {
                outcome,
                cleanup: CleanupState::Completed,
            }
        }
    }

    fn spec() -> ChildRpcSpec {
        ChildRpcSpec {
            executable: PinnedExecutable {
                path: "/private/controller/rpc/generation-worker".into(),
                binding: EntryIdentity {
                    device: 1,
                    inode: 9,
                    owner: unsafe { libc::geteuid() },
                    kind: libc::S_IFREG as u32,
                    mode: 0o755,
                },
            },
            detached_runner_executable: "/installed/bin/worker".into(),
            config: "/captured/config.toml".into(),
            environment: [
                ("HOME", "/captured/home"),
                ("XDG_CONFIG_HOME", "/captured/config"),
                ("XDG_STATE_HOME", "/captured/state"),
                ("XDG_DATA_HOME", "/captured/data"),
                ("XDG_CACHE_HOME", "/captured/cache"),
                ("PATH", "/usr/bin:/bin"),
                (DETACHED_RUNNER_EXECUTABLE_ENV, "/inherited/wrong-worker"),
            ]
            .map(|(key, value)| (key.into(), value.into()))
            .to_vec(),
        }
    }

    fn context(flag: Arc<AtomicBool>) -> ServerContext {
        ServerContext {
            runtime: Arc::new(Clock),
            deadline: Duration::from_secs(24),
            cancelled: flag,
        }
    }
    fn frame(body: serde_json::Value) -> Vec<u8> {
        encode_json_frame(&serde_json::json!({ "protocol_version": 7, "request_id": "0123456789abcdef0123456789abcdef", "command": "task.wait.poll", "body": body })).unwrap()
    }

    #[test]
    fn child_uses_fixed_rpc_argv_captured_roots_and_pinned_image_with_installed_runner_input() {
        let input = frame(serde_json::json!({"run": "fixture"}));
        let request = decode_request(&input).unwrap();
        let mut result = result_fixture(&request, serde_json::json!({"ok": true}), 75);
        result.stderr = vec![3];
        let runner = Arc::new(RecordingTrackedRunner::new(vec![ProcessCompletion {
            outcome: Ok(result),
            cleanup: CleanupState::Completed,
        }]));
        let executor = ChildRpcExecutor::new(runner.clone(), spec());
        let completion = executor.run(&input, &context(Arc::new(AtomicBool::new(false))));
        let output = completion.outcome.unwrap();
        assert_eq!(output.status.code(), Some(75));
        assert_eq!(output.stderr, [3]);
        assert_eq!(completion.cleanup, CleanupState::Completed);
        let calls = runner.calls();
        assert_eq!(calls.len(), 1);
        let request = &calls[0];
        assert_eq!(
            request.program,
            OsString::from("/private/controller/rpc/generation-worker")
        );
        assert_eq!(
            request.args,
            [
                "--config",
                "/captured/config.toml",
                "host",
                "controller-rpc"
            ]
            .map(OsString::from)
        );
        assert_eq!(request.stdin.as_deref(), Some(input.as_slice()));
        assert_eq!(request.policy.stdout_limit, MAX_FRAME_BYTES + 4);
        assert_eq!(request.policy.stderr_limit, 256 * 1024);
        assert_eq!(request.policy.deadline, Duration::from_secs(17));
        assert!(request.isolate_parent_environment);
        assert!(request.environment_remove.is_empty());
        let env: std::collections::BTreeMap<_, _> = request.environment.iter().cloned().collect();
        assert_eq!(env[&OsString::from("HOME")], "/captured/home");
        assert_eq!(env[&OsString::from("XDG_CONFIG_HOME")], "/captured/config");
        assert_eq!(env[&OsString::from("XDG_STATE_HOME")], "/captured/state");
        assert_eq!(env[&OsString::from("XDG_DATA_HOME")], "/captured/data");
        assert_eq!(env[&OsString::from("XDG_CACHE_HOME")], "/captured/cache");
        assert_eq!(
            env[&OsString::from(DETACHED_RUNNER_EXECUTABLE_ENV)],
            "/installed/bin/worker"
        );
        assert_eq!(
            request
                .environment
                .iter()
                .filter(|(key, _)| key == DETACHED_RUNNER_EXECUTABLE_ENV)
                .count(),
            1
        );
    }

    #[test]
    fn completed_errors_unknown_cleanup_and_guard_caps_preserve_the_tracked_result() {
        let request = request_fixture("task.wait.poll", serde_json::json!({"run": "fixture"}));
        let input = encode_json_frame(&serde_json::json!({
            "protocol_version": request.protocol_version(), "request_id": request.request_id(),
            "command": request.command(), "body": request.body(),
        }))
        .unwrap();
        let runner = Arc::new(RecordingTrackedRunner::new(vec![
            ProcessCompletion {
                outcome: Err(ProcessError::Cancelled.into()),
                cleanup: CleanupState::Completed,
            },
            ProcessCompletion {
                outcome: Err(WorkerError::Unavailable("abandoned fixture capture".into())),
                cleanup: CleanupState::Unknown,
            },
            ProcessCompletion {
                outcome: Ok(result_fixture(
                    &request,
                    serde_json::json!({"ok": true}),
                    75,
                )),
                cleanup: CleanupState::Completed,
            },
        ]));
        let executor = ChildRpcExecutor::new(runner.clone(), spec());
        let mut ctx = context(Arc::new(AtomicBool::new(false)));
        ctx.deadline = Duration::from_secs(100);
        let cancelled = executor.run(&input, &ctx);
        assert_eq!(cancelled.cleanup, CleanupState::Completed);
        assert!(matches!(
            cancelled.outcome,
            Err(WorkerError::Process(ProcessError::Cancelled))
        ));
        let abandoned = executor.run(&input, &ctx);
        assert_eq!(abandoned.cleanup, CleanupState::Unknown);
        assert!(
            matches!(abandoned.outcome, Err(WorkerError::Unavailable(message)) if message == "abandoned fixture capture")
        );
        let nonzero = executor.run(&input, &ctx);
        assert_eq!(nonzero.cleanup, CleanupState::Completed);
        assert_eq!(nonzero.outcome.unwrap().status.code(), Some(75));
        assert!(
            runner
                .calls()
                .iter()
                .all(|request| request.policy.deadline == REQUEST_GUARD)
        );
        ctx.deadline = ctx.runtime.now();
        let expired = executor.run(&input, &ctx);
        assert_eq!(expired.cleanup, CleanupState::Completed);
        assert!(matches!(
            expired.outcome,
            Err(WorkerError::Process(ProcessError::Cancelled))
        ));
        assert_eq!(
            runner.calls().len(),
            3,
            "expired context must not spawn a fourth child"
        );
    }

    #[test]
    fn invalid_launch_environment_or_request_never_reaches_the_runner() {
        for invalid in 0..4 {
            let runner = Arc::new(Runner::default());
            let mut launch = spec();
            let mut input = frame(serde_json::json!({"run": "fixture"}));
            match invalid {
                0 => launch
                    .environment
                    .push(("DYLD_INSERT_LIBRARIES".into(), "/untrusted.dylib".into())),
                1 => launch.config = "relative-config".into(),
                2 => launch.detached_runner_executable = launch.executable.path.clone(),
                _ => {
                    input = frame(serde_json::json!({"run": "fixture", "executable": "/untrusted"}))
                }
            }
            let outcome = ChildRpcExecutor::new(runner.clone(), launch)
                .run(&input, &context(Arc::new(AtomicBool::new(false))));
            assert!(outcome.outcome.is_err());
            assert_eq!(outcome.cleanup, CleanupState::Completed);
            assert!(runner.calls.lock().unwrap().is_empty());
        }
        // Valid input proves this is an admission test, not a blanket rejection.
        let runner = Arc::new(Runner::default());
        assert!(
            ChildRpcExecutor::new(runner.clone(), spec())
                .run(
                    &frame(serde_json::json!({"run": "fixture"})),
                    &context(Arc::new(AtomicBool::new(false)))
                )
                .outcome
                .is_ok()
        );
    }

    #[test]
    fn cancellation_is_live_inside_the_tracked_runner() {
        let flag = Arc::new(AtomicBool::new(false));
        let runner = Arc::new(Runner {
            stop_during_entry: Some(flag.clone()),
            ..Runner::default()
        });
        let result = ChildRpcExecutor::new(runner.clone(), spec()).run(
            &frame(serde_json::json!({"run": "fixture"})),
            &context(flag),
        );
        assert!(matches!(
            result.outcome,
            Err(WorkerError::Process(ProcessError::Cancelled))
        ));
        assert_eq!(runner.calls.lock().unwrap().len(), 1);
        assert_eq!(result.cleanup, CleanupState::Completed);
    }
}

mod control_cases {

    use super::*;
    use std::sync::mpsc;

    struct GatedImageSource {
        source: ScriptedImageSource,
        entered: Mutex<Option<oneshot::Sender<std::thread::ThreadId>>>,
        gate: Mutex<mpsc::Receiver<()>>,
    }
    impl RunningImageSource for GatedImageSource {
        fn capture(&self) -> Result<RunningImage, ChannelFailure> {
            self.entered
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(std::thread::current().id())
                .unwrap();
            self.gate
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .unwrap();
            self.source.capture()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gated_control_work_does_not_block_runtime_or_queue_a_replacement() {
        let control = NativeControl::new();
        let (entered, entry) = oneshot::channel();
        let (release, gate) = mpsc::channel();
        let source = GatedImageSource {
            source: ScriptedImageSource::new(vec![Ok(RunningImage {
                path: "/installed/bin/worker".into(),
                device: 3,
                inode: 37,
            })]),
            entered: Mutex::new(Some(entered)),
            gate: Mutex::new(gate),
        };
        let done = control
            .try_run(Box::new(move || source.capture()))
            .expect("first metadata job is admitted");
        assert_ne!(entry.await.unwrap(), std::thread::current().id());
        assert!(matches!(
            control.try_run(Box::new(|| 99)),
            Err(ChannelFailure::Unavailable(ChannelReason::Busy))
        ));
        // A signal/control task can still run on this current-thread runtime.
        let signal = Arc::new(AtomicBool::new(false));
        let observed = signal.clone();
        assert_eq!(
            tokio::spawn(async move {
                observed.store(true, Ordering::Release);
                11
            })
            .await
            .unwrap(),
            11
        );
        assert!(signal.load(Ordering::Acquire));
        release.send(()).unwrap();
        let image = done.await.unwrap().unwrap();
        assert_eq!(image.path, std::path::Path::new("/installed/bin/worker"));
        assert_eq!((image.device, image.inode), (3, 37));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completed_native_work_releases_the_control_slot() {
        let control = NativeControl::new();
        for expected in 0..12 {
            let result = control.try_run(Box::new(move || expected)).unwrap();
            assert_eq!(result.await.unwrap(), expected);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panicked_native_work_closes_its_result_and_releases_the_slot() {
        let control = NativeControl::new();
        let result = control
            .try_run::<()>(Box::new(|| panic!("fixture metadata panic")))
            .unwrap();
        assert!(result.await.is_err());
        assert_eq!(control.try_run(Box::new(|| 17)).unwrap().await.unwrap(), 17);
    }
}

#[test]
fn gate_executor_preserves_unknown_cleanup_on_error() {
    let executor: Box<dyn ChannelExecutor> =
        Box::new(RecordingExecutor::new(vec![ProcessCompletion {
            outcome: Err(ProcessError::Cancelled.into()),
            cleanup: CleanupState::Unknown,
        }]));
    let ctx = ServerContext {
        runtime: Arc::new(ManualRuntime::default()),
        deadline: Duration::from_secs(30),
        cancelled: Arc::new(AtomicBool::new(false)),
    };
    let completion = executor.run(b"fixture-frame", &ctx);
    assert_eq!(completion.cleanup, CleanupState::Unknown);
    assert!(completion.outcome.is_err());
}
