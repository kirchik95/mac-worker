// Private I/O barriers stay inline; public product/service cases live in the
// seeded consolidated controller target. These hooks are absent from production.
use super::*;
use crate::controller::{
    channel::testing::{ManualRuntime, StubCodec, identity_fixture, result_fixture},
    decode_request,
};
use std::io;

struct Executor {
    calls: AtomicUsize,
}
impl ChannelExecutor for Executor {
    fn run(&self, frame: &[u8], _: &ServerContext) -> ProcessCompletion {
        self.calls.fetch_add(1, Ordering::AcqRel);
        ProcessCompletion {
            outcome: Ok(result_fixture(
                &decode_request(frame).unwrap(),
                serde_json::json!({"ok": true}),
                0,
            )),
            cleanup: CleanupState::Completed,
        }
    }
}
struct Fixture {
    _directory: tempfile::TempDir,
    identity: SocketIdentity,
    clock: Arc<ManualRuntime>,
    executor: Arc<Executor>,
    service: SocketService,
}
impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let mut identity = identity_fixture();
        identity.service.account.uid = unsafe { libc::geteuid() };
        identity.service.socket_path = directory.path().join("s");
        let listener = UnixListener::bind(&identity.service.socket_path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let clock = Arc::new(ManualRuntime::default());
        let executor = Arc::new(Executor {
            calls: AtomicUsize::new(0),
        });
        let service = SocketService::start(
            listener,
            identity.service.clone(),
            ServerDeps {
                codec: Arc::new(StubCodec),
                executor: executor.clone(),
                runtime: clock.clone(),
            },
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        service.wait_ready().await.unwrap();
        Self {
            _directory: directory,
            identity,
            clock,
            executor,
            service,
        }
    }
    async fn connect(&self) -> UnixStream {
        let stream = UnixStream::connect(&self.identity.service.socket_path)
            .await
            .unwrap();
        send(&stream, &StubCodec.encode_hello(&self.identity).unwrap()).await;
        StubCodec
            .decode_ready(&receive(&stream).await.unwrap(), &self.identity)
            .unwrap();
        stream
    }
    async fn close(&self) -> ShutdownEvidence {
        self.service
            .shutdown(&ServerContext {
                runtime: self.clock.clone(),
                deadline: self.clock.now() + SETUP_GUARD,
                cancelled: Arc::new(AtomicBool::new(false)),
            })
            .await
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
    .unwrap();
}
fn poll() -> Vec<u8> {
    crate::controller::encode_json_frame(&serde_json::json!({"protocol_version": 7, "request_id": "0123456789abcdef0123456789abcdef", "command": "task.wait.poll", "body": {"run": "fixture"}})).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn completion_handoff_rejects_next_bytes_while_the_final_reply_write_is_held() {
    let fixture = Fixture::new().await;
    let stream = fixture.connect().await;
    let barrier = Arc::new(WriteBarrier::default());
    *fixture.service.state.before_final_write.lock().unwrap() = Some(barrier.clone());
    send(&stream, &poll()).await;
    barrier.entered.notified().await;
    send(&stream, &poll()).await;
    closed(&stream).await;
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 1);
    fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn quiet_probe_detects_early_bytes_before_the_runtime_refreshes_readiness() {
    let (server, client) = UnixStream::pair().unwrap();
    client.writable().await.unwrap();
    quiet(&server).unwrap();
    assert_eq!(client.try_write(b"x").unwrap(), 1);
    // No await/driver poll separates the completed write and the admission
    // probe: a cached WouldBlock is insufficient evidence of no early input.
    assert!(quiet(&server).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn completion_handoff_accepts_next_bytes_ready_at_final_write_completion() {
    let fixture = Fixture::new().await;
    let stream = fixture.connect().await;
    let barrier = Arc::new(WriteBarrier::default());
    *fixture.service.state.after_final_write.lock().unwrap() = Some(barrier.clone());
    send(&stream, &poll()).await;
    receive(&stream).await.unwrap();
    barrier.entered.notified().await;
    // The final byte has reached the client, while the server is held at the
    // completed-write handoff. Input is ready before it resumes that handoff.
    send(&stream, &poll()).await;
    barrier.release.notify_one();
    receive(&stream)
        .await
        .expect("a complete reply makes the next request legal");
    assert_eq!(fixture.executor.calls.load(Ordering::Acquire), 2);
    fixture.close().await;
}
