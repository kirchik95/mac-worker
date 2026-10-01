//! Deterministic sibling-track doubles, not production channel implementations.
//! Scripted cleanup is explicit: exhausted scripts return Unknown, never an
//! invented completion proof. Grammar/security coverage belongs to real codecs,
//! filesystem and transport implementations, not StubCodec or fake bindings.

use std::{
    collections::VecDeque,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};

use super::contracts::*;
use crate::{
    controller::protocol::{ControllerRequest, decode_frame, encode_json_frame, parse_request},
    error::{ProcessError, WorkerError},
    job::ProcessIdentity,
    paths::PathLayout,
    process::{
        CleanupState, ProcessCompletion, ProcessPolicy, ProcessRequest, ProcessResult,
        ProcessRunner, TrackedProcessRunner,
    },
    protocol::PROTOCOL_VERSION,
};

pub fn identity_fixture() -> SocketIdentity {
    SocketIdentity {
        route_sha256: RouteDigest::parse(&"a".repeat(64)).unwrap(),
        service: ServiceIdentity {
            protocol_version: PROTOCOL_VERSION,
            channel_version: CHANNEL_VERSION,
            controller_client_id: "0123456789ab4def8123456789abcdef".parse().unwrap(),
            account: ControllerAccount {
                uid: 501,
                username: "controller".into(),
                home: "/Users/controller".into(),
            },
            leader: ProcessIdentity::new(42, 123_456).unwrap(),
            service_generation: UuidString::parse("01234567-89ab-4def-8123-456789abcdef").unwrap(),
            socket_path: "/private/controller/rpc/s".into(),
            features: vec!["controller.events".into(), "controller.socket".into()],
            journal_id: Some(UuidString::parse("fedcba98-7654-4321-8123-456789abcdef").unwrap()),
        },
    }
}

pub fn request_fixture(command: &str, body: Value) -> ControllerRequest {
    parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "0123456789ab4def8123456789abcdef", "command": command, "body": body,
        }))
        .unwrap(),
    )
    .unwrap()
}

/// Complete legacy read envelope, including framed stdout and original status.
pub fn result_fixture(request: &ControllerRequest, result: Value, exit_code: i32) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(exit_code << 8),
        stdout: encode_json_frame(&json!({
            "protocol_version": PROTOCOL_VERSION, "command": request.command(),
            "request_id": request.request_id(), "payload_sha256": request.payload_sha256(), "result": result,
        })).unwrap(),
        stderr: Vec::new(),
    }
}

#[derive(Default)]
pub struct ManualRuntime {
    now: Mutex<Duration>,
    cancelled: AtomicBool,
}
impl ManualRuntime {
    pub fn advance(&self, elapsed: Duration) {
        let mut now = self.now.lock().unwrap();
        *now = now.saturating_add(elapsed);
    }
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
}
impl ChannelRuntime for ManualRuntime {
    fn now(&self) -> Duration {
        *self.now.lock().unwrap()
    }
    fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

pub struct RecordingRunner {
    replies: Mutex<VecDeque<Result<ProcessResult, WorkerError>>>,
    calls: Mutex<Vec<ProcessRequest>>,
}
impl RecordingRunner {
    pub fn new(replies: Vec<Result<ProcessResult, WorkerError>>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            calls: Mutex::new(Vec::new()),
        }
    }
    pub fn calls(&self) -> Vec<ProcessRequest> {
        self.calls.lock().unwrap().clone()
    }
}
impl ProcessRunner for RecordingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.calls.lock().unwrap().push(request.clone());
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(exhausted()))
    }
    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.run(request)
    }
    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        if should_stop() {
            return Err(ProcessError::Cancelled.into());
        }
        self.run(request)
    }
}

pub struct RecordingTrackedRunner {
    replies: Mutex<VecDeque<ProcessCompletion>>,
    calls: Mutex<Vec<ProcessRequest>>,
}
impl RecordingTrackedRunner {
    pub fn new(replies: Vec<ProcessCompletion>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            calls: Mutex::new(Vec::new()),
        }
    }
    pub fn calls(&self) -> Vec<ProcessRequest> {
        self.calls.lock().unwrap().clone()
    }
}
impl ProcessRunner for RecordingTrackedRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.run_interruptible_with_cleanup(request, &|| false)
            .outcome
    }
    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.run(request)
    }
    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        self.run_interruptible_with_cleanup(request, should_stop)
            .outcome
    }
}
impl TrackedProcessRunner for RecordingTrackedRunner {
    fn run_interruptible_with_cleanup(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> ProcessCompletion {
        self.calls.lock().unwrap().push(request.clone());
        let stopped = should_stop();
        let mut completion = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(unknown_completion);
        if stopped {
            completion.outcome = Err(ProcessError::Cancelled.into());
        }
        completion
    }
}

pub struct ScriptedImageSource {
    replies: Mutex<VecDeque<Result<RunningImage, ChannelFailure>>>,
}
impl ScriptedImageSource {
    pub fn new(replies: Vec<Result<RunningImage, ChannelFailure>>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
        }
    }
}
impl RunningImageSource for ScriptedImageSource {
    fn capture(&self) -> Result<RunningImage, ChannelFailure> {
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Err(missing_script()))
    }
}

pub struct ScriptedIdentitySource {
    replies: Mutex<VecDeque<Result<SocketIdentity, ChannelFailure>>>,
}
impl ScriptedIdentitySource {
    pub fn new(replies: Vec<Result<SocketIdentity, ChannelFailure>>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
        }
    }
}
impl IdentitySource for ScriptedIdentitySource {
    fn read(
        &self,
        _raw: &dyn ProcessRunner,
        _route: &ConfiguredRoute,
        _master: Option<&MasterPlan>,
        ctx: &ClientContext<'_>,
    ) -> Result<SocketIdentity, ChannelFailure> {
        ctx.check()?;
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Err(missing_script()))
    }
}

#[derive(Default)]
pub struct MemoryPinStore {
    pin: Mutex<Option<Pin>>,
}
impl MemoryPinStore {
    pub fn pin(&self) -> Option<Pin> {
        self.pin.lock().unwrap().clone()
    }
}
impl PinStore for MemoryPinStore {
    fn verify_or_create(
        &self,
        _paths: &PathLayout,
        identity: &SocketIdentity,
    ) -> Result<(), ChannelFailure> {
        identity.validate()?;
        let candidate = Pin::from_identity(identity);
        let mut pin = self.pin.lock().unwrap();
        match pin.as_ref() {
            Some(existing) if existing != &candidate => {
                Err(ChannelFailure::Unavailable(ChannelReason::PinMismatch))
            }
            Some(_) => Ok(()),
            None => {
                *pin = Some(candidate);
                Ok(())
            }
        }
    }
    fn repin(
        &self,
        _paths: &PathLayout,
        identity: &SocketIdentity,
        expected: crate::job::ClientId,
    ) -> Result<(), ChannelFailure> {
        identity.validate()?;
        if identity.service.controller_client_id != expected {
            return Err(ChannelFailure::Unavailable(ChannelReason::PinMismatch));
        }
        *self.pin.lock().unwrap() = Some(Pin::from_identity(identity));
        Ok(())
    }
}

struct ForwardState {
    opens: usize,
    cancels: usize,
    resolutions: usize,
    disposition: ForwardDisposition,
    failure: Option<ForwardOpenFailure>,
}
pub struct FakeForwardControl {
    local: PathBuf,
    state: Arc<Mutex<ForwardState>>,
}
impl FakeForwardControl {
    pub fn new(local: PathBuf) -> Self {
        Self {
            local,
            state: Arc::new(Mutex::new(ForwardState {
                opens: 0,
                cancels: 0,
                resolutions: 0,
                disposition: ForwardDisposition::Cleaned,
                failure: None,
            })),
        }
    }
    pub fn opens(&self) -> usize {
        self.state.lock().unwrap().opens
    }
    pub fn cancels(&self) -> usize {
        self.state.lock().unwrap().cancels
    }
    pub fn resolutions(&self) -> usize {
        self.state.lock().unwrap().resolutions
    }
    pub fn set_disposition(&self, disposition: ForwardDisposition) {
        self.state.lock().unwrap().disposition = disposition;
    }
    pub fn fail_next(&self, failure: ForwardOpenFailure) {
        self.state.lock().unwrap().failure = Some(failure);
    }
}
impl ForwardControl for FakeForwardControl {
    fn resolve(
        &self,
        _raw: &dyn ProcessRunner,
        _route: &ConfiguredRoute,
        ctx: &ClientContext<'_>,
    ) -> Result<MasterPlan, ChannelFailure> {
        ctx.check()?;
        self.state.lock().unwrap().resolutions += 1;
        Ok(MasterPlan {
            control_path: "/private/fake-master/s".into(),
            parent: entry(libc::S_IFDIR.into(), 0o700),
            bootstrap_request: ProcessRequest {
                program: "fake-ssh".into(),
                args: vec!["-S".into(), "/private/fake-master/s".into()],
                environment: Vec::new(),
                environment_remove: Vec::new(),
                stdin: None,
                policy: ProcessPolicy {
                    stdout_limit: MAX_FRAME_BYTES + 4,
                    stderr_limit: 256 * 1024,
                    deadline: REQUEST_GUARD,
                },
                isolate_parent_environment: false,
            },
        })
    }
    fn open(
        &self,
        _raw: &dyn ProcessRunner,
        _master: &MasterPlan,
        _identity: &SocketIdentity,
        ctx: &ClientContext<'_>,
    ) -> Result<Box<dyn ForwardLease>, ForwardOpenFailure> {
        ctx.check().map_err(|failure| ForwardOpenFailure {
            failure,
            disposition: ForwardDisposition::Cleaned,
        })?;
        let mut state = self.state.lock().unwrap();
        state.opens += 1;
        if let Some(failure) = state.failure.take() {
            return Err(failure);
        }
        Ok(Box::new(FakeForwardLease {
            local: self.local.clone(),
            state: self.state.clone(),
        }))
    }
}
struct FakeForwardLease {
    local: PathBuf,
    state: Arc<Mutex<ForwardState>>,
}
impl ForwardLease for FakeForwardLease {
    fn local_socket(&self) -> &Path {
        &self.local
    }
    fn verify(&self) -> Result<(), ChannelFailure> {
        Ok(())
    }
    fn cancel(&mut self, _raw: &dyn ProcessRunner, ctx: &CleanupContext) -> ForwardDisposition {
        let mut state = self.state.lock().unwrap();
        state.cancels += 1;
        if ctx.check().is_err() {
            ForwardDisposition::Retained
        } else {
            state.disposition
        }
    }
}

struct ConnectorState {
    replies: VecDeque<Result<ProcessResult, ChannelFailure>>,
    frames: Vec<Vec<u8>>,
    connections: usize,
    closes: usize,
}
pub struct ScriptedConnector {
    state: Arc<Mutex<ConnectorState>>,
}
impl ScriptedConnector {
    pub fn new(replies: Vec<Result<ProcessResult, ChannelFailure>>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectorState {
                replies: replies.into(),
                frames: Vec::new(),
                connections: 0,
                closes: 0,
            })),
        }
    }
    pub fn frames(&self) -> Vec<Vec<u8>> {
        self.state.lock().unwrap().frames.clone()
    }
    pub fn connections(&self) -> usize {
        self.state.lock().unwrap().connections
    }
    pub fn closes(&self) -> usize {
        self.state.lock().unwrap().closes
    }
}
impl SocketConnector for ScriptedConnector {
    fn connect(
        &self,
        _local: &Path,
        _identity: &SocketIdentity,
        ctx: &ClientContext<'_>,
    ) -> Result<Box<dyn SocketSession>, ChannelFailure> {
        ctx.check()?;
        self.state.lock().unwrap().connections += 1;
        Ok(Box::new(ScriptedSession {
            state: self.state.clone(),
            closed: false,
        }))
    }
}
struct ScriptedSession {
    state: Arc<Mutex<ConnectorState>>,
    closed: bool,
}
impl SocketSession for ScriptedSession {
    fn exchange(
        &mut self,
        frame: &[u8],
        _request: &ControllerRequest,
        ctx: &ClientContext<'_>,
    ) -> Result<ProcessResult, ChannelFailure> {
        ctx.check()?;
        if self.closed {
            return Err(ChannelFailure::Unavailable(ChannelReason::ForwardLost));
        }
        let mut state = self.state.lock().unwrap();
        state.frames.push(frame.to_vec());
        state.replies.pop_front().unwrap_or(Err(missing_script()))
    }
    fn close(&mut self) {
        if !self.closed {
            self.state.lock().unwrap().closes += 1;
            self.closed = true;
        }
    }
}

pub struct RecordingExecutor {
    replies: Mutex<VecDeque<ProcessCompletion>>,
    frames: Mutex<Vec<Vec<u8>>>,
}
impl RecordingExecutor {
    pub fn new(replies: Vec<ProcessCompletion>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            frames: Mutex::new(Vec::new()),
        }
    }
    pub fn frames(&self) -> Vec<Vec<u8>> {
        self.frames.lock().unwrap().clone()
    }
}
impl ChannelExecutor for RecordingExecutor {
    fn run(&self, frame: &[u8], ctx: &ServerContext) -> ProcessCompletion {
        self.frames.lock().unwrap().push(frame.to_vec());
        let mut completion = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(unknown_completion);
        if let Err(failure) = ctx.check() {
            completion.outcome = Err(match failure {
                ChannelFailure::Unavailable(ChannelReason::Cancelled) => {
                    ProcessError::Cancelled.into()
                }
                _ => ProcessError::DeadlineExceeded {
                    deadline: Duration::ZERO,
                }
                .into(),
            });
        }
        completion
    }
}

/// Evidence is selected by the fixture; no real ownership or refusal proof.
pub struct FakeForwardPaths {
    local: PathBuf,
    allocations: AtomicUsize,
    cleanups: AtomicUsize,
    disposition: Mutex<ForwardDisposition>,
}
impl Default for FakeForwardPaths {
    fn default() -> Self {
        Self::new("/private/fake-forward/s".into())
    }
}
impl FakeForwardPaths {
    pub fn new(local: PathBuf) -> Self {
        Self {
            local,
            allocations: AtomicUsize::new(0),
            cleanups: AtomicUsize::new(0),
            disposition: Mutex::new(ForwardDisposition::Retained),
        }
    }
    pub fn allocations(&self) -> usize {
        self.allocations.load(Ordering::Acquire)
    }
    pub fn cleanups(&self) -> usize {
        self.cleanups.load(Ordering::Acquire)
    }
    pub fn set_disposition(&self, disposition: ForwardDisposition) {
        *self.disposition.lock().unwrap() = disposition;
    }
}
impl ForwardPaths for FakeForwardPaths {
    fn allocate(&self, _paths: &PathLayout) -> Result<ForwardPath, ChannelFailure> {
        self.allocations.fetch_add(1, Ordering::AcqRel);
        Ok(ForwardPath {
            directory: self.local.parent().unwrap().into(),
            directory_identity: entry(libc::S_IFDIR.into(), 0o700),
            socket_path: self.local.clone(),
        })
    }
    fn validate_socket(&self, _path: &ForwardPath) -> Result<EntryIdentity, ChannelFailure> {
        Ok(entry(libc::S_IFSOCK.into(), 0o600))
    }
    fn cleanup_if_refused(
        &self,
        _path: &ForwardPath,
        _socket: Option<EntryIdentity>,
        ctx: &CleanupContext,
    ) -> ForwardDisposition {
        self.cleanups.fetch_add(1, Ordering::AcqRel);
        if ctx.check().is_err() {
            ForwardDisposition::Retained
        } else {
            *self.disposition.lock().unwrap()
        }
    }
}

/// Simple fixture codec, using identities directly for hello/ready. Does not
/// implement the production hello/reply grammar or prove security acceptance.
#[derive(Default)]
pub struct StubCodec;
impl StubCodec {
    pub fn new() -> Self {
        Self
    }
}
impl ChannelCodec for StubCodec {
    fn decoder(&self) -> Box<dyn FrameDecoder> {
        Box::new(StubDecoder::default())
    }
    fn encode_hello(&self, expected: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        encode_json_frame(expected).map_err(|_| missing_script())
    }
    fn decode_hello(&self, payload: &[u8]) -> Result<SocketIdentity, ChannelFailure> {
        serde_json::from_slice(payload).map_err(|_| missing_script())
    }
    fn encode_ready(&self, identity: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        self.encode_hello(identity)
    }
    fn decode_ready(
        &self,
        payload: &[u8],
        expected: &SocketIdentity,
    ) -> Result<(), ChannelFailure> {
        verify_expected_service(expected, &self.decode_hello(payload)?)
    }
    fn encode_reply(
        &self,
        request: &ControllerRequest,
        result: &ProcessResult,
    ) -> Result<Vec<u8>, ChannelFailure> {
        let payload: Value =
            serde_json::from_slice(decode_frame(&result.stdout).map_err(|_| missing_script())?)
                .map_err(|_| missing_script())?;
        let code = result
            .status
            .code()
            .filter(|code| (0..=255).contains(code))
            .ok_or_else(missing_script)?;
        encode_json_frame(&json!({ "kind": "reply", "request_id": request.request_id(), "payload_sha256": request.payload_sha256(), "exit_code": code, "payload": payload })).map_err(|_| missing_script())
    }
    fn decode_reply(
        &self,
        payload: &[u8],
        request: &ControllerRequest,
    ) -> Result<ProcessResult, ChannelFailure> {
        let value: Value = serde_json::from_slice(payload).map_err(|_| missing_script())?;
        if value["request_id"] != request.request_id()
            || value["payload_sha256"] != request.payload_sha256()
        {
            return Err(ChannelFailure::UnverifiedReply);
        }
        let code = value["exit_code"]
            .as_i64()
            .filter(|code| (0..=255).contains(code))
            .ok_or_else(missing_script)?;
        Ok(ProcessResult {
            status: ExitStatus::from_raw((code as i32) << 8),
            stdout: encode_json_frame(&value["payload"]).map_err(|_| missing_script())?,
            stderr: Vec::new(),
        })
    }
}
#[derive(Default)]
struct StubDecoder {
    prefix: Vec<u8>,
    payload: Vec<u8>,
    length: Option<usize>,
}
impl FrameDecoder for StubDecoder {
    fn feed(&mut self, input: &[u8]) -> Result<DecodeProgress, ChannelFailure> {
        let mut consumed = 0;
        if self.length.is_none() {
            let count = (4 - self.prefix.len()).min(input.len());
            self.prefix.extend_from_slice(&input[..count]);
            consumed += count;
            if self.prefix.len() < 4 {
                return Ok(DecodeProgress {
                    consumed,
                    payload: None,
                });
            }
            let length = u32::from_be_bytes(self.prefix.as_slice().try_into().unwrap()) as usize;
            if length == 0 || length > MAX_FRAME_BYTES {
                return Err(missing_script());
            }
            self.length = Some(length);
        }
        let length = self.length.unwrap();
        let count = (length - self.payload.len()).min(input.len() - consumed);
        self.payload
            .extend_from_slice(&input[consumed..consumed + count]);
        consumed += count;
        if self.payload.len() == length {
            let payload = std::mem::take(&mut self.payload);
            self.prefix.clear();
            self.length = None;
            Ok(DecodeProgress {
                consumed,
                payload: Some(payload),
            })
        } else {
            Ok(DecodeProgress {
                consumed,
                payload: None,
            })
        }
    }
    fn retained_bytes(&self) -> usize {
        self.prefix.len() + self.payload.len()
    }
}

fn entry(kind: u32, mode: u32) -> EntryIdentity {
    EntryIdentity {
        device: 1,
        inode: 2,
        owner: 501,
        kind,
        mode,
    }
}
fn missing_script() -> ChannelFailure {
    ChannelFailure::Unavailable(ChannelReason::InvalidFrame)
}
fn exhausted() -> WorkerError {
    WorkerError::Protocol("channel fixture script exhausted".into())
}
fn unknown_completion() -> ProcessCompletion {
    ProcessCompletion {
        outcome: Err(exhausted()),
        cleanup: CleanupState::Unknown,
    }
}
