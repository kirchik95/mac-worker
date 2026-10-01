//! Scoped controller read-loop policy and frozen sibling handoff coverage.
use std::{
    collections::{HashMap, VecDeque},
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use mac_worker::{
    controller::channel::{
        client::ChannelProcessRunner,
        contracts::*,
        testing::{
            FakeForwardControl, ManualRuntime, MemoryPinStore, RecordingRunner, ScriptedConnector,
            ScriptedIdentitySource, identity_fixture, request_fixture, result_fixture,
        },
    },
    controller::{ControllerRequest, decode_request, encode_json_frame},
    error::{ProcessError, WorkerError},
    job::ClientId,
    paths::PathLayout,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
};

// T1 doubles cover sibling contracts. Local gated stages below exercise live
// borrowed cancellation and shared-budget consumption after operation entry.
fn scoped_identity() -> SocketIdentity {
    let mut identity = identity_fixture();
    identity.route_sha256 = route().digest().unwrap();
    identity.service.journal_id = None;
    identity
}

struct FrozenFixture {
    raw: Arc<RecordingRunner>,
    identity: Arc<ScriptedIdentitySource>,
    pins: Arc<MemoryPinStore>,
    forwards: Arc<FakeForwardControl>,
    connector: Arc<ScriptedConnector>,
    runtime: Arc<ManualRuntime>,
}
impl FrozenFixture {
    fn new(
        reads: Vec<Result<ProcessResult, ChannelFailure>>,
        raw: Vec<Result<ProcessResult, WorkerError>>,
    ) -> Self {
        Self::with_identities(vec![Ok(scoped_identity())], reads, raw)
    }
    fn with_identities(
        identities: Vec<Result<SocketIdentity, ChannelFailure>>,
        reads: Vec<Result<ProcessResult, ChannelFailure>>,
        raw: Vec<Result<ProcessResult, WorkerError>>,
    ) -> Self {
        Self {
            raw: Arc::new(RecordingRunner::new(raw)),
            identity: Arc::new(ScriptedIdentitySource::new(identities)),
            pins: Arc::new(MemoryPinStore::default()),
            forwards: Arc::new(FakeForwardControl::new("/private/fake-forward/s".into())),
            connector: Arc::new(ScriptedConnector::new(reads)),
            runtime: Arc::new(ManualRuntime::default()),
        }
    }
    fn deps(&self) -> ClientDeps {
        ClientDeps {
            identity: self.identity.clone(),
            pins: self.pins.clone(),
            forwards: self.forwards.clone(),
            connector: self.connector.clone(),
            runtime: self.runtime.clone(),
        }
    }
    fn runner(&self, scope: ReadLoopScope) -> ChannelProcessRunner<Arc<dyn ProcessRunner>> {
        ChannelProcessRunner::new(
            self.raw.clone() as Arc<dyn ProcessRunner>,
            scope,
            route(),
            paths(),
            self.deps(),
        )
    }
}
fn read_result(request: &ProcessRequest) -> ProcessResult {
    let parsed = decode_request(request.stdin.as_ref().unwrap()).unwrap();
    result_fixture(&parsed, serde_json::json!({"path":"socket"}), 0)
}

struct Gate {
    stage: &'static str,
    entered: mpsc::SyncSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}
struct Data {
    runtime: Arc<ManualRuntime>,
    identity: Mutex<SocketIdentity>,
    steps: Mutex<Vec<&'static str>>,
    contexts: Mutex<Vec<(&'static str, Duration, Duration)>>,
    raw_calls: Mutex<Vec<(&'static str, ProcessRequest)>>,
    raw_error: Mutex<Option<WorkerError>>,
    frames: Mutex<Vec<Vec<u8>>>,
    replies: Mutex<VecDeque<Result<ProcessResult, ChannelFailure>>>,
    failures: Mutex<HashMap<&'static str, ChannelFailure>>,
    advances: Mutex<HashMap<&'static str, Duration>>,
    forward_failure: Mutex<Option<ForwardOpenFailure>>,
    disposition: Mutex<ForwardDisposition>,
    gate: Mutex<Option<Arc<Gate>>>,
    pins: MemoryPinStore,
    cleanup_deadlines: Mutex<Vec<Duration>>,
    return_cancel: Mutex<Option<(&'static str, Arc<AtomicBool>)>>,
    residue: AtomicBool,
}
#[derive(Clone)]
struct Fixture(Arc<Data>);
impl Fixture {
    fn new() -> Self {
        Self(Arc::new(Data {
            runtime: Arc::new(ManualRuntime::default()),
            identity: Mutex::new(scoped_identity()),
            steps: Mutex::new(Vec::new()),
            contexts: Mutex::new(Vec::new()),
            raw_calls: Mutex::new(Vec::new()),
            raw_error: Mutex::new(None),
            frames: Mutex::new(Vec::new()),
            replies: Mutex::new(VecDeque::new()),
            failures: Mutex::new(HashMap::new()),
            advances: Mutex::new(HashMap::new()),
            forward_failure: Mutex::new(None),
            disposition: Mutex::new(ForwardDisposition::Cleaned),
            gate: Mutex::new(None),
            pins: MemoryPinStore::default(),
            cleanup_deadlines: Mutex::new(Vec::new()),
            return_cancel: Mutex::new(None),
            residue: AtomicBool::new(false),
        }))
    }
    fn runner(&self, scope: ReadLoopScope) -> ChannelProcessRunner<Arc<dyn ProcessRunner>> {
        ChannelProcessRunner::new(
            Arc::new(self.clone()) as Arc<dyn ProcessRunner>,
            scope,
            route(),
            paths(),
            self.deps(),
        )
    }
    fn deps(&self) -> ClientDeps {
        ClientDeps {
            identity: Arc::new(self.clone()),
            pins: Arc::new(self.clone()),
            forwards: Arc::new(self.clone()),
            connector: Arc::new(self.clone()),
            runtime: self.0.runtime.clone(),
        }
    }
    fn stage(&self, name: &'static str, ctx: &ClientContext<'_>) -> Result<(), ChannelFailure> {
        ctx.check()?;
        self.0.steps.lock().unwrap().push(name);
        self.0
            .contexts
            .lock()
            .unwrap()
            .push((name, ctx.deadline, ctx.remaining()));
        if let Some(elapsed) = self.0.advances.lock().unwrap().get(name) {
            self.0.runtime.advance(*elapsed);
        }
        let gate = self.0.gate.lock().unwrap().clone();
        if let Some(gate) = gate.filter(|gate| gate.stage == name) {
            gate.entered.send(()).unwrap();
            gate.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .expect("fixture hang guard");
        }
        ctx.check()?;
        let outcome = self
            .0
            .failures
            .lock()
            .unwrap()
            .remove(name)
            .map_or(Ok(()), Err);
        if outcome.is_ok()
            && let Some((stage, stopped)) = &*self.0.return_cancel.lock().unwrap()
            && *stage == name
        {
            stopped.store(true, Ordering::SeqCst);
        }
        outcome
    }
    fn raw(
        &self,
        mode: &'static str,
        request: &ProcessRequest,
    ) -> Result<ProcessResult, WorkerError> {
        self.0
            .raw_calls
            .lock()
            .unwrap()
            .push((mode, request.clone()));
        if let Some(error) = self.0.raw_error.lock().unwrap().take() {
            return Err(error);
        }
        Ok(marker("raw", 0))
    }
    fn steps(&self) -> Vec<&'static str> {
        self.0.steps.lock().unwrap().clone()
    }
    fn count(&self, stage: &'static str) -> usize {
        self.steps().iter().filter(|value| **value == stage).count()
    }
    fn fail(&self, stage: &'static str, failure: ChannelFailure) {
        self.0.failures.lock().unwrap().insert(stage, failure);
    }
    fn gate(&self, stage: &'static str) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        *self.0.gate.lock().unwrap() = Some(Arc::new(Gate {
            stage,
            entered: entered_tx,
            release: Mutex::new(release_rx),
        }));
        (entered_rx, release_tx)
    }
}
impl ProcessRunner for Fixture {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.raw("run", request)
    }
    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.raw("new_session", request)
    }
    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        if should_stop() {
            return Err(mac_worker::error::ProcessError::Cancelled.into());
        }
        let result = self.raw("interruptible", request);
        let gate = self.0.gate.lock().unwrap().clone();
        if let Some(gate) = gate.filter(|gate| gate.stage == "raw") {
            gate.entered.send(()).unwrap();
            gate.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .expect("raw entry hang guard");
        }
        if should_stop() {
            return Err(mac_worker::error::ProcessError::Cancelled.into());
        }
        result
    }
}
impl IdentitySource for Fixture {
    fn read(
        &self,
        _raw: &dyn ProcessRunner,
        configured: &ConfiguredRoute,
        master: Option<&MasterPlan>,
        ctx: &ClientContext<'_>,
    ) -> Result<SocketIdentity, ChannelFailure> {
        assert_eq!(configured.ssh, "controller-alias");
        assert_eq!(
            master.unwrap().control_path,
            PathBuf::from("/cache/ssh/master")
        );
        self.stage("identity", ctx)?;
        Ok(self.0.identity.lock().unwrap().clone())
    }
}
impl PinStore for Fixture {
    fn verify_or_create(
        &self,
        layout: &PathLayout,
        identity: &SocketIdentity,
    ) -> Result<(), ChannelFailure> {
        assert_eq!(layout.cache, PathBuf::from("/cache/mac-worker"));
        self.0.steps.lock().unwrap().push("pin");
        if let Some(failure) = self.0.failures.lock().unwrap().remove("pin") {
            return Err(failure);
        }
        self.0.pins.verify_or_create(layout, identity)
    }
    fn repin(
        &self,
        _layout: &PathLayout,
        _identity: &SocketIdentity,
        _expected: ClientId,
    ) -> Result<(), ChannelFailure> {
        panic!("policy must never repin")
    }
}
impl ForwardControl for Fixture {
    fn resolve(
        &self,
        _raw: &dyn ProcessRunner,
        configured: &ConfiguredRoute,
        ctx: &ClientContext<'_>,
    ) -> Result<MasterPlan, ChannelFailure> {
        assert_eq!(configured.ssh, "controller-alias");
        self.stage("resolve", ctx)?;
        Ok(MasterPlan {
            control_path: "/cache/ssh/master".into(),
            parent: EntryIdentity {
                device: 1,
                inode: 2,
                owner: 501,
                kind: u32::from(libc::S_IFDIR),
                mode: 0o700,
            },
            bootstrap_request: process_request(
                "task.list",
                serde_json::json!({"controller_health":true}),
                1,
            ),
        })
    }
    fn open(
        &self,
        _raw: &dyn ProcessRunner,
        master: &MasterPlan,
        identity: &SocketIdentity,
        ctx: &ClientContext<'_>,
    ) -> Result<Box<dyn ForwardLease>, ForwardOpenFailure> {
        assert_eq!(master.control_path, PathBuf::from("/cache/ssh/master"));
        assert_eq!(
            identity.route_sha256,
            self.0.identity.lock().unwrap().route_sha256
        );
        if let Err(failure) = self.stage("open", ctx) {
            let disposition = self
                .0
                .forward_failure
                .lock()
                .unwrap()
                .take()
                .map_or(ForwardDisposition::Cleaned, |failure| failure.disposition);
            self.0.residue.store(
                disposition == ForwardDisposition::Retained,
                Ordering::SeqCst,
            );
            return Err(ForwardOpenFailure {
                failure,
                disposition,
            });
        }
        if let Some(failure) = self.0.forward_failure.lock().unwrap().take() {
            self.0.residue.store(
                failure.disposition == ForwardDisposition::Retained,
                Ordering::SeqCst,
            );
            return Err(failure);
        }
        self.0.residue.store(true, Ordering::SeqCst);
        Ok(Box::new(self.clone()))
    }
}
impl ForwardLease for Fixture {
    fn local_socket(&self) -> &Path {
        Path::new("/cache/channel/owned/s")
    }
    fn verify(&self) -> Result<(), ChannelFailure> {
        self.0.steps.lock().unwrap().push("verify");
        self.0
            .failures
            .lock()
            .unwrap()
            .remove("verify")
            .map_or(Ok(()), Err)
    }
    fn cancel(&mut self, _raw: &dyn ProcessRunner, ctx: &CleanupContext) -> ForwardDisposition {
        self.0.steps.lock().unwrap().push("cancel");
        self.0.cleanup_deadlines.lock().unwrap().push(ctx.deadline);
        assert!(ctx.runtime.now() < ctx.deadline);
        if let Some(elapsed) = self.0.advances.lock().unwrap().get("cancel") {
            self.0.runtime.advance(*elapsed);
        }
        let disposition = *self.0.disposition.lock().unwrap();
        if disposition == ForwardDisposition::Cleaned {
            self.0.residue.store(false, Ordering::SeqCst);
        }
        disposition
    }
}
impl SocketConnector for Fixture {
    fn connect(
        &self,
        local: &Path,
        identity: &SocketIdentity,
        ctx: &ClientContext<'_>,
    ) -> Result<Box<dyn SocketSession>, ChannelFailure> {
        assert_eq!(local, Path::new("/cache/channel/owned/s"));
        assert_eq!(
            identity.route_sha256,
            self.0.identity.lock().unwrap().route_sha256
        );
        self.stage("connect", ctx)?;
        Ok(Box::new(self.clone()))
    }
}
impl SocketSession for Fixture {
    fn exchange(
        &mut self,
        frame: &[u8],
        request: &ControllerRequest,
        ctx: &ClientContext<'_>,
    ) -> Result<ProcessResult, ChannelFailure> {
        self.0.frames.lock().unwrap().push(frame.to_vec());
        self.stage("write", ctx)?;
        self.stage("read", ctx)?;
        self.0
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| {
                Ok(result_fixture(
                    request,
                    serde_json::json!({"path":"socket"}),
                    0,
                ))
            })
    }
    fn close(&mut self) {
        self.0.steps.lock().unwrap().push("close");
    }
}
fn route() -> ConfiguredRoute {
    ConfiguredRoute {
        ssh: "controller-alias".into(),
        remote_binary: "~/.local/bin/worker".into(),
        ssh_config_file: None,
    }
}
fn paths() -> PathLayout {
    PathLayout {
        config: "/config/worker.toml".into(),
        state: "/state/mac-worker".into(),
        cache: "/cache/mac-worker".into(),
        data: "/data/mac-worker".into(),
    }
}
fn marker(value: &str, exit: i32) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(exit << 8),
        stdout: value.as_bytes().to_vec(),
        stderr: Vec::new(),
    }
}
fn process_request(command: &str, body: serde_json::Value, sequence: u64) -> ProcessRequest {
    ProcessRequest {
        program: "/usr/bin/ssh".into(),
        args: ["-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "ForwardAgent=no", "-o", "ClearAllForwardings=yes", "--", "controller-alias", "~/.local/bin/worker host controller-rpc"].into_iter().map(Into::into).collect(),
        environment: Vec::new(), environment_remove: Vec::new(), isolate_parent_environment: false,
        stdin: Some(encode_json_frame(&serde_json::json!({"protocol_version":PROTOCOL_VERSION,"request_id":format!("{sequence:032x}"),"command":command,"body":body})).unwrap()),
        policy: ProcessPolicy { stdout_limit: 1024 * 1024 + 4, stderr_limit: 256 * 1024, deadline: Duration::from_secs(30) },
    }
}
fn wait(sequence: u64) -> ProcessRequest {
    process_request(
        "task.wait.poll",
        serde_json::json!({"task_id":"11111111111141118111111111111111"}),
        sequence,
    )
}
fn socket_result(result: &ProcessResult) -> bool {
    result.stdout.windows(6).any(|value| value == b"socket")
}

#[test]
fn exact_controller_rpc_reads_use_only_their_loop_scope() {
    let mut cases = vec![
        (ReadLoopScope::Wait, wait(1)),
        (
            ReadLoopScope::LogsFollow,
            process_request(
                "task.logs",
                serde_json::json!({"task_id":"11111111111141118111111111111111","follow":true}),
                2,
            ),
        ),
        (
            ReadLoopScope::LogsFollow,
            process_request(
                "task.list",
                serde_json::json!({"controller_health":true}),
                3,
            ),
        ),
        (
            ReadLoopScope::EventsFollow,
            process_request(
                "task.list",
                serde_json::json!({"controller_events":{"op":"read"}}),
                4,
            ),
        ),
        (
            ReadLoopScope::Notify,
            process_request(
                "task.list",
                serde_json::json!({"controller_events":{"op":"read"}}),
                5,
            ),
        ),
    ];
    cases.extend([
        (
            ReadLoopScope::Wait,
            process_request(
                "task.wait.poll",
                serde_json::json!({"run":"fixture-run"}),
                6,
            ),
        ),
        (
            ReadLoopScope::Wait,
            process_request(
                "task.wait.poll",
                serde_json::json!({"task_id":"11111111111141118111111111111111", "run":null}),
                7,
            ),
        ),
    ]);
    for scope in [ReadLoopScope::EventsFollow, ReadLoopScope::Notify] {
        for body in [
            serde_json::json!({"controller_events":{"op":"tasks", "task_ids":["11111111111141118111111111111111"]}}),
            serde_json::json!({"controller_events":{"op":"repair"}}),
            serde_json::json!({"controller_health":true}),
        ] {
            cases.push((scope, process_request("task.list", body, 8)));
        }
    }
    for (scope, request) in cases {
        let fixture = FrozenFixture::new(vec![Ok(read_result(&request))], Vec::new());
        let runner = fixture.runner(scope);
        assert!(
            socket_result(&runner.run(&request).unwrap()),
            "scope {scope:?}"
        );
        assert_eq!(fixture.connector.frames(), [request.stdin.unwrap()]);
        assert!(fixture.raw.calls().is_empty());
        assert_eq!(fixture.forwards.opens(), 1);
        assert_eq!(
            fixture.pins.pin(),
            Some(Pin::from_identity(&scoped_identity()))
        );
    }
}

#[test]
fn excluded_families_delegate_identical_requests_without_setup() {
    let families = [
        "task.submit",
        "task.say",
        "task.cancel",
        "task.close",
        "task.batch",
        "checkpoint.submit",
        "task.reconcile",
        "task.publish-retry",
        "controller.drain",
        "controller.transfer.source",
        "controller.transfer.result",
        "controller.transfer.source.prepare",
        "controller.transfer.source.finish",
        "controller.transfer.result.prepare",
        "task.status",
        "task.list",
        "task.diff",
        "task.result",
        "controller.health",
    ];
    let mut requests: Vec<_> = families
        .iter()
        .map(|family| process_request(family, serde_json::json!({}), 1))
        .collect();
    requests.extend([true, false].map(|drained| {
        process_request(
            "controller.drain",
            serde_json::json!({"drained":drained}),
            2,
        )
    }));
    for scope in [
        ReadLoopScope::Wait,
        ReadLoopScope::LogsFollow,
        ReadLoopScope::EventsFollow,
        ReadLoopScope::Notify,
    ] {
        let fixture = FrozenFixture::new(
            Vec::new(),
            requests.iter().map(|_| Ok(marker("raw", 0))).collect(),
        );
        let runner = fixture.runner(scope);
        for request in &requests {
            assert_eq!(runner.run(request).unwrap().stdout, b"raw");
        }
        assert_eq!(fixture.raw.calls(), requests);
        assert_eq!(fixture.forwards.resolutions(), 0);
        assert_eq!(fixture.forwards.opens(), 0);
        assert_eq!(fixture.connector.connections(), 0);
        assert_eq!(fixture.pins.pin(), None);
    }
}

#[test]
fn identical_logs_outside_follow_scope_remain_raw() {
    let request = process_request(
        "task.logs",
        serde_json::json!({"task_id":"11111111111141118111111111111111"}),
        1,
    );
    for scope in [
        ReadLoopScope::Wait,
        ReadLoopScope::EventsFollow,
        ReadLoopScope::Notify,
    ] {
        let fixture = FrozenFixture::new(Vec::new(), vec![Ok(marker("raw", 0))]);
        let runner = fixture.runner(scope);
        assert_eq!(runner.run(&request).unwrap().stdout, b"raw");
        assert_eq!(
            fixture.raw.calls().as_slice(),
            std::slice::from_ref(&request)
        );
        assert_eq!(fixture.forwards.resolutions(), 0);
        assert_eq!(fixture.connector.connections(), 0);
        assert_eq!(fixture.pins.pin(), None);
    }
}

#[test]
fn custom_capture_limits_preserve_the_complete_raw_process_policy() {
    for (stdout_limit, stderr_limit) in [
        (8, 256 * 1024),
        (MAX_FRAME_BYTES + 4, 8),
        (2 * (MAX_FRAME_BYTES + 4), 256 * 1024),
        (MAX_FRAME_BYTES + 4, 512 * 1024),
    ] {
        let mut request = wait(1);
        request.policy.stdout_limit = stdout_limit;
        request.policy.stderr_limit = stderr_limit;
        request.policy.deadline = Duration::from_secs(60);
        let fixture =
            FrozenFixture::new(vec![Ok(read_result(&request))], vec![Ok(marker("raw", 0))]);
        let runner = fixture.runner(ReadLoopScope::Wait);
        assert_eq!(runner.run(&request).unwrap().stdout, b"raw");
        assert_eq!(fixture.raw.calls(), [request]);
        assert_eq!(fixture.forwards.resolutions(), 0);
        assert_eq!(fixture.connector.connections(), 0);
        assert_eq!(fixture.pins.pin(), None);
    }
}

#[test]
fn unrelated_processes_and_invalid_frames_remain_raw() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    let mut cases = Vec::new();
    let mut request = wait(1);
    request.program = "git".into();
    cases.push(request);
    let mut request = wait(1);
    request.args[9] = "other-host".into();
    cases.push(request);
    let mut request = wait(1);
    request.args[10] = "~/.local/bin/worker dashboard --controller-viewer".into();
    cases.push(request);
    let mut request = wait(1);
    request.args[10] = "~/.local/bin/worker host controller-upload-pack".into();
    cases.push(request);
    let mut request = wait(1);
    request.stdin.as_mut().unwrap().push(0);
    cases.push(request);
    let mut request = wait(1);
    request.stdin = None;
    cases.push(request);
    let mut request = wait(1);
    request
        .environment
        .push(("HOME".into(), "/elsewhere".into()));
    cases.push(request);
    let mut request = wait(1);
    request
        .args
        .splice(0..0, ["-L".into(), "arbitrary:forward".into()]);
    cases.push(request);
    for request in cases {
        assert_eq!(runner.run(&request).unwrap().stdout, b"raw");
        assert_eq!(
            fixture.0.raw_calls.lock().unwrap().last().unwrap().1,
            request
        );
    }
    assert!(fixture.steps().is_empty());
}

#[test]
fn raw_methods_preserve_new_session_and_borrowed_interruptible_modes() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    let request = wait(1);
    runner.run_in_new_session(&request).unwrap();
    let excluded = process_request("task.cancel", serde_json::json!({}), 2);
    runner.run_interruptible(&excluded, &|| false).unwrap();
    assert_eq!(
        *fixture.0.raw_calls.lock().unwrap(),
        [("new_session", request), ("interruptible", excluded)]
    );
    assert!(fixture.steps().is_empty());
}

#[test]
fn adapter_accepts_a_borrowed_raw_runner() {
    let request = wait(1);
    let fixture = FrozenFixture::new(vec![Ok(read_result(&request))], Vec::new());
    let runner = ChannelProcessRunner::new(
        &*fixture.raw as &dyn ProcessRunner,
        ReadLoopScope::Wait,
        route(),
        paths(),
        fixture.deps(),
    );
    assert!(socket_result(&runner.run(&request).unwrap()));
    assert!(fixture.raw.calls().is_empty());
}

#[test]
fn setup_is_lazy_ordered_and_reuses_one_session() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    assert!(fixture.steps().is_empty());
    assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
    assert!(socket_result(&runner.run(&wait(1)).unwrap()));
    assert_eq!(
        fixture.steps(),
        [
            "resolve", "identity", "pin", "open", "connect", "verify", "write", "read"
        ]
    );
    assert!(socket_result(&runner.run(&wait(2)).unwrap()));
    assert_eq!(fixture.count("resolve"), 1);
    assert_eq!(fixture.count("open"), 1);
    assert_eq!(fixture.count("connect"), 1);
    assert_eq!(fixture.0.frames.lock().unwrap().len(), 2);
}

#[test]
fn cold_short_budget_skips_setup_but_ready_session_can_read() {
    for budget in [Duration::from_millis(1), Duration::from_secs(5)] {
        let fixture = Fixture::new();
        let runner = fixture.runner(ReadLoopScope::Wait);
        let mut request = wait(1);
        request.policy.deadline = budget;
        assert_eq!(runner.run(&request).unwrap().stdout, b"raw");
        assert!(fixture.steps().is_empty());
        assert_eq!(fixture.0.raw_calls.lock().unwrap()[0].1, request);
        assert!(socket_result(&runner.run(&wait(2)).unwrap()));
        let mut request = wait(3);
        request.policy.deadline = budget;
        assert!(socket_result(&runner.run(&request).unwrap()));
        assert_eq!(fixture.count("open"), 1);
    }
}

#[test]
fn setup_guard_and_application_share_the_original_budget() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    fixture.0.advances.lock().unwrap().extend([
        ("resolve", Duration::from_millis(250)),
        ("identity", Duration::from_millis(500)),
        ("open", Duration::from_millis(750)),
        ("connect", Duration::from_secs(1)),
    ]);
    assert!(socket_result(&runner.run(&wait(1)).unwrap()));
    assert_eq!(
        *fixture.0.contexts.lock().unwrap(),
        [
            ("resolve", Duration::from_secs(5), Duration::from_secs(5)),
            (
                "identity",
                Duration::from_secs(5),
                Duration::from_millis(4750)
            ),
            ("open", Duration::from_secs(5), Duration::from_millis(4250)),
            (
                "connect",
                Duration::from_secs(5),
                Duration::from_millis(3500)
            ),
            (
                "write",
                Duration::from_secs(30),
                Duration::from_millis(27500)
            ),
            (
                "read",
                Duration::from_secs(30),
                Duration::from_millis(27500)
            ),
        ]
    );
}

#[test]
fn setup_failure_falls_back_before_application_with_remaining_budget() {
    for stage in ["resolve", "identity", "pin", "open", "connect"] {
        let fixture = Fixture::new();
        let runner = fixture.runner(ReadLoopScope::Wait);
        fixture.fail(
            stage,
            ChannelFailure::Unavailable(ChannelReason::ServiceUnavailable),
        );
        if stage != "pin" {
            fixture
                .0
                .advances
                .lock()
                .unwrap()
                .insert(stage, Duration::from_secs(2));
        }
        let request = wait(1);
        assert_eq!(
            runner.run(&request).unwrap().stdout,
            b"raw",
            "stage {stage}"
        );
        assert!(fixture.0.frames.lock().unwrap().is_empty());
        let calls = fixture.0.raw_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let mut expected = request.clone();
        if stage != "pin" {
            expected.policy.deadline = Duration::from_secs(28);
        }
        assert_eq!(calls[0], ("interruptible", expected));
    }
}

#[test]
fn setup_guard_expiry_can_use_the_live_application_budget() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    fixture
        .0
        .advances
        .lock()
        .unwrap()
        .insert("identity", Duration::from_secs(5));
    assert_eq!(runner.run(&wait(1)).unwrap().stdout, b"raw");
    assert_eq!(fixture.count("open"), 0);
    assert_eq!(
        fixture.0.raw_calls.lock().unwrap()[0].1.policy.deadline,
        Duration::from_secs(25)
    );
}

#[test]
fn unsafe_or_unavailable_identity_and_pin_never_send_socket_reads() {
    for case in [
        "route",
        "protocol",
        "channel",
        "feature",
        "pin",
        "account",
        "client",
        "generation",
    ] {
        let fixture = Fixture::new();
        let runner = fixture.runner(ReadLoopScope::Wait);
        match case {
            "route" => {
                let mut different = route();
                different.ssh = "elsewhere".into();
                fixture.0.identity.lock().unwrap().route_sha256 = different.digest().unwrap();
            }
            "protocol" => fixture.0.identity.lock().unwrap().service.protocol_version = 6,
            "channel" => fixture.0.identity.lock().unwrap().service.channel_version = 2,
            "feature" => fixture.0.identity.lock().unwrap().service.features.clear(),
            "pin" => fixture.fail(
                "pin",
                ChannelFailure::Unavailable(ChannelReason::UnsafePath),
            ),
            "account" | "client" => {
                let mut pinned = fixture.0.identity.lock().unwrap().clone();
                if case == "account" {
                    pinned.service.account.uid = 999;
                } else {
                    pinned.service.controller_client_id = ClientId::generate();
                }
                fixture.0.pins.verify_or_create(&paths(), &pinned).unwrap();
            }
            "generation" => fixture.fail(
                "connect",
                ChannelFailure::Unavailable(ChannelReason::PinMismatch),
            ),
            _ => unreachable!(),
        }
        assert_eq!(runner.run(&wait(1)).unwrap().stdout, b"raw", "case {case}");
        assert!(fixture.0.frames.lock().unwrap().is_empty());
        if case != "generation" {
            assert_eq!(fixture.count("open"), 0, "case {case}");
        }
    }
}

#[test]
fn absent_journal_does_not_disable_wait_or_logs_channel() {
    for (scope, request) in [
        (ReadLoopScope::Wait, wait(1)),
        (
            ReadLoopScope::LogsFollow,
            process_request(
                "task.logs",
                serde_json::json!({"task_id":"11111111111141118111111111111111"}),
                2,
            ),
        ),
    ] {
        let fixture = Fixture::new();
        let runner = fixture.runner(scope);
        assert!(
            fixture
                .0
                .identity
                .lock()
                .unwrap()
                .service
                .journal_id
                .is_none()
        );
        assert!(socket_result(&runner.run(&request).unwrap()));
    }
}

#[test]
fn concurrent_poll_uses_raw_without_queueing_or_second_setup() {
    let fixture = Fixture::new();
    let runner = Arc::new(fixture.runner(ReadLoopScope::Wait));
    let (entered, release) = fixture.gate("read");
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| runner.run(&wait(1)));
        entered
            .recv_timeout(Duration::from_secs(30))
            .expect("read entry");
        assert_eq!(runner.run(&wait(2)).unwrap().stdout, b"raw");
        assert_eq!(fixture.count("open"), 1);
        assert_eq!(fixture.0.frames.lock().unwrap().len(), 1);
        release.send(()).unwrap();
        assert!(socket_result(&reader.join().unwrap().unwrap()));
    });
}

#[test]
fn configured_ssh_file_is_matched_exactly() {
    let fixture = Fixture::new();
    let mut configured = route();
    configured.ssh_config_file = Some("/config/managed file".into());
    fixture.0.identity.lock().unwrap().route_sha256 = configured.digest().unwrap();
    let runner = ChannelProcessRunner::new(
        &fixture,
        ReadLoopScope::Wait,
        configured,
        paths(),
        fixture.deps(),
    );
    let mut request = wait(1);
    request
        .args
        .splice(0..0, ["-F".into(), "/config/managed file".into()]);
    assert!(socket_result(&runner.run(&request).unwrap()));
    let mut other = request.clone();
    other.args[1] = "/config/other".into();
    assert_eq!(runner.run(&other).unwrap().stdout, b"raw");
    assert_eq!(fixture.count("resolve"), 1);
    assert_eq!(fixture.0.raw_calls.lock().unwrap().last().unwrap().1, other);
}

#[test]
fn partial_write_or_lost_reply_has_one_identical_raw_fallback() {
    for (stage, reason) in [
        ("write", ChannelReason::ForwardLost),
        ("read", ChannelReason::ForwardLost),
        ("read", ChannelReason::Timeout),
        ("read", ChannelReason::InvalidFrame),
        ("read", ChannelReason::Busy),
    ] {
        let fixture = Fixture::new();
        let runner = fixture.runner(ReadLoopScope::Wait);
        fixture.fail(stage, ChannelFailure::Unavailable(reason));
        fixture
            .0
            .advances
            .lock()
            .unwrap()
            .insert(stage, Duration::from_secs(3));
        let mut request = wait(1);
        // Preserve noncanonical whitespace and the caller's complete original
        // frame, rather than rebuilding JSON after an ambiguous transmission.
        request.stdin = Some(
            mac_worker::controller::encode_frame(
                format!(
                    "  {}\n",
                    std::str::from_utf8(
                        mac_worker::controller::decode_frame(request.stdin.as_ref().unwrap())
                            .unwrap()
                    )
                    .unwrap()
                )
                .as_bytes(),
            )
            .unwrap(),
        );
        assert_eq!(
            runner.run(&request).unwrap().stdout,
            b"raw",
            "{stage}/{reason:?}"
        );
        assert_eq!(
            *fixture.0.frames.lock().unwrap(),
            [request.stdin.clone().unwrap()]
        );
        let calls = fixture.0.raw_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let mut expected = request;
        expected.policy.deadline = Duration::from_secs(27);
        assert_eq!(calls[0], ("interruptible", expected));
        assert_eq!(fixture.count("close"), 1);
        assert_eq!(fixture.count("cancel"), 1);
        let steps = fixture.steps();
        assert!(
            steps.iter().position(|step| *step == "close").unwrap()
                < steps.iter().position(|step| *step == "cancel").unwrap()
        );
    }
}

#[test]
fn fallback_error_is_returned_without_an_adapter_retry() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    fixture.fail(
        "read",
        ChannelFailure::Unavailable(ChannelReason::ForwardLost),
    );
    *fixture.0.raw_error.lock().unwrap() = Some(WorkerError::Io(std::io::Error::from(
        std::io::ErrorKind::BrokenPipe,
    )));
    assert!(
        matches!(runner.run(&wait(1)), Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe)
    );
    assert_eq!(fixture.0.frames.lock().unwrap().len(), 1);
    assert_eq!(fixture.0.raw_calls.lock().unwrap().len(), 1);
    assert_eq!(fixture.count("open"), 1);
}

#[test]
fn same_read_outer_retry_stays_raw_after_eligibility_advances() {
    let request = wait(1);
    let next = wait(2);
    let fixture = FrozenFixture::with_identities(
        vec![Ok(scoped_identity()), Ok(scoped_identity())],
        vec![
            Err(ChannelFailure::Unavailable(ChannelReason::ForwardLost)),
            Ok(read_result(&next)),
        ],
        vec![Ok(marker("raw", 0)), Ok(marker("raw", 0))],
    );
    let runner = fixture.runner(ReadLoopScope::Wait);
    assert_eq!(runner.run(&request).unwrap().stdout, b"raw");
    fixture.runtime.advance(Duration::from_secs(100));
    assert_eq!(runner.run(&request).unwrap().stdout, b"raw");
    assert_eq!(fixture.connector.frames(), [request.stdin.clone().unwrap()]);
    assert_eq!(fixture.raw.calls(), [request.clone(), request]);
    assert_eq!(fixture.forwards.opens(), 1);
    assert!(socket_result(&runner.run(&next).unwrap()));
    assert_eq!(fixture.forwards.opens(), 2);
    assert_eq!(fixture.connector.frames().len(), 2);
}

#[test]
fn unverified_complete_reply_never_replays_and_retires_the_command() {
    let fixture = FrozenFixture::new(
        vec![Err(ChannelFailure::UnverifiedReply)],
        vec![Ok(marker("raw", 0))],
    );
    let runner = fixture.runner(ReadLoopScope::Wait);
    assert!(matches!(
        runner.run(&wait(1)),
        Err(WorkerError::Unavailable(_))
    ));
    assert!(fixture.raw.calls().is_empty());
    assert_eq!(fixture.connector.closes(), 1);
    assert_eq!(fixture.forwards.cancels(), 1);
    fixture.runtime.advance(Duration::from_secs(100));
    assert_eq!(runner.run(&wait(2)).unwrap().stdout, b"raw");
    assert_eq!(fixture.forwards.opens(), 1);
    assert_eq!(fixture.connector.frames().len(), 1);
}

#[test]
fn verified_application_error_preserves_stdout_status_and_stderr() {
    for (exit, code) in [(0, "ok"), (69, "CURSOR_INVALID"), (75, "CONTROLLER_BUSY")] {
        let request = wait(1);
        let stdout = if exit == 0 {
            read_result(&request).stdout
        } else {
            encode_json_frame(
                &mac_worker::job::HostControlError::new(code, "fixture application outcome")
                    .unwrap(),
            )
            .unwrap()
        };
        let fixture = FrozenFixture::new(
            vec![Ok(ProcessResult {
                status: ExitStatus::from_raw(exit << 8),
                stdout: stdout.clone(),
                stderr: b"bounded original".to_vec(),
            })],
            Vec::new(),
        );
        let runner = fixture.runner(ReadLoopScope::Wait);
        let outcome = runner.run(&request).unwrap();
        assert_eq!(outcome.status.code(), Some(exit));
        assert_eq!(outcome.stdout, stdout);
        assert_eq!(outcome.stderr, b"bounded original");
        assert!(fixture.raw.calls().is_empty());
        assert_eq!(fixture.connector.closes(), 0);
    }
}

#[test]
fn changed_forward_binding_is_checked_before_application_bytes() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    assert!(socket_result(&runner.run(&wait(1)).unwrap()));
    fixture.fail(
        "verify",
        ChannelFailure::Unavailable(ChannelReason::UnsafePath),
    );
    assert_eq!(runner.run(&wait(2)).unwrap().stdout, b"raw");
    assert_eq!(fixture.0.frames.lock().unwrap().len(), 1);
    assert_eq!(fixture.count("close"), 1);
    assert_eq!(fixture.count("cancel"), 1);
}

#[test]
fn independent_borrowed_cancellation_stays_live_inside_every_stage() {
    for stage in ["resolve", "identity", "open", "connect", "write", "read"] {
        let fixture = Fixture::new();
        let runner = fixture.runner(ReadLoopScope::Wait);
        let (entered, release) = fixture.gate(stage);
        let stopped = Arc::new(AtomicBool::new(false));
        let local = std::rc::Rc::new(std::cell::Cell::new(0));
        let should_stop = || {
            local.set(local.get() + 1);
            stopped.load(Ordering::SeqCst)
        };
        std::thread::scope(|scope| {
            let signal = stopped.clone();
            scope.spawn(move || {
                entered
                    .recv_timeout(Duration::from_secs(30))
                    .expect("operation entry");
                signal.store(true, Ordering::SeqCst);
                release.send(()).unwrap();
            });
            assert!(
                matches!(
                    runner.run_interruptible(&wait(1), &should_stop),
                    Err(WorkerError::Process(ProcessError::Cancelled))
                ),
                "stage {stage}"
            );
        });
        assert!(!fixture.0.runtime.cancelled());
        assert!(
            fixture.0.raw_calls.lock().unwrap().is_empty(),
            "post-cancel fallback at {stage}"
        );
        if matches!(stage, "write" | "read") {
            assert_eq!(fixture.count("close"), 1, "stage {stage}");
        }
        if matches!(stage, "connect" | "write" | "read") {
            assert_eq!(fixture.count("cancel"), 1, "stage {stage}");
        }
    }
}

#[test]
fn cancellation_after_a_dependency_returns_success_still_closes_and_stops() {
    for stage in ["resolve", "identity", "open", "connect", "write", "read"] {
        let fixture = Fixture::new();
        let runner = fixture.runner(ReadLoopScope::Wait);
        let stopped = Arc::new(AtomicBool::new(false));
        *fixture.0.return_cancel.lock().unwrap() = Some((stage, stopped.clone()));
        let local = std::rc::Rc::new(());
        let should_stop = || {
            let _ = &local;
            stopped.load(Ordering::SeqCst)
        };
        assert!(
            matches!(
                runner.run_interruptible(&wait(1), &should_stop),
                Err(WorkerError::Process(ProcessError::Cancelled))
            ),
            "stage {stage}"
        );
        assert!(!fixture.0.runtime.cancelled());
        assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
        if matches!(stage, "connect" | "write" | "read") {
            assert_eq!(fixture.count("close"), 1, "stage {stage}");
        }
    }
}

#[test]
fn original_deadline_expiry_at_each_stage_never_falls_back() {
    for stage in ["resolve", "identity", "open", "connect", "write", "read"] {
        let fixture = Fixture::new();
        let runner = fixture.runner(ReadLoopScope::Wait);
        fixture
            .0
            .advances
            .lock()
            .unwrap()
            .insert(stage, Duration::from_secs(30));
        assert!(
            matches!(runner.run(&wait(1)), Err(WorkerError::Process(ProcessError::DeadlineExceeded { deadline })) if deadline == Duration::from_secs(30)),
            "stage {stage}"
        );
        assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
        if matches!(stage, "write" | "read") {
            assert_eq!(fixture.count("close"), 1);
        }
    }
}

#[test]
fn cancellation_before_next_call_closes_an_existing_session() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    assert!(socket_result(&runner.run(&wait(1)).unwrap()));
    let token = std::rc::Rc::new(true);
    assert!(matches!(
        runner.run_interruptible(&wait(2), &|| *token),
        Err(WorkerError::Process(ProcessError::Cancelled))
    ));
    assert_eq!(fixture.count("close"), 1);
    assert_eq!(fixture.count("cancel"), 1);
    assert_eq!(fixture.0.frames.lock().unwrap().len(), 1);
    assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
}

#[test]
fn command_cancellation_allows_only_clock_bounded_cleanup() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    assert!(socket_result(&runner.run(&wait(1)).unwrap()));
    fixture.0.runtime.advance(Duration::from_secs(10));
    fixture.0.runtime.cancel();
    assert!(matches!(
        runner.run(&wait(2)),
        Err(WorkerError::Process(ProcessError::Cancelled))
    ));
    assert_eq!(fixture.count("close"), 1);
    assert_eq!(fixture.count("cancel"), 1);
    assert_eq!(
        *fixture.0.cleanup_deadlines.lock().unwrap(),
        [Duration::from_secs(15)]
    );
    assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
}

#[test]
fn cleanup_time_consumes_original_budget_before_fallback() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    fixture.fail(
        "read",
        ChannelFailure::Unavailable(ChannelReason::ForwardLost),
    );
    fixture.0.advances.lock().unwrap().extend([
        ("read", Duration::from_secs(26)),
        ("cancel", Duration::from_secs(4)),
    ]);
    assert!(
        matches!(runner.run(&wait(1)), Err(WorkerError::Process(ProcessError::DeadlineExceeded { deadline })) if deadline == Duration::from_secs(30))
    );
    assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
    assert_eq!(fixture.count("close"), 1);
    assert_eq!(
        *fixture.0.cleanup_deadlines.lock().unwrap(),
        [Duration::from_secs(31)]
    );
}

#[test]
fn raw_fallback_preserves_the_live_borrowed_predicate() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    fixture.fail(
        "read",
        ChannelFailure::Unavailable(ChannelReason::ForwardLost),
    );
    let (entered, release) = fixture.gate("raw");
    let stopped = Arc::new(AtomicBool::new(false));
    let token = std::rc::Rc::new(());
    let should_stop = || {
        let _ = &token;
        stopped.load(Ordering::SeqCst)
    };
    std::thread::scope(|scope| {
        let signal = stopped.clone();
        scope.spawn(move || {
            entered
                .recv_timeout(Duration::from_secs(30))
                .expect("raw fallback entry");
            signal.store(true, Ordering::SeqCst);
            release.send(()).unwrap();
        });
        assert!(matches!(
            runner.run_interruptible(&wait(1), &should_stop),
            Err(WorkerError::Process(ProcessError::Cancelled))
        ));
    });
    assert!(!fixture.0.runtime.cancelled());
    assert_eq!(fixture.0.frames.lock().unwrap().len(), 1);
    assert_eq!(fixture.0.raw_calls.lock().unwrap().len(), 1);
}

#[test]
fn zero_call_budget_expires_without_application_or_setup() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    let mut request = wait(1);
    request.policy.deadline = Duration::ZERO;
    assert!(
        matches!(runner.run(&request), Err(WorkerError::Process(ProcessError::DeadlineExceeded { deadline })) if deadline.is_zero())
    );
    assert!(fixture.steps().is_empty());
    assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
}

#[test]
fn application_guard_caps_an_extended_process_policy() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    let mut request = wait(1);
    request.policy.deadline = Duration::from_secs(60);
    fixture
        .0
        .advances
        .lock()
        .unwrap()
        .insert("read", Duration::from_secs(30));
    assert!(
        matches!(runner.run(&request), Err(WorkerError::Process(ProcessError::DeadlineExceeded { deadline })) if deadline == Duration::from_secs(30))
    );
    assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
    assert_eq!(fixture.count("close"), 1);
}

#[test]
fn reconnect_eligibility_uses_one_two_four_then_five_seconds() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    let mut sequence = 0;
    for (index, delay) in [1, 2, 4, 5, 5].into_iter().enumerate() {
        fixture.fail(
            "read",
            ChannelFailure::Unavailable(ChannelReason::ForwardLost),
        );
        sequence += 1;
        assert_eq!(runner.run(&wait(sequence)).unwrap().stdout, b"raw");
        assert_eq!(fixture.count("open"), index + 1);
        let count = fixture.0.frames.lock().unwrap().len();
        fixture
            .0
            .runtime
            .advance(Duration::from_secs(delay) - Duration::from_millis(1));
        sequence += 1;
        assert_eq!(
            runner.run(&wait(sequence)).unwrap().stdout,
            b"raw",
            "delay {delay}"
        );
        assert_eq!(fixture.count("open"), index + 1);
        assert_eq!(fixture.0.frames.lock().unwrap().len(), count);
        fixture.0.runtime.advance(Duration::from_millis(1));
    }
    assert!(socket_result(&runner.run(&wait(sequence + 1)).unwrap()));
    assert_eq!(fixture.count("open"), 6);
}

#[test]
fn reconnect_backoff_resets_only_after_a_verified_application_reply() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    fixture.fail(
        "read",
        ChannelFailure::Unavailable(ChannelReason::ForwardLost),
    );
    assert_eq!(runner.run(&wait(1)).unwrap().stdout, b"raw");
    fixture.0.runtime.advance(Duration::from_secs(1));
    fixture.fail(
        "read",
        ChannelFailure::Unavailable(ChannelReason::ForwardLost),
    );
    assert_eq!(runner.run(&wait(2)).unwrap().stdout, b"raw");
    // The successful raw fallback did not reset the second channel backoff.
    fixture.0.runtime.advance(Duration::from_secs(1));
    assert_eq!(runner.run(&wait(3)).unwrap().stdout, b"raw");
    assert_eq!(fixture.count("open"), 2);
    fixture.0.runtime.advance(Duration::from_secs(1));
    assert!(socket_result(&runner.run(&wait(4)).unwrap()));
    fixture.fail(
        "read",
        ChannelFailure::Unavailable(ChannelReason::ForwardLost),
    );
    assert_eq!(runner.run(&wait(5)).unwrap().stdout, b"raw");
    fixture.0.runtime.advance(Duration::from_millis(999));
    assert_eq!(runner.run(&wait(6)).unwrap().stdout, b"raw");
    fixture.0.runtime.advance(Duration::from_millis(1));
    assert!(socket_result(&runner.run(&wait(7)).unwrap()));
    assert_eq!(fixture.count("open"), 4);
}

#[test]
fn recoverable_setup_and_application_failures_can_reconnect() {
    for (stage, reason) in [
        ("resolve", ChannelReason::ServiceUnavailable),
        ("identity", ChannelReason::ServiceUnavailable),
        ("open", ChannelReason::Busy),
        ("connect", ChannelReason::ForwardLost),
        ("read", ChannelReason::InvalidFrame),
        ("read", ChannelReason::Timeout),
    ] {
        let fixture = Fixture::new();
        let runner = fixture.runner(ReadLoopScope::Wait);
        fixture.fail(stage, ChannelFailure::Unavailable(reason));
        assert_eq!(runner.run(&wait(1)).unwrap().stdout, b"raw");
        let resolutions = fixture.count("resolve");
        fixture.0.runtime.advance(Duration::from_millis(999));
        assert_eq!(
            runner.run(&wait(2)).unwrap().stdout,
            b"raw",
            "{stage}/{reason:?}"
        );
        assert_eq!(fixture.count("resolve"), resolutions);
        fixture.0.runtime.advance(Duration::from_millis(1));
        assert!(
            socket_result(&runner.run(&wait(3)).unwrap()),
            "{stage}/{reason:?}"
        );
    }
}

#[test]
fn one_caller_owns_setup_while_a_concurrent_caller_uses_raw() {
    let fixture = Fixture::new();
    let runner = Arc::new(fixture.runner(ReadLoopScope::Wait));
    let (entered, release) = fixture.gate("resolve");
    std::thread::scope(|scope| {
        let opening = scope.spawn(|| runner.run(&wait(1)));
        entered
            .recv_timeout(Duration::from_secs(30))
            .expect("setup owner entry");
        assert_eq!(runner.run(&wait(2)).unwrap().stdout, b"raw");
        assert_eq!(fixture.count("resolve"), 1);
        assert_eq!(fixture.count("open"), 0);
        release.send(()).unwrap();
        assert!(socket_result(&opening.join().unwrap().unwrap()));
    });
    assert_eq!(fixture.count("open"), 1);
}

#[test]
fn unsupported_unsafe_and_mismatched_routes_retire_until_command_exit() {
    for (stage, reason) in [
        ("resolve", ChannelReason::Unsupported),
        ("resolve", ChannelReason::UnsafePath),
        ("pin", ChannelReason::PinMismatch),
        ("pin", ChannelReason::UnsafePath),
        ("connect", ChannelReason::PinMismatch),
        ("verify", ChannelReason::UnsafePath),
    ] {
        let fixture = Fixture::new();
        let runner = fixture.runner(ReadLoopScope::Wait);
        fixture.fail(stage, ChannelFailure::Unavailable(reason));
        assert_eq!(runner.run(&wait(1)).unwrap().stdout, b"raw");
        let resolutions = fixture.count("resolve");
        let opens = fixture.count("open");
        for sequence in 2..50 {
            fixture.0.runtime.advance(Duration::from_secs(60));
            assert_eq!(
                runner.run(&wait(sequence)).unwrap().stdout,
                b"raw",
                "{stage}/{reason:?}"
            );
        }
        assert_eq!(fixture.count("resolve"), resolutions);
        assert_eq!(fixture.count("open"), opens);
        assert!(fixture.0.frames.lock().unwrap().is_empty());
    }
}

#[test]
fn retained_cancel_from_exit_zero_error_keeps_one_owned_residue() {
    // T5 interprets the control exit/error/refusal evidence. T6 consumes only
    // its published Retained disposition; exit zero is never client proof.
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    *fixture.0.disposition.lock().unwrap() = ForwardDisposition::Retained;
    fixture.fail(
        "read",
        ChannelFailure::Unavailable(ChannelReason::ForwardLost),
    );
    assert_eq!(runner.run(&wait(1)).unwrap().stdout, b"raw");
    assert_eq!(runner.close(), ForwardDisposition::Retained);
    for sequence in 2..100 {
        fixture.0.runtime.advance(Duration::from_secs(60));
        assert_eq!(runner.run(&wait(sequence)).unwrap().stdout, b"raw");
    }
    assert_eq!(fixture.count("open"), 1);
    assert_eq!(fixture.count("close"), 1);
    assert_eq!(fixture.count("cancel"), 1);
    assert_eq!(fixture.0.frames.lock().unwrap().len(), 1);
    assert!(fixture.0.residue.load(Ordering::SeqCst));
    *fixture.0.disposition.lock().unwrap() = ForwardDisposition::Cleaned;
    assert_eq!(runner.close(), ForwardDisposition::Retained);
}

#[test]
fn retained_open_failure_never_attempts_another_allocation() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    *fixture.0.forward_failure.lock().unwrap() = Some(ForwardOpenFailure {
        failure: ChannelFailure::Unavailable(ChannelReason::ForwardLost),
        disposition: ForwardDisposition::Retained,
    });
    assert_eq!(runner.run(&wait(1)).unwrap().stdout, b"raw");
    for sequence in 2..100 {
        fixture.0.runtime.advance(Duration::from_secs(60));
        assert_eq!(runner.run(&wait(sequence)).unwrap().stdout, b"raw");
    }
    assert_eq!(fixture.count("open"), 1);
    assert_eq!(fixture.count("connect"), 0);
    assert_eq!(fixture.count("cancel"), 0);
    assert!(fixture.0.frames.lock().unwrap().is_empty());
    assert!(fixture.0.residue.load(Ordering::SeqCst));
    assert_eq!(runner.close(), ForwardDisposition::Retained);
}

#[test]
fn interrupted_unacknowledged_open_retains_and_permanently_retires() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    *fixture.0.forward_failure.lock().unwrap() = Some(ForwardOpenFailure {
        failure: ChannelFailure::Unavailable(ChannelReason::Cancelled),
        disposition: ForwardDisposition::Retained,
    });
    let (entered, release) = fixture.gate("open");
    let stopped = Arc::new(AtomicBool::new(false));
    let local = std::rc::Rc::new(());
    let should_stop = || {
        let _ = &local;
        stopped.load(Ordering::SeqCst)
    };
    std::thread::scope(|scope| {
        let signal = stopped.clone();
        scope.spawn(move || {
            entered
                .recv_timeout(Duration::from_secs(30))
                .expect("unacknowledged open entry");
            signal.store(true, Ordering::SeqCst);
            release.send(()).unwrap();
        });
        assert!(matches!(
            runner.run_interruptible(&wait(1), &should_stop),
            Err(WorkerError::Process(ProcessError::Cancelled))
        ));
    });
    assert!(!fixture.0.runtime.cancelled());
    *fixture.0.gate.lock().unwrap() = None;
    for sequence in 2..100 {
        fixture.0.runtime.advance(Duration::from_secs(60));
        assert_eq!(runner.run(&wait(sequence)).unwrap().stdout, b"raw");
    }
    assert_eq!(fixture.count("open"), 1);
    assert_eq!(fixture.count("connect"), 0);
    assert_eq!(fixture.count("cancel"), 0);
    assert!(fixture.0.frames.lock().unwrap().is_empty());
    assert!(fixture.0.residue.load(Ordering::SeqCst));
    assert_eq!(runner.close(), ForwardDisposition::Retained);
}

#[test]
fn close_and_drop_are_idempotent_and_do_not_reopen_the_command() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    assert!(socket_result(&runner.run(&wait(1)).unwrap()));
    assert_eq!(runner.close(), ForwardDisposition::Cleaned);
    assert_eq!(runner.close(), ForwardDisposition::Cleaned);
    fixture.0.runtime.advance(Duration::from_secs(60));
    assert_eq!(runner.run(&wait(2)).unwrap().stdout, b"raw");
    drop(runner);
    assert_eq!(fixture.count("open"), 1);
    assert_eq!(fixture.count("close"), 1);
    assert_eq!(fixture.count("cancel"), 1);
    assert!(!fixture.0.residue.load(Ordering::SeqCst));
}

#[test]
fn gate_logs_bytes_require_the_follow_loop_scope() {
    let request = request_fixture(
        "task.logs",
        serde_json::json!({"task_id": "0123456789ab4def8123456789abcdef"}),
    );
    assert!(eligible_read(ReadLoopScope::LogsFollow, &request));
    assert!(!eligible_read(ReadLoopScope::Wait, &request));
    assert!(!eligible_read(ReadLoopScope::EventsFollow, &request));
    assert!(!eligible_read(ReadLoopScope::Notify, &request));
}
