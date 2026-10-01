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

use super::*;
use crate::controller::channel::contracts::*;
use crate::{
    controller::{ControllerReadReply, ControllerRequest, encode_json_frame},
    job::{ClientId, ProcessIdentity},
    process::ProcessPolicy,
    protocol::PROTOCOL_VERSION,
};

#[derive(Default)]
struct ManualRuntime {
    now: Mutex<Duration>,
    cancelled: AtomicBool,
}
impl ManualRuntime {
    fn advance(&self, elapsed: Duration) {
        *self.now.lock().unwrap() += elapsed;
    }
}
impl ChannelRuntime for ManualRuntime {
    fn now(&self) -> Duration {
        *self.now.lock().unwrap()
    }
    fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
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
    pin: Mutex<Option<SocketIdentity>>,
    cleanup_deadlines: Mutex<Vec<Duration>>,
}
#[derive(Clone)]
struct Fixture(Arc<Data>);
impl Fixture {
    fn new() -> Self {
        let route = route();
        Self(Arc::new(Data {
            runtime: Arc::new(ManualRuntime::default()),
            identity: Mutex::new(SocketIdentity {
                route_sha256: route.digest().unwrap(),
                service: ServiceIdentity {
                    protocol_version: PROTOCOL_VERSION,
                    channel_version: CHANNEL_VERSION,
                    controller_client_id: ClientId::generate(),
                    account: ControllerAccount {
                        uid: 501,
                        username: "controller".into(),
                        home: "/Users/controller".into(),
                    },
                    leader: ProcessIdentity::new(42, 7).unwrap(),
                    service_generation: UuidString::new_v4(),
                    socket_path: "/Users/controller/state/rpc/s".into(),
                    features: vec!["controller.socket".into()],
                    journal_id: None,
                },
            }),
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
            pin: Mutex::new(None),
            cleanup_deadlines: Mutex::new(Vec::new()),
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
        self.0
            .failures
            .lock()
            .unwrap()
            .remove(name)
            .map_or(Ok(()), Err)
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
            return Err(crate::error::ProcessError::Cancelled.into());
        }
        self.raw("interruptible", request)
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
        let mut pin = self.0.pin.lock().unwrap();
        if let Some(pin) = &*pin
            && (pin.route_sha256 != identity.route_sha256
                || pin.service.account != identity.service.account
                || pin.service.controller_client_id != identity.service.controller_client_id)
        {
            return Err(ChannelFailure::Unavailable(ChannelReason::PinMismatch));
        }
        *pin = Some(identity.clone());
        Ok(())
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
            return Err(ForwardOpenFailure {
                failure,
                disposition: ForwardDisposition::Cleaned,
            });
        }
        if let Some(failure) = self.0.forward_failure.lock().unwrap().take() {
            return Err(failure);
        }
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
        *self.0.disposition.lock().unwrap()
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
                Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: encode_json_frame(&ControllerReadReply::from_request(
                        request,
                        serde_json::json!({"path":"socket"}),
                    ))
                    .unwrap(),
                    stderr: Vec::new(),
                })
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
    let cases = [
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
    for (scope, request) in cases {
        let fixture = Fixture::new();
        let runner = fixture.runner(scope);
        assert!(
            socket_result(&runner.run(&request).unwrap()),
            "scope {scope:?}"
        );
        assert_eq!(*fixture.0.frames.lock().unwrap(), [request.stdin.unwrap()]);
        assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
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
        "controller.transfer.source.prepare",
        "controller.transfer.source.finish",
        "controller.transfer.result.prepare",
        "task.status",
        "task.list",
        "task.diff",
        "task.result",
        "controller.health",
    ];
    for scope in [
        ReadLoopScope::Wait,
        ReadLoopScope::LogsFollow,
        ReadLoopScope::EventsFollow,
        ReadLoopScope::Notify,
    ] {
        let fixture = Fixture::new();
        let runner = fixture.runner(scope);
        for family in families {
            let request = process_request(family, serde_json::json!({}), 1);
            assert_eq!(runner.run(&request).unwrap().stdout, b"raw");
            assert_eq!(
                fixture.0.raw_calls.lock().unwrap().last().unwrap(),
                &("run", request)
            );
        }
        assert!(fixture.steps().is_empty());
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
        let fixture = Fixture::new();
        let runner = fixture.runner(scope);
        assert_eq!(runner.run(&request).unwrap().stdout, b"raw");
        assert!(fixture.steps().is_empty());
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
    let fixture = Fixture::new();
    let runner = ChannelProcessRunner::new(
        &fixture as &dyn ProcessRunner,
        ReadLoopScope::Wait,
        route(),
        paths(),
        fixture.deps(),
    );
    assert!(socket_result(&runner.run(&wait(1)).unwrap()));
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
                *fixture.0.pin.lock().unwrap() = Some(pinned);
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
            crate::controller::encode_frame(
                format!(
                    "  {}\n",
                    std::str::from_utf8(
                        crate::controller::decode_frame(request.stdin.as_ref().unwrap()).unwrap()
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
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    fixture.fail(
        "read",
        ChannelFailure::Unavailable(ChannelReason::ForwardLost),
    );
    let request = wait(1);
    assert_eq!(runner.run(&request).unwrap().stdout, b"raw");
    fixture.0.runtime.advance(Duration::from_secs(100));
    assert_eq!(runner.run(&request).unwrap().stdout, b"raw");
    assert_eq!(fixture.0.frames.lock().unwrap().len(), 1);
    assert_eq!(fixture.count("open"), 1);
    assert!(socket_result(&runner.run(&wait(2)).unwrap()));
    assert_eq!(fixture.count("open"), 2);
}

#[test]
fn unverified_complete_reply_never_replays_and_retires_the_command() {
    let fixture = Fixture::new();
    let runner = fixture.runner(ReadLoopScope::Wait);
    fixture
        .0
        .replies
        .lock()
        .unwrap()
        .push_back(Err(ChannelFailure::UnverifiedReply));
    assert!(matches!(
        runner.run(&wait(1)),
        Err(WorkerError::Unavailable(_))
    ));
    assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
    assert_eq!(fixture.count("close"), 1);
    assert_eq!(fixture.count("cancel"), 1);
    fixture.0.runtime.advance(Duration::from_secs(100));
    assert_eq!(runner.run(&wait(2)).unwrap().stdout, b"raw");
    assert_eq!(fixture.count("open"), 1);
    assert_eq!(fixture.0.frames.lock().unwrap().len(), 1);
}

#[test]
fn verified_application_error_preserves_stdout_status_and_stderr() {
    for (exit, code) in [(0, "ok"), (69, "CURSOR_INVALID"), (75, "CONTROLLER_BUSY")] {
        let fixture = Fixture::new();
        let runner = fixture.runner(ReadLoopScope::Wait);
        let request = wait(1);
        let parsed = crate::controller::decode_request(request.stdin.as_ref().unwrap()).unwrap();
        let stdout = if exit == 0 {
            encode_json_frame(&ControllerReadReply::from_request(
                &parsed,
                serde_json::json!({"path":"socket"}),
            ))
            .unwrap()
        } else {
            encode_json_frame(
                &crate::job::HostControlError::new(code, "fixture application outcome").unwrap(),
            )
            .unwrap()
        };
        fixture
            .0
            .replies
            .lock()
            .unwrap()
            .push_back(Ok(ProcessResult {
                status: ExitStatus::from_raw(exit << 8),
                stdout: stdout.clone(),
                stderr: b"bounded original".to_vec(),
            }));
        let outcome = runner.run(&wait(1)).unwrap();
        assert_eq!(outcome.status.code(), Some(exit));
        assert_eq!(outcome.stdout, stdout);
        assert_eq!(outcome.stderr, b"bounded original");
        assert!(fixture.0.raw_calls.lock().unwrap().is_empty());
        assert_eq!(fixture.count("close"), 0);
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
