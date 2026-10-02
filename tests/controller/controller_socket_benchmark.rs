//! Paired local fixture observations; elapsed times are never speed assertions.
use mac_worker::test_support::channel::{server_eligible_read, testing::request_fixture};
use serde_json::json;

use mac_worker::test_support::{
    agents::agent::{AgentKind, PermissionPolicy},
    channel::{
        client::ChannelProcessRunner,
        codec::{SessionCodec, io::FramedSocketConnector},
        contracts::*,
        files::PrivateChannelFiles,
        forward::MasterForwardControl,
        identity::StdioIdentitySource,
        pin::PrivatePinStore,
        server::NativeControl,
        testing::ScriptedImageSource,
    },
    client_state::ClientStateStore,
    controller::{
        ControllerLeader, ControllerRequest, controller_rpc_ssh_request, decode_frame,
        decode_request, encode_json_frame, parse_request,
        runtime::{
            LeaderChannel, LeaderChannelConfig, LeaderChannelDeps, LeaderChannelShutdown,
            SystemChannelRuntime,
        },
    },
    core::{
        config::{ControllerConfig, SshConfig},
        error::WorkerError,
        paths::PathLayout,
    },
    events::{
        EventBatch, EventCursor, JournalReader, NewEvent, Seq,
        journal::{ControllerJournal, JournalOptions},
    },
    host::process::{
        CleanupState, ProcessCompletion, ProcessPolicy, ProcessRequest, ProcessResult,
        ProcessRunner, SystemProcessRunner, TrackedProcessRunner,
    },
    task::model::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
        TurnTerminal,
    },
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsString,
    fs,
    io::{Read, Write},
    net::Shutdown,
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

const GUARD: Duration = Duration::from_secs(30);
const WARMUPS: usize = 10;
const SAMPLES: usize = 200;
const TEST_NAME: &str = "controller_socket_benchmark::fixture_transport_cost_observations";

#[test]
fn gate_measurement_reads_exclude_mutation_traffic() {
    assert!(server_eligible_read(&request_fixture(
        "task.wait.poll",
        json!({"run": "fixture"})
    )));
    assert!(!server_eligible_read(&request_fixture(
        "task.submit",
        json!({"run": "fixture"})
    )));
}

#[test]
#[ignore = "spawns real isolated RPC children and records 200 paired samples per class"]
fn fixture_transport_cost_observations() {
    // Isolate HOME used by the production SSH namespace without mutating the
    // environment of another test. The reexec runs only this ignored test.
    if std::env::var_os("P3D_ROOT").is_none() {
        let root = tempfile::Builder::new()
            .prefix("p3d")
            .tempdir_in("/private/tmp")
            .unwrap();
        fs::create_dir(root.path().join("h")).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--ignored", "--nocapture"])
            .env_clear()
            .env("P3D_ROOT", root.path())
            .env("HOME", root.path().join("h"))
            .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
            .env("MAC_WORKER_TEST_SSH", root.path().join("fake-ssh"))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let pid = child.id();
        let (tx, rx) = mpsc::sync_channel(1);
        let waiter = thread::spawn(move || {
            let _ = tx.send(child.wait());
        });
        let status = rx
            .recv_timeout(Duration::from_secs(1800))
            .unwrap_or_else(|_| {
                unsafe {
                    libc::kill(pid as i32, libc::SIGKILL);
                }
                panic!("observation child hang guard");
            })
            .unwrap();
        waiter.join().unwrap();
        assert!(status.success(), "isolated observation child failed");
        return;
    }
    let rows = fixture_observations();
    assert!(
        rows.len() >= 6,
        "missing paired stdio/socket observations for wait, logs and events"
    );
    for row in rows {
        assert_eq!(row["samples"], SAMPLES);
        assert_eq!(row["warmups"], WARMUPS);
        assert_eq!(row["mutation_channel_attempts"], 0);
        assert_eq!(row["worker_children"], row["rpc_reads"]);
        assert!(row["max_supervisors"].as_u64().unwrap() <= 8);
        if let Some(retained) = row["max_decoder_retained_bytes"].as_u64() {
            assert!(retained <= 1024 * 1024 + 4);
        }
        if row["transport"] == "socket" && row["scenario"].as_str().unwrap().starts_with("warm_") {
            assert_eq!(row["ssh_application_executions"], 0);
            assert_eq!(row["socket_children"], row["rpc_reads"]);
        }
        println!("{row}");
    }
}

fn fixture_observations() -> Vec<serde_json::Value> {
    let root = PathBuf::from(std::env::var_os("P3D_ROOT").unwrap())
        .canonicalize()
        .unwrap();
    write_fake_ssh(&root.join("fake-ssh"));
    let mux = Arc::new(FakeMux::new(root.join("h/.cache/mac-worker/ssh/m")));
    let raw = Fixture::new(&root.join("a"), mux.clone());
    let socket = Fixture::new(&root.join("b"), mux.clone());
    let mut rows = cold_cli_observations(&raw, &socket);
    assert_eq!(
        rows.len(),
        6,
        "missing complete paired real CLI cold lifetimes"
    );
    rows.extend(retained_observations(&raw, &socket));
    for class in [
        ReadClass::Wait,
        ReadClass::Logs,
        ReadClass::Events,
        ReadClass::WaitingEvents,
    ] {
        rows.extend(paired_rpc_observations(&raw, &socket, class, false));
    }
    rows.extend(paired_rpc_observations(
        &raw,
        &socket,
        ReadClass::Wait,
        true,
    ));
    // Drain writes publish journal records. Probe exclusions only after the
    // paired read observations so both roots retain the same 32-record seed.
    socket.reset();
    mutation_exclusion_probe(&socket);
    raw.shutdown();
    socket.shutdown();
    assert!(mux.forwards.lock().unwrap().is_empty());
    use sha2::{Digest, Sha256};
    let binary_sha256 = format!("{:x}", Sha256::digest(fs::read(&socket.installed).unwrap()));
    for row in &mut rows {
        row["build"] = json!({"package_version":env!("CARGO_PKG_VERSION"),"worker_sha256":binary_sha256,
            "os":std::env::consts::OS,"arch":std::env::consts::ARCH,"protocol_version":7,"channel_version":CHANNEL_VERSION});
        row["raw_mutation_probes"] = json!(2);
    }
    rows
}

fn ns(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap()
}

#[derive(Clone, Default)]
struct Stats {
    ssh_application_executions: u64,
    ssh_bootstrap_executions: u64,
    ssh_resolution_processes: u64,
    ssh_control_processes: u64,
    ssh_processes: u64,
    stdio_children: u64,
    socket_children: u64,
    socket_attempts: u64,
    mutation_channel_attempts: u64,
    allocations: u64,
    cancels: u64,
    pin_creates: u64,
    pin_verifies: u64,
    bytes_in: u64,
    bytes_out: u64,
    max_request_bytes: usize,
    max_reply_bytes: usize,
    max_decoder_retained_bytes: usize,
    active_supervisors: usize,
    max_supervisors: usize,
    child_ns: u64,
    rpc_ns: u64,
    setup_ns: u64,
    teardown_ns: u64,
    first_application: Option<Instant>,
    last_application: Option<Instant>,
    last_allocation: Option<PathBuf>,
    requested_wait_ms: u64,
}
type Metrics = Arc<Mutex<Stats>>;

#[derive(Default)]
struct Clock {
    start: Mutex<Option<Instant>>,
    advance_ns: AtomicU64,
}
impl Clock {
    fn advance(&self) {
        self.advance_ns
            .fetch_add(ns(Duration::from_secs(1)), Ordering::SeqCst);
    }
}
impl ChannelRuntime for Clock {
    fn now(&self) -> Duration {
        let mut start = self.start.lock().unwrap();
        let elapsed = start.get_or_insert_with(Instant::now).elapsed();
        elapsed + Duration::from_nanos(self.advance_ns.load(Ordering::SeqCst))
    }
    fn cancelled(&self) -> bool {
        false
    }
}

struct RpcProbe(Metrics);
impl ProcessRunner for RpcProbe {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        SystemProcessRunner.run(request)
    }
}
impl TrackedProcessRunner for RpcProbe {
    fn run_interruptible_with_cleanup(
        &self,
        request: &ProcessRequest,
        stop: &dyn Fn() -> bool,
    ) -> ProcessCompletion {
        let frame = request.stdin.as_ref().unwrap();
        let read = decode_request(frame).unwrap();
        {
            let mut stats = self.0.lock().unwrap();
            if !server_eligible_read(&read) {
                stats.mutation_channel_attempts += 1;
            }
            assert!(
                server_eligible_read(&read),
                "mutation reached RPC child supervisor"
            );
            stats.socket_children += 1;
            stats.bytes_in += frame.len() as u64;
            stats.max_request_bytes = stats.max_request_bytes.max(frame.len());
            stats.active_supervisors += 1;
            stats.max_supervisors = stats.max_supervisors.max(stats.active_supervisors);
            stats.first_application.get_or_insert_with(Instant::now);
            stats.requested_wait_ms += read.body()["wait_ms"].as_u64().unwrap_or(0);
        }
        let start = Instant::now();
        let result = SystemProcessRunner.run_interruptible_with_cleanup(request, stop);
        assert_eq!(result.cleanup, CleanupState::Completed);
        let mut stats = self.0.lock().unwrap();
        stats.child_ns += ns(start.elapsed());
        stats.active_supervisors -= 1;
        stats.last_application = Some(Instant::now());
        if let Ok(output) = &result.outcome {
            stats.bytes_out += output.stdout.len() as u64;
            stats.max_reply_bytes = stats.max_reply_bytes.max(output.stdout.len());
        }
        result
    }
}

type GenerationThread = (
    tokio::sync::oneshot::Sender<()>,
    thread::JoinHandle<LeaderChannelShutdown>,
);
struct Fixture {
    paths: PathLayout,
    laptop: PathLayout,
    installed: PathBuf,
    stats: Metrics,
    cursor: EventCursor,
    raw: Arc<FakeSsh>,
    server: Mutex<Option<GenerationThread>>,
}
impl Fixture {
    fn new(root: &Path, mux: Arc<FakeMux>) -> Self {
        fs::create_dir(root).unwrap();
        let home = root.join("h");
        fs::create_dir(&home).unwrap();
        let environment = BTreeMap::from([
            ("HOME".into(), home.clone().into_os_string()),
            ("XDG_CONFIG_HOME".into(), root.join("c").into_os_string()),
            ("XDG_STATE_HOME".into(), root.join("s").into_os_string()),
            ("XDG_CACHE_HOME".into(), root.join("k").into_os_string()),
            ("XDG_DATA_HOME".into(), root.join("d").into_os_string()),
            ("PATH".into(), "/usr/bin:/bin:/usr/sbin:/sbin".into()),
        ]);
        let paths = PathLayout::discover(None, &environment, &home).unwrap();
        fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
        fs::write(
            &paths.config,
            "version=1\n[[workers]]\nname='unused'\nssh='must-not-connect.invalid'\nslots=1\n",
        )
        .unwrap();
        seed_task(&paths);
        let installed = root.join("worker");
        fs::copy(env!("CARGO_BIN_EXE_worker"), &installed).unwrap();
        let metadata = fs::metadata(&installed).unwrap();
        let image = RunningImage {
            path: installed.clone(),
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let leader = Arc::new(ControllerLeader::acquire(&paths.controller_state_root()).unwrap());
        let event_runtime = mac_worker::test_support::runtime::ControllerEventRuntime::system();
        let journal = ControllerJournal::initialize_for_leader(
            &paths,
            &leader,
            JournalOptions {
                runtime: event_runtime.clone(),
            },
        )
        .unwrap();
        let cursor = journal
            .append(
                EventBatch::try_new(
                    (0..32)
                        .map(|i| NewEvent::ControllerDrainChanged {
                            drained: i % 2 == 0,
                        })
                        .collect(),
                )
                .unwrap(),
                mac_worker::test_support::events::EventRuntime::now(event_runtime.as_ref()) + GUARD,
            )
            .unwrap();
        assert_eq!(
            journal
                .window(
                    mac_worker::test_support::events::EventRuntime::now(event_runtime.as_ref())
                        + GUARD
                )
                .unwrap()
                .head_seq,
            Seq::new(32)
        );
        let stats = Arc::new(Mutex::new(Stats::default()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let config = LeaderChannelConfig {
            paths: paths.clone(),
            home: home.clone(),
            environment: environment.clone(),
            client_id: ClientStateStore::open(&paths.state).unwrap().client_id(),
            journal: Some(journal),
        };
        let deps = LeaderChannelDeps {
            image: Arc::new(ScriptedImageSource::new(vec![Ok(image)])),
            runner: Arc::new(RpcProbe(stats.clone())),
            runtime: Arc::new(SystemChannelRuntime::new(shutdown.clone())),
            control: Arc::new(NativeControl::new()),
        };
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let channel = LeaderChannel::start(config, leader, deps, shutdown);
                    tokio::time::timeout(GUARD, async {
                        while !channel.ready() {
                            assert!(!channel.retired());
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    ready_tx.send(()).unwrap();
                    let _ = stop_rx.await;
                    channel.shutdown().await
                })
        });
        ready_rx.recv_timeout(GUARD).unwrap();
        let laptop = PathLayout {
            config: root.join("lc"),
            state: root.join("ls/mac-worker"),
            cache: root.join("lk/mac-worker"),
            data: root.join("ld/mac-worker"),
        };
        let raw = Arc::new(FakeSsh {
            stats: stats.clone(),
            mux,
            installed: installed.clone(),
            config: paths.config.clone(),
            environment: environment.clone(),
        });
        raw.mux
            .metrics
            .lock()
            .unwrap()
            .insert(installed.to_str().unwrap().into(), stats.clone());
        Self {
            paths,
            laptop,
            installed,
            stats,
            cursor,
            raw,
            server: Mutex::new(Some((stop_tx, server))),
        }
    }
    fn adapter(
        &self,
        class: ReadClass,
        break_next: Arc<AtomicBool>,
        clock: Arc<Clock>,
    ) -> ChannelProcessRunner<Arc<FakeSsh>> {
        ChannelProcessRunner::new(
            self.raw.clone(),
            class.scope(),
            route(),
            self.laptop.clone(),
            ClientDeps {
                identity: Arc::new(IdentityProbe(self.stats.clone())),
                pins: Arc::new(PinProbe(self.stats.clone())),
                forwards: Arc::new(ForwardProbe {
                    inner: MasterForwardControl::new(
                        self.laptop.clone(),
                        Arc::new(PrivateChannelFiles::new()),
                        SshConfig {
                            multiplex: true,
                            config_file: None,
                        },
                    ),
                    stats: self.stats.clone(),
                }),
                connector: Arc::new(ConnectorProbe {
                    inner: FramedSocketConnector::new(Arc::new(CodecProbe(self.stats.clone()))),
                    stats: self.stats.clone(),
                    break_next,
                }),
                runtime: clock,
            },
        )
    }
    fn reset(&self) {
        let mut stats = self.stats.lock().unwrap();
        assert_eq!(stats.active_supervisors, 0);
        *stats = Stats::default();
    }
    fn prepare_cli(&self, socket: bool) {
        fs::write(&self.laptop.config, format!(
            "version=1\n[controller]\nenabled=true\nssh='fixture'\nremote_binary='~/.local/bin/worker'\n[ssh]\nmultiplex={socket}\n[[workers]]\nname='unused'\nssh='must-not-connect.invalid'\nslots=1\n"
        )).unwrap();
        let mut environment: BTreeMap<String, String> = self
            .raw
            .environment
            .iter()
            .map(|(key, value)| (key.to_str().unwrap().into(), value.to_str().unwrap().into()))
            .collect();
        environment.insert(
            "MAC_WORKER_TEST_SSH".into(),
            PathBuf::from(std::env::var_os("P3D_ROOT").unwrap())
                .join("fake-ssh")
                .to_str()
                .unwrap()
                .into(),
        );
        let remote = self.installed.with_file_name("remote-env.json");
        fs::write(&remote, serde_json::to_vec(&environment).unwrap()).unwrap();
        fs::set_permissions(remote, fs::Permissions::from_mode(0o600)).unwrap();
    }
    fn cli_request(&self, class: ReadClass) -> ProcessRequest {
        let mut args: Vec<OsString> = vec![
            "--config".into(),
            self.laptop.config.clone().into_os_string(),
            "--json".into(),
            "task".into(),
        ];
        match class {
            ReadClass::Wait => args.extend([
                "wait".into(),
                "--task-id".into(),
                ids().0.to_string().into(),
                "--timeout".into(),
                "30s".into(),
            ]),
            ReadClass::Logs => args.extend([
                "logs".into(),
                ids().0.to_string().into(),
                "--follow".into(),
                "--raw".into(),
            ]),
            _ => unreachable!("cold CLI observations cover wait and followed logs"),
        }
        ProcessRequest {
            program: self.installed.clone().into_os_string(),
            args,
            environment: vec![
                ("HOME".into(), std::env::var_os("HOME").unwrap()),
                ("PATH".into(), "/usr/bin:/bin:/usr/sbin:/sbin".into()),
                (
                    "XDG_CONFIG_HOME".into(),
                    self.installed.with_file_name("lconfig").into_os_string(),
                ),
                (
                    "XDG_STATE_HOME".into(),
                    self.laptop.state.parent().unwrap().as_os_str().to_owned(),
                ),
                (
                    "XDG_CACHE_HOME".into(),
                    self.laptop.cache.parent().unwrap().as_os_str().to_owned(),
                ),
                (
                    "XDG_DATA_HOME".into(),
                    self.laptop.data.parent().unwrap().as_os_str().to_owned(),
                ),
                (
                    "MAC_WORKER_TEST_SSH".into(),
                    PathBuf::from(std::env::var_os("P3D_ROOT").unwrap())
                        .join("fake-ssh")
                        .into_os_string(),
                ),
                (
                    "P3D_MASTER".into(),
                    self.raw.mux.path.clone().into_os_string(),
                ),
                ("P3D_WORKER".into(), self.installed.clone().into_os_string()),
                (
                    "P3D_CONFIG".into(),
                    self.paths.config.clone().into_os_string(),
                ),
                (
                    "P3D_REMOTE_ENV".into(),
                    self.installed
                        .with_file_name("remote-env.json")
                        .into_os_string(),
                ),
                ("P3D_RECORD".into(), "1".into()),
            ],
            environment_remove: vec![],
            stdin: None,
            policy: ProcessPolicy {
                stdout_limit: 8192,
                stderr_limit: 8192,
                deadline: GUARD,
            },
            isolate_parent_environment: true,
        }
    }
    fn shutdown(&self) {
        if let Some((tx, thread)) = self.server.lock().unwrap().take() {
            let _ = tx.send(());
            let proof = thread.join().unwrap();
            assert_eq!(proof.rpc.unknown, 0);
            assert_eq!(proof.files, ForwardDisposition::Cleaned);
            assert!(
                !self
                    .paths
                    .controller_state_root()
                    .join("rpc/service.json")
                    .exists()
            );
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn route() -> ConfiguredRoute {
    ConfiguredRoute {
        ssh: "fixture".into(),
        remote_binary: "~/.local/bin/worker".into(),
        ssh_config_file: None,
    }
}
fn ids() -> (TaskId, TurnId) {
    (
        TaskId::new(uuid::Uuid::from_u128(0x018f0f4a6b5c7d8e9f00112233445566)),
        TurnId::new(uuid::Uuid::from_u128(0x018f0f4a6b5c7d8e9f00112233445577)),
    )
}
fn seed_task(paths: &PathLayout) {
    let (task, turn) = ids();
    let store = ClientStateStore::open(&paths.state).unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        session_import: None,
        task_id: task,
        run_id: None,
        project_id: "a".repeat(64),
        worktree_id: "b".repeat(64),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: "a".repeat(40).parse().unwrap(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
        title: None,
        prompt: "fixture".into(),
        created_at_millis: 1,
    })
    .unwrap();
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        None,
        false,
        Some(meta.base_oid().clone()),
        None,
        vec![],
        vec![],
        None,
        vec![TurnSummary::new(
            1,
            turn,
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            None,
            false,
            None,
            Some(2),
        )],
        2,
    )
    .unwrap();
    store
        .create_task(
            LocalTaskRecord::new(
                meta,
                status,
                None,
                None,
                None,
                "c".repeat(64),
                None,
                true,
                None,
            )
            .unwrap(),
        )
        .unwrap();
    let log = "fixture log: representative readable output\n".repeat(96);
    store
        .open_runner_log(task, turn)
        .unwrap()
        .write_all(log.as_bytes())
        .unwrap();
    let checkpoint = paths
        .state
        .join("runners")
        .join(task.to_string())
        .join(format!("{turn}.checkpoint.json"));
    fs::write(&checkpoint, serde_json::to_vec(&json!({"version":1,"task_id":task,"turn_id":turn,"committed":{"offsets":[0,0],"len":log.len(),"accepted":true,
        "completion":{"outcome":TaskOutcome::Done,"drained":true}},"pending":null})).unwrap()).unwrap();
    fs::set_permissions(checkpoint, fs::Permissions::from_mode(0o600)).unwrap();
}

struct FakeSsh {
    stats: Metrics,
    mux: Arc<FakeMux>,
    installed: PathBuf,
    config: PathBuf,
    environment: BTreeMap<OsString, OsString>,
}
impl ProcessRunner for FakeSsh {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.run_interruptible(request, &|| false)
    }
    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        assert_eq!(
            request.program,
            PathBuf::from(std::env::var_os("P3D_ROOT").unwrap())
                .join("fake-ssh")
                .as_os_str()
        );
        let args = &request.args;
        let is_rpc = request.stdin.is_some();
        {
            let mut stats = self.stats.lock().unwrap();
            stats.ssh_processes += 1;
            if args.iter().any(|arg| arg == "-G") {
                stats.ssh_resolution_processes += 1;
            } else if let Some(operation) = args.windows(2).find(|pair| pair[0] == "-O") {
                stats.ssh_control_processes += 1;
                if operation[1] == "forward" {
                    stats.allocations += 1;
                }
                if operation[1] == "cancel" {
                    stats.cancels += 1;
                }
                assert!(
                    args.windows(2)
                        .any(|pair| pair[0] == "-F" && pair[1] == "/dev/null")
                );
            } else {
                assert!(
                    args.last()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .ends_with("host controller-rpc")
                );
                let frame = request.stdin.as_ref().unwrap();
                let read = decode_request(frame).unwrap();
                if read.body().get("controller_socket").is_some() {
                    stats.ssh_bootstrap_executions += 1;
                } else {
                    stats.ssh_application_executions += 1;
                    stats.stdio_children += 1;
                }
                stats.bytes_in += frame.len() as u64;
                stats.max_request_bytes = stats.max_request_bytes.max(frame.len());
            }
        }
        let mut local = request.clone();
        local.environment = self.environment.clone().into_iter().collect();
        local.environment.extend([
            ("P3D_MASTER".into(), self.mux.path.clone().into_os_string()),
            ("P3D_WORKER".into(), self.installed.clone().into_os_string()),
            ("P3D_CONFIG".into(), self.config.clone().into_os_string()),
            ("MAC_WORKER_TEST_SSH".into(), request.program.clone()),
        ]);
        local.isolate_parent_environment = true;
        let start = Instant::now();
        let output = SystemProcessRunner.run_interruptible(&local, stop);
        let mut stats = self.stats.lock().unwrap();
        if is_rpc
            && request.stdin.as_ref().is_some_and(|frame| {
                decode_request(frame)
                    .unwrap()
                    .body()
                    .get("controller_socket")
                    .is_none()
            })
        {
            stats.rpc_ns += ns(start.elapsed());
        }
        if let Ok(output) = &output {
            stats.bytes_out += output.stdout.len() as u64;
            stats.max_reply_bytes = stats.max_reply_bytes.max(output.stdout.len());
        }
        output
    }
}

fn write_fake_ssh(path: &Path) {
    fs::write(path, r#"#!/usr/bin/python3
import json, os, socket, struct, subprocess, sys, time
args = sys.argv[1:]
record = os.environ.get('P3D_RECORD') == '1'
def mux(message):
    if record:
        message['fixture'] = os.environ['P3D_WORKER']
    with socket.socket(socket.AF_UNIX) as stream:
        stream.settimeout(30)
        stream.connect(os.environ['P3D_MASTER'])
        data = json.dumps(message).encode()
        stream.sendall(struct.pack('>I', len(data)) + data)
        with stream.makefile('rb') as reader:
            return json.loads(reader.readline())
if '-G' in args:
    if record:
        mux({'op':'resolution'})
    print('controlpath ' + os.environ['P3D_MASTER'])
    sys.exit(0)
if '-O' in args:
    assert args[args.index('-F') + 1] == '/dev/null', args
    op = args[args.index('-O') + 1]
    pair = args[args.index('-L') + 1] if '-L' in args else None
    result = mux({'op':op, 'pair':pair})
    if not result['ok']:
        print('mux_client_forward: fixture cancellation unacknowledged', file=sys.stderr)
    sys.exit(result['exit'])
assert args[-1].endswith('host controller-rpc'), args
frame = sys.stdin.buffer.read()
command = [os.environ['P3D_WORKER'], '--config', os.environ['P3D_CONFIG'], 'host', 'controller-rpc']
if not record:
    sys.exit(subprocess.run(command, input=frame).returncode)
request = json.loads(frame[4:])
assert len(frame) == 4 + struct.unpack('>I', frame[:4])[0]
bootstrap = 'controller_socket' in request['body']
assert bootstrap or request['command'] in ('task.wait.poll', 'task.logs', 'task.list')
mux({'op':'rpc_start', 'bootstrap':bootstrap, 'bytes':len(frame), 'wait_ms':request['body'].get('wait_ms', 0)})
with open(os.environ['P3D_REMOTE_ENV']) as source:
    remote_environment = json.load(source)
start = time.perf_counter_ns()
output = subprocess.run(command, input=frame, capture_output=True, env=remote_environment, timeout=30)
assert len(output.stdout) <= 1024 * 1024 + 4
mux({'op':'rpc_end', 'bootstrap':bootstrap, 'bytes':len(output.stdout), 'worker_ns':time.perf_counter_ns() - start})
sys.stdout.buffer.write(output.stdout)
sys.stderr.buffer.write(output.stderr)
sys.exit(output.returncode)
"#).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

// Telemetry is sent by actual fake-SSH processes only for fresh CLI commands.
// Per-RPC scenarios use the instrumented ProcessRunner instead, never both.
fn record_cli_ssh(request: &Value, stats: &mut Stats) {
    match request["op"].as_str().unwrap() {
        "resolution" => {
            stats.ssh_processes += 1;
            stats.ssh_resolution_processes += 1;
        }
        "check" | "forward" | "cancel" => {
            stats.ssh_processes += 1;
            stats.ssh_control_processes += 1;
            match request["op"].as_str().unwrap() {
                "forward" => {
                    stats.allocations += 1;
                    let (local, _) = request["pair"].as_str().unwrap().split_once(':').unwrap();
                    stats.last_allocation = Some(local.into());
                }
                "cancel" => stats.cancels += 1,
                _ => {}
            }
        }
        "rpc_start" => {
            stats.ssh_processes += 1;
            if request["bootstrap"].as_bool().unwrap() {
                stats.ssh_bootstrap_executions += 1;
            } else {
                stats.ssh_application_executions += 1;
                stats.stdio_children += 1;
                stats.first_application.get_or_insert_with(Instant::now);
                stats.requested_wait_ms += request["wait_ms"].as_u64().unwrap();
            }
            let bytes = request["bytes"].as_u64().unwrap() as usize;
            assert!(bytes <= 1024 * 1024 + 4);
            stats.bytes_in += bytes as u64;
            stats.max_request_bytes = stats.max_request_bytes.max(bytes);
        }
        "rpc_end" => {
            if !request["bootstrap"].as_bool().unwrap() {
                stats.child_ns += request["worker_ns"].as_u64().unwrap();
                stats.last_application = Some(Instant::now());
            }
            let bytes = request["bytes"].as_u64().unwrap() as usize;
            assert!(bytes <= 1024 * 1024 + 4);
            stats.bytes_out += bytes as u64;
            stats.max_reply_bytes = stats.max_reply_bytes.max(bytes);
        }
        other => panic!("unexpected CLI telemetry: {other}"),
    }
}

// Test-only mux producer. Its cancel acknowledgement follows listener closure
// and relay joins; it deliberately leaves the inode for production refusal proof.
struct FakeMux {
    path: PathBuf,
    forwards: Arc<Mutex<HashMap<String, Relay>>>,
    retain_cancel: Arc<AtomicBool>,
    metrics: Arc<Mutex<HashMap<String, Metrics>>>,
    stop: Arc<AtomicBool>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}
impl FakeMux {
    fn new(path: PathBuf) -> Self {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::set_permissions(path.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
        let listener = UnixListener::bind(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let forwards = Arc::new(Mutex::new(HashMap::<String, Relay>::new()));
        let retain_cancel = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(Mutex::new(HashMap::<String, Metrics>::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (owned_forwards, retain, stopped) =
            (forwards.clone(), retain_cancel.clone(), stop.clone());
        let recorded = metrics.clone();
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                if stopped.load(Ordering::SeqCst) {
                    break;
                }
                let Some(mut stream) = accepted_fixture_stream(stream) else {
                    continue;
                };
                let mut prefix = [0; 4];
                // The ordinary transport may probe the master and immediately close.
                if stream.read_exact(&mut prefix).is_err() {
                    continue;
                }
                let length = u32::from_be_bytes(prefix) as usize;
                assert!(length <= 8192);
                let mut bytes = vec![0; length];
                stream.read_exact(&mut bytes).unwrap();
                let request: Value = serde_json::from_slice(&bytes).unwrap();
                if let Some(fixture) = request["fixture"].as_str() {
                    let stats = recorded.lock().unwrap().get(fixture).unwrap().clone();
                    record_cli_ssh(&request, &mut stats.lock().unwrap());
                }
                let retained = request["op"] == "cancel" && retain.load(Ordering::SeqCst);
                match request["op"].as_str().unwrap() {
                    "resolution" | "rpc_start" | "rpc_end" => {}
                    "check" => assert!(request["pair"].is_null()),
                    "forward" => {
                        let pair = request["pair"].as_str().unwrap();
                        let (local, remote) = pair.split_once(':').unwrap();
                        assert!(
                            owned_forwards
                                .lock()
                                .unwrap()
                                .insert(pair.into(), Relay::new(local.into(), remote.into()))
                                .is_none()
                        );
                    }
                    "cancel" if !retained => {
                        owned_forwards
                            .lock()
                            .unwrap()
                            .remove(request["pair"].as_str().unwrap())
                            .unwrap()
                            .stop();
                    }
                    "cancel" => {}
                    other => panic!("unexpected fake mux operation: {other}"),
                }
                writeln!(stream, "{}", json!({"ok":!retained,"exit":0})).unwrap();
            }
        });
        Self {
            path,
            forwards,
            retain_cancel,
            metrics,
            stop,
            thread: Mutex::new(Some(handle)),
        }
    }
}
impl Drop for FakeMux {
    fn drop(&mut self) {
        for (_, relay) in self.forwards.lock().unwrap().drain() {
            relay.stop();
        }
        self.stop.store(true, Ordering::SeqCst);
        let _ = UnixStream::connect(&self.path);
        self.thread.lock().unwrap().take().unwrap().join().unwrap();
    }
}
struct Relay {
    path: PathBuf,
    stop: Arc<AtomicBool>,
    streams: Arc<Mutex<Vec<UnixStream>>>,
    thread: thread::JoinHandle<()>,
}
impl Relay {
    fn new(path: PathBuf, remote: PathBuf) -> Self {
        let listener = UnixListener::bind(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let streams = Arc::new(Mutex::new(Vec::new()));
        let (stopped, live) = (stop.clone(), streams.clone());
        let thread = thread::spawn(move || {
            for connection in listener.incoming() {
                if stopped.load(Ordering::SeqCst) {
                    break;
                }
                let Some(mut local) = accepted_fixture_stream(connection) else {
                    continue;
                };
                let mut upstream = UnixStream::connect(&remote).unwrap();
                upstream.set_read_timeout(Some(GUARD)).unwrap();
                *live.lock().unwrap() =
                    vec![local.try_clone().unwrap(), upstream.try_clone().unwrap()];
                let (mut send, mut receive) =
                    (local.try_clone().unwrap(), upstream.try_clone().unwrap());
                let upload = thread::spawn(move || {
                    let _ = std::io::copy(&mut send, &mut receive);
                    let _ = receive.shutdown(Shutdown::Write);
                });
                let _ = std::io::copy(&mut upstream, &mut local);
                let _ = local.shutdown(Shutdown::Write);
                upload.join().unwrap();
                live.lock().unwrap().clear();
            }
        });
        Self {
            path,
            stop,
            streams,
            thread,
        }
    }
    fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        for stream in self.streams.lock().unwrap().iter() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        let _ = UnixStream::connect(&self.path);
        self.thread.join().unwrap();
    }
}

fn accepted_fixture_stream(result: std::io::Result<UnixStream>) -> Option<UnixStream> {
    let mut stream = result.unwrap();
    match stream.set_read_timeout(Some(GUARD)) {
        Ok(()) => Some(stream),
        // Darwin can reject SO_RCVTIMEO after a short-lived liveness/refusal
        // probe has already disconnected. Prove EOF before discarding it.
        Err(error) if error.raw_os_error() == Some(libc::EINVAL) => {
            stream.set_nonblocking(true).unwrap();
            assert_eq!(
                stream.read(&mut [0]).unwrap(),
                0,
                "timeout failure must be an already closed fixture probe"
            );
            None
        }
        Err(error) => panic!("fixture socket timeout failed: {error}"),
    }
}

struct IdentityProbe(Metrics);
impl IdentitySource for IdentityProbe {
    fn read(
        &self,
        raw: &dyn ProcessRunner,
        route: &ConfiguredRoute,
        master: Option<&MasterPlan>,
        ctx: &ClientContext<'_>,
    ) -> Result<SocketIdentity, ChannelFailure> {
        let start = Instant::now();
        let result = StdioIdentitySource::new().read(raw, route, master, ctx);
        self.0.lock().unwrap().setup_ns += ns(start.elapsed());
        result
    }
}
struct PinProbe(Metrics);
impl PinStore for PinProbe {
    fn verify_or_create(
        &self,
        paths: &PathLayout,
        identity: &SocketIdentity,
    ) -> Result<(), ChannelFailure> {
        let exists = paths
            .controller_cache_root()
            .join("channel/pins")
            .join(format!("{}.json", identity.route_sha256))
            .exists();
        let start = Instant::now();
        let result = PrivatePinStore::new().verify_or_create(paths, identity);
        let mut stats = self.0.lock().unwrap();
        stats.setup_ns += ns(start.elapsed());
        if exists {
            stats.pin_verifies += 1;
        } else {
            stats.pin_creates += 1;
        }
        result
    }
    fn repin(
        &self,
        _: &PathLayout,
        _: &SocketIdentity,
        _: mac_worker::test_support::host::job::ClientId,
    ) -> Result<(), ChannelFailure> {
        panic!("observation must not repin")
    }
}
struct ForwardProbe {
    inner: MasterForwardControl,
    stats: Metrics,
}
impl ForwardControl for ForwardProbe {
    fn resolve(
        &self,
        raw: &dyn ProcessRunner,
        route: &ConfiguredRoute,
        ctx: &ClientContext<'_>,
    ) -> Result<MasterPlan, ChannelFailure> {
        let start = Instant::now();
        let result = self.inner.resolve(raw, route, ctx);
        self.stats.lock().unwrap().setup_ns += ns(start.elapsed());
        result
    }
    fn open(
        &self,
        raw: &dyn ProcessRunner,
        master: &MasterPlan,
        identity: &SocketIdentity,
        ctx: &ClientContext<'_>,
    ) -> Result<Box<dyn ForwardLease>, ForwardOpenFailure> {
        let start = Instant::now();
        let result = self.inner.open(raw, master, identity, ctx);
        self.stats.lock().unwrap().setup_ns += ns(start.elapsed());
        result.map(|inner| {
            Box::new(LeaseProbe {
                inner,
                stats: self.stats.clone(),
            }) as Box<dyn ForwardLease>
        })
    }
}
struct LeaseProbe {
    inner: Box<dyn ForwardLease>,
    stats: Metrics,
}
impl ForwardLease for LeaseProbe {
    fn local_socket(&self) -> &Path {
        self.inner.local_socket()
    }
    fn verify(&self) -> Result<(), ChannelFailure> {
        self.inner.verify()
    }
    fn cancel(&mut self, raw: &dyn ProcessRunner, ctx: &CleanupContext) -> ForwardDisposition {
        let start = Instant::now();
        let result = self.inner.cancel(raw, ctx);
        self.stats.lock().unwrap().teardown_ns += ns(start.elapsed());
        result
    }
}
struct ConnectorProbe {
    inner: FramedSocketConnector,
    stats: Metrics,
    break_next: Arc<AtomicBool>,
}
impl SocketConnector for ConnectorProbe {
    fn connect(
        &self,
        path: &Path,
        identity: &SocketIdentity,
        ctx: &ClientContext<'_>,
    ) -> Result<Box<dyn SocketSession>, ChannelFailure> {
        let start = Instant::now();
        let result = self.inner.connect(path, identity, ctx);
        self.stats.lock().unwrap().setup_ns += ns(start.elapsed());
        result.map(|inner| {
            Box::new(SessionProbe {
                inner,
                stats: self.stats.clone(),
                break_next: self.break_next.clone(),
            }) as Box<dyn SocketSession>
        })
    }
}
struct SessionProbe {
    inner: Box<dyn SocketSession>,
    stats: Metrics,
    break_next: Arc<AtomicBool>,
}
impl SocketSession for SessionProbe {
    fn exchange(
        &mut self,
        frame: &[u8],
        request: &ControllerRequest,
        ctx: &ClientContext<'_>,
    ) -> Result<ProcessResult, ChannelFailure> {
        self.stats.lock().unwrap().socket_attempts += 1;
        let start = Instant::now();
        let result = if self.break_next.swap(false, Ordering::SeqCst) {
            self.inner.close();
            Err(ChannelFailure::Unavailable(ChannelReason::ForwardLost))
        } else {
            self.inner.exchange(frame, request, ctx)
        };
        self.stats.lock().unwrap().rpc_ns += ns(start.elapsed());
        result
    }
    fn close(&mut self) {
        self.inner.close();
    }
}
struct CodecProbe(Metrics);
impl ChannelCodec for CodecProbe {
    fn decoder(&self) -> Box<dyn FrameDecoder> {
        Box::new(DecoderProbe {
            inner: SessionCodec::new().decoder(),
            stats: self.0.clone(),
        })
    }
    fn encode_hello(&self, identity: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        SessionCodec::new().encode_hello(identity)
    }
    fn decode_hello(&self, payload: &[u8]) -> Result<SocketIdentity, ChannelFailure> {
        SessionCodec::new().decode_hello(payload)
    }
    fn encode_ready(&self, identity: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        SessionCodec::new().encode_ready(identity)
    }
    fn decode_ready(
        &self,
        payload: &[u8],
        expected: &SocketIdentity,
    ) -> Result<(), ChannelFailure> {
        SessionCodec::new().decode_ready(payload, expected)
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
struct DecoderProbe {
    inner: Box<dyn FrameDecoder>,
    stats: Metrics,
}
impl FrameDecoder for DecoderProbe {
    fn feed(&mut self, bytes: &[u8]) -> Result<DecodeProgress, ChannelFailure> {
        let progress = self.inner.feed(bytes)?;
        let mut stats = self.stats.lock().unwrap();
        stats.max_decoder_retained_bytes = stats
            .max_decoder_retained_bytes
            .max(self.inner.retained_bytes())
            .max(progress.payload.as_ref().map_or(0, Vec::len));
        stats.max_reply_bytes = stats.max_reply_bytes.max(
            progress
                .payload
                .as_ref()
                .map_or(0, |payload| payload.len() + 4),
        );
        Ok(progress)
    }
    fn retained_bytes(&self) -> usize {
        self.inner.retained_bytes()
    }
}

#[derive(Clone, Copy)]
enum ReadClass {
    Wait,
    Logs,
    Events,
    WaitingEvents,
}
impl ReadClass {
    fn name(self) -> &'static str {
        match self {
            Self::Wait => "wait_zero",
            Self::Logs => "logs_zero",
            Self::Events => "events_zero",
            Self::WaitingEvents => "events_requested_wait",
        }
    }
    fn scope(self) -> ReadLoopScope {
        match self {
            Self::Wait => ReadLoopScope::Wait,
            Self::Logs => ReadLoopScope::LogsFollow,
            _ => ReadLoopScope::EventsFollow,
        }
    }
    fn requested_wait_ms(self) -> u64 {
        if matches!(self, Self::WaitingEvents) {
            5
        } else {
            0
        }
    }
    fn verify(self, value: &Value) {
        match self {
            Self::Wait => {
                assert_eq!(value["quiescent"], true);
                assert_eq!(value["exit_code"], 0);
                assert_eq!(value["task_ids"], json!([ids().0]));
            }
            Self::Logs => {
                assert_eq!(value["complete"], true);
                assert_eq!(value["next_offset"], 4224);
                assert_eq!(value["turn_id"], json!(ids().1));
            }
            Self::Events | Self::WaitingEvents => {
                assert_eq!(value["type"], "batch");
                assert_eq!(value["next_after"]["seq"], "32");
                assert_eq!(
                    value["events"].as_array().unwrap().len(),
                    if matches!(self, Self::Events) { 32 } else { 0 }
                );
            }
        }
    }
    fn request(self, fixture: &Fixture) -> ProcessRequest {
        let (command, body) = match self {
            Self::Wait => ("task.wait.poll", json!({"task_id":ids().0})),
            Self::Logs => (
                "task.logs",
                json!({"task_id":ids().0,"follow":true,"offset":0,"limit":8192,"wait_ms":0}),
            ),
            Self::Events | Self::WaitingEvents => {
                let mut cursor = fixture.cursor;
                if matches!(self, Self::Events) {
                    cursor.seq = Seq::ZERO;
                }
                (
                    "task.list",
                    json!({"controller_events":{"op":"read","after":cursor,"limit":32,"wait_ms":self.requested_wait_ms()}}),
                )
            }
        };
        let read = parse_request(&serde_json::to_vec(&json!({"protocol_version":7,"request_id":uuid::Uuid::new_v4().simple().to_string(),"command":command,"body":body})).unwrap()).unwrap();
        let mut process = controller_rpc_ssh_request(&ControllerConfig {
            enabled: true,
            ssh: "fixture".into(),
            remote_binary: "~/.local/bin/worker".into(),
        })
        .unwrap();
        process.stdin = Some(encode_json_frame(&json!({"protocol_version":7,"request_id":read.request_id(),"command":command,"body":body})).unwrap());
        process
    }
}
fn read_value(output: ProcessResult) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: Value = serde_json::from_slice(decode_frame(&output.stdout).unwrap()).unwrap();
    let mut result = reply["result"].clone();
    fn comparable(value: &mut Value) {
        match value {
            Value::Object(fields) => {
                fields.remove("journal_id");
                fields.remove("time_millis");
                for value in fields.values_mut() {
                    comparable(value);
                }
            }
            Value::Array(values) => {
                for value in values {
                    comparable(value);
                }
            }
            _ => {}
        }
    }
    comparable(&mut result);
    result
}

#[derive(Default)]
struct Samples {
    command: Vec<u64>,
    rpc: Vec<u64>,
    setup: Vec<u64>,
    teardown: Vec<u64>,
    child: Vec<u64>,
}
struct RowSpec<'a> {
    scenario: &'a str,
    transport: &'a str,
    class: ReadClass,
    reads: u64,
    fallback: u64,
    reconnect: u64,
    teardown: u64,
}
impl Samples {
    fn push(&mut self, start: Instant, before: Stats, after: &Stats) {
        self.command.push(ns(start.elapsed()));
        self.rpc.push(after.rpc_ns - before.rpc_ns);
        self.setup.push(after.setup_ns - before.setup_ns);
        self.teardown.push(after.teardown_ns - before.teardown_ns);
        self.child.push(after.child_ns - before.child_ns);
    }
    fn row(&self, spec: RowSpec<'_>, stats: &Stats) -> Value {
        let RowSpec {
            scenario,
            transport,
            class,
            reads,
            fallback,
            reconnect,
            teardown,
        } = spec;
        let mut row = json!({"scenario":scenario,"transport":transport,"order":"paired; even stdio/socket, odd socket/stdio","samples":SAMPLES,"warmups":WARMUPS,
            "unit":"ns","command":summary(&self.command),"rpc":summary(&self.rpc),"setup":summary(&self.setup),"teardown":summary(&self.teardown),
            "socket_worker_startup_handler_framing":if transport == "socket" { Some(summary(&self.child)) } else { None },
            "requested_wait_ms_per_read":class.requested_wait_ms(),"requested_wait_total_ms":class.requested_wait_ms()*reads,
            "rpc_reads":reads,"worker_children":stats.stdio_children+stats.socket_children,"socket_children":stats.socket_children,
            "worker_children_including_setup":stats.stdio_children+stats.socket_children+stats.ssh_bootstrap_executions,
            "ssh_application_executions":stats.ssh_application_executions,"ssh_bootstrap_executions":stats.ssh_bootstrap_executions,
            "ssh_resolution_processes":stats.ssh_resolution_processes,"ssh_control_processes":stats.ssh_control_processes,"ssh_processes":stats.ssh_processes,
            "os_processes":stats.ssh_processes+stats.stdio_children+stats.ssh_bootstrap_executions+stats.socket_children,
            "socket_application_attempts":stats.socket_attempts,"mutation_channel_attempts":stats.mutation_channel_attempts,"allocations":stats.allocations,"cancels":stats.cancels});
        row.as_object_mut().unwrap().extend(json!({
            "pin_creates":stats.pin_creates,"pin_verifies":stats.pin_verifies,"bytes_in":stats.bytes_in,"bytes_out":stats.bytes_out,
            "max_request_frame_bytes":stats.max_request_bytes,"max_reply_frame_bytes":stats.max_reply_bytes,"max_decoder_retained_bytes":stats.max_decoder_retained_bytes,
            "configured_scratch_bytes":READ_SCRATCH_BYTES,"max_supervisors":stats.max_supervisors,"fallback":fallback,"reconnect":reconnect,
            "cancel_disposition":"cleaned","final_teardown_ns":teardown,"live_ssh_S_measured":false,
            "cost_model":{"stdio":"S+W+H+D","socket":"W+H+D+O","expected_difference":"S-O"},
            "limitation":"local Python fake SSH/mux cost; no live network/session S or latency improvement assertion"}).as_object().unwrap().clone());
        row
    }
}
fn summary(values: &[u64]) -> Value {
    assert!(!values.is_empty());
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let percentile = |percent: usize| sorted[(percent * sorted.len()).div_ceil(100) - 1];
    json!({"mean":values.iter().map(|value| *value as f64).sum::<f64>()/values.len() as f64,"p50":percentile(50),"p95":percentile(95)})
}

fn cold_cli_observations(raw: &Fixture, socket: &Fixture) -> Vec<Value> {
    raw.prepare_cli(false);
    socket.prepare_cli(true);
    let mut rows = Vec::new();
    let pin_path = socket
        .laptop
        .controller_cache_root()
        .join("channel/pins")
        .join(format!("{}.json", route().digest().unwrap()));
    for (class, create_pin) in [
        (ReadClass::Wait, true),
        (ReadClass::Wait, false),
        (ReadClass::Logs, false),
    ] {
        let scenario = format!(
            "cold_cli_{}_pin_{}",
            class.name(),
            if create_pin { "create" } else { "verify" }
        );
        eprintln!("observing {scenario}: {WARMUPS} warmups, {SAMPLES} alternating pairs");
        let mut samples = [Samples::default(), Samples::default()];
        for index in 0..WARMUPS + SAMPLES {
            if index == WARMUPS {
                raw.reset();
                socket.reset();
            }
            let mut values = [Vec::new(), Vec::new()];
            for path in if index % 2 == 0 { [0, 1] } else { [1, 0] } {
                let fixture = if path == 0 { raw } else { socket };
                let prior_pin = if path == 1 {
                    if create_pin && pin_path.exists() {
                        fs::remove_file(&pin_path).unwrap();
                    }
                    if create_pin {
                        None
                    } else {
                        Some(fs::read(&pin_path).unwrap())
                    }
                } else {
                    None
                };
                {
                    let mut stats = fixture.stats.lock().unwrap();
                    stats.first_application = None;
                    stats.last_application = None;
                    stats.last_allocation = None;
                }
                let before = fixture.stats.lock().unwrap().clone();
                let request = fixture.cli_request(class);
                let start = Instant::now();
                let completion =
                    SystemProcessRunner.run_interruptible_with_cleanup(&request, &|| false);
                let end = Instant::now();
                assert_eq!(completion.cleanup, CleanupState::Completed);
                let output = completion.outcome.unwrap();
                assert!(
                    output.status.success(),
                    "{scenario}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                assert!(output.stderr.is_empty());
                match class {
                    ReadClass::Wait => {
                        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
                        assert_eq!(report["task_ids"], json!([ids().0]));
                        assert_eq!(report["exit_code"], 0);
                    }
                    ReadClass::Logs => assert_eq!(
                        output.stdout,
                        "fixture log: representative readable output\n"
                            .repeat(96)
                            .into_bytes()
                    ),
                    _ => unreachable!(),
                }
                values[path] = output.stdout;
                let mut after = fixture.stats.lock().unwrap();
                assert_eq!(after.active_supervisors, 0);
                let reads = if matches!(class, ReadClass::Logs) {
                    2
                } else {
                    1
                };
                assert_eq!(
                    after.stdio_children + after.socket_children
                        - before.stdio_children
                        - before.socket_children,
                    reads
                );
                if path == 1 {
                    assert_eq!(
                        after.ssh_application_executions, 0,
                        "real CLI must route all loop reads through socket"
                    );
                    assert_eq!(after.allocations - before.allocations, 1);
                    assert_eq!(after.cancels - before.cancels, 1);
                    assert_eq!(
                        after.ssh_bootstrap_executions - before.ssh_bootstrap_executions,
                        1
                    );
                    assert_eq!(
                        after.ssh_resolution_processes - before.ssh_resolution_processes,
                        1
                    );
                    assert_eq!(
                        after.ssh_control_processes - before.ssh_control_processes,
                        3
                    );
                    let local = after.last_allocation.as_ref().unwrap();
                    assert!(!local.exists(), "completed CLI must remove its socket");
                    assert!(
                        !local.parent().unwrap().exists(),
                        "completed CLI must remove its allocation"
                    );
                    let bytes = fs::read(&pin_path).unwrap();
                    let pin: Pin = serde_json::from_slice(&bytes).unwrap();
                    pin.validate().unwrap();
                    assert_eq!(pin.route_sha256, route().digest().unwrap());
                    assert_eq!(
                        pin.controller_client_id,
                        ClientStateStore::open(&socket.paths.state)
                            .unwrap()
                            .client_id()
                    );
                    if let Some(prior) = prior_pin {
                        assert_eq!(bytes, prior, "verification must preserve the stable pin");
                        after.pin_verifies += 1;
                    } else {
                        after.pin_creates += 1;
                    }
                } else {
                    assert_eq!(after.allocations, 0);
                    assert_eq!(
                        after.ssh_application_executions - before.ssh_application_executions,
                        reads
                    );
                }
                assert!(fixture.raw.mux.forwards.lock().unwrap().is_empty());
                assert!(
                    fixture.raw.mux.path.exists(),
                    "command owns only its forward, not the shared master"
                );
                if index >= WARMUPS {
                    let first = after.first_application.unwrap();
                    let last = after.last_application.unwrap();
                    assert!(start <= first && first <= last && last <= end);
                    let sample = &mut samples[path];
                    sample.command.push(ns(end.duration_since(start)));
                    sample.setup.push(ns(first.duration_since(start)));
                    sample.rpc.push(ns(last.duration_since(first)));
                    sample.teardown.push(ns(end.duration_since(last)));
                    sample.child.push(after.child_ns - before.child_ns);
                }
            }
            assert_eq!(values[0], values[1], "paired real CLI output must match");
        }
        for (path, samples) in samples.iter().enumerate() {
            let fixture = if path == 0 { raw } else { socket };
            let stats = fixture.stats.lock().unwrap();
            let mut row = samples.row(
                RowSpec {
                    scenario: &scenario,
                    transport: if path == 0 { "stdio" } else { "socket" },
                    class,
                    reads: SAMPLES as u64
                        * if matches!(class, ReadClass::Logs) {
                            2
                        } else {
                            1
                        },
                    fallback: 0,
                    reconnect: 0,
                    teardown: 0,
                },
                &stats,
            );
            row["measurement_scope"] =
                json!("complete fresh CLI launch through exit, capture and proven child cleanup");
            row["component_boundaries"] = json!(
                "setup: before first application child; RPC: first start to last completed child (includes health/log sequence); teardown: last child through rendering, cancel, refusal proof, deletion and exit"
            );
            row["cli_processes"] = json!(SAMPLES);
            row["os_processes"] = json!(row["os_processes"].as_u64().unwrap() + SAMPLES as u64);
            row["master_state"] =
                json!("preexisting private fixture master; never shortened production namespace");
            row["pin_state"] = json!(if create_pin {
                "absent before each command"
            } else {
                "existing stable pin"
            });
            row["requested_wait_ms_per_read"] = Value::Null;
            row["requested_wait_total_ms"] = json!(stats.requested_wait_ms);
            row["waiting_state"] = json!(
                "quiescent wait / completed readable logs; long-poll budget supplied but no deliberate wait"
            );
            row["max_decoder_retained_bytes"] = Value::Null;
            row["buffer_observation"] = json!(
                "actual child request/reply frame maxima; default CLI decoder cannot be instrumented; configured one-frame cap and 8-KiB scratch reported"
            );
            row["configured_frame_payload_bytes"] = json!(MAX_FRAME_BYTES);
            row["socket_application_attempts"] = json!(stats.socket_children);
            rows.push(row);
        }
    }
    rows
}

fn paired_rpc_observations(
    raw: &Fixture,
    socket: &Fixture,
    class: ReadClass,
    recovery: bool,
) -> Vec<Value> {
    let fault = Arc::new(AtomicBool::new(false));
    let clock = Arc::new(Clock::default());
    let adapter = socket.adapter(class, fault.clone(), clock.clone());
    let mut samples = [Samples::default(), Samples::default()];
    for index in 0..WARMUPS + SAMPLES {
        if index == WARMUPS {
            raw.reset();
            socket.reset();
        }
        let mut values = [Value::Null, Value::Null];
        for path in if index % 2 == 0 { [0, 1] } else { [1, 0] } {
            let fixture = if path == 0 { raw } else { socket };
            let before = fixture.stats.lock().unwrap().clone();
            let start = Instant::now();
            if recovery && path == 1 {
                fault.store(true, Ordering::SeqCst);
            }
            values[path] = read_value(
                if path == 0 {
                    fixture.raw.run(&class.request(fixture))
                } else {
                    adapter.run(&class.request(fixture))
                }
                .unwrap(),
            );
            class.verify(&values[path]);
            if recovery {
                if path == 1 {
                    clock.advance();
                }
                let next = read_value(
                    if path == 0 {
                        fixture.raw.run(&class.request(fixture))
                    } else {
                        adapter.run(&class.request(fixture))
                    }
                    .unwrap(),
                );
                assert_eq!(values[path], next);
            }
            if index >= WARMUPS {
                samples[path].push(start, before, &fixture.stats.lock().unwrap());
            }
        }
        assert_eq!(
            values[0], values[1],
            "comparable seeded roots must produce equivalent reads"
        );
    }
    let close_start = Instant::now();
    assert_eq!(adapter.close(), ForwardDisposition::Cleaned);
    let teardown = ns(close_start.elapsed());
    assert!(socket.raw.mux.forwards.lock().unwrap().is_empty());
    let reads = SAMPLES as u64 * if recovery { 2 } else { 1 };
    let scenario = if recovery {
        "fallback_reconnect_wait_zero".into()
    } else {
        format!("warm_{}", class.name())
    };
    samples
        .iter()
        .enumerate()
        .map(|(path, samples)| {
            let fixture = if path == 0 { raw } else { socket };
            let stats = fixture.stats.lock().unwrap();
            assert_eq!(stats.stdio_children + stats.socket_children, reads);
            assert_eq!(stats.active_supervisors, 0);
            if path == 1 && recovery {
                assert_eq!(stats.ssh_application_executions, SAMPLES as u64);
                assert_eq!(stats.socket_children, SAMPLES as u64);
                assert_eq!(stats.allocations, SAMPLES as u64);
            }
            samples.row(
                RowSpec {
                    scenario: &scenario,
                    transport: if path == 0 { "stdio" } else { "socket" },
                    class,
                    reads,
                    fallback: if path == 1 && recovery {
                        SAMPLES as u64
                    } else {
                        0
                    },
                    reconnect: if path == 1 && recovery {
                        SAMPLES as u64
                    } else {
                        0
                    },
                    teardown: if path == 1 { teardown } else { 0 },
                },
                &stats,
            )
        })
        .collect()
}

fn mutation_exclusion_probe(fixture: &Fixture) {
    let adapter = fixture.adapter(
        ReadClass::Wait,
        Arc::new(AtomicBool::new(false)),
        Arc::new(Clock::default()),
    );
    for drained in [true, false] {
        let mut process = ReadClass::Wait.request(fixture);
        process.stdin = Some(
            encode_json_frame(
                &json!({"protocol_version":7,"request_id":uuid::Uuid::new_v4().simple().to_string(),
            "command":"controller.drain","body":{"drained":drained}}),
            )
            .unwrap(),
        );
        assert_eq!(
            read_value(adapter.run(&process).unwrap()),
            json!({"drained":drained})
        );
    }
    let stats = fixture.stats.lock().unwrap();
    assert_eq!(stats.ssh_application_executions, 2);
    assert_eq!(stats.socket_attempts, 0);
    assert_eq!(stats.socket_children, 0);
    assert_eq!(stats.allocations, 0);
    drop(stats);
    assert_eq!(adapter.close(), ForwardDisposition::Cleaned);
}

fn retained_observations(raw: &Fixture, socket: &Fixture) -> Vec<Value> {
    let fault = Arc::new(AtomicBool::new(false));
    let clock = Arc::new(Clock::default());
    let adapter = socket.adapter(ReadClass::Wait, fault.clone(), clock.clone());
    for _ in 0..WARMUPS {
        read_value(adapter.run(&ReadClass::Wait.request(socket)).unwrap());
        read_value(raw.raw.run(&ReadClass::Wait.request(raw)).unwrap());
    }
    socket.raw.mux.retain_cancel.store(true, Ordering::SeqCst);
    fault.store(true, Ordering::SeqCst);
    let trigger = Instant::now();
    read_value(adapter.run(&ReadClass::Wait.request(socket)).unwrap());
    let trigger_ns = ns(trigger.elapsed());
    let (pair, local) = {
        let forwards = socket.raw.mux.forwards.lock().unwrap();
        assert_eq!(forwards.len(), 1);
        let (pair, relay) = forwards.iter().next().unwrap();
        (pair.clone(), relay.path.clone())
    };
    let binding = fs::symlink_metadata(&local).unwrap();
    raw.reset();
    socket.reset();
    let mut samples = [Samples::default(), Samples::default()];
    for index in 0..SAMPLES {
        clock.advance();
        let mut values = [Value::Null, Value::Null];
        for path in if index % 2 == 0 { [0, 1] } else { [1, 0] } {
            let fixture = if path == 0 { raw } else { socket };
            let before = fixture.stats.lock().unwrap().clone();
            let start = Instant::now();
            values[path] = read_value(
                if path == 0 {
                    fixture.raw.run(&ReadClass::Wait.request(fixture))
                } else {
                    adapter.run(&ReadClass::Wait.request(fixture))
                }
                .unwrap(),
            );
            samples[path].push(start, before, &fixture.stats.lock().unwrap());
        }
        assert_eq!(values[0], values[1]);
    }
    assert_eq!(adapter.close(), ForwardDisposition::Retained);
    let after = fs::symlink_metadata(&local).unwrap();
    assert_eq!((after.dev(), after.ino()), (binding.dev(), binding.ino()));
    assert!(local.parent().unwrap().exists());
    let mut rows = Vec::new();
    for (path, samples) in samples.iter().enumerate() {
        let stats = if path == 0 {
            raw.stats.lock().unwrap()
        } else {
            socket.stats.lock().unwrap()
        };
        assert_eq!(stats.allocations, 0);
        assert_eq!(stats.cancels, 0);
        assert_eq!(stats.socket_attempts, 0);
        let mut row = samples.row(
            RowSpec {
                scenario: "retired_after_unacknowledged_cancel",
                transport: if path == 0 { "stdio" } else { "socket" },
                class: ReadClass::Wait,
                reads: SAMPLES as u64,
                fallback: 0,
                reconnect: 0,
                teardown: 0,
            },
            &stats,
        );
        if path == 1 {
            row["cancel_disposition"] = json!("retained");
            row["retained_allocations"] = json!(1);
            row["eligibility_advances_after_retirement"] = json!(SAMPLES);
            row["retirement_trigger_ns"] = json!(trigger_ns);
            row["retirement_trigger_fallbacks"] = json!(1);
        }
        rows.push(row);
    }
    // Dismantle the test producer only after observing retained exact bindings.
    // Production has preserved the socket/directory and performs no deletion.
    socket.raw.mux.retain_cancel.store(false, Ordering::SeqCst);
    socket
        .raw
        .mux
        .forwards
        .lock()
        .unwrap()
        .remove(&pair)
        .unwrap()
        .stop();
    rows
}
