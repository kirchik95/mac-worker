//! Controller channel integration: isolated leaders, scoped loops and recovery.
use mac_worker::controller::channel::{server_eligible_read, testing::request_fixture};
use serde_json::json;

#[test]
fn gate_raw_operator_setters_are_ineligible() {
    for command in ["controller.drain", "task.reconcile", "task.publish-retry"] {
        assert!(!server_eligible_read(&request_fixture(command, json!({}))));
    }
}

// Share T7a's isolated leader fixtures; T7b's loop/route cases follow this block.
mod t7a {
    use mac_worker::{
        controller::{
            ControllerRequest, channel::contracts::*, channel::testing::request_fixture,
            decode_frame, encode_json_frame,
        },
        paths::PathLayout,
    };
    use serde_json::{Value, json};
    use std::{
        collections::BTreeMap,
        ffi::{OsStr, OsString},
        fs,
        io::{BufRead, BufReader, Write},
        os::unix::fs::{MetadataExt, PermissionsExt},
        path::PathBuf,
        process::{Child, Command, Output, Stdio},
        sync::mpsc,
        time::{Duration, Instant},
    };

    const GUARD: Duration = Duration::from_secs(30);

    // Metadata-only retention tests: these bytes are never executed. Advancing
    // the injected clock must never accelerate unlinking a real worker image.
    mod link_retention {
        use super::*;
        use mac_worker::controller::{
            ControllerLeader,
            channel::{
                files::{LeaderSocketLease, RetentionTime, bind_leader_at},
                testing::{ManualRuntime, identity_fixture},
            },
        };
        use std::cell::Cell;

        struct Fixture {
            _temp: tempfile::TempDir,
            paths: PathLayout,
            image: RunningImage,
            clock: ManualRuntime,
            epoch: Cell<u64>,
        }
        struct Generation {
            _leader: ControllerLeader,
            lease: LeaderSocketLease,
        }
        impl Fixture {
            fn new() -> Self {
                let temp = tempfile::tempdir_in("/private/tmp").unwrap();
                let root = temp.path().canonicalize().unwrap();
                let paths = PathLayout {
                    config: root.join("c"),
                    state: root.join("s"),
                    cache: root.join("k"),
                    data: root.join("d"),
                };
                let path = root.join("metadata-only-image");
                fs::write(&path, b"never execute this fixture").unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
                let metadata = fs::metadata(&path).unwrap();
                Self {
                    _temp: temp,
                    paths,
                    image: RunningImage {
                        path,
                        device: metadata.dev(),
                        inode: metadata.ino(),
                    },
                    clock: ManualRuntime::default(),
                    epoch: Cell::new(1),
                }
            }
            fn start(&self) -> Generation {
                let leader =
                    ControllerLeader::acquire(&self.paths.controller_state_root()).unwrap();
                let generation = UuidString::new_v4();
                let now = RetentionTime::new(self.epoch.get().to_string(), self.clock.now());
                let mut lease =
                    bind_leader_at(&self.paths, &leader, &self.image, &generation, &now)
                        .unwrap_or_else(|error| {
                            panic!(
                                "bind failed: {error:?}; entries {:?}",
                                fs::read_dir(self.paths.controller_state_root().join("rpc"))
                                    .unwrap()
                                    .map(|entry| entry.unwrap().file_name())
                                    .collect::<Vec<_>>()
                            )
                        });
                let mut service = identity_fixture().service;
                service.leader = leader.identity();
                service.service_generation = generation;
                service.socket_path = self.paths.controller_state_root().join("rpc/s");
                lease.publish(&service).unwrap();
                Generation {
                    _leader: leader,
                    lease,
                }
            }
            fn stop(&self, mut generation: Generation, proven: bool) -> PinnedExecutable {
                let image = generation.lease.executable();
                let now = RetentionTime::new(self.epoch.get().to_string(), self.clock.now());
                assert_eq!(
                    generation.lease.withdraw_at(proven, &now),
                    if proven {
                        ForwardDisposition::Cleaned
                    } else {
                        ForwardDisposition::Retained
                    }
                );
                image
            }
            fn links(&self) -> usize {
                fs::read_dir(self.paths.controller_state_root().join("rpc"))
                    .unwrap()
                    .filter_map(Result::ok)
                    .filter(|entry| {
                        let name = entry.file_name();
                        let name = name.to_string_lossy();
                        name.len() == 33 && name.starts_with('e')
                    })
                    .count()
            }
        }

        #[test]
        fn image_link_retention_current_shutdown_keeps_link_without_discovery() {
            let fixture = Fixture::new();
            let image = fixture.stop(fixture.start(), true);
            assert!(
                image.path.exists(),
                "RPC exit/shutdown must retain the current generation image"
            );
            assert!(!fixture.paths.controller_state_root().join("rpc/s").exists());
            assert!(
                !fixture
                    .paths
                    .controller_state_root()
                    .join("rpc/service.json")
                    .exists()
            );
            assert_eq!(fixture.links(), 1);
        }

        #[test]
        fn image_link_retention_previous_survives_new_generation_even_past_grace() {
            let fixture = Fixture::new();
            let old = fixture.stop(fixture.start(), true);
            fixture.clock.advance(Duration::from_secs(3600));
            let next = fixture.start();
            assert!(
                old.path.exists(),
                "the previous generation is excluded from cleanup"
            );
            assert!(next.lease.executable().path.exists());
            assert_eq!(fixture.links(), 2);
            fixture.stop(next, true);
            assert!(old.path.exists());
        }

        #[test]
        fn image_link_retention_older_link_waits_for_grace_and_normal_residue_is_two() {
            let fixture = Fixture::new();
            let first = fixture.stop(fixture.start(), true);
            let second = fixture.stop(fixture.start(), true);
            fixture.clock.advance(Duration::from_secs(600));
            let third = fixture.start();
            assert!(
                first.path.exists(),
                "exactly ten minutes is not older than the grace period"
            );
            assert!(second.path.exists());
            let third = fixture.stop(third, true);
            fixture.clock.advance(Duration::from_secs(1));
            let fourth = fixture.start();
            assert!(!first.path.exists());
            assert!(!second.path.exists());
            assert!(third.path.exists(), "previous link must remain");
            assert!(fourth.lease.executable().path.exists());
            assert_eq!(
                fixture.links(),
                2,
                "settled eligible residue is bounded to current and previous"
            );
            fixture.stop(fourth, true);
            assert_eq!(fixture.links(), 2);
        }

        #[test]
        fn image_link_retention_last_proven_exit_refreshes_creation_age() {
            let fixture = Fixture::new();
            let first = fixture.start();
            fixture.clock.advance(Duration::from_secs(599));
            let first = fixture.stop(first, true);
            fixture.stop(fixture.start(), true);
            fixture.clock.advance(Duration::from_secs(2));
            let third = fixture.start();
            assert!(
                first.path.exists(),
                "creation age cannot replace the later proven exit stamp"
            );
            fixture.stop(third, true);
            fixture.clock.advance(Duration::from_secs(599));
            let fourth = fixture.start();
            assert!(
                !first.path.exists(),
                "older-than-previous proven exits become eligible after grace"
            );
            fixture.stop(fourth, true);
        }

        #[test]
        fn image_link_retention_unknown_exit_never_becomes_age_proof() {
            let fixture = Fixture::new();
            let first = fixture.stop(fixture.start(), false);
            // Simulate the crashed old process identity, without exec or a PID
            // liveness approximation as RPC cleanup proof.
            let path = fixture
                .paths
                .controller_state_root()
                .join("rpc/service.json");
            let mut record: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            record["service"]["leader"] =
                serde_json::to_value(mac_worker::job::ProcessIdentity::new(999999, 1).unwrap())
                    .unwrap();
            fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
            fixture.clock.advance(Duration::from_secs(3600));
            fixture.stop(fixture.start(), true);
            let third = fixture.start();
            assert!(
                first.path.exists(),
                "age and dead leader are not proven RPC exit"
            );
            assert_eq!(
                fixture.links(),
                3,
                "uncertain residue takes precedence over the normal bound"
            );
            fixture.stop(third, true);
        }

        #[test]
        fn image_link_retention_exact_binding_mismatch_preserves_replacement() {
            let fixture = Fixture::new();
            let first = fixture.stop(fixture.start(), true);
            assert!(first.path.exists());
            fs::rename(&first.path, fixture._temp.path().join("saved-original")).unwrap();
            fs::write(&first.path, b"substituted executable").unwrap();
            fs::set_permissions(&first.path, fs::Permissions::from_mode(0o755)).unwrap();
            fixture.clock.advance(Duration::from_secs(3600));
            fixture.stop(fixture.start(), true);
            let third = fixture.start();
            assert_eq!(fs::read(&first.path).unwrap(), b"substituted executable");
            fixture.stop(third, true);
        }

        #[test]
        fn image_link_retention_unknown_clock_epoch_keeps_old_links() {
            let fixture = Fixture::new();
            let first = fixture.stop(fixture.start(), true);
            fixture.stop(fixture.start(), true);
            fixture.clock.advance(Duration::from_secs(3600));
            fixture.epoch.set(2);
            let third = fixture.start();
            assert!(
                first.path.exists(),
                "a different boot/clock epoch cannot authorize age cleanup"
            );
            fixture.stop(third, true);
        }
    }

    struct Fixture {
        _temp: tempfile::TempDir,
        home: PathBuf,
        installed: PathBuf,
        paths: PathLayout,
        environment: BTreeMap<OsString, OsString>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // Do not let TempDir defeat retention immediately after real-image
            // execution. Preserve those fixture roots for conservative cleanup.
            if self.installed.with_file_name("private-rpc").exists()
                || fs::read_dir(self.paths.controller_state_root().join("rpc"))
                    .ok()
                    .is_some_and(|entries| {
                        entries.filter_map(Result::ok).any(|entry| {
                            let name = entry.file_name();
                            let name = name.to_string_lossy();
                            name.len() == 33 && name.starts_with('e')
                        })
                    })
            {
                self._temp.disable_cleanup(true);
            }
        }
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir_in("/private/tmp").unwrap();
            let root = temp.path().canonicalize().unwrap();
            let home = root.join("home");
            fs::create_dir(&home).unwrap();
            let fake_ssh = root.join("fake-ssh");
            fs::write(&fake_ssh, "#!/bin/sh\nexit 69\n").unwrap();
            fs::set_permissions(&fake_ssh, fs::Permissions::from_mode(0o755)).unwrap();
            let environment = BTreeMap::from([
                ("HOME".into(), home.clone().into_os_string()),
                ("XDG_STATE_HOME".into(), root.join("s").into_os_string()),
                ("XDG_CONFIG_HOME".into(), root.join("c").into_os_string()),
                ("XDG_CACHE_HOME".into(), root.join("k").into_os_string()),
                ("XDG_DATA_HOME".into(), root.join("d").into_os_string()),
                ("PATH".into(), "/usr/bin:/bin:/usr/sbin:/sbin".into()),
                ("MAC_WORKER_TEST_SSH".into(), fake_ssh.into_os_string()),
            ]);
            let paths = PathLayout::discover(None, &environment, &home).unwrap();
            fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
            fs::write(
                &paths.config,
                "version=1\n[[workers]]\nname='unused'\nssh='unused'\nslots=1\n",
            )
            .unwrap();
            let installed = root.join("worker");
            fs::copy(env!("CARGO_BIN_EXE_worker"), &installed).unwrap();
            Self {
                _temp: temp,
                home,
                installed,
                paths,
                environment,
            }
        }

        fn command(&self, binary: &std::path::Path) -> Command {
            let mut command = Command::new(binary);
            command
                .env_clear()
                .envs(&self.environment)
                .arg("--config")
                .arg(&self.paths.config);
            command
        }

        fn rpc_at(
            &self,
            binary: &std::path::Path,
            request: &ControllerRequest,
            input: Option<&OsStr>,
        ) -> Output {
            let mut command = self.command(binary);
            command
                .args(["host", "controller-rpc"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if let Some(input) = input {
                command.env(DETACHED_RUNNER_EXECUTABLE_ENV, input);
            }
            let mut child = command.spawn().unwrap();
            child.stdin.take().unwrap().write_all(&encode_json_frame(&json!({
                "protocol_version": request.protocol_version(), "request_id": request.request_id(),
                "command": request.command(), "body": request.body(),
            })).unwrap()).unwrap();
            child.wait_with_output().unwrap()
        }

        fn rpc(&self, request: &ControllerRequest) -> Output {
            self.rpc_at(&self.installed, request, None)
        }

        fn leader(&self) -> Leader {
            let mut child = self
                .command(&self.installed)
                .args(["controller", "run"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (tx, rx) = mpsc::channel();
            let reader = std::thread::spawn(move || {
                let mut line = String::new();
                BufReader::new(stdout).read_line(&mut line).unwrap();
                let _ = tx.send(line);
            });
            let leader = Leader(child);
            assert_eq!(
                rx.recv_timeout(GUARD).unwrap(),
                "controller leader acquired\n"
            );
            reader.join().unwrap();
            leader
        }

        fn record(&self) -> ServiceRecord {
            let path = self.paths.controller_state_root().join("rpc/service.json");
            let start = Instant::now();
            loop {
                if let Ok(bytes) = fs::read(&path) {
                    return serde_json::from_slice(&bytes).unwrap();
                }
                assert!(
                    start.elapsed() < GUARD,
                    "leader never published a ready generation"
                );
                std::thread::yield_now();
            }
        }

        fn identity(&self) -> (Output, Value) {
            let output = self.rpc(&request_fixture(
                "task.list",
                json!({
                    "controller_socket": {"op": "identity", "route_sha256": "a".repeat(64)}
                }),
            ));
            let value = serde_json::from_slice(decode_frame(&output.stdout).unwrap()).unwrap();
            (output, value)
        }

        fn features(&self) -> Vec<String> {
            let output = self.rpc(&request_fixture(
                "task.list",
                json!({"controller_health": true}),
            ));
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let value: Value =
                serde_json::from_slice(decode_frame(&output.stdout).unwrap()).unwrap();
            serde_json::from_value(value["result"]["features"].clone()).unwrap()
        }
    }

    struct Leader(Child);
    impl Leader {
        fn stop(&mut self) {
            assert_eq!(unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM) }, 0);
            let start = Instant::now();
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success());
                    return;
                }
                assert!(start.elapsed() < GUARD, "leader shutdown hang guard");
                std::thread::yield_now();
            }
        }
    }
    impl Drop for Leader {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn selector_uses_existing_client_id_without_creating_state_or_receipts() {
        let fixture = Fixture::new();
        let (output, reply) = fixture.identity();
        assert!(output.status.success(), "{reply}");
        assert_eq!(
            reply["result"],
            json!({"unavailable": "service_unavailable"})
        );
        assert!(!fixture.paths.state.exists());
        assert!(!fixture.paths.controller_state_root().exists());
    }

    #[test]
    fn selector_mixed_namespace_is_rejected_before_durable_dispatch() {
        let fixture = Fixture::new();
        for body in [
            json!({"controller_socket": {"op":"bad"}}),
            json!({"controller_socket": {}, "controller_events": {"op":"repair"}}),
            json!({"controller_socket": {}, "controller_health":true}),
        ] {
            let output = fixture.rpc(&request_fixture("task.list", body));
            assert!(!output.status.success());
            assert!(
                !fixture.paths.state.exists(),
                "selector must not initialize the client store"
            );
            assert!(!fixture.paths.controller_state_root().exists());
        }
    }

    #[test]
    fn leader_image_readiness_publication_and_feature_shutdown() {
        let fixture = Fixture::new();
        assert!(!fixture.features().contains(&"controller.socket".to_owned()));
        let mut leader = fixture.leader();
        let record = fixture.record();
        assert_eq!(record.service.leader.pid(), leader.0.id());
        let installed = fs::metadata(&fixture.installed).unwrap();
        let link = fs::metadata(&record.executable.path).unwrap();
        assert_eq!((link.dev(), link.ino()), (installed.dev(), installed.ino()));
        assert_ne!(record.executable.path, fixture.installed);
        assert_eq!(link.mode() & 0o7777, installed.mode() & 0o7777);
        assert_eq!(
            fs::metadata(record.service.socket_path.parent().unwrap())
                .unwrap()
                .mode()
                & 0o7777,
            0o700
        );
        assert_eq!(
            fs::metadata(&record.service.socket_path).unwrap().mode() & 0o7777,
            0o600
        );
        assert_eq!(
            fs::metadata(
                fixture
                    .paths
                    .controller_state_root()
                    .join("rpc/service.json")
            )
            .unwrap()
            .mode()
                & 0o7777,
            0o600
        );
        let (output, reply) = fixture.identity();
        assert!(output.status.success(), "{reply}");
        let available: SocketIdentityResult =
            serde_json::from_value(reply["result"].clone()).unwrap();
        let SocketIdentityResult::Available(identity) = available else {
            panic!("ready service unavailable")
        };
        assert_eq!(identity.service, record.service);
        let id = fs::read_to_string(fixture.paths.state.join("client-id")).unwrap();
        assert_eq!(identity.service.controller_client_id.to_string(), id.trim());
        assert!(
            !fixture
                .paths
                .controller_state_root()
                .join(format!(
                    "req-{}.json",
                    reply["request_id"].as_str().unwrap()
                ))
                .exists()
        );
        assert_eq!(
            fs::read_dir(fixture.paths.controller_state_root().join("active"))
                .unwrap()
                .count(),
            0
        );
        assert!(fixture.features().contains(&"controller.socket".to_owned()));
        leader.stop();
        assert!(!fixture.features().contains(&"controller.socket".to_owned()));
        assert!(record.executable.path.exists());
        assert!(!record.service.socket_path.exists());
        assert!(
            !fixture
                .paths
                .controller_state_root()
                .join("rpc/service.json")
                .exists()
        );
        assert_eq!(
            fs::read_to_string(fixture.paths.state.join("client-id")).unwrap(),
            id
        );
    }

    #[test]
    fn image_rpc_runner_invalid_input_fails_before_dispatch() {
        let fixture = Fixture::new();
        for input in ["", "relative-worker", "/missing/worker", "/bin/sh\n"] {
            let output = fixture.rpc_at(
                &fixture.installed,
                &request_fixture("task.list", json!({})),
                Some(OsStr::new(input)),
            );
            assert!(
                !output.status.success(),
                "invalid runner input accepted: {input:?}"
            );
            assert!(
                !fixture.paths.state.exists(),
                "invalid runner input reached dispatch"
            );
            assert!(!fixture.paths.controller_state_root().exists());
        }
        let output = fixture.rpc(&request_fixture("task.list", json!({})));
        assert!(
            output.status.success(),
            "no-input stdio behavior remains available"
        );
    }

    async fn send(stream: &tokio::net::UnixStream, bytes: &[u8]) {
        let mut offset = 0;
        while offset < bytes.len() {
            stream.writable().await.unwrap();
            match stream.try_write(&bytes[offset..]) {
                Ok(n) => offset += n,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("{error}"),
            }
        }
    }

    async fn receive(stream: &tokio::net::UnixStream) -> Option<Vec<u8>> {
        use mac_worker::controller::channel::codec::SessionCodec;
        let mut decoder = SessionCodec::new().decoder();
        loop {
            stream.readable().await.unwrap();
            let mut bytes = [0; 8192];
            match stream.try_read(&mut bytes) {
                Ok(0) => return None,
                Ok(n) => {
                    if let Some(payload) = decoder.feed(&bytes[..n]).unwrap().payload {
                        return Some(payload);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("{error}"),
            }
        }
    }

    struct LiveRpc {
        courier: PathBuf,
    }
    impl ChannelExecutor for LiveRpc {
        fn run(&self, frame: &[u8], ctx: &ServerContext) -> mac_worker::process::ProcessCompletion {
            use mac_worker::process::{
                ProcessPolicy, ProcessRequest, SystemProcessRunner, TrackedProcessRunner,
            };
            let script = format!(
                "import os,sys,socket,signal\nsys.stdin.buffer.read()\ns=socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM)\ns.sendto(str(os.getpid()).encode(), {})\nwhile True: signal.pause()\n",
                serde_json::to_string(&self.courier).unwrap()
            );
            SystemProcessRunner.run_interruptible_with_cleanup(
                &ProcessRequest {
                    program: "/usr/bin/python3".into(),
                    args: vec!["-c".into(), script.into()],
                    environment: Vec::new(),
                    environment_remove: Vec::new(),
                    stdin: Some(frame.to_vec()),
                    policy: ProcessPolicy {
                        stdout_limit: 1024 * 1024 + 4,
                        stderr_limit: 256 * 1024,
                        deadline: GUARD,
                    },
                    isolate_parent_environment: true,
                },
                &|| {
                    ctx.cancelled.load(std::sync::atomic::Ordering::Acquire)
                        || ctx.runtime.cancelled()
                },
            )
        }
    }

    struct RpcGroup(i32);
    impl Drop for RpcGroup {
        fn drop(&mut self) {
            unsafe {
                libc::killpg(self.0, libc::SIGKILL);
            }
        }
    }

    fn shutdown_exit(mode: &str) {
        use mac_worker::controller::{
            channel::{
                codec::SessionCodec,
                server::{NativeControl, ServerDeps, SocketService},
                testing::{ManualRuntime, identity_fixture},
            },
            runtime::run_tick_loop_with_shutdown,
        };
        use std::{
            os::unix::net::{UnixDatagram, UnixListener},
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
        };
        let temp = tempfile::tempdir_in("/private/tmp").unwrap();
        let courier = UnixDatagram::bind(temp.path().join("entry")).unwrap();
        courier.set_read_timeout(Some(GUARD)).unwrap();
        let control = NativeControl::new();
        let pid_rx = control
            .try_run(Box::new(move || {
                let mut bytes = [0; 64];
                let n = courier.recv(&mut bytes).unwrap();
                std::str::from_utf8(&bytes[..n])
                    .unwrap()
                    .parse::<i32>()
                    .unwrap()
            }))
            .unwrap();
        let mut identity = identity_fixture();
        identity.service.socket_path = temp.path().join("s");
        let listener = UnixListener::bind(&identity.service.socket_path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let flag = Arc::new(AtomicBool::new(false));
        let clock = Arc::new(ManualRuntime::default());
        let codec = Arc::new(SessionCodec::new());
        let (service, stream, pid) = runtime.block_on(async {
            let service = SocketService::start(listener, identity.service.clone(), ServerDeps {
                codec: codec.clone(), executor: Arc::new(LiveRpc { courier: temp.path().join("entry") }), runtime: clock.clone(),
            }, flag.clone()).unwrap();
            service.wait_ready().await.unwrap();
            let stream = tokio::net::UnixStream::connect(&identity.service.socket_path).await.unwrap();
            send(&stream, &codec.encode_hello(&identity).unwrap()).await;
            codec.decode_ready(&receive(&stream).await.unwrap(), &identity).unwrap();
            let request = request_fixture("task.wait.poll", json!({"task_id":"018f0f4a6b5c7d8e9f00112233445566"}));
            send(&stream, &encode_json_frame(&json!({"protocol_version":7,"request_id":request.request_id(),"command":request.command(),"body":request.body()})).unwrap()).await;
            let pid = tokio::time::timeout(GUARD, pid_rx).await.unwrap().unwrap();
            (service, stream, pid)
        });
        let _group = RpcGroup(pid);
        assert_eq!(unsafe { libc::killpg(pid, 0) }, 0);
        let finalized = AtomicBool::new(false);
        let joined = AtomicBool::new(false);
        let (tick_tx, tick_rx) = tokio::sync::oneshot::channel();
        let mut tick_tx = Some(tick_tx);
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = std::sync::Mutex::new(release_rx);
        let result = run_tick_loop_with_shutdown(
            &runtime,
            &flag,
            || {
                if mode == "signal" {
                    tick_tx.take().unwrap().send(()).unwrap();
                    release_rx.lock().unwrap().recv_timeout(GUARD).unwrap();
                    assert!(
                        finalized.load(Ordering::Acquire),
                        "tick join began before channel finalization"
                    );
                    joined.store(true, Ordering::Release);
                    Ok(None)
                } else if mode == "tick" {
                    Err(mac_worker::error::WorkerError::Unavailable(
                        "TICK_FIXTURE: tick failed".into(),
                    ))
                } else if mode == "emit" {
                    Ok(Some("diagnostic".into()))
                } else {
                    panic!("injected diagnostic sender closure")
                }
            },
            async {
                if mode == "signal" {
                    tick_rx.await.unwrap();
                } else {
                    std::future::pending::<()>().await;
                }
            },
            |_| Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into()),
            async {
                assert!(flag.load(Ordering::Acquire));
                let evidence = service
                    .shutdown(&ServerContext {
                        runtime: clock.clone(),
                        deadline: clock.now() + SETUP_GUARD,
                        cancelled: flag.clone(),
                    })
                    .await;
                assert_eq!(evidence.unknown, 0);
                assert_eq!(evidence.completed, 1);
                assert!(
                    receive(&stream).await.is_none(),
                    "stream must close before runtime exits"
                );
                assert_eq!(
                    unsafe { libc::killpg(pid, 0) },
                    -1,
                    "actual RPC group still live before runtime exits"
                );
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::ESRCH)
                );
                finalized.store(true, Ordering::Release);
                if mode == "signal" {
                    release_tx.send(()).unwrap();
                }
            },
        );
        assert!(
            finalized.load(Ordering::Acquire),
            "before-unpoll hook was skipped for {mode}"
        );
        match mode {
            "signal" => {
                result.unwrap();
                assert!(joined.load(Ordering::Acquire));
            }
            "tick" => assert_eq!(result.unwrap_err().public_code(), "TICK_FIXTURE"),
            "emit" => assert!(
                matches!(result, Err(mac_worker::error::WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe)
            ),
            _ => assert_eq!(result.unwrap_err().public_code(), "CONTROLLER_TRANSPORT"),
        }
    }

    #[test]
    fn shutdown_signal_closes_stream_and_rpc_group_before_blocked_tick_join() {
        shutdown_exit("signal");
    }
    #[test]
    fn shutdown_tick_error_preserves_result_after_rpc_group_termination() {
        shutdown_exit("tick");
    }
    #[test]
    fn shutdown_diagnostic_write_error_preserves_result_after_rpc_group_termination() {
        shutdown_exit("emit");
    }
    #[test]
    fn shutdown_diagnostic_channel_closure_terminates_rpc_before_runtime_exit() {
        shutdown_exit("closed");
    }

    fn seed_task(
        fixture: &Fixture,
        parked: bool,
    ) -> (mac_worker::task::TaskId, mac_worker::task::TurnId) {
        use mac_worker::{
            agent::{AgentKind, PermissionPolicy},
            client_state::ClientStateStore,
            job::{CommandSummary, QueueEntry, QueueEntryKind},
            scheduler::WorkerPreference,
            supervisor::SystemProcessInspector,
            task::{
                ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits,
                TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId,
                TurnSummary, TurnTerminal,
            },
        };
        let (task, turn) = (TaskId::generate(), TurnId::generate());
        let store = ClientStateStore::open(&fixture.paths.state).unwrap();
        let meta = TaskMeta::new(TaskMetaInput {
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
            if parked {
                TaskState::Queued
            } else {
                TaskState::Open
            },
            if parked {
                None
            } else {
                Some(TaskOutcome::Done)
            },
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
                if parked {
                    None
                } else {
                    Some(TurnTerminal::Succeeded)
                },
                if parked {
                    None
                } else {
                    Some(TaskOutcome::Done)
                },
                None,
                false,
                None,
                if parked { None } else { Some(2) },
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
        if parked {
            store.write_turn_prompt(task, turn, "fixture").unwrap();
            let owner = SystemProcessInspector
                .identity_for_pid(std::process::id())
                .unwrap();
            store
                .enqueue(
                    QueueEntry::new(
                        turn,
                        store.client_id(),
                        "a".repeat(64),
                        "b".repeat(64),
                        CommandSummary::argv(2).unwrap(),
                        vec![],
                        WorkerPreference::Automatic,
                        QueueEntryKind::TaskTurn,
                        None,
                        owner,
                        1,
                    )
                    .unwrap(),
                )
                .unwrap();
            store.park_row(turn).unwrap();
        } else {
            store
                .open_runner_log(task, turn)
                .unwrap()
                .write_all(b"fixture log\n")
                .unwrap();
            let path = fixture
                .paths
                .state
                .join("runners")
                .join(task.to_string())
                .join(format!("{turn}.checkpoint.json"));
            fs::write(&path, serde_json::to_vec(&json!({"version":1,"task_id":task,"turn_id":turn,"committed":{
                "offsets":[0,0],"len":12,"accepted":true,"completion":{"outcome":TaskOutcome::Done,"drained":true}
            },"pending":null})).unwrap()).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        (task, turn)
    }

    #[test]
    fn image_rpc_runner_launch_input() {
        use std::os::unix::net::UnixDatagram;
        let fixture = Fixture::new();
        let (task, turn) = seed_task(&fixture, true);
        let rpc_link = fixture.installed.with_file_name("private-rpc");
        fs::hard_link(&fixture.installed, &rpc_link).unwrap();
        let marker = fixture.installed.with_file_name("installed-runner");
        let courier_path = fixture.installed.with_file_name("runner-entry");
        let courier = UnixDatagram::bind(&courier_path).unwrap();
        courier.set_read_timeout(Some(GUARD)).unwrap();
        fs::write(&marker, format!("#!/usr/bin/python3\nimport os,sys,socket,signal\ns=socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM)\ns.sendto(__import__('json').dumps([sys.argv,os.getpid(),os.getpgrp()]).encode(), {})\nwhile True: signal.pause()\n", serde_json::to_string(&courier_path).unwrap())).unwrap();
        fs::set_permissions(&marker, fs::Permissions::from_mode(0o755)).unwrap();
        let request = request_fixture("task.wait.poll", json!({"task_id":task}));
        let output = fixture.rpc_at(&rpc_link, &request, Some(marker.as_os_str()));
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let mut bytes = [0; 4096];
        let n = courier
            .recv(&mut bytes)
            .expect("poll must launch the explicit installed runner");
        let (args, pid, group): (Vec<String>, i32, i32) =
            serde_json::from_slice(&bytes[..n]).unwrap();
        let _group = RpcGroup(group);
        assert_eq!(pid, group, "runner has its own session");
        assert_eq!(args[0], marker.to_str().unwrap());
        assert_eq!(
            &args[1..6],
            [
                "--config",
                fixture.paths.config.to_str().unwrap(),
                "runner",
                &task.to_string(),
                &turn.to_string()
            ]
        );
        assert_eq!(
            unsafe { libc::killpg(group, 0) },
            0,
            "runner survives RPC exit"
        );
    }

    #[test]
    fn feature_absent_on_unsafe_bind_image_and_record_with_stdio_healthy() {
        for kind in ["bind", "image", "record"] {
            let fixture = Fixture::new();
            let rpc = fixture.paths.controller_state_root().join("rpc");
            let residue = if kind == "image" {
                fs::set_permissions(&fixture.installed, fs::Permissions::from_mode(0o777)).unwrap();
                None
            } else {
                fs::create_dir_all(&rpc).unwrap();
                fs::set_permissions(
                    fixture.paths.controller_state_root(),
                    fs::Permissions::from_mode(0o700),
                )
                .unwrap();
                fs::set_permissions(&rpc, fs::Permissions::from_mode(0o700)).unwrap();
                let path = rpc.join(if kind == "bind" { "s" } else { "service.json" });
                fs::write(&path, b"unproven fixture residue").unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
                Some(path)
            };
            let mut leader = fixture.leader();
            assert!(
                !fixture.features().contains(&"controller.socket".into()),
                "{kind}"
            );
            leader.stop();
            if let Some(path) = residue {
                assert_eq!(fs::read(path).unwrap(), b"unproven fixture residue");
            }
            assert_eq!(
                fs::metadata(&fixture.installed).unwrap().mode() & 0o777,
                if kind == "image" { 0o777 } else { 0o755 }
            );
        }
    }

    #[test]
    fn journal_missing_or_failed_still_serves_wait_and_logs_with_event_errors_unchanged() {
        use mac_worker::controller::channel::{
            codec::{FramedSocketConnector, SessionCodec},
            testing::ManualRuntime,
        };
        use std::sync::Arc;
        for journal in ["missing", "failed"] {
            let fixture = Fixture::new();
            let (task, _) = seed_task(&fixture, false);
            if journal == "failed" {
                fs::create_dir_all(fixture.paths.controller_state_root()).unwrap();
                fs::set_permissions(
                    fixture.paths.controller_state_root(),
                    fs::Permissions::from_mode(0o700),
                )
                .unwrap();
                fs::write(
                    fixture.paths.controller_state_root().join("events"),
                    b"invalid journal root",
                )
                .unwrap();
            }
            let mut leader = fixture.leader();
            let record = fixture.record();
            if journal == "missing" {
                fs::rename(
                    fixture.paths.controller_state_root().join("events"),
                    fixture
                        .paths
                        .controller_state_root()
                        .join("events-retained"),
                )
                .unwrap();
            } else {
                assert!(record.service.journal_id.is_none());
            }
            assert!(fixture.features().contains(&"controller.socket".into()));
            let identity = SocketIdentity {
                route_sha256: RouteDigest::parse(&"a".repeat(64)).unwrap(),
                service: record.service,
            };
            let codec = Arc::new(SessionCodec::new());
            let clock = ManualRuntime::default();
            let ctx = ClientContext {
                runtime: &clock,
                deadline: GUARD,
                should_stop: &|| false,
            };
            let mut session = FramedSocketConnector::new(codec)
                .connect(&identity.service.socket_path, &identity, &ctx)
                .unwrap();
            for request in [
                request_fixture("task.wait.poll", json!({"task_id":task})),
                request_fixture(
                    "task.logs",
                    json!({"task_id":task,"offset":0,"limit":4096,"wait_ms":0,"raw":true}),
                ),
            ] {
                let frame = encode_json_frame(&json!({"protocol_version":7,"request_id":request.request_id(),"command":request.command(),"body":request.body()})).unwrap();
                let reply = session.exchange(&frame, &request, &ctx).unwrap();
                assert!(
                    reply.status.success(),
                    "{}",
                    String::from_utf8_lossy(&reply.stdout)
                );
                let reply: Value =
                    serde_json::from_slice(decode_frame(&reply.stdout).unwrap()).unwrap();
                if request.command() == "task.wait.poll" {
                    assert_eq!(reply["result"]["quiescent"], true);
                } else {
                    assert_eq!(reply["result"]["next_offset"], 12);
                }
            }
            let events = request_fixture(
                "task.list",
                json!({"controller_events":{"op":"read","wait_ms":0}}),
            );
            let raw = fixture.rpc(&events);
            let frame = encode_json_frame(&json!({"protocol_version":7,"request_id":events.request_id(),"command":"task.list","body":events.body()})).unwrap();
            let channel = session.exchange(&frame, &events, &ctx).unwrap();
            let channel_error: Value =
                serde_json::from_slice(decode_frame(&channel.stdout).unwrap()).unwrap();
            let raw_error: Value =
                serde_json::from_slice(decode_frame(&raw.stdout).unwrap()).unwrap();
            assert_eq!(
                channel_error, raw_error,
                "event epoch/error authority remains in its existing RPC"
            );
            assert_eq!(channel.status.code(), raw.status.code());
            assert!(!channel.status.success());
            session.close();
            leader.stop();
        }
    }

    fn native_inputs(
        fixture: &Fixture,
    ) -> (
        std::sync::Arc<mac_worker::controller::ControllerLeader>,
        mac_worker::controller::runtime::LeaderChannelConfig,
        mac_worker::controller::runtime::LeaderChannelDeps,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        use mac_worker::{
            client_state::ClientStateStore,
            controller::{
                channel::{server::NativeControl, testing::ScriptedImageSource},
                runtime::{LeaderChannelConfig, LeaderChannelDeps, SystemChannelRuntime},
            },
            process::SystemProcessRunner,
        };
        use std::sync::{Arc, atomic::AtomicBool};
        let leader = Arc::new(
            mac_worker::controller::ControllerLeader::acquire(
                &fixture.paths.controller_state_root(),
            )
            .unwrap(),
        );
        let client_id = ClientStateStore::open(&fixture.paths.state)
            .unwrap()
            .client_id();
        let metadata = fs::metadata(&fixture.installed).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        (
            leader,
            LeaderChannelConfig {
                paths: fixture.paths.clone(),
                home: fixture.home.clone(),
                environment: fixture.environment.clone(),
                client_id,
                journal: None,
            },
            LeaderChannelDeps {
                image: Arc::new(ScriptedImageSource::new(vec![Ok(RunningImage {
                    path: fixture.installed.clone(),
                    device: metadata.dev(),
                    inode: metadata.ino(),
                })])),
                runner: Arc::new(SystemProcessRunner),
                runtime: Arc::new(SystemChannelRuntime::new(stop.clone())),
                control: Arc::new(NativeControl::new()),
            },
            stop,
        )
    }

    async fn channel_ready(channel: &mac_worker::controller::runtime::LeaderChannel) {
        tokio::time::timeout(GUARD, async {
            while !channel.ready() {
                assert!(!channel.retired(), "optional channel unexpectedly retired");
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn journal_initialization_hint_never_queries_window_as_a_startup_precondition() {
        use mac_worker::controller::{
            events::{
                JournalReader,
                journal::{ControllerJournal, JournalFaultPoint, JournalOptions},
            },
            runtime::LeaderChannel,
        };
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        let fixture = Fixture::new();
        let (leader, mut config, deps, stop) = native_inputs(&fixture);
        let unhealthy = Arc::new(AtomicBool::new(false));
        let probes = Arc::new(AtomicUsize::new(0));
        let (hook_unhealthy, hook_probes) = (unhealthy.clone(), probes.clone());
        let journal = ControllerJournal::initialize_for_leader_with_hook(
            &fixture.paths,
            &leader,
            JournalOptions {
                runtime: mac_worker::ControllerEventRuntime::system(),
            },
            Arc::new(move |point| {
                if matches!(point, JournalFaultPoint::ReadAttempt)
                    && hook_unhealthy.load(Ordering::Acquire)
                {
                    hook_probes.fetch_add(1, Ordering::AcqRel);
                    return Err(std::io::Error::other("journal disk is unavailable"));
                }
                Ok(())
            }),
        )
        .unwrap();
        let event_runtime = mac_worker::ControllerEventRuntime::system();
        let epoch = journal
            .window(
                mac_worker::controller::events::EventRuntime::now(event_runtime.as_ref()) + GUARD,
            )
            .unwrap()
            .journal_id;
        unhealthy.store(true, Ordering::Release);
        config.journal = Some(journal);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            let channel = LeaderChannel::start(config, leader, deps, stop);
            channel_ready(&channel).await;
            let record = fixture.record();
            let result = channel.shutdown().await;
            (record, result)
        });
        assert_eq!(
            probes.load(Ordering::Acquire),
            0,
            "startup must use its initialized epoch without touching journal health/window"
        );
        assert_eq!(
            result.0.service.journal_id.unwrap().as_str(),
            epoch.to_string()
        );
        assert_eq!(result.1.files, ForwardDisposition::Cleaned);
    }

    #[test]
    fn shutdown_gated_image_native_startup_keeps_signal_pollable_with_lock_held() {
        use mac_worker::controller::runtime::{LeaderChannel, run_tick_loop_with_shutdown};
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        };
        struct GatedImage {
            root: PathBuf,
            caller: std::thread::ThreadId,
            entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
            release: Mutex<mpsc::Receiver<()>>,
            exited: mpsc::Sender<()>,
        }
        impl RunningImageSource for GatedImage {
            fn capture(&self) -> Result<RunningImage, ChannelFailure> {
                assert_ne!(std::thread::current().id(), self.caller);
                assert_eq!(
                    mac_worker::controller::ControllerLeader::acquire(&self.root)
                        .err()
                        .unwrap()
                        .public_code(),
                    "CONTROLLER_LOCK_HELD"
                );
                self.entered
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                self.release.lock().unwrap().recv_timeout(GUARD).unwrap();
                self.exited.send(()).unwrap();
                Err(ChannelFailure::Unavailable(
                    ChannelReason::ServiceUnavailable,
                ))
            }
        }
        let fixture = Fixture::new();
        let (leader, config, mut deps, stop) = native_inputs(&fixture);
        let (tx, entered) = tokio::sync::oneshot::channel();
        let (release, rx) = mpsc::channel();
        let (exited_tx, exited_rx) = mpsc::channel();
        deps.image = Arc::new(GatedImage {
            root: fixture.paths.controller_state_root(),
            caller: std::thread::current().id(),
            entered: Mutex::new(Some(tx)),
            release: Mutex::new(rx),
            exited: exited_tx,
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let channel = {
            let _entered = runtime.enter();
            LeaderChannel::start(config, leader.clone(), deps, stop.clone())
        };
        let finalized = AtomicBool::new(false);
        run_tick_loop_with_shutdown(
            &runtime,
            &stop,
            || Ok(None),
            async {
                entered.await.unwrap();
            },
            |_| Ok(()),
            async {
                let result = channel.shutdown().await;
                assert_eq!(result.files, ForwardDisposition::Retained);
                assert_eq!(result.rpc.completed, 0);
                finalized.store(true, Ordering::Release);
                release.send(()).unwrap();
            },
        )
        .unwrap();
        assert!(finalized.load(Ordering::Acquire));
        exited_rx.recv_timeout(GUARD).unwrap();
        assert!(
            !fixture
                .paths
                .controller_state_root()
                .join("rpc/service.json")
                .exists()
        );
        assert!(
            !stop.load(Ordering::Acquire)
                || !fixture.features().contains(&"controller.socket".into())
        );
    }

    #[test]
    fn shutdown_gated_withdrawal_control_retains_image_and_original_tick_error() {
        use mac_worker::controller::runtime::{LeaderChannel, run_tick_loop_with_shutdown};
        let fixture = Fixture::new();
        let (leader, config, deps, stop) = native_inputs(&fixture);
        let control = deps.control.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let channel = runtime.block_on(async {
            let channel = LeaderChannel::start(config, leader.clone(), deps, stop.clone());
            channel_ready(&channel).await;
            channel
        });
        let record = fixture.record();
        let (release, wait) = mpsc::channel();
        let (entered_tx, entered_rx) = mpsc::channel();
        let completed = control
            .try_run(Box::new(move || {
                entered_tx.send(()).unwrap();
                wait.recv_timeout(GUARD).unwrap();
            }))
            .unwrap();
        entered_rx.recv_timeout(GUARD).unwrap();
        let result = run_tick_loop_with_shutdown(
            &runtime,
            &stop,
            || {
                Err(mac_worker::error::WorkerError::Unavailable(
                    "TICK_FIXTURE: retained control".into(),
                ))
            },
            std::future::pending(),
            |_| Ok(()),
            async {
                let result = channel.shutdown().await;
                assert_eq!(result.rpc.unknown, 0);
                assert_eq!(result.files, ForwardDisposition::Retained);
                release.send(()).unwrap();
                completed.await.unwrap();
            },
        );
        assert_eq!(result.unwrap_err().public_code(), "TICK_FIXTURE");
        assert!(record.executable.path.exists());
        assert!(
            fixture
                .paths
                .controller_state_root()
                .join("rpc/service.json")
                .exists()
        );
        assert!(!fixture.features().contains(&"controller.socket".into()));
    }

    #[test]
    fn feature_unknown_rpc_cleanup_retires_only_channel_and_preserves_image() {
        use mac_worker::{
            controller::{channel::testing::RecordingTrackedRunner, runtime::LeaderChannel},
            process::{CleanupState, ProcessCompletion},
        };
        use std::sync::{Arc, atomic::Ordering};
        let fixture = Fixture::new();
        let (leader, config, mut deps, stop) = native_inputs(&fixture);
        deps.runner = Arc::new(RecordingTrackedRunner::new(
            (0..8)
                .map(|_| ProcessCompletion {
                    outcome: Err(mac_worker::error::WorkerError::Unavailable(
                        "RPC_FIXTURE: unknown exit".into(),
                    )),
                    cleanup: CleanupState::Unknown,
                })
                .collect(),
        ));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            let channel = LeaderChannel::start(config, leader.clone(), deps, stop.clone());
            channel_ready(&channel).await;
            let record = fixture.record();
            let identity = SocketIdentity { route_sha256: RouteDigest::parse(&"a".repeat(64)).unwrap(), service: record.service.clone() };
            let codec = mac_worker::controller::channel::codec::SessionCodec::new();
            for _ in 0..8 {
                let stream = tokio::net::UnixStream::connect(&identity.service.socket_path).await.unwrap();
                send(&stream, &codec.encode_hello(&identity).unwrap()).await;
                codec.decode_ready(&receive(&stream).await.unwrap(), &identity).unwrap();
                send(&stream, &encode_json_frame(&json!({"protocol_version":7,"request_id":mac_worker::job::ClientId::generate(),"command":"task.wait.poll","body":{"task_id":"018f0f4a6b5c7d8e9f00112233445566"}})).unwrap()).await;
                assert!(receive(&stream).await.is_none());
            }
            tokio::time::timeout(GUARD, async { while !channel.retired() { tokio::task::yield_now().await; } }).await.unwrap();
            assert!(!stop.load(Ordering::Acquire), "channel retirement cannot stop the controller leader");
            assert_eq!(mac_worker::controller::ControllerLeader::acquire(&fixture.paths.controller_state_root()).err().unwrap().public_code(), "CONTROLLER_LOCK_HELD");
            (record, channel.shutdown().await)
        });
        assert_eq!(result.1.rpc.unknown, 8);
        assert_eq!(result.1.files, ForwardDisposition::Retained);
        assert!(result.0.executable.path.exists());
        assert!(!fixture.features().contains(&"controller.socket".into()));
    }

    // T7c combines the accepted implementations; these barriers use real
    // worker processes and disposable state, never a remote worker.
    mod t7c {
        use super::*;
        use crate::task_state_fixture::TaskStateFixture;
        use mac_worker::{
            client_state::ClientStateStore,
            controller::{
                channel::{codec::SessionCodec, pin::PrivatePinStore},
                runtime::LeaderChannel,
            },
            process::ProcessRunner,
            task::{TaskId, TurnId},
            transfer_repo::TransferRepo,
        };
        use std::{os::fd::AsRawFd, path::Path, sync::Arc};

        fn frame(request: &ControllerRequest) -> Vec<u8> {
            encode_json_frame(
                &json!({"protocol_version":7,"request_id":request.request_id(),
                "command":request.command(),"body":request.body()}),
            )
            .unwrap()
        }
        async fn connect(record: &ServiceRecord) -> tokio::net::UnixStream {
            let identity = SocketIdentity {
                route_sha256: RouteDigest::parse(&"a".repeat(64)).unwrap(),
                service: record.service.clone(),
            };
            let stream = tokio::net::UnixStream::connect(&record.service.socket_path)
                .await
                .unwrap();
            send(
                &stream,
                &SessionCodec::new().encode_hello(&identity).unwrap(),
            )
            .await;
            SessionCodec::new()
                .decode_ready(&receive(&stream).await.unwrap(), &identity)
                .unwrap();
            stream
        }
        async fn read(stream: &tokio::net::UnixStream, request: &ControllerRequest) -> Value {
            send(stream, &frame(request)).await;
            let payload = receive(stream).await.expect("complete child reply");
            let result = SessionCodec::new().decode_reply(&payload, request).unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stdout)
            );
            serde_json::from_slice(decode_frame(&result.stdout).unwrap()).unwrap()
        }
        fn pin_path(fixture: &Fixture) -> PathBuf {
            fixture
                .paths
                .controller_cache_root()
                .join("channel/pins")
                .join(format!("{}.json", "a".repeat(64)))
        }

        // Copied from bad365b: read.rs, lifecycle.rs, control.rs and store.rs.
        // These DTOs deliberately do not consume the new channel wrapper.
        mod baseline {
            use serde::Deserialize;
            use serde_json::Value;
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            pub struct Reply<T> {
                pub protocol_version: u32,
                pub command: String,
                pub request_id: String,
                pub payload_sha256: String,
                pub result: T,
            }
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            pub struct Wait {
                pub task_ids: Vec<mac_worker::task::TaskId>,
                pub quiescent: bool,
                pub exit_code: u8,
            }
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            pub struct Drain {
                pub drained: bool,
            }
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            pub struct Logs {
                pub task_id: mac_worker::task::TaskId,
                pub turn_id: mac_worker::task::TurnId,
                pub turn_number: u32,
                pub agent: String,
                pub offset: u64,
                pub next_offset: u64,
                pub exhausted: bool,
                pub complete: bool,
                pub raw: bool,
                pub bytes_base64: String,
                #[serde(default)]
                pub failure: Option<String>,
            }
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            pub struct Ack {
                pub protocol_version: u32,
                pub status: String,
                pub request_id: String,
                pub payload_sha256: String,
                #[serde(default)]
                pub task_id: Option<String>,
                #[serde(default)]
                pub turn_id: Option<String>,
                pub created_at_millis: u64,
                #[serde(default)]
                pub result: Option<Value>,
            }
            // bad365b read.rs: task.list rejects keys before calling list().
            pub fn task_list(
                request: &mac_worker::controller::ControllerRequest,
            ) -> Result<(), mac_worker::error::WorkerError> {
                if request
                    .body()
                    .as_object()
                    .is_none_or(|body| !body.is_empty())
                {
                    return Err(mac_worker::error::WorkerError::Protocol(
                        "CONTROLLER_TRANSPORT: task.list has unknown fields".into(),
                    ));
                }
                Ok(())
            }
        }
        fn legacy_reply<T: serde::de::DeserializeOwned>(
            fixture: &Fixture,
            request: &ControllerRequest,
        ) -> baseline::Reply<T> {
            let output = fixture.rpc(request);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stdout)
            );
            let decoded: baseline::Reply<T> =
                serde_json::from_slice(decode_frame(&output.stdout).unwrap()).unwrap();
            assert_eq!(decoded.protocol_version, 7);
            assert_eq!(decoded.command, request.command());
            assert_eq!(decoded.request_id, request.request_id());
            assert_eq!(decoded.payload_sha256, request.payload_sha256());
            decoded
        }
        #[test]
        fn compatibility_old_strict_stdio_read_logs_wait_drain_and_ack() {
            let fixture = Fixture::new();
            let (task, turn) = seed_task(&fixture, false);
            let mut leader = fixture.leader();
            fixture.record();
            let list = legacy_reply::<Value>(&fixture, &request_fixture("task.list", json!({})));
            assert!(list.result["tasks"].is_array());
            let wait = legacy_reply::<baseline::Wait>(
                &fixture,
                &request_fixture("task.wait.poll", json!({"task_id":task})),
            )
            .result;
            assert_eq!(wait.task_ids, [task]);
            assert!(wait.quiescent);
            assert_eq!(wait.exit_code, 0);
            let logs = legacy_reply::<baseline::Logs>(
                &fixture,
                &request_fixture(
                    "task.logs",
                    json!({"task_id":task,"offset":0,"limit":64,"raw":true}),
                ),
            )
            .result;
            assert_eq!(
                (logs.task_id, logs.turn_id, logs.turn_number),
                (task, turn, 1)
            );
            assert_eq!(logs.agent, "codex");
            assert_eq!((logs.offset, logs.next_offset), (0, 12));
            assert!(logs.exhausted && logs.complete && logs.raw);
            assert!(logs.failure.is_none());
            assert_eq!(logs.bytes_base64, "Zml4dHVyZSBsb2cK");
            for value in [true, false] {
                let reply = legacy_reply::<baseline::Drain>(
                    &fixture,
                    &request_fixture("controller.drain", json!({"drained":value})),
                );
                assert_eq!(reply.result.drained, value);
            }
            let request = request_fixture("checkpoint.submit", json!({"prompt":"baseline ACK"}));
            let output = fixture.rpc(&request);
            assert!(output.status.success());
            let ack: baseline::Ack =
                serde_json::from_slice(decode_frame(&output.stdout).unwrap()).unwrap();
            assert_eq!(ack.protocol_version, 7);
            assert_eq!(ack.status, "acked");
            assert_eq!(ack.request_id, request.request_id());
            assert_eq!(ack.payload_sha256, request.payload_sha256());
            assert!(ack.created_at_millis > 0);
            ack.task_id.as_deref().unwrap().parse::<TaskId>().unwrap();
            ack.turn_id.as_deref().unwrap().parse::<TurnId>().unwrap();
            assert_eq!(
                ack.result.unwrap(),
                json!({"executor":"fake-checkpoint","request_id":request.request_id()})
            );
            leader.stop();
        }

        #[test]
        fn compatibility_old_selector_rejects_without_receipts_and_new_read_falls_back() {
            use mac_worker::{
                controller::channel::{
                    client::ChannelProcessRunner,
                    testing::{FakeForwardControl, ManualRuntime, ScriptedConnector},
                },
                controller::controller_rpc_ssh_request,
            };
            use std::{os::unix::process::ExitStatusExt, process::ExitStatus, sync::Mutex};
            struct Old {
                calls: Mutex<Vec<Vec<u8>>>,
            }
            impl ProcessRunner for Old {
                fn run(
                    &self,
                    request: &mac_worker::process::ProcessRequest,
                ) -> Result<mac_worker::process::ProcessResult, mac_worker::error::WorkerError>
                {
                    let bytes = request.stdin.as_ref().unwrap();
                    self.calls.lock().unwrap().push(bytes.clone());
                    let req = mac_worker::controller::decode_request(bytes).unwrap();
                    if req.body().get("controller_socket").is_some() {
                        baseline::task_list(&req)?;
                        panic!("old server admitted socket selector");
                    }
                    Ok(mac_worker::process::ProcessResult { status: ExitStatus::from_raw(0), stdout: encode_json_frame(&json!({"protocol_version":7,"command":req.command(),"request_id":req.request_id(),"payload_sha256":req.payload_sha256(),"result":{"task_ids":[],"quiescent":true,"exit_code":0}})).unwrap(), stderr:vec![] })
                }
            }
            let fixture = Fixture::new();
            let route = ConfiguredRoute {
                ssh: "old-controller".into(),
                remote_binary: "~/.local/bin/worker".into(),
                ssh_config_file: None,
            };
            let raw = Old {
                calls: Mutex::default(),
            };
            let forwards = Arc::new(FakeForwardControl::new("/private/fake-forward/s".into()));
            let connector = Arc::new(ScriptedConnector::new(vec![]));
            let channel = ChannelProcessRunner::new(
                &raw,
                ReadLoopScope::Wait,
                route.clone(),
                fixture.paths.clone(),
                ClientDeps {
                    identity: Arc::new(
                        mac_worker::controller::channel::identity::StdioIdentitySource::new(),
                    ),
                    pins: Arc::new(PrivatePinStore::new()),
                    forwards: forwards.clone(),
                    connector: connector.clone(),
                    runtime: Arc::new(ManualRuntime::default()),
                },
            );
            let req = request_fixture(
                "task.wait.poll",
                json!({"task_id":"018f0f4a6b5c7d8e9f00112233445566"}),
            );
            let config = mac_worker::config::Config::parse(
                "version=1\n[controller]\nenabled=true\nssh='old-controller'\n",
            )
            .unwrap();
            let mut process = controller_rpc_ssh_request(&config.controller).unwrap();
            process.stdin = Some(frame(&req));
            let mut priming = process.clone();
            priming.stdin = Some(
                encode_json_frame(&json!({
                    "protocol_version":req.protocol_version(),
                    "request_id":"11111111111141118111111111111111",
                    "command":req.command(),
                    "body":req.body(),
                }))
                .unwrap(),
            );
            assert!(channel.run(&priming).unwrap().status.success());
            assert_eq!(
                raw.calls.lock().unwrap().as_slice(),
                [priming.stdin.unwrap()]
            );
            assert_eq!(forwards.resolutions(), 0);
            assert_eq!(forwards.opens(), 0);
            assert_eq!(forwards.cancels(), 0);
            assert_eq!(connector.connections(), 0);
            assert!(connector.frames().is_empty());
            assert!(
                !fixture
                    .paths
                    .controller_cache_root()
                    .join("channel")
                    .exists()
            );
            assert!(!fixture.paths.state.exists());
            assert!(!fixture.paths.controller_state_root().exists());
            assert!(channel.run(&process).unwrap().status.success());
            assert_eq!(raw.calls.lock().unwrap().len(), 3);
            let identity =
                mac_worker::controller::decode_request(&raw.calls.lock().unwrap()[1]).unwrap();
            assert_eq!(identity.command(), "task.list");
            assert_eq!(identity.body()["controller_socket"]["op"], "identity");
            assert_eq!(raw.calls.lock().unwrap()[2], frame(&req));
            assert_eq!(forwards.opens(), 0);
            assert_eq!(connector.connections(), 0);
            assert!(!fixture.paths.state.exists());
            assert!(!fixture.paths.controller_state_root().exists());
        }

        #[test]
        fn compatibility_restart_pin_and_installed_image_replacement_rollback() {
            let fixture = Fixture::new();
            let (task, _) = seed_task(&fixture, false);
            let mut first = fixture.leader();
            let old = fixture.record();
            let (_, identity) = fixture.identity();
            let SocketIdentityResult::Available(identity) =
                serde_json::from_value(identity["result"].clone()).unwrap()
            else {
                panic!()
            };
            let pins = PrivatePinStore::new();
            pins.verify_or_create(&fixture.paths, &identity).unwrap();
            let bytes = fs::read(pin_path(&fixture)).unwrap();
            let inode = fs::metadata(pin_path(&fixture)).unwrap().ino();
            let replacement = fixture.installed.with_file_name("replacement");
            fs::copy(env!("CARGO_BIN_EXE_worker"), &replacement).unwrap();
            let new_inode = fs::metadata(&replacement).unwrap().ino();
            let rollback = fixture.installed.with_file_name("rollback");
            fs::rename(&fixture.installed, &rollback).unwrap();
            fs::rename(&replacement, &fixture.installed).unwrap();
            assert_ne!(old.executable.binding.inode, new_inode);
            assert_eq!(
                fs::metadata(&old.executable.path).unwrap().ino(),
                old.executable.binding.inode
            );
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let stream = connect(&old).await;
                let result = read(
                    &stream,
                    &request_fixture("task.wait.poll", json!({"task_id":task})),
                )
                .await;
                assert_eq!(result["result"]["quiescent"], true);
            });
            first.stop();
            assert!(old.executable.path.exists());
            let mut second = fixture.leader();
            let next = fixture.record();
            assert_eq!(next.executable.binding.inode, new_inode);
            assert_ne!(old.service.leader, next.service.leader);
            assert_ne!(
                old.service.service_generation,
                next.service.service_generation
            );
            let (_, refreshed) = fixture.identity();
            let SocketIdentityResult::Available(refreshed) =
                serde_json::from_value(refreshed["result"].clone()).unwrap()
            else {
                panic!()
            };
            pins.verify_or_create(&fixture.paths, &refreshed).unwrap();
            assert_eq!(fs::read(pin_path(&fixture)).unwrap(), bytes);
            assert_eq!(fs::metadata(pin_path(&fixture)).unwrap().ino(), inode);
            second.stop();
            fs::rename(&rollback, &fixture.installed).unwrap();
            let mut third = fixture.leader();
            let restored = fixture.record();
            assert_eq!(
                restored.executable.binding.inode,
                old.executable.binding.inode
            );
            assert!(
                fixture
                    .rpc(&request_fixture("task.wait.poll", json!({"task_id":task})))
                    .status
                    .success()
            );
            third.stop();
        }

        #[test]
        fn compatibility_account_and_reinstall_mismatch_emit_no_socket_reads() {
            use mac_worker::controller::{
                channel::{
                    client::ChannelProcessRunner,
                    testing::{
                        FakeForwardControl, ManualRuntime, RecordingRunner, ScriptedConnector,
                        ScriptedIdentitySource, identity_fixture, result_fixture,
                    },
                },
                controller_rpc_ssh_request,
            };
            let fixture = Fixture::new();
            let config = mac_worker::config::Config::parse(
                "version=1\n[controller]\nenabled=true\nssh='mismatch'\n",
            )
            .unwrap();
            let route = ConfiguredRoute::new(&config.controller, &config.ssh).unwrap();
            let mut original = identity_fixture();
            original.route_sha256 = route.digest().unwrap();
            PrivatePinStore::new()
                .verify_or_create(&fixture.paths, &original)
                .unwrap();
            let pin = fixture
                .paths
                .controller_cache_root()
                .join("channel/pins")
                .join(format!("{}.json", original.route_sha256));
            let bytes = fs::read(&pin).unwrap();
            let inode = fs::metadata(&pin).unwrap().ino();
            for variant in 0..4 {
                let mut changed = original.clone();
                match variant {
                    0 => {
                        changed.service.controller_client_id = mac_worker::job::ClientId::generate()
                    }
                    1 => changed.service.account.uid += 1,
                    2 => changed.service.account.username = "reinstalled-account".into(),
                    _ => changed.service.account.home = "/Users/reinstalled-account".into(),
                }
                let req = request_fixture(
                    "task.wait.poll",
                    json!({"task_id":"018f0f4a6b5c7d8e9f00112233445566"}),
                );
                let raw = RecordingRunner::new(vec![Ok(result_fixture(
                    &req,
                    json!({"task_ids":[req.body()["task_id"]],"quiescent":true,"exit_code":0}),
                    0,
                ))]);
                let forwards = Arc::new(FakeForwardControl::new(
                    original.service.socket_path.clone(),
                ));
                let connector = Arc::new(ScriptedConnector::new(vec![]));
                let client = ChannelProcessRunner::new(
                    &raw,
                    ReadLoopScope::Wait,
                    route.clone(),
                    fixture.paths.clone(),
                    ClientDeps {
                        identity: Arc::new(ScriptedIdentitySource::new(vec![Ok(changed)])),
                        pins: Arc::new(PrivatePinStore::new()),
                        forwards: forwards.clone(),
                        connector: connector.clone(),
                        runtime: Arc::new(ManualRuntime::default()),
                    },
                );
                let mut process = controller_rpc_ssh_request(&config.controller).unwrap();
                process.stdin = Some(frame(&req));
                assert!(client.run(&process).unwrap().status.success());
                assert_eq!(raw.calls().len(), 1);
                assert_eq!(raw.calls()[0].stdin, process.stdin);
                assert_eq!(forwards.opens(), 0);
                assert_eq!(connector.connections(), 0);
                assert_eq!(fs::read(&pin).unwrap(), bytes);
                assert_eq!(fs::metadata(&pin).unwrap().ino(), inode);
            }
        }

        #[test]
        fn compatibility_notify_cache_and_lock_remain_independent_when_config_route_changes() {
            use mac_worker::controller::{
                channel::testing::identity_fixture,
                events::{NotifyState, notify::NotifyCache},
            };
            use sha2::{Digest, Sha256};
            let fixture = Fixture::new();
            let mut config = mac_worker::config::Config::parse(
                "version=1\n[controller]\nenabled=true\nssh='cache-controller'\n",
            )
            .unwrap();
            let mut state = NotifyState::empty();
            state.repair_needed = true;
            let cache = NotifyCache::open(&fixture.paths, &config.controller).unwrap();
            cache.save(&state).unwrap();
            let mut hash = Sha256::new();
            hash.update(b"cache-controller");
            hash.update([0xff]);
            hash.update(b"~/.local/bin/worker");
            let directory = fixture
                .paths
                .controller_cache_root()
                .join("events")
                .join(format!("{:x}", hash.finalize()));
            let snapshots: Vec<_> = ["notify.json", "notify.lock"]
                .into_iter()
                .map(|name| {
                    let path = directory.join(name);
                    let meta = fs::metadata(&path).unwrap();
                    (
                        path.clone(),
                        fs::read(path).unwrap(),
                        meta.ino(),
                        meta.mode(),
                    )
                })
                .collect();
            let mut digests = vec![];
            for name in ["old-config", "new-config"] {
                config.ssh.config_file = Some(fixture.installed.with_file_name(name));
                let route = ConfiguredRoute::new(&config.controller, &config.ssh).unwrap();
                let mut identity = identity_fixture();
                identity.route_sha256 = route.digest().unwrap();
                PrivatePinStore::new()
                    .verify_or_create(&fixture.paths, &identity)
                    .unwrap();
                digests.push(identity.route_sha256);
            }
            assert_ne!(digests[0], digests[1]);
            for (path, bytes, inode, mode) in snapshots {
                assert_eq!(fs::read(&path).unwrap(), bytes);
                let meta = fs::metadata(path).unwrap();
                assert_eq!(meta.ino(), inode);
                assert_eq!(meta.mode(), mode);
            }
            assert_eq!(cache.load().unwrap(), state);
            drop(cache);
            assert_eq!(
                NotifyCache::open(&fixture.paths, &config.controller)
                    .unwrap()
                    .load()
                    .unwrap(),
                state
            );
        }

        #[test]
        fn raw_opposing_drain_and_intervening_publish_retry_reconcile_preserve_actual_effects() {
            use crate::fixture::{ControllerBridge, IsolatedHost};
            use mac_worker::{
                controller::{
                    channel::{
                        client::ChannelProcessRunner,
                        testing::{
                            FakeForwardControl, ManualRuntime, ScriptedConnector,
                            ScriptedIdentitySource,
                        },
                    },
                    controller_rpc_ssh_request,
                },
                error::WorkerError,
                job::{CommandSummary, QueueEntry, QueueEntryKind},
                outbox::OutboxRetryResponse,
                process::{ProcessRequest, ProcessResult},
                scheduler::WorkerPreference,
                supervisor::SystemProcessInspector,
                task::{DeliveryState, OriginDelivery},
            };
            use std::{
                os::unix::process::ExitStatusExt,
                process::ExitStatus,
                sync::{
                    Mutex,
                    atomic::{AtomicU32, Ordering},
                },
            };
            struct Worker {
                task: TaskId,
                turn: TurnId,
                base: mac_worker::task::BaseOid,
                calls: AtomicU32,
            }
            impl ProcessRunner for Worker {
                fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
                    assert_eq!(request.program, "/usr/bin/ssh");
                    assert!(request.args.iter().any(|arg| arg == "fixture-worker"));
                    assert!(
                        request
                            .args
                            .last()
                            .unwrap()
                            .to_string_lossy()
                            .ends_with(&format!("host outbox-retry {}", self.task))
                    );
                    let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
                    let delivery = OriginDelivery::new(
                        self.turn,
                        DeliveryState::Retrying,
                        self.base.clone(),
                        "https://example.test/repo".into(),
                        "refs/heads/retry-fixture".into(),
                        attempt,
                        1,
                        None,
                        None,
                        1,
                        3,
                    )
                    .unwrap();
                    Ok(ProcessResult {
                        status: ExitStatus::from_raw(0),
                        stdout: serde_json::to_vec(&OutboxRetryResponse::new(vec![delivery]))
                            .unwrap(),
                        stderr: vec![],
                    })
                }
            }
            struct Observed<'a> {
                bridge: ControllerBridge<'a>,
                calls: Mutex<Vec<ProcessRequest>>,
            }
            impl ProcessRunner for Observed<'_> {
                fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
                    self.calls.lock().unwrap().push(request.clone());
                    self.bridge.run(request)
                }
            }
            let controller = IsolatedHost::new(true);
            let laptop = IsolatedHost::new(false);
            let record = controller.seed(true, false);
            let task = record.meta().task_id();
            let turn = record.status().turns()[0].turn_id();
            let worker = Worker {
                task,
                turn,
                base: record.meta().base_oid().clone(),
                calls: AtomicU32::new(0),
            };
            let raw = Observed {
                bridge: ControllerBridge {
                    controller: &controller,
                    worker: &worker,
                },
                calls: Mutex::default(),
            };
            let config = mac_worker::config::Config::load(&laptop.paths.config).unwrap();
            let forwards = Arc::new(FakeForwardControl::new("/private/unused-forward".into()));
            let connector = Arc::new(ScriptedConnector::new(vec![]));
            let client = ChannelProcessRunner::new(
                &raw,
                ReadLoopScope::Wait,
                ConfiguredRoute::new(&config.controller, &config.ssh).unwrap(),
                laptop.paths.clone(),
                ClientDeps {
                    identity: Arc::new(ScriptedIdentitySource::new(vec![])),
                    pins: Arc::new(PrivatePinStore::new()),
                    forwards: forwards.clone(),
                    connector: connector.clone(),
                    runtime: Arc::new(ManualRuntime::default()),
                },
            );
            let store = ClientStateStore::open(&controller.paths.state).unwrap();
            let owner = SystemProcessInspector
                .identity_for_pid(std::process::id())
                .unwrap();
            store
                .enqueue(
                    QueueEntry::new(
                        turn,
                        store.client_id(),
                        record.meta().project_id().into(),
                        record.meta().worktree_id().into(),
                        CommandSummary::argv(2).unwrap(),
                        vec![],
                        WorkerPreference::Automatic,
                        QueueEntryKind::TaskTurn,
                        None,
                        owner,
                        1,
                    )
                    .unwrap(),
                )
                .unwrap();
            store.park_row(turn).unwrap();
            let send = |request: &ControllerRequest| {
                let mut process = controller_rpc_ssh_request(&config.controller).unwrap();
                process.stdin = Some(frame(request));
                let result = client.run(&process).unwrap();
                assert!(
                    result.status.success(),
                    "{}",
                    String::from_utf8_lossy(&result.stdout)
                );
                assert_eq!(raw.calls.lock().unwrap().last().unwrap(), &process);
                serde_json::from_slice::<Value>(decode_frame(&result.stdout).unwrap()).unwrap()
            };
            for (index, drained) in [true, false, true].into_iter().enumerate() {
                let request = request_fixture("controller.drain", json!({"drained":drained}));
                assert_eq!(send(&request)["result"]["drained"], drained);
                assert_eq!(
                    mac_worker::controller::drain::is_drained(
                        &controller.paths.controller_state_root()
                    )
                    .unwrap(),
                    drained
                );
                let publish = request_fixture("task.publish-retry", json!({"task_id":task}));
                assert_eq!(send(&publish)["result"]["deliveries"][0]["attempt"], index);
                assert_eq!(
                    store.load_task(task).unwrap().deliveries()[0].attempt(),
                    index as u32
                );
                store
                    .record_replacement_failure(turn, "RUNNER_EARLY_EXIT", 1)
                    .unwrap();
                assert!(
                    store
                        .queue_entry(turn)
                        .unwrap()
                        .unwrap()
                        .replacement_failure()
                        .is_some()
                );
                let reconcile = request_fixture("task.reconcile", json!({}));
                send(&reconcile);
                assert!(
                    store
                        .queue_entry(turn)
                        .unwrap()
                        .unwrap()
                        .replacement_failure()
                        .is_none(),
                    "raw reconcile clears the current failure budget each time"
                );
            }
            assert_eq!(raw.calls.lock().unwrap().len(), 9);
            assert_eq!(worker.calls.load(Ordering::SeqCst), 3);
            assert_eq!(forwards.resolutions(), 0);
            assert_eq!(forwards.opens(), 0);
            assert_eq!(connector.connections(), 0);
            assert!(
                !laptop
                    .paths
                    .controller_cache_root()
                    .join("channel")
                    .exists()
            );
            assert!(
                !controller
                    .paths
                    .controller_state_root()
                    .join("active")
                    .exists()
            );
        }

        struct LocalStdio {
            installed: PathBuf,
            config: PathBuf,
            environment: BTreeMap<OsString, OsString>,
            calls: std::sync::Mutex<Vec<ControllerRequest>>,
            processes: std::sync::Mutex<Vec<mac_worker::process::ProcessRequest>>,
        }
        impl LocalStdio {
            fn new(fixture: &Fixture) -> Self {
                Self {
                    installed: fixture.installed.clone(),
                    config: fixture.paths.config.clone(),
                    environment: fixture.environment.clone(),
                    calls: std::sync::Mutex::default(),
                    processes: std::sync::Mutex::default(),
                }
            }
        }
        impl ProcessRunner for LocalStdio {
            fn run(
                &self,
                process: &mac_worker::process::ProcessRequest,
            ) -> Result<mac_worker::process::ProcessResult, mac_worker::error::WorkerError>
            {
                self.run_interruptible(process, &|| false)
            }
            fn run_interruptible(
                &self,
                process: &mac_worker::process::ProcessRequest,
                stop: &dyn Fn() -> bool,
            ) -> Result<mac_worker::process::ProcessResult, mac_worker::error::WorkerError>
            {
                self.calls.lock().unwrap().push(
                    mac_worker::controller::decode_request(process.stdin.as_ref().unwrap())
                        .unwrap(),
                );
                self.processes.lock().unwrap().push(process.clone());
                let mut local = process.clone();
                local.program = self.installed.as_os_str().to_owned();
                local.args = vec![
                    "--config".into(),
                    self.config.as_os_str().to_owned(),
                    "host".into(),
                    "controller-rpc".into(),
                ];
                local.environment = self
                    .environment
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                local.isolate_parent_environment = true;
                mac_worker::process::SystemProcessRunner.run_interruptible(&local, stop)
            }
        }
        #[test]
        fn journal_actual_epoch_reset_uses_event_reply_while_wait_logs_and_pin_stay_valid() {
            use mac_worker::controller::{
                channel::{
                    codec::FramedSocketConnector, identity::StdioIdentitySource,
                    testing::FakeForwardControl,
                },
                events::{
                    EventReadResult, EventSource, ReadQuery, client::ControllerEventClient,
                    testing::ManualEventRuntime,
                },
            };
            let fixture = Fixture::new();
            let (task, _) = seed_task(&fixture, false);
            let mut leader = fixture.leader();
            let record = fixture.record();
            let replacement = Fixture::new();
            let mut replacement_leader = replacement.leader();
            let replacement_record = replacement.record();
            replacement_leader.stop();
            let laptop = Fixture::new();
            let config=mac_worker::config::Config::parse("version=1\n[controller]\nenabled=true\nssh='journal-fixture'\n[ssh]\nmultiplex=true\n").unwrap();
            let raw = Arc::new(LocalStdio::new(&fixture));
            let forwards = Arc::new(FakeForwardControl::new(record.service.socket_path.clone()));
            let clock = Arc::new(ManualEventRuntime::new());
            let source = ControllerEventClient::for_read_loop(
                raw.clone(),
                ReadLoopScope::EventsFollow,
                &laptop.paths,
                &config,
                clock.clone(),
                ClientDeps {
                    identity: Arc::new(StdioIdentitySource::new()),
                    pins: Arc::new(PrivatePinStore::new()),
                    forwards: forwards.clone(),
                    connector: Arc::new(FramedSocketConnector::new(Arc::new(SessionCodec::new()))),
                    runtime: Arc::new(
                        mac_worker::controller::channel::testing::ManualRuntime::default(),
                    ),
                },
            );
            source.discover(GUARD).unwrap();
            {
                let calls = raw.calls.lock().unwrap();
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].command(), "task.list");
                assert_eq!(calls[0].body(), &json!({"controller_health":true}));
            }
            assert_eq!(raw.processes.lock().unwrap().len(), 1);
            assert_eq!(forwards.resolutions(), 0);
            assert_eq!(forwards.opens(), 0);
            assert_eq!(forwards.cancels(), 0);
            assert!(
                !laptop
                    .paths
                    .controller_cache_root()
                    .join("channel")
                    .exists()
            );
            let EventReadResult::SnapshotRequired(initial) =
                source.read(ReadQuery::default(), GUARD).unwrap()
            else {
                panic!("cold cursor needs baseline")
            };
            assert_eq!(
                initial.window.journal_id.to_string(),
                record.service.journal_id.as_ref().unwrap().as_str()
            );
            let old_cursor = initial.window.cursor();
            assert!(matches!(
                source
                    .read(
                        ReadQuery {
                            after: Some(old_cursor),
                            wait_ms: 0,
                            limit: 1
                        },
                        GUARD
                    )
                    .unwrap(),
                EventReadResult::Batch(_)
            ));
            let route = ConfiguredRoute::new(&config.controller, &config.ssh).unwrap();
            let pin = laptop
                .paths
                .controller_cache_root()
                .join("channel/pins")
                .join(format!("{}.json", route.digest().unwrap()));
            let pin_bytes = fs::read(&pin).unwrap();
            let pin_inode = fs::metadata(&pin).unwrap().ino();
            fs::rename(
                fixture.paths.controller_state_root().join("events"),
                fixture.paths.controller_state_root().join("old-events"),
            )
            .unwrap();
            fs::rename(
                replacement.paths.controller_state_root().join("events"),
                fixture.paths.controller_state_root().join("events"),
            )
            .unwrap();
            let EventReadResult::SnapshotRequired(reset) = source
                .read(
                    ReadQuery {
                        after: Some(old_cursor),
                        wait_ms: 0,
                        limit: 1,
                    },
                    GUARD,
                )
                .unwrap()
            else {
                panic!("actual changed epoch must reset the cursor")
            };
            assert_eq!(reset.reason, "journal_changed");
            assert_eq!(
                reset.window.journal_id.to_string(),
                replacement_record.service.journal_id.unwrap().as_str()
            );
            assert_ne!(reset.window.journal_id, old_cursor.journal_id);
            assert_eq!(forwards.opens(), 1, "journal hint cannot break the session");
            assert_eq!(
                raw.calls.lock().unwrap().len(),
                2,
                "only the priming read and trusted identity bootstrap use stdio"
            );
            assert_eq!(fs::read(pin).unwrap(), pin_bytes);
            assert_eq!(
                fs::metadata(
                    laptop
                        .paths
                        .controller_cache_root()
                        .join("channel/pins")
                        .join(format!("{}.json", route.digest().unwrap()))
                )
                .unwrap()
                .ino(),
                pin_inode
            );
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let stream = connect(&record).await;
                read(
                    &stream,
                    &request_fixture("task.wait.poll", json!({"task_id":task})),
                )
                .await;
                read(
                    &stream,
                    &request_fixture("task.logs", json!({"task_id":task,"raw":true,"wait_ms":0})),
                )
                .await;
            });
            drop(source);
            assert_eq!(forwards.cancels(), 1);
            leader.stop();
        }

        // A transfer-cache lock is a real cross-process barrier. The runner
        // finishes a committed log and launches its next parked row; no SSH
        // or agent process is necessary to exercise that production handoff.
        fn completed_parked(
            fixture: &Fixture,
            project: &Path,
            worktree: &str,
        ) -> (TaskId, TurnId, fs::File) {
            let (task, turn) = seed_task(fixture, true);
            let store = ClientStateStore::open(&fixture.paths.state).unwrap();
            let mut record = serde_json::to_value(store.load_task(task).unwrap()).unwrap();
            record["meta"]["worktree_id"] = json!(worktree);
            let record = serde_json::from_value(record).unwrap();
            store.replace_task_fixture(record).unwrap();
            store
                .write_task_project_path(&store.load_task(task).unwrap(), project)
                .unwrap();
            mac_worker::controller::registry::ProjectRegistry::open(
                &fixture.paths.controller_state_root(),
            )
            .unwrap()
            .register(&"a".repeat(64), worktree, project)
            .unwrap();
            // The queue identity must match the task's frozen worktree.
            let queue_path = fixture.paths.state.join("queue/state.json");
            let mut queue: Value = serde_json::from_slice(&fs::read(&queue_path).unwrap()).unwrap();
            for entry in queue["entries"].as_array_mut().unwrap() {
                if entry["job_id"] == turn.to_string() {
                    entry["worktree_id"] = json!(worktree);
                }
            }
            let queue: mac_worker::job::QueueSnapshot = serde_json::from_value(queue).unwrap();
            let mut bytes = serde_json::to_vec(&queue).unwrap();
            bytes.push(b'\n');
            fs::write(&queue_path, bytes).unwrap();
            store.open_runner_log(task, turn).unwrap();
            let checkpoint = fixture
                .paths
                .state
                .join("runners")
                .join(task.to_string())
                .join(format!("{turn}.checkpoint.json"));
            fs::write(&checkpoint, serde_json::to_vec(&json!({"version":1,"task_id":task,"turn_id":turn,"committed":{"offsets":[0,0],"len":0,"accepted":true,"completion":{"outcome":{"kind":"done"},"drained":true}},"pending":null})).unwrap()).unwrap();
            fs::set_permissions(&checkpoint, fs::Permissions::from_mode(0o600)).unwrap();
            let transfer = TransferRepo::open_or_create_controller_cache(
                &fixture.paths.cache,
                &"a".repeat(64),
                worktree,
            )
            .unwrap();
            drop(transfer);
            let id = TransferRepo::controller_transfer_cache_id(&"a".repeat(64), worktree).unwrap();
            let lock = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(
                    fixture
                        .paths
                        .cache
                        .join("controller-transfer")
                        .join(format!("{id}.lock")),
                )
                .unwrap();
            assert_eq!(
                unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
                0
            );
            (task, turn, lock)
        }
        async fn runner(fixture: &Fixture, task: TaskId) -> mac_worker::job::ProcessIdentity {
            tokio::time::timeout(GUARD, async {
                loop {
                    if let Some(runner) = ClientStateStore::open(&fixture.paths.state)
                        .unwrap()
                        .load_task(task)
                        .unwrap()
                        .runner()
                    {
                        return runner.process_identity();
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("runner launch barrier")
        }
        fn command_line(pid: u32) -> String {
            let output = Command::new("/bin/ps")
                .args(["-p", &pid.to_string(), "-o", "command="])
                .output()
                .unwrap();
            assert!(output.status.success());
            String::from_utf8(output.stdout).unwrap()
        }
        fn repo(fixture: &Fixture) -> PathBuf {
            let path = fixture.installed.with_file_name("project");
            fs::create_dir(&path).unwrap();
            let status = Command::new("/usr/bin/git")
                .arg("init")
                .arg("-q")
                .arg(&path)
                .env_clear()
                .env("HOME", &fixture.home)
                .env("PATH", "/usr/bin:/bin")
                .status()
                .unwrap();
            assert!(status.success());
            path
        }
        fn runner_handoff(replace: bool) {
            let fixture = Fixture::new();
            let project = repo(&fixture);
            let (first_task, first_turn, first_lock) =
                completed_parked(&fixture, &project, &"b".repeat(64));
            let (leader, config, mut deps, stop) = native_inputs(&fixture);
            let (reporter, courier) = children::reporter(&fixture);
            deps.runner = reporter.clone();
            use mac_worker::controller::events::{
                JournalReader, NewEvent,
                journal::{ControllerJournal, JournalOptions},
                testing::{ManualEventRuntime, RecordingSink},
            };
            let journal = ControllerJournal::initialize_for_leader(
                &fixture.paths,
                &leader,
                JournalOptions {
                    runtime: Arc::new(ManualEventRuntime::new()),
                },
            )
            .unwrap();
            let sink = Arc::new(RecordingSink::new());
            let hints = mac_worker::client_state::events::DeferredHints::begin(sink.clone());
            hints.capture(NewEvent::ControllerDrainChanged { drained: true });
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let channel = LeaderChannel::start(config, leader.clone(), deps, stop);
                channel_ready(&channel).await;
                let record = fixture.record();
                let stream = connect(&record).await;
                let reply = read(
                    &stream,
                    &request_fixture("task.wait.poll", json!({"task_id":first_task})),
                )
                .await;
                let (_, rpc_group, _, _) = children::entered(&courier).await;
                children::finished(&reporter, 1).await;
                assert_eq!(unsafe { libc::killpg(rpc_group, 0) }, -1);
                assert_eq!(reply["result"]["quiescent"], false);
                let first = runner(&fixture, first_task).await;
                assert!(command_line(first.pid()).starts_with(fixture.installed.to_str().unwrap()));
                let (second_task, second_turn, second_lock) =
                    completed_parked(&fixture, &project, &"d".repeat(64));
                assert!(
                    sink.batches().is_empty(),
                    "parent DeferredHints stay deferred"
                );
                assert!(
                    journal.window(GUARD).unwrap().head_seq.as_u64() > 0,
                    "socket child flushes its own publisher before process completion"
                );
                let state_lock = children::state_lock(&fixture);
                send(
                    &stream,
                    &frame(&request_fixture(
                        "task.logs",
                        json!({"task_id":first_task,"raw":true,"wait_ms":0}),
                    )),
                )
                .await;
                let (blocked_pid, blocked_group, _, _) = children::entered(&courier).await;
                assert_eq!(unsafe { libc::kill(blocked_pid, 0) }, 0);
                drop(stream);
                children::finished(&reporter, 2).await;
                assert_eq!(unsafe { libc::killpg(blocked_group, 0) }, -1);
                assert_eq!(
                    unsafe { libc::kill(first.pid() as i32, 0) },
                    0,
                    "cancelled transient RPC cannot cancel the detached runner"
                );
                drop(state_lock);
                assert!(
                    !ClientStateStore::open(&fixture.paths.state)
                        .unwrap()
                        .queue_entry(first_turn)
                        .unwrap()
                        .unwrap()
                        .is_cancel_requested()
                );
                let shutdown = channel.shutdown().await;
                assert_eq!(shutdown.rpc.completed, 2);
                assert_eq!(shutdown.rpc.unknown, 0);
                assert_eq!(shutdown.files, ForwardDisposition::Cleaned);
                assert!(record.executable.path.exists());
                assert!(!record.service.socket_path.exists());
                assert_eq!(
                    unsafe { libc::kill(first.pid() as i32, 0) },
                    0,
                    "detached runner survives discovery withdrawal with the image retained"
                );
                if replace {
                    let replacement = fixture.installed.with_file_name("next-worker");
                    fs::copy(env!("CARGO_BIN_EXE_worker"), &replacement).unwrap();
                    assert_ne!(
                        fs::metadata(&replacement).unwrap().ino(),
                        record.executable.binding.inode
                    );
                    fs::rename(replacement, &fixture.installed).unwrap();
                }
                drop(leader);
                let (leader, config, deps, stop) = native_inputs(&fixture);
                let next = LeaderChannel::start(config, leader.clone(), deps, stop);
                channel_ready(&next).await;
                let next_record = fixture.record();
                assert_ne!(
                    next_record.service.service_generation,
                    record.service.service_generation
                );
                if replace {
                    assert_ne!(
                        next_record.executable.binding.inode,
                        record.executable.binding.inode
                    );
                }
                drop(first_lock);
                let second = runner(&fixture, second_task).await;
                assert_ne!(first.pid(), second.pid());
                let command = command_line(second.pid());
                assert!(
                    command.starts_with(fixture.installed.to_str().unwrap()),
                    "handoff used {command}"
                );
                assert!(!command.contains(record.executable.path.to_str().unwrap()));
                let store = ClientStateStore::open(&fixture.paths.state).unwrap();
                assert!(
                    store.queue_entry(first_turn).unwrap().is_none(),
                    "first runner finished and retired its turn"
                );
                drop(second_lock);
                tokio::time::timeout(GUARD, async {
                    while store.queue_entry(second_turn).unwrap().is_some() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("second runner completes after handoff");
                assert!(store.load_task(second_task).unwrap().runner().is_none());
                assert_eq!(next.shutdown().await.files, ForwardDisposition::Cleaned);
            });
            assert!(sink.batches().is_empty());
            hints.finish();
            assert_eq!(sink.batches().len(), 1);
        }
        #[test]
        fn image_runner_handoff_after_rpc_link_withdrawal_and_leader_restart() {
            runner_handoff(false);
        }
        #[test]
        fn image_runner_handoff_executes_installed_replacement_after_link_withdrawal() {
            runner_handoff(true);
        }

        #[test]
        fn image_runner_handoff_state_fence_waits_for_an_active_writer() {
            let fixture = Fixture::new();
            seed_task(&fixture, false);
            let mut writer = Command::new("/usr/bin/python3")
                .args([
                    "-c",
                    "import fcntl,sys\nf=open(sys.argv[1],'r+')\nfcntl.flock(f,fcntl.LOCK_EX)\nprint('locked',flush=True)\nsys.stdin.buffer.read(1)\nfcntl.flock(f,fcntl.LOCK_UN)\nsys.stdin.buffer.read(1)\ntry:\n fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)\nexcept BlockingIOError:\n sys.exit(0)\nsys.exit(1)\n",
                ])
                .arg(fixture.paths.state.join("jobs.lock"))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let stdout = writer.stdout.take().unwrap();
            let (tx, rx) = mpsc::channel();
            let reader = std::thread::spawn(move || {
                let mut line = String::new();
                BufReader::new(stdout).read_line(&mut line).unwrap();
                let _ = tx.send(line);
            });
            assert_eq!(rx.recv_timeout(GUARD).unwrap(), "locked\n");
            reader.join().unwrap();
            let mut contended = false;
            let lock = children::state_lock_on_contention(&fixture, || {
                if !contended {
                    contended = true;
                    writer.stdin.as_mut().unwrap().write_all(b"\n").unwrap();
                }
            });
            assert!(contended, "the real writer must hold the state fence first");
            writer.stdin.as_mut().unwrap().write_all(b"\n").unwrap();
            assert!(
                writer.wait().unwrap().success(),
                "the returned guard must exclude the real writer"
            );
            drop(lock);
        }

        mod children {
            use super::*;
            use mac_worker::{
                controller::channel::server::ShutdownEvidence,
                error::WorkerError,
                process::{
                    CleanupState, ProcessCompletion, ProcessRequest, ProcessResult,
                    SystemProcessRunner, TrackedProcessRunner,
                },
            };
            use std::{
                io::Read,
                os::unix::net::UnixDatagram,
                sync::{
                    Mutex,
                    atomic::{AtomicBool, Ordering},
                },
            };

            pub(super) struct ReportingRunner {
                courier: PathBuf,
                calls: Mutex<Vec<ProcessRequest>>,
                completions: Mutex<Vec<CleanupState>>,
                unknown: AtomicBool,
            }
            impl ProcessRunner for ReportingRunner {
                fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
                    self.run_interruptible_with_cleanup(request, &|| false)
                        .outcome
                }
            }
            impl TrackedProcessRunner for ReportingRunner {
                fn run_interruptible_with_cleanup(
                    &self,
                    request: &ProcessRequest,
                    stop: &dyn Fn() -> bool,
                ) -> ProcessCompletion {
                    self.calls.lock().unwrap().push(request.clone());
                    // Report only after stdin EOF, then exec the actual pinned
                    // worker in this same PID/group with identical framed stdin.
                    let program = request.program.to_str().unwrap();
                    let args: Vec<_> = request
                        .args
                        .iter()
                        .map(|arg| arg.to_str().unwrap())
                        .collect();
                    let script = format!(
                        "import os,sys,socket,json,base64\nb=sys.stdin.buffer.read()\ns=socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM)\ns.sendto(json.dumps([os.getpid(),os.getpgrp(),base64.b64encode(b).decode(),dict(os.environ)]).encode(), {})\nr,w=os.pipe()\nos.write(w,b)\nos.close(w)\nos.dup2(r,0)\nos.close(r)\nos.execv({},[{}]+{})\n",
                        serde_json::to_string(&self.courier).unwrap(),
                        serde_json::to_string(program).unwrap(),
                        serde_json::to_string(program).unwrap(),
                        serde_json::to_string(&args).unwrap()
                    );
                    let mut observed = request.clone();
                    observed.program = "/usr/bin/python3".into();
                    observed.args = vec!["-c".into(), script.into()];
                    let mut completion =
                        SystemProcessRunner.run_interruptible_with_cleanup(&observed, stop);
                    if self.unknown.load(Ordering::Acquire) {
                        completion.cleanup = CleanupState::Unknown;
                    }
                    self.completions.lock().unwrap().push(completion.cleanup);
                    completion
                }
            }
            pub(super) fn reporter(fixture: &Fixture) -> (Arc<ReportingRunner>, UnixDatagram) {
                let path = fixture.installed.with_file_name("child-courier");
                let socket = UnixDatagram::bind(&path).unwrap();
                socket.set_nonblocking(true).unwrap();
                (
                    Arc::new(ReportingRunner {
                        courier: path,
                        calls: Mutex::default(),
                        completions: Mutex::default(),
                        unknown: AtomicBool::new(false),
                    }),
                    socket,
                )
            }
            pub(super) async fn entered(
                socket: &UnixDatagram,
            ) -> (i32, i32, String, BTreeMap<String, String>) {
                tokio::time::timeout(GUARD, async {
                    loop {
                        let mut bytes = [0; 16 * 1024];
                        match socket.recv(&mut bytes) {
                            Ok(n) => return serde_json::from_slice(&bytes[..n]).unwrap(),
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                tokio::task::yield_now().await
                            }
                            Err(error) => panic!("{error}"),
                        }
                    }
                })
                .await
                .unwrap()
            }
            pub(super) async fn finished(reporter: &ReportingRunner, count: usize) {
                tokio::time::timeout(GUARD, async {
                    while reporter.completions.lock().unwrap().len() < count {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            pub(super) fn state_lock(fixture: &Fixture) -> fs::File {
                state_lock_on_contention(fixture, || {})
            }
            pub(super) fn state_lock_on_contention(
                fixture: &Fixture,
                mut on_contention: impl FnMut(),
            ) -> fs::File {
                let lock = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(fixture.paths.state.join("jobs.lock"))
                    .unwrap();
                // A detached runner can still be publishing its state when
                // the test reaches this barrier. Wait for the actual fence,
                // then keep it held while the socket RPC starts and blocks.
                let deadline = Instant::now() + GUARD;
                loop {
                    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0
                    {
                        return lock;
                    }
                    let error = std::io::Error::last_os_error();
                    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock, "{error}");
                    assert!(
                        Instant::now() < deadline,
                        "state fence acquisition hang guard"
                    );
                    on_contention();
                    std::thread::yield_now();
                }
            }

            #[test]
            fn isolation_actual_children_have_distinct_pids_captured_roots_eof_and_fresh_config() {
                use base64::Engine;
                let fixture = Fixture::new();
                let (task, _) = seed_task(&fixture, false);
                let (leader, config, mut deps, stop) = native_inputs(&fixture);
                let (reporter, courier) = reporter(&fixture);
                deps.runner = reporter.clone();
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let channel = LeaderChannel::start(config, leader, deps, stop);
                    channel_ready(&channel).await;
                    let record = fixture.record();
                    let stream = connect(&record).await;
                    let mut pids = std::collections::HashSet::new();
                    for (index, (command, body)) in [
                        ("task.wait.poll", json!({"task_id":task})),
                        ("task.logs", json!({"task_id":task,"wait_ms":0,"raw":true})),
                        ("task.list", json!({"controller_health":true})),
                        ("task.wait.poll", json!({"task_id":task})),
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        let req = request_fixture(command, body);
                        send(&stream, &frame(&req)).await;
                        let (pid, group, bytes, environment) = entered(&courier).await;
                        assert!(pids.insert(pid));
                        assert_ne!(pid, std::process::id() as i32);
                        assert_eq!(pid, group);
                        assert_eq!(
                            base64::engine::general_purpose::STANDARD
                                .decode(bytes)
                                .unwrap(),
                            frame(&req)
                        );
                        for key in [
                            "HOME",
                            "XDG_CONFIG_HOME",
                            "XDG_STATE_HOME",
                            "XDG_DATA_HOME",
                            "XDG_CACHE_HOME",
                        ] {
                            assert_eq!(
                                environment[key],
                                fixture.environment[OsStr::new(key)].to_str().unwrap()
                            );
                        }
                        assert_eq!(
                            environment[DETACHED_RUNNER_EXECUTABLE_ENV],
                            fixture.installed.to_str().unwrap()
                        );
                        let result = SessionCodec::new()
                            .decode_reply(&receive(&stream).await.unwrap(), &req)
                            .unwrap();
                        assert!(result.status.success());
                        finished(&reporter, index + 1).await;
                        assert_eq!(unsafe { libc::killpg(group, 0) }, -1);
                        assert_eq!(
                            std::io::Error::last_os_error().raw_os_error(),
                            Some(libc::ESRCH)
                        );
                    }
                    let captured = fs::read(&fixture.paths.config).unwrap();
                    fs::write(&fixture.paths.config, "invalid TOML [").unwrap();
                    let request = request_fixture("task.wait.poll", json!({"task_id":task}));
                    send(&stream, &frame(&request)).await;
                    entered(&courier).await;
                    let error = SessionCodec::new()
                        .decode_reply(&receive(&stream).await.unwrap(), &request)
                        .unwrap();
                    assert!(!error.status.success());
                    let error: mac_worker::job::HostControlError =
                        serde_json::from_slice(decode_frame(&error.stdout).unwrap()).unwrap();
                    assert_eq!(error.error().code(), "CONFIG");
                    finished(&reporter, 5).await;
                    fs::write(&fixture.paths.config, captured).unwrap();
                    let request = request_fixture("task.wait.poll", json!({"task_id":task}));
                    let reply = read(&stream, &request).await;
                    entered(&courier).await;
                    assert_eq!(reply["result"]["quiescent"], true);
                    finished(&reporter, 6).await;
                    assert!(
                        reporter
                            .completions
                            .lock()
                            .unwrap()
                            .iter()
                            .all(|state| *state == CleanupState::Completed)
                    );
                    for call in reporter.calls.lock().unwrap().iter() {
                        assert_eq!(call.program, record.executable.path.as_os_str());
                        assert_eq!(
                            call.args,
                            vec![
                                OsString::from("--config"),
                                fixture.paths.config.as_os_str().to_owned(),
                                "host".into(),
                                "controller-rpc".into()
                            ]
                        );
                    }
                    assert_eq!(channel.shutdown().await.rpc.completed, 6);
                });
            }

            #[test]
            fn isolation_cross_process_locks_and_twelve_completed_cancels_preserve_service() {
                let fixture = Fixture::new();
                let (task, _) = seed_task(&fixture, false);
                let (leader, config, mut deps, stop) = native_inputs(&fixture);
                let (reporter, courier) = reporter(&fixture);
                deps.runner = reporter.clone();
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let channel=LeaderChannel::start(config,leader,deps,stop); channel_ready(&channel).await; let record=fixture.record();
                    let lock=state_lock(&fixture);
                    for count in 1..=12 {
                        let stream=connect(&record).await;
                        let req=request_fixture("task.logs",json!({"task_id":task,"raw":true,"wait_ms":0})); send(&stream,&frame(&req)).await;
                        let (pid,group,_,_)=entered(&courier).await;
                        // Same-process flock would admit this locked state. A
                        // real child cannot reply while the parent owns it.
                        let mut byte=[0;1]; assert!(matches!(stream.try_read(&mut byte),Err(error) if error.kind()==std::io::ErrorKind::WouldBlock));
                        assert_eq!(unsafe{libc::kill(pid,0)},0); drop(stream); finished(&reporter,count).await;
                        assert_eq!(reporter.completions.lock().unwrap()[count-1],CleanupState::Completed);
                        assert_eq!(unsafe{libc::killpg(group,0)},-1); assert_eq!(std::io::Error::last_os_error().raw_os_error(),Some(libc::ESRCH));
                        assert!(!channel.retired());
                    }
                    drop(lock);
                    let stream=connect(&record).await; assert_eq!(read(&stream,&request_fixture("task.wait.poll",json!({"task_id":task}))).await["result"]["quiescent"],true);
                    entered(&courier).await; finished(&reporter,13).await;
                    let shutdown=channel.shutdown().await; assert_eq!(shutdown.rpc,ShutdownEvidence{completed:13,unknown:0}); assert_eq!(shutdown.files,ForwardDisposition::Cleaned);
                });
            }

            #[test]
            fn isolation_eight_actual_rpc_exits_with_deliberately_unknown_proof_retire_without_replacements()
             {
                let fixture = Fixture::new();
                let (task, _) = seed_task(&fixture, false);
                let (leader, config, mut deps, stop) = native_inputs(&fixture);
                let (reporter, courier) = reporter(&fixture);
                deps.runner = reporter.clone();
                reporter.unknown.store(true, Ordering::Release);
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let channel = LeaderChannel::start(config, leader.clone(), deps, stop.clone());
                    channel_ready(&channel).await;
                    let record = fixture.record();
                    for count in 1..=8 {
                        let stream = connect(&record).await;
                        let req = request_fixture("task.wait.poll", json!({"task_id":task}));
                        send(&stream, &frame(&req)).await;
                        entered(&courier).await;
                        // Outcomes can complete; missing native cleanup proof
                        // still retains the slot and its I/O budget.
                        let _ = receive(&stream).await;
                        finished(&reporter, count).await;
                    }
                    tokio::time::timeout(GUARD, async {
                        while !channel.retired() {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    reporter.unknown.store(false, Ordering::Release);
                    assert!(!stop.load(Ordering::Acquire));
                    assert_eq!(reporter.calls.lock().unwrap().len(), 8);
                    assert!(
                        tokio::net::UnixStream::connect(&record.service.socket_path)
                            .await
                            .is_err()
                    );
                    assert_eq!(
                        mac_worker::controller::ControllerLeader::acquire(
                            &fixture.paths.controller_state_root()
                        )
                        .err()
                        .unwrap()
                        .public_code(),
                        "CONTROLLER_LOCK_HELD"
                    );
                    let shutdown = channel.shutdown().await;
                    assert_eq!(
                        shutdown.rpc,
                        ShutdownEvidence {
                            completed: 0,
                            unknown: 8
                        }
                    );
                    assert_eq!(shutdown.files, ForwardDisposition::Retained);
                    assert!(record.executable.path.exists());
                    assert_eq!(reporter.calls.lock().unwrap().len(), 8);
                });
            }

            #[test]
            fn isolation_actual_leader_stdout_is_diagnostics_and_single_sigint_or_term_stops_it() {
                for signal in [libc::SIGINT, libc::SIGTERM] {
                    let fixture = Fixture::new();
                    let (task, _) = seed_task(&fixture, false);
                    let mut child = fixture
                        .command(&fixture.installed)
                        .args(["controller", "run"])
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .spawn()
                        .unwrap();
                    let stdout = child.stdout.take().unwrap();
                    let (entered_tx, entered_rx) = mpsc::channel();
                    let reader = std::thread::spawn(move || {
                        let mut reader = BufReader::new(stdout);
                        let mut line = String::new();
                        reader.read_line(&mut line).unwrap();
                        entered_tx.send(line.clone()).unwrap();
                        let mut tail = vec![];
                        reader.read_to_end(&mut tail).unwrap();
                        (line, tail)
                    });
                    assert_eq!(
                        entered_rx.recv_timeout(GUARD).unwrap(),
                        "controller leader acquired\n"
                    );
                    let record = fixture.record();
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    rt.block_on(async {
                        let stream = connect(&record).await;
                        for _ in 0..16 {
                            read(
                                &stream,
                                &request_fixture("task.wait.poll", json!({"task_id":task})),
                            )
                            .await;
                        }
                    });
                    assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
                    let start = Instant::now();
                    while child.try_wait().unwrap().is_none() {
                        assert!(start.elapsed() < GUARD);
                        std::thread::yield_now();
                    }
                    let output = child.wait_with_output().unwrap();
                    assert!(
                        output.status.success(),
                        "{}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                    let (line, tail) = reader.join().unwrap();
                    assert_eq!(line, "controller leader acquired\n");
                    assert!(
                        tail.is_empty(),
                        "RPC frames reached leader stdout: {tail:?}"
                    );
                    assert!(record.executable.path.exists());
                    assert!(!record.service.socket_path.exists());
                }
            }
        }

        mod recovery {
            use super::*;
            use mac_worker::{
                controller::{
                    channel::{
                        client::ChannelProcessRunner,
                        codec::FramedSocketConnector,
                        identity::StdioIdentitySource,
                        testing::{
                            FakeForwardControl, ManualRuntime, identity_fixture, result_fixture,
                        },
                    },
                    controller_rpc_ssh_request, decode_request, encode_frame,
                },
                error::WorkerError,
                process::{ProcessRequest, ProcessResult},
            };
            use std::{
                cell::Cell,
                io::{Read, Write},
                os::unix::net::{UnixListener, UnixStream},
                sync::{
                    Mutex,
                    atomic::{AtomicBool, Ordering},
                },
            };

            #[derive(Clone, Copy, Debug)]
            enum Fault {
                BeforeSend,
                PartialSend,
                FullSend,
                PartialReply,
                Oversize,
                WrongId,
                WrongDigest,
                Cancel,
                Expire,
                Error,
                WrongTurn,
                BadCursor,
                Valid,
            }
            struct Raw {
                identity: SocketIdentity,
                calls: Mutex<Vec<ProcessRequest>>,
                identity_calls: Mutex<Vec<ProcessRequest>>,
                answer: ProcessResult,
            }
            fn copy_result(result: &ProcessResult) -> ProcessResult {
                ProcessResult {
                    status: result.status,
                    stdout: result.stdout.clone(),
                    stderr: result.stderr.clone(),
                }
            }
            impl ProcessRunner for Raw {
                fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
                    let request = decode_request(process.stdin.as_ref().unwrap()).unwrap();
                    if request.body().get("controller_socket").is_some() {
                        self.identity_calls.lock().unwrap().push(process.clone());
                        return Ok(result_fixture(
                            &request,
                            serde_json::to_value(SocketIdentityResult::Available(
                                self.identity.clone(),
                            ))
                            .unwrap(),
                            0,
                        ));
                    }
                    self.calls.lock().unwrap().push(process.clone());
                    Ok(copy_result(&self.answer))
                }
            }
            fn payload(stream: &mut UnixStream) -> Option<Vec<u8>> {
                let mut prefix = [0; 4];
                stream.read_exact(&mut prefix).ok()?;
                let length = u32::from_be_bytes(prefix) as usize;
                assert!(length <= MAX_FRAME_BYTES);
                let mut bytes = vec![0; length];
                stream.read_exact(&mut bytes).unwrap();
                Some(bytes)
            }
            fn process(request: &ControllerRequest) -> ProcessRequest {
                let config = mac_worker::config::Config::parse(
                    "version=1\n[controller]\nenabled=true\nssh='fault-peer'\n",
                )
                .unwrap();
                let mut process = controller_rpc_ssh_request(&config.controller).unwrap();
                process.stdin = Some(frame(request));
                process
            }
            fn run_fault(fault: Fault) {
                let fixture = Fixture::new();
                let (task, turn) = seed_task(&fixture, false);
                let request = match fault {
                    Fault::WrongTurn => request_fixture(
                        "task.logs",
                        json!({"task_id":task,"turn_id":turn,"offset":0,"limit":64,"raw":true}),
                    ),
                    Fault::BadCursor => request_fixture(
                        "task.list",
                        json!({"controller_events":{"op":"read","after":{"journal_id":"11111111-1111-4111-8111-111111111111","seq":"0"},"limit":1,"wait_ms":0}}),
                    ),
                    _ => request_fixture("task.wait.poll", json!({"task_id":task})),
                };
                // Ordinary outcomes come from the real host RPC child, including
                // its strict EOF entry; only transport corruption is scripted.
                let output = fixture.rpc(&request);
                let mut result = ProcessResult {
                    status: output.status,
                    stdout: output.stdout,
                    stderr: output.stderr,
                };
                if matches!(fault, Fault::BadCursor) {
                    result = result_fixture(
                        &request,
                        json!({"type":"batch","schema_version":1,"journal_id":"22222222-2222-4222-8222-222222222222","oldest_seq":"1","head_seq":"0","next_after":{"journal_id":"22222222-2222-4222-8222-222222222222","seq":"0"},"events":[],"has_more":false}),
                        0,
                    );
                }
                if matches!(fault, Fault::Oversize) {
                    result = result_fixture(&request, json!({"padding":""}), 0);
                    let base = decode_frame(&result.stdout).unwrap().len();
                    result = result_fixture(
                        &request,
                        json!({"padding":"x".repeat(MAX_FRAME_BYTES-base)}),
                        0,
                    );
                    assert_eq!(result.stdout.len(), MAX_FRAME_BYTES + 4);
                    assert!(SessionCodec::new().encode_reply(&request, &result).is_err());
                }
                if matches!(fault, Fault::Error) {
                    use std::{os::unix::process::ExitStatusExt, process::ExitStatus};
                    result = ProcessResult {
                        status: ExitStatus::from_raw(75 << 8),
                        stdout: encode_json_frame(
                            &serde_json::to_value(
                                mac_worker::job::HostControlError::new(
                                    "CAPACITY_BUSY",
                                    "fixture capacity",
                                )
                                .unwrap(),
                            )
                            .unwrap(),
                        )
                        .unwrap(),
                        stderr: b"private child detail".to_vec(),
                    };
                }
                if matches!(fault, Fault::WrongTurn) {
                    let mut value: Value =
                        serde_json::from_slice(decode_frame(&result.stdout).unwrap()).unwrap();
                    value["result"]["turn_id"] = json!(TurnId::generate());
                    result.stdout = encode_json_frame(&value).unwrap();
                }
                let route = ConfiguredRoute {
                    ssh: "fault-peer".into(),
                    remote_binary: "~/.local/bin/worker".into(),
                    ssh_config_file: None,
                };
                let mut identity = identity_fixture();
                identity.route_sha256 = route.digest().unwrap();
                let socket = fixture.installed.with_file_name("fault-s");
                let listener = UnixListener::bind(&socket).unwrap();
                let raw = Arc::new(Raw {
                    identity: identity.clone(),
                    calls: Mutex::default(),
                    identity_calls: Mutex::default(),
                    answer: copy_result(&result),
                });
                let clock = Arc::new(ManualRuntime::default());
                let stopped = Arc::new(AtomicBool::new(false));
                let observed = Arc::new(Mutex::new(Vec::new()));
                let (server_clock, server_stop, received) =
                    (clock.clone(), stopped.clone(), observed.clone());
                let server_request = request.clone();
                let server_result = copy_result(&result);
                let server = std::thread::spawn(move || {
                    listener.set_nonblocking(true).unwrap();
                    let start = Instant::now();
                    let (mut stream, _) = loop {
                        match listener.accept() {
                            Ok(accepted) => break accepted,
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                assert!(
                                    start.elapsed() < GUARD,
                                    "socket fixture accept hang guard"
                                );
                                std::thread::yield_now();
                            }
                            Err(error) => panic!("{error}"),
                        }
                    };
                    stream.set_nonblocking(false).unwrap();
                    stream.set_read_timeout(Some(GUARD)).unwrap();
                    let hello = payload(&mut stream).unwrap();
                    let expected = SessionCodec::new().decode_hello(&hello).unwrap();
                    assert_eq!(expected, identity);
                    stream
                        .write_all(&SessionCodec::new().encode_ready(&expected).unwrap())
                        .unwrap();
                    server_clock.advance(Duration::from_secs(2));
                    if matches!(fault, Fault::BeforeSend) {
                        stream.shutdown(std::net::Shutdown::Both).unwrap();
                        return;
                    }
                    if matches!(fault, Fault::PartialSend) {
                        let mut prefix = [0; 6];
                        stream.read_exact(&mut prefix).unwrap();
                        received.lock().unwrap().extend(prefix);
                        server_clock.advance(Duration::from_secs(3));
                        return;
                    }
                    let body = payload(&mut stream).unwrap();
                    let received_frame = encode_frame(&body).unwrap();
                    let actual_request = decode_request(&received_frame).unwrap();
                    let server_result = if matches!(fault, Fault::BadCursor) {
                        assert_eq!(actual_request.command(), server_request.command());
                        assert_eq!(actual_request.body(), server_request.body());
                        let inner: Value =
                            serde_json::from_slice(decode_frame(&server_result.stdout).unwrap())
                                .unwrap();
                        result_fixture(&actual_request, inner["result"].clone(), 0)
                    } else {
                        assert_eq!(received_frame, frame(&server_request));
                        server_result
                    };
                    *received.lock().unwrap() = received_frame;
                    server_clock.advance(Duration::from_secs(3));
                    match fault {
                        Fault::FullSend | Fault::Oversize => {}
                        Fault::Cancel | Fault::Expire => {
                            if matches!(fault, Fault::Cancel) {
                                server_stop.store(true, Ordering::Release);
                            } else {
                                server_clock.advance(Duration::from_secs(30));
                            }
                            let mut byte = [0; 1];
                            assert_eq!(
                                stream.read(&mut byte).unwrap(),
                                0,
                                "cancel closes application stream"
                            );
                        }
                        _ => {
                            let reply = SessionCodec::new()
                                .encode_reply(&actual_request, &server_result)
                                .unwrap();
                            if matches!(fault, Fault::PartialReply) {
                                stream.write_all(&reply[..7]).unwrap();
                            } else if matches!(fault, Fault::WrongId | Fault::WrongDigest) {
                                let mut wrapper: Value =
                                    serde_json::from_slice(decode_frame(&reply).unwrap()).unwrap();
                                if matches!(fault, Fault::WrongId) {
                                    wrapper["request_id"] =
                                        json!("018f0f4a6b5c7d8e9f00112233445588");
                                } else {
                                    wrapper["payload_sha256"] = json!("0".repeat(64));
                                }
                                stream
                                    .write_all(&encode_json_frame(&wrapper).unwrap())
                                    .unwrap();
                            } else {
                                stream.write_all(&reply).unwrap();
                            }
                        }
                    }
                });
                let forwards = Arc::new(FakeForwardControl::new(socket));
                let scope = match fault {
                    Fault::WrongTurn => ReadLoopScope::LogsFollow,
                    Fault::BadCursor => ReadLoopScope::EventsFollow,
                    _ => ReadLoopScope::Wait,
                };
                assert!(
                    eligible_read(scope, &request),
                    "fault fixture must reach the socket"
                );
                let client = Arc::new(ChannelProcessRunner::new(
                    raw.clone(),
                    scope,
                    route,
                    fixture.paths.clone(),
                    ClientDeps {
                        identity: Arc::new(StdioIdentitySource::new()),
                        pins: Arc::new(PrivatePinStore::new()),
                        forwards: forwards.clone(),
                        connector: Arc::new(FramedSocketConnector::new(Arc::new(
                            SessionCodec::new(),
                        ))),
                        runtime: clock.clone(),
                    },
                ));
                let original = process(&request);
                let mut priming = original.clone();
                priming.stdin = Some(
                    encode_json_frame(&json!({
                        "protocol_version":request.protocol_version(),
                        "request_id":"11111111111141118111111111111111",
                        "command":request.command(),
                        "body":request.body(),
                    }))
                    .unwrap(),
                );
                let first = client.run(&priming).unwrap();
                assert_eq!(first.status, result.status);
                assert_eq!(first.stdout, result.stdout);
                assert_eq!(first.stderr, result.stderr);
                assert_eq!(
                    raw.calls.lock().unwrap().as_slice(),
                    std::slice::from_ref(&priming)
                );
                assert!(raw.identity_calls.lock().unwrap().is_empty());
                assert_eq!(forwards.resolutions(), 0);
                assert_eq!(forwards.opens(), 0);
                assert_eq!(forwards.cancels(), 0);
                assert!(observed.lock().unwrap().is_empty());
                assert!(
                    !fixture
                        .paths
                        .controller_cache_root()
                        .join("channel")
                        .exists()
                );
                // This captures a non-Send Cell; only the server's atomic flag
                // crosses threads. The borrowed predicate remains live in I/O.
                let calls = Cell::new(0);
                let stop = || {
                    calls.set(calls.get() + 1);
                    stopped.load(Ordering::Acquire)
                };
                if matches!(fault, Fault::BadCursor) {
                    use mac_worker::controller::events::{
                        EventCursor, EventSource, ReadQuery, Seq, client::ControllerEventClient,
                        testing::ManualEventRuntime,
                    };
                    let config = mac_worker::config::Config::parse(
                        "version=1\n[controller]\nenabled=true\nssh='fault-peer'\n",
                    )
                    .unwrap();
                    let source = ControllerEventClient::new(
                        client.clone(),
                        config.controller,
                        Arc::new(ManualEventRuntime::new()),
                    );
                    let query = ReadQuery {
                        after: Some(EventCursor {
                            journal_id: "11111111-1111-4111-8111-111111111111".parse().unwrap(),
                            seq: Seq::ZERO,
                        }),
                        limit: 1,
                        wait_ms: 0,
                    };
                    assert!(
                        source.read(query, GUARD).is_err(),
                        "typed event client rejects a valid batch from another epoch"
                    );
                    server.join().unwrap();
                    assert_eq!(
                        raw.calls.lock().unwrap().len(),
                        1,
                        "typed cursor rejection cannot replay the read"
                    );
                    assert_eq!(raw.identity_calls.lock().unwrap().len(), 1);
                    client.close();
                    assert_eq!(forwards.opens(), 1);
                    assert_eq!(forwards.cancels(), 1);
                    return;
                }
                let answer = client.run_interruptible(&original, &stop);
                server.join().unwrap();
                if matches!(
                    fault,
                    Fault::Cancel | Fault::Expire | Fault::WrongId | Fault::WrongDigest
                ) {
                    assert!(answer.is_err(), "{fault:?} admitted a reply");
                    assert_eq!(
                        raw.calls.lock().unwrap().len(),
                        1,
                        "{fault:?} replayed an unverified/cancelled read"
                    );
                } else if matches!(fault, Fault::Error) {
                    let answer = answer.unwrap();
                    assert_eq!(answer.status.code(), Some(75));
                    assert_eq!(
                        serde_json::from_slice::<Value>(decode_frame(&answer.stdout).unwrap())
                            .unwrap(),
                        serde_json::from_slice::<Value>(decode_frame(&result.stdout).unwrap())
                            .unwrap()
                    );
                    assert!(answer.stderr.is_empty());
                    assert_eq!(raw.calls.lock().unwrap().len(), 1);
                } else if matches!(fault, Fault::WrongTurn | Fault::Valid) {
                    let answer = answer.unwrap();
                    assert!(answer.status.success());
                    assert_eq!(
                        serde_json::from_slice::<Value>(decode_frame(&answer.stdout).unwrap())
                            .unwrap(),
                        serde_json::from_slice::<Value>(decode_frame(&result.stdout).unwrap())
                            .unwrap()
                    );
                    assert_eq!(raw.calls.lock().unwrap().len(), 1);
                    if matches!(fault, Fault::WrongTurn) {
                        let reply: mac_worker::controller::ControllerReadReply<
                            mac_worker::controller::read::ControllerTaskLogsResult,
                        > = serde_json::from_slice(decode_frame(&answer.stdout).unwrap()).unwrap();
                        use mac_worker::controller::ControllerReadIdentity;
                        assert!(reply.result().verify_payload(&request).is_err());
                    }
                } else {
                    assert_eq!(answer.unwrap().stdout, result.stdout);
                    let reads = raw.calls.lock().unwrap();
                    assert_eq!(reads.len(), 2);
                    assert_eq!(reads[1].stdin, original.stdin);
                    let mut expected = original.clone();
                    expected.policy.deadline = reads[1].policy.deadline;
                    assert_eq!(reads[1], expected);
                    assert_eq!(
                        reads[1].policy.deadline,
                        Duration::from_secs(if matches!(fault, Fault::BeforeSend) {
                            28
                        } else {
                            25
                        })
                    );
                    drop(reads);
                    clock.advance(Duration::from_secs(10));
                    client.run(&original).unwrap();
                    assert_eq!(forwards.opens(), 1, "same-ID retry must remain stdio");
                }
                if !matches!(fault, Fault::BadCursor) {
                    assert!(calls.get() > 0);
                }
                client.close();
                assert_eq!(forwards.opens(), 1);
                assert_eq!(forwards.cancels(), 1);
                let reads = raw.calls.lock().unwrap();
                assert!(reads.iter().all(|read| {
                    decode_request(read.stdin.as_ref().unwrap())
                        .unwrap()
                        .command()
                        == request.command()
                }));
                if matches!(fault, Fault::PartialSend) {
                    assert_eq!(*observed.lock().unwrap(), frame(&request)[..6]);
                }
            }
            #[test]
            fn recovery_loss_before_partial_full_send_handler_and_partial_reply_keeps_one_identical_fallback()
             {
                for fault in [
                    Fault::BeforeSend,
                    Fault::PartialSend,
                    Fault::FullSend,
                    Fault::PartialReply,
                ] {
                    run_fault(fault);
                }
            }
            #[test]
            fn recovery_actual_handler_lock_and_generation_loss_uses_one_identical_budgeted_stdio_fallback()
             {
                let fixture = Fixture::new();
                let (task, _) = seed_task(&fixture, false);
                let (leader, config, mut deps, stop) = native_inputs(&fixture);
                let (reporter, courier) = children::reporter(&fixture);
                deps.runner = reporter.clone();
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let channel=LeaderChannel::start(config,leader,deps,stop);channel_ready(&channel).await;let record=fixture.record();
                    let config=mac_worker::config::Config::parse("version=1\n[controller]\nenabled=true\nssh='handler-fixture'\n[ssh]\nmultiplex=true\n").unwrap();
                    let laptop=Fixture::new();let raw=Arc::new(LocalStdio::new(&fixture));let forwards=Arc::new(FakeForwardControl::new(record.service.socket_path.clone()));let clock=Arc::new(ManualRuntime::default());
                    let route=ConfiguredRoute::new(&config.controller,&config.ssh).unwrap();let identity=SocketIdentity {route_sha256:route.digest().unwrap(),service:record.service.clone()};
                    // This native leader is the test process, whose argv is
                    // deliberately not the installed `controller run` role.
                    // Real stdio bootstrap is covered by the CLI-leader cases.
                    let client=Arc::new(ChannelProcessRunner::new(raw.clone(),ReadLoopScope::LogsFollow,route,laptop.paths.clone(),ClientDeps {
                        identity:Arc::new(mac_worker::controller::channel::testing::ScriptedIdentitySource::new(vec![Ok(identity)])),pins:Arc::new(PrivatePinStore::new()),forwards:forwards.clone(),connector:Arc::new(FramedSocketConnector::new(Arc::new(SessionCodec::new()))),runtime:clock.clone()}));
                    let request=request_fixture("task.logs",json!({"task_id":task,"raw":true,"offset":0,"wait_ms":0}));
                    let mut original=controller_rpc_ssh_request(&config.controller).unwrap();original.stdin=Some(frame(&request));
                    let mut priming = original.clone();
                    priming.stdin = Some(encode_json_frame(&json!({
                        "protocol_version":request.protocol_version(),
                        "request_id":"11111111111141118111111111111111",
                        "command":request.command(),
                        "body":request.body(),
                    })).unwrap());
                    assert!(client.run(&priming).unwrap().status.success());
                    assert_eq!(raw.processes.lock().unwrap().as_slice(), std::slice::from_ref(&priming));
                    assert_eq!(raw.calls.lock().unwrap().len(), 1);
                    assert_eq!(forwards.resolutions(), 0);
                    assert_eq!(forwards.opens(), 0);
                    assert_eq!(forwards.cancels(), 0);
                    assert!(!laptop.paths.controller_cache_root().join("channel").exists());
                    let lock=children::state_lock(&fixture);let work_client=client.clone();let process=original.clone();
                    let (result_tx,result_rx)=tokio::sync::oneshot::channel();
                    let work=std::thread::spawn(move || {let _=result_tx.send(work_client.run(&process));});
                    let (pid,group,bytes,_)=children::entered(&courier).await;
                    use base64::Engine;
                    assert_eq!(base64::engine::general_purpose::STANDARD.decode(bytes).unwrap(),original.stdin.as_ref().unwrap().as_slice());
                    // A changed argv is visible before dyld has entered the
                    // worker. Prove the actual RPC reached StateLock: it takes
                    // the directory lock before blocking on our jobs.lock.
                    let state = fs::File::open(&fixture.paths.state).unwrap();
                    tokio::time::timeout(GUARD, async {
                        loop {
                            if unsafe { libc::flock(state.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == -1 {
                                let error = std::io::Error::last_os_error();
                                assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock, "{error}");
                                break;
                            }
                            assert_eq!(unsafe { libc::flock(state.as_raw_fd(), libc::LOCK_UN) }, 0);
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("actual RPC blocked on the state fence");
                    assert!(command_line(pid as u32).starts_with(record.executable.path.to_str().unwrap()));
                    assert_eq!(unsafe{libc::kill(pid,0)},0);clock.advance(Duration::from_secs(5));
                    let shutdown=channel.shutdown().await;assert_eq!(shutdown.rpc.completed,1);assert_eq!(shutdown.rpc.unknown,0);assert_eq!(shutdown.files,ForwardDisposition::Cleaned);
                    assert_eq!(unsafe{libc::killpg(group,0)},-1);
                    // Shutdown now retains generation links. Withdraw this
                    // fixture's image explicitly to keep exercising generation loss.
                    fs::remove_file(&record.executable.path).unwrap();
                    assert!(!record.executable.path.exists());drop(lock);
                    let result=tokio::time::timeout(GUARD,result_rx).await.expect("fallback result hang guard").unwrap().expect("stdio fallback process");
                    assert!(result.status.success(),"stdio fallback status={} stderr={} stdout={}",result.status,String::from_utf8_lossy(&result.stderr),String::from_utf8_lossy(&result.stdout));work.join().unwrap();
                    let calls=raw.calls.lock().unwrap();assert_eq!(calls.len(),2);assert_eq!(frame(&calls[1]),original.stdin.unwrap());drop(calls);
                    let processes=raw.processes.lock().unwrap();assert_eq!(processes.len(),2);let mut expected=processes[1].clone();expected.policy.deadline=Duration::from_secs(30);
                    let mut original=controller_rpc_ssh_request(&config.controller).unwrap();original.stdin=Some(frame(&request));assert_eq!(expected,original);assert_eq!(processes[1].policy.deadline,Duration::from_secs(25));drop(processes);
                    assert_eq!(forwards.opens(),1);assert_eq!(forwards.cancels(),1);assert_eq!(client.close(),ForwardDisposition::Cleaned);
                    let reply:mac_worker::controller::ControllerReadReply<mac_worker::controller::read::ControllerTaskLogsResult>=serde_json::from_slice(decode_frame(&result.stdout).unwrap()).unwrap();
                    use mac_worker::controller::ControllerReadIdentity;
                    reply.verify_envelope(&request).unwrap();reply.result().verify_payload(&request).unwrap();
                });
            }
            #[test]
            fn recovery_maximum_inner_reply_falls_back_without_truncation() {
                run_fault(Fault::Oversize);
            }
            #[test]
            fn recovery_live_borrowed_cancel_and_expiry_send_no_fallback_or_task_cancel() {
                for fault in [Fault::Cancel, Fault::Expire] {
                    run_fault(fault);
                }
            }
            #[test]
            fn recovery_wrong_complete_id_and_digest_are_never_replayed() {
                for fault in [Fault::WrongId, Fault::WrongDigest] {
                    run_fault(fault);
                }
            }
            #[test]
            fn recovery_verified_capacity_errors_and_typed_turn_cursor_errors_do_not_fallback() {
                for fault in [
                    Fault::Error,
                    Fault::WrongTurn,
                    Fault::BadCursor,
                    Fault::Valid,
                ] {
                    run_fault(fault);
                }
            }
        }
    }
}

// T7b owns this loop/raw/route block; leader wiring is maintained by T7a.
// Each mux-enabled fixture gets process-level HOME/XDG isolation as well as
// RuntimeContext isolation. Raw SSH argv construction also reads process HOME.
fn isolated_loop_fixture(name: &str) -> bool {
    if std::env::var_os("MAC_WORKER_T7B_LOOP_FIXTURE").is_some() {
        return false;
    }
    let root = tempfile::Builder::new()
        .prefix("p3b")
        .tempdir_in("/private/tmp")
        .unwrap();
    let request = mac_worker::process::ProcessRequest {
        program: std::env::current_exe().unwrap().into_os_string(),
        args: vec![
            "--exact".into(),
            format!("controller_socket_wiring::{name}").into(),
            "--nocapture".into(),
        ],
        environment: vec![
            ("MAC_WORKER_T7B_LOOP_FIXTURE".into(), "1".into()),
            ("HOME".into(), root.path().as_os_str().to_owned()),
            (
                "XDG_CONFIG_HOME".into(),
                root.path().join("config").into_os_string(),
            ),
            (
                "XDG_CACHE_HOME".into(),
                root.path().join("cache").into_os_string(),
            ),
            (
                "XDG_STATE_HOME".into(),
                root.path().join("state").into_os_string(),
            ),
            (
                "XDG_DATA_HOME".into(),
                root.path().join("data").into_os_string(),
            ),
        ],
        environment_remove: vec!["MAC_WORKER_TEST_SSH".into()],
        stdin: None,
        policy: mac_worker::process::ProcessPolicy {
            stdout_limit: 4 * 1024 * 1024,
            stderr_limit: 4 * 1024 * 1024,
            deadline: std::time::Duration::from_secs(60),
        },
        isolate_parent_environment: false,
    };
    use mac_worker::process::ProcessRunner;
    let result = mac_worker::process::SystemProcessRunner
        .run(&request)
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    true
}

mod loop_fixtures {
    use clap::Parser;
    use mac_worker::{
        RuntimeContext,
        cli::Cli,
        config::Config,
        controller::{
            ControllerRequest,
            channel::{
                ChannelFailure, ClientContext, ClientDeps, ConfiguredRoute, SocketConnector,
                SocketIdentity, SocketSession,
                identity::StdioIdentitySource,
                pin::{Pin, PrivatePinStore},
                testing::{FakeForwardControl, ManualRuntime, identity_fixture, result_fixture},
            },
            decode_request,
        },
        error::WorkerError,
        paths::PathLayout,
        process::{ProcessRequest, ProcessResult, ProcessRunner},
        task::{TaskOutcome, TaskState, TaskStatus, TurnSummary, TurnTerminal},
    };
    use serde_json::{Value, json};
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        path::PathBuf,
        sync::{Arc, Mutex},
    };
    pub const TASK: &str = "018f0f4a6b5c7d8e9f00112233445566";
    const TURN: &str = "018f0f4a6b5c7d8e9f00112233445577";
    pub struct Fixture {
        pub runtime: RuntimeContext,
        pub paths: PathLayout,
        pub config: Config,
        pub endpoint: Arc<Endpoint>,
        pub forwards: Arc<FakeForwardControl>,
        pub clock: Arc<ManualRuntime>,
        pub pins: Arc<PrivatePinStore>,
        pub connector: Arc<Connector>,
        pub order: Arc<Mutex<Vec<String>>>,
        environment: BTreeMap<OsString, OsString>,
        _temp: tempfile::TempDir,
    }
    impl Fixture {
        pub fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().canonicalize().unwrap();
            let home = root.join("home");
            let environment = BTreeMap::from([
                (OsString::from("HOME"), home.clone().into_os_string()),
                (
                    OsString::from("XDG_CONFIG_HOME"),
                    root.join("config").into_os_string(),
                ),
                (
                    OsString::from("XDG_CACHE_HOME"),
                    root.join("cache").into_os_string(),
                ),
                (
                    OsString::from("XDG_DATA_HOME"),
                    root.join("data").into_os_string(),
                ),
                (
                    OsString::from("XDG_STATE_HOME"),
                    root.join("state").into_os_string(),
                ),
            ]);
            for path in environment.values() {
                std::fs::create_dir_all(PathBuf::from(path)).unwrap();
            }
            let paths = PathLayout::discover(None, &environment, &home).unwrap();
            std::fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
            let text = "version = 1\n[controller]\nenabled = true\nssh = 'controller-host'\n[ssh]\nmultiplex = true\n";
            std::fs::write(&paths.config, text).unwrap();
            let config = Config::parse(text).unwrap();
            let order = Arc::new(Mutex::new(vec![]));
            let mut identity = identity_fixture();
            identity.route_sha256 = ConfiguredRoute::new(&config.controller, &config.ssh)
                .unwrap()
                .digest()
                .unwrap();
            Self {
                runtime: RuntimeContext::isolated(environment.clone(), home, root),
                paths,
                config,
                endpoint: Arc::new(Endpoint {
                    order: order.clone(),
                    identity: Some(identity),
                    ..Endpoint::default()
                }),
                forwards: Arc::new(FakeForwardControl::new("/private/fake-forward/s".into())),
                clock: Arc::new(ManualRuntime::default()),
                pins: Arc::new(PrivatePinStore::new()),
                connector: Arc::new(Connector {
                    order: order.clone(),
                    ..Connector::default()
                }),
                order,
                environment,
                _temp: temp,
            }
        }
        pub fn in_project(mut self, project: &std::path::Path) -> Self {
            self.runtime = RuntimeContext::isolated(
                self.environment.clone(),
                PathBuf::from(self.environment.get(std::ffi::OsStr::new("HOME")).unwrap()),
                project.to_owned(),
            );
            self
        }
        pub fn deps(&self) -> ClientDeps {
            ClientDeps {
                identity: Arc::new(StdioIdentitySource::new()),
                pins: self.pins.clone(),
                forwards: self.forwards.clone(),
                connector: self.connector.clone(),
                runtime: self.clock.clone(),
            }
        }
        pub fn pin(&self) -> Option<Pin> {
            let digest = ConfiguredRoute::new(&self.config.controller, &self.config.ssh)
                .unwrap()
                .digest()
                .unwrap();
            let path = self
                .paths
                .controller_cache_root()
                .join("channel/pins")
                .join(format!("{digest}.json"));
            match std::fs::read(path) {
                Ok(bytes) => Some(serde_json::from_slice(&bytes).unwrap()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("read isolated pin: {error}"),
            }
        }
        pub fn assert_authenticated_pin(&self) {
            assert_eq!(self.endpoint.identity_calls.lock().unwrap().len(), 1);
            assert_eq!(
                self.pin(),
                Some(Pin::from_identity(self.endpoint.identity.as_ref().unwrap()))
            );
        }
        pub fn run(&self, args: &[&str]) -> (u8, String, String) {
            let context = self
                .runtime
                .clone()
                .with_controller_channel_dependencies(self.deps());
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let exit = mac_worker::run_with_io_in_context(
                Cli::try_parse_from(args).unwrap(),
                &*self.endpoint,
                &context,
                &mut stdout,
                &mut stderr,
            );
            (
                exit,
                String::from_utf8(stdout).unwrap(),
                String::from_utf8(stderr).unwrap(),
            )
        }
    }
    #[derive(Default)]
    pub struct Endpoint {
        pub calls: Mutex<Vec<ControllerRequest>>,
        pub identity_calls: Mutex<Vec<ControllerRequest>>,
        pub processes: Mutex<Vec<ProcessRequest>>,
        identity: Option<SocketIdentity>,
        order: Arc<Mutex<Vec<String>>>,
    }
    impl ProcessRunner for Endpoint {
        fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if process.program == "/bin/ps" {
                let mut output = result_fixture(
                    &mac_worker::controller::channel::testing::request_fixture(
                        "task.list",
                        json!({}),
                    ),
                    json!({}),
                    0,
                );
                output.stdout.clear();
                return Ok(output);
            }
            if process.program == "/usr/bin/git" {
                if process.args.iter().any(|arg| {
                    arg.to_str()
                        .is_some_and(|arg| arg.starts_with("--receive-pack="))
                }) {
                    self.order.lock().unwrap().push("git:source.push".into());
                    return Ok(result_fixture(
                        &mac_worker::controller::channel::testing::request_fixture(
                            "task.list",
                            json!({}),
                        ),
                        json!({}),
                        0,
                    ));
                }
                return mac_worker::process::SystemProcessRunner.run(process);
            }
            assert!(process.program == "/usr/bin/ssh" || process.program == "fake-ssh");
            self.processes.lock().unwrap().push(process.clone());
            if let Some(frame) = &process.stdin
                && let Ok(request) = decode_request(frame)
                && request.body().get("controller_socket").is_some()
            {
                assert_eq!(request.command(), "task.list");
                assert_eq!(request.body()["controller_socket"]["op"], "identity");
                self.identity_calls.lock().unwrap().push(request.clone());
                self.order
                    .lock()
                    .unwrap()
                    .push("stdio:channel.identity".into());
                return Ok(result_fixture(
                    &request,
                    json!({"available":self.identity.as_ref().unwrap()}),
                    0,
                ));
            }
            if !process
                .args
                .last()
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with("host controller-rpc")
            {
                let mut output = result_fixture(
                    &mac_worker::controller::channel::testing::request_fixture(
                        "task.list",
                        json!({}),
                    ),
                    json!({}),
                    0,
                );
                output.stdout = b"{}".to_vec();
                return Ok(output);
            }
            let request = decode_request(process.stdin.as_ref().unwrap()).unwrap();
            self.order
                .lock()
                .unwrap()
                .push(format!("stdio:{}", request.command()));
            self.calls.lock().unwrap().push(request.clone());
            Ok(reply(&request, &self.calls.lock().unwrap()))
        }
    }
    fn status(task_id: &str, active: bool, cancelled: bool) -> Value {
        let outcome = if cancelled {
            TaskOutcome::Cancelled
        } else {
            TaskOutcome::Done
        };
        let terminal = if cancelled {
            TurnTerminal::Cancelled
        } else {
            TurnTerminal::Succeeded
        };
        let status = TaskStatus::new(
            if active {
                TaskState::Active
            } else {
                TaskState::Open
            },
            (!active).then_some(outcome.clone()),
            Some("mini-1".into()),
            !active,
            (!active).then(|| "a".repeat(40).parse().unwrap()),
            None,
            vec![],
            vec![],
            None,
            vec![TurnSummary::new(
                1,
                TURN.parse().unwrap(),
                (!active).then_some(terminal),
                (!active).then_some(outcome),
                Some(!active),
                false,
                Some(1),
                (!active).then_some(2),
            )],
            2,
        )
        .unwrap();
        json!({"task_id":task_id,"run_id":null,"status":status,"warnings":[],"events":[],"runner":null,"exit_code":null})
    }
    pub fn reply(request: &ControllerRequest, calls: &[ControllerRequest]) -> ProcessResult {
        let cancelled = calls.iter().any(|seen| seen.command() == "task.cancel");
        let task_id = request
            .body()
            .get("task_id")
            .and_then(Value::as_str)
            .unwrap_or(TASK);
        if matches!(request.command(), "task.submit" | "task.batch") {
            let mut output = result_fixture(request, Value::Null, 0);
            output.stdout = mac_worker::controller::encode_json_frame(&json!({
                "protocol_version":7,"status":"acked","request_id":request.request_id(),"payload_sha256":request.payload_sha256(),"task_id":task_id,"turn_id":request.body().get("turn_id").cloned().unwrap_or(json!(TURN)),"created_at_millis":1,
                "result": if request.command() == "task.batch" { json!({"run_id":request.body()["run_id"],"task_ids":[TASK]}) } else { status(task_id,false,false) }
            })).unwrap();
            return output;
        }
        if matches!(
            request.command(),
            "task.say" | "task.cancel" | "task.close" | "checkpoint.submit"
        ) {
            let mut output = result_fixture(request, Value::Null, 0);
            output.stdout = mac_worker::controller::encode_json_frame(&json!({
                "protocol_version":7,"status":"acked","request_id":request.request_id(),"payload_sha256":request.payload_sha256(),"task_id":TASK,"created_at_millis":1,"result":status(TASK,false,cancelled)
            })).unwrap();
            return output;
        }
        let result = match request.command() {
            "task.wait.poll" => json!({"task_ids":[task_id],"quiescent":true,"exit_code":0}),
            "task.status" => status(task_id, !cancelled, cancelled),
            "controller.transfer.source.prepare" => {
                let mut result = request.body().clone();
                result["token"] = json!("0123456789ab4def8123456789abcdef");
                result
            }
            "controller.transfer.source.finish" => {
                json!({"token":request.body()["token"],"request_id":request.body()["request_id"],"oid":request.body()["expected_oid"],"request_ref":format!("refs/mac-worker/requests/{}",request.body()["request_id"].as_str().unwrap())})
            }
            "task.logs" => {
                json!({"task_id":TASK,"turn_id":TURN,"turn_number":1,"agent":"codex","raw":true,"offset":request.body()["offset"],"next_offset":request.body()["offset"],"exhausted":true,"complete":true,"bytes_base64":""})
            }
            "task.list" if request.body().get("controller_health").is_some() => {
                json!({"features":["controller.events","controller.task-logs-wait"],"state":"healthy","reason":"tick_succeeded","leader_running":true})
            }
            "task.list" if request.body().get("controller_events").is_some() => {
                match request.body()["controller_events"]["op"].as_str().unwrap() {
                    "read" if request.body()["controller_events"]["after"].is_null() => {
                        json!({"type":"snapshot_required","reason":"bootstrap","window":{"journal_id":"01234567-89ab-4def-8123-456789abcdef","oldest_seq":"1","head_seq":"0"}})
                    }
                    "read" => {
                        json!({"type":"batch","schema_version":1,"journal_id":"01234567-89ab-4def-8123-456789abcdef","oldest_seq":"1","head_seq":"0","next_after":request.body()["controller_events"]["after"],"events":[],"has_more":false})
                    }
                    "tasks" => {
                        json!({"rows":[],"missing":[TASK],"proof_after":null,"baseline_after":null})
                    }
                    "repair" => {
                        json!({"rows":[],"next":null,"complete":true,"restart":false,"baseline_after":request.body()["controller_events"]["baseline_after"]})
                    }
                    other => panic!("unexpected selector {other}"),
                }
            }
            _ => json!({}),
        };
        result_fixture(request, result, 0)
    }
    #[derive(Default)]
    pub struct Connector {
        pub requests: Arc<Mutex<Vec<ControllerRequest>>>,
        order: Arc<Mutex<Vec<String>>>,
        pub fail_next: Arc<std::sync::atomic::AtomicBool>,
    }
    impl SocketConnector for Connector {
        fn connect(
            &self,
            _: &std::path::Path,
            _: &SocketIdentity,
            ctx: &ClientContext<'_>,
        ) -> Result<Box<dyn SocketSession>, ChannelFailure> {
            ctx.check()?;
            Ok(Box::new(Session(
                self.requests.clone(),
                self.order.clone(),
                self.fail_next.clone(),
            )))
        }
    }
    struct Session(
        Arc<Mutex<Vec<ControllerRequest>>>,
        Arc<Mutex<Vec<String>>>,
        Arc<std::sync::atomic::AtomicBool>,
    );
    impl SocketSession for Session {
        fn exchange(
            &mut self,
            _: &[u8],
            request: &ControllerRequest,
            ctx: &ClientContext<'_>,
        ) -> Result<ProcessResult, ChannelFailure> {
            ctx.check()?;
            self.0.lock().unwrap().push(request.clone());
            self.1
                .lock()
                .unwrap()
                .push(format!("socket:{}", request.command()));
            if self.2.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Err(ChannelFailure::Unavailable(
                    mac_worker::controller::channel::ChannelReason::ForwardLost,
                ));
            }
            Ok(reply(request, &[]))
        }
        fn close(&mut self) {}
    }
    pub fn commands(requests: &Mutex<Vec<ControllerRequest>>) -> Vec<String> {
        requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request.command().to_owned())
            .collect()
    }
}

#[test]
fn loop_wait_mutation_separation() {
    if isolated_loop_fixture("loop_wait_mutation_separation") {
        return;
    }
    use loop_fixtures::*;
    for args in [
        vec!["worker", "task", "wait", "--task-id", TASK],
        vec![
            "worker",
            "task",
            "wait",
            "--run",
            "018f0f4a6b5c7d8e9f00112233445599",
        ],
        vec![
            "worker",
            "task",
            "say",
            TASK,
            "--message",
            "steer",
            "--wait",
        ],
        vec![
            "worker",
            "task",
            "say",
            TASK,
            "--message",
            "steer",
            "--interrupt",
        ],
    ] {
        let fixture = Fixture::new();
        let (exit, _, stderr) = fixture.run(&args);
        assert_eq!(exit, 0, "{args:?}: {stderr}");
        assert!(fixture.connector.requests.lock().unwrap().is_empty());
        assert_eq!(
            commands(&fixture.endpoint.calls)
                .iter()
                .filter(|command| command.as_str() == "task.wait.poll")
                .count(),
            1,
            "{args:?}"
        );
        assert_eq!(fixture.forwards.resolutions(), 0);
        assert_eq!(fixture.forwards.opens(), 0);
        assert_eq!(fixture.forwards.cancels(), 0);
        assert!(fixture.endpoint.identity_calls.lock().unwrap().is_empty());
        assert!(fixture.pin().is_none());
        if args.contains(&"--interrupt") {
            assert_eq!(
                *fixture.order.lock().unwrap(),
                [
                    "stdio:task.status",
                    "stdio:task.cancel",
                    "stdio:task.wait.poll",
                    "stdio:task.status",
                    "stdio:task.say"
                ]
            );
        } else if args.contains(&"say") {
            assert_eq!(
                *fixture.order.lock().unwrap(),
                [
                    "stdio:task.say",
                    "stdio:task.wait.poll",
                    "stdio:task.status"
                ]
            );
        }
    }
}

#[test]
fn loop_submit_and_batch_transfer_finish_before_wait_setup() {
    if isolated_loop_fixture("loop_submit_and_batch_transfer_finish_before_wait_setup") {
        return;
    }
    use loop_fixtures::*;
    let repo = crate::support::GitRepo::init();
    repo.write("tracked", b"seed\n");
    repo.commit_all("seed");
    let batch = repo.root().join("batch.toml");
    std::fs::write(
        &batch,
        "version = 1\nagent = 'codex'\n[[tasks]]\nprompt = 'fixture task'\n",
    )
    .unwrap();
    for args in [
        vec![
            "worker",
            "task",
            "submit",
            "--project",
            repo.root().to_str().unwrap(),
            "--agent",
            "codex",
            "--prompt",
            "fixture",
            "--wait",
        ],
        vec!["worker", "task", "batch", batch.to_str().unwrap(), "--wait"],
    ] {
        let fixture = Fixture::new().in_project(repo.root());
        let (exit, _, stderr) = fixture.run(&args);
        assert_eq!(exit, 0, "{args:?}: {stderr}");
        let order = fixture.order.lock().unwrap();
        assert_eq!(
            &order[..4],
            [
                "stdio:controller.transfer.source.prepare",
                "git:source.push",
                "stdio:controller.transfer.source.finish",
                if args[2] == "batch" {
                    "stdio:task.batch"
                } else {
                    "stdio:task.submit"
                }
            ]
        );
        assert_eq!(order[4], "stdio:task.wait.poll");
        assert!(fixture.connector.requests.lock().unwrap().is_empty());
        assert_eq!(fixture.forwards.resolutions(), 0);
        assert_eq!(fixture.forwards.opens(), 0);
        assert_eq!(fixture.forwards.cancels(), 0);
        assert!(fixture.endpoint.identity_calls.lock().unwrap().is_empty());
        assert!(fixture.pin().is_none());
    }
}

#[test]
fn raw_exclusions_stay_stdio() {
    if isolated_loop_fixture("raw_exclusions_stay_stdio") {
        return;
    }
    use loop_fixtures::*;
    for args in [
        vec!["worker", "task", "status", TASK],
        vec!["worker", "task", "list"],
        vec!["worker", "task", "logs", TASK, "--raw"],
        vec!["worker", "task", "diff", TASK],
        vec!["worker", "task", "result", TASK],
        vec!["worker", "task", "reconcile"],
        vec!["worker", "task", "publish-retry", TASK],
        vec!["worker", "task", "cancel", TASK],
        vec!["worker", "task", "close", TASK],
        vec!["worker", "controller", "status"],
        vec!["worker", "controller", "drain"],
        vec!["worker", "controller", "drain", "--off"],
    ] {
        let fixture = Fixture::new();
        fixture.run(&args);
        assert!(
            !fixture.endpoint.processes.lock().unwrap().is_empty(),
            "did not reach raw transport: {args:?}"
        );
        assert_eq!(fixture.forwards.resolutions(), 0, "{args:?}");
        assert_eq!(fixture.forwards.opens(), 0, "{args:?}");
        assert!(fixture.connector.requests.lock().unwrap().is_empty());
        assert!(fixture.pin().is_none());
    }
}

#[test]
fn loop_interrupt_and_follow_wait_share_command_retirement() {
    if isolated_loop_fixture("loop_interrupt_and_follow_wait_share_command_retirement") {
        return;
    }
    use loop_fixtures::*;
    let fixture = Fixture::new();
    fixture
        .forwards
        .set_disposition(mac_worker::controller::channel::ForwardDisposition::Retained);
    fixture
        .connector
        .fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (exit, _, stderr) = fixture.run(&[
        "worker",
        "task",
        "say",
        TASK,
        "--message",
        "steer",
        "--interrupt",
        "--wait",
    ]);
    assert_eq!(exit, 0, "{stderr}");
    assert_eq!(
        fixture.forwards.opens(),
        1,
        "uncertain cleanup must retire every later wait in this command"
    );
    assert_eq!(fixture.forwards.cancels(), 1);
    assert_eq!(commands(&fixture.connector.requests), ["task.wait.poll"]);
    assert_eq!(
        commands(&fixture.endpoint.calls)
            .iter()
            .filter(|command| command.as_str() == "task.wait.poll")
            .count(),
        2
    );
}

#[test]
fn raw_controller_retry_preserves_saved_mutation_route() {
    if isolated_loop_fixture("raw_controller_retry_preserves_saved_mutation_route") {
        return;
    }
    use loop_fixtures::*;
    // CP1 checkpoint envelopes have no separate CLI command. Their supported
    // laptop entry point is the same saved-envelope retry path.
    for (command, body) in [
        (
            "task.say",
            json!({"task_id":TASK,"message":"saved message"}),
        ),
        ("checkpoint.submit", json!({"prompt":"saved checkpoint"})),
    ] {
        let fixture = Fixture::new();
        let request = request_fixture(command, body);
        mac_worker::controller::persist_operation_envelope(
            &fixture.paths.controller_cache_root(),
            &request,
        )
        .unwrap();
        let (exit, _, stderr) =
            fixture.run(&["worker", "controller", "retry", request.request_id()]);
        assert_eq!(exit, 0, "{stderr}");
        assert_eq!(commands(&fixture.endpoint.calls), [command]);
        assert_eq!(fixture.endpoint.calls.lock().unwrap()[0], request);
        assert_eq!(fixture.forwards.resolutions(), 0);
        assert!(fixture.connector.requests.lock().unwrap().is_empty());
    }
}

#[test]
fn raw_fetch_doctor_and_local_wait_allocate_no_channel() {
    if isolated_loop_fixture("raw_fetch_doctor_and_local_wait_allocate_no_channel") {
        return;
    }
    use loop_fixtures::*;
    let repo = crate::support::GitRepo::init();
    repo.write("tracked", b"seed\n");
    repo.commit_all("seed");
    let fixture = Fixture::new().in_project(repo.root());
    fixture.run(&["worker", "task", "fetch", TASK]);
    assert_eq!(
        commands(&fixture.endpoint.calls),
        ["controller.transfer.result.prepare"]
    );
    fixture.run(&[
        "worker",
        "doctor",
        "--project",
        repo.root().to_str().unwrap(),
    ]);
    assert_eq!(fixture.forwards.resolutions(), 0);
    std::fs::write(
        &fixture.paths.config,
        "version=1\n[controller]\nenabled=false\n",
    )
    .unwrap();
    let (exit, _, _) = fixture.run(&["worker", "task", "wait", "--task-id", TASK]);
    assert_ne!(exit, 0);
    assert_eq!(fixture.forwards.resolutions(), 0);
    assert!(fixture.pin().is_none());
    assert!(fixture.connector.requests.lock().unwrap().is_empty());
    assert!(
        clap::Parser::try_parse_from(["worker", "events"])
            .map(|_: mac_worker::cli::Cli| ())
            .is_err()
    );
}

#[test]
fn route_loop_scopes_only() {
    if isolated_loop_fixture("route_loop_scopes_only") {
        return;
    }
    use loop_fixtures::*;
    let fixture = Fixture::new();
    let (exit, _, stderr) = fixture.run(&["worker", "task", "logs", TASK, "--follow", "--raw"]);
    assert_eq!(exit, 0, "{stderr}");
    let requests = fixture.connector.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].command(), "task.logs");
    assert_eq!(requests[0].body()["wait_ms"], 15_000);
    let raw = fixture.endpoint.calls.lock().unwrap();
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].command(), "task.list");
    assert_eq!(raw[0].body(), &json!({"controller_health":true}));
    assert_eq!(fixture.forwards.opens(), 1);
    assert_eq!(fixture.forwards.cancels(), 1);
    fixture.assert_authenticated_pin();
}

#[test]
fn loop_short_wait_skips_cold_setup() {
    if isolated_loop_fixture("loop_short_wait_skips_cold_setup") {
        return;
    }
    use loop_fixtures::*;
    let fixture = Fixture::new();
    let (exit, _, stderr) = fixture.run(&[
        "worker",
        "task",
        "wait",
        "--task-id",
        TASK,
        "--timeout",
        "1s",
    ]);
    assert_eq!(exit, 0, "{stderr}");
    assert_eq!(commands(&fixture.endpoint.calls), ["task.wait.poll"]);
    assert_eq!(fixture.forwards.resolutions(), 0);
    assert!(
        fixture.endpoint.processes.lock().unwrap()[0]
            .policy
            .deadline
            <= std::time::Duration::from_secs(1)
    );
    assert!(fixture.pin().is_none());
}

#[test]
fn route_events_and_notify_own_one_scoped_runner() {
    if isolated_loop_fixture("route_events_and_notify_own_one_scoped_runner") {
        return;
    }
    use loop_fixtures::*;
    use mac_worker::controller::{
        channel::ReadLoopScope,
        events::{
            EventSource, ReadQuery, TaskAddressQuery, TaskRepairQuery,
            client::ControllerEventClient, testing::ManualEventRuntime,
        },
    };
    use std::{sync::Arc, time::Duration};
    for scope in [ReadLoopScope::EventsFollow, ReadLoopScope::Notify] {
        let fixture = Fixture::new();
        let clock = Arc::new(ManualEventRuntime::new());
        {
            let source = ControllerEventClient::for_read_loop(
                fixture.endpoint.clone(),
                scope,
                &fixture.paths,
                &fixture.config,
                clock.clone(),
                fixture.deps(),
            );
            source.discover(Duration::from_secs(30)).unwrap();
            source
                .read(ReadQuery::default(), Duration::from_secs(30))
                .unwrap();
            if scope == ReadLoopScope::Notify {
                source
                    .tasks(
                        TaskAddressQuery {
                            task_ids: vec![TASK.parse().unwrap()],
                            include_titles: false,
                            proof_after: None,
                        },
                        Duration::from_secs(30),
                    )
                    .unwrap();
                source
                    .repair(
                        TaskRepairQuery {
                            after: None,
                            limit: 128,
                            baseline_after: None,
                        },
                        Duration::from_secs(30),
                    )
                    .unwrap();
            }
            let raw = fixture.endpoint.calls.lock().unwrap();
            assert_eq!(raw.len(), 1);
            assert_eq!(raw[0].command(), "task.list");
            assert_eq!(raw[0].body(), &json!({"controller_health":true}));
            assert_eq!(fixture.forwards.opens(), 1);
        }
        assert_eq!(fixture.forwards.cancels(), 1);
        fixture.assert_authenticated_pin();
        assert_eq!(
            fixture.connector.requests.lock().unwrap().len(),
            if scope == ReadLoopScope::Notify { 3 } else { 1 }
        );
    }
}

#[test]
fn route_event_runtime_cancellation_is_shared_with_channel_setup() {
    if isolated_loop_fixture("route_event_runtime_cancellation_is_shared_with_channel_setup") {
        return;
    }
    use loop_fixtures::*;
    use mac_worker::controller::{
        channel::ReadLoopScope,
        events::{EventSource, client::ControllerEventClient, testing::ManualEventRuntime},
    };
    use std::{sync::Arc, time::Duration};
    let fixture = Fixture::new();
    let clock = Arc::new(ManualEventRuntime::new());
    struct CancellingIdentity {
        clock: Arc<ManualEventRuntime>,
        inner: Arc<dyn mac_worker::controller::channel::IdentitySource>,
    }
    impl mac_worker::controller::channel::IdentitySource for CancellingIdentity {
        fn read(
            &self,
            raw: &dyn mac_worker::process::ProcessRunner,
            route: &mac_worker::controller::channel::ConfiguredRoute,
            master: Option<&mac_worker::controller::channel::MasterPlan>,
            context: &mac_worker::controller::channel::ClientContext<'_>,
        ) -> Result<
            mac_worker::controller::channel::SocketIdentity,
            mac_worker::controller::channel::ChannelFailure,
        > {
            let identity = self.inner.read(raw, route, master, context)?;
            self.clock.cancel();
            Ok(identity)
        }
    }
    let mut deps = fixture.deps();
    deps.identity = Arc::new(CancellingIdentity {
        clock: clock.clone(),
        inner: deps.identity,
    });
    let source = ControllerEventClient::for_read_loop(
        fixture.endpoint.clone(),
        ReadLoopScope::EventsFollow,
        &fixture.paths,
        &fixture.config,
        clock.clone(),
        deps,
    );
    assert!(source.discover(Duration::from_secs(30)).is_ok());
    assert!(
        source
            .read(Default::default(), Duration::from_secs(30))
            .is_err()
    );
    assert_eq!(fixture.forwards.resolutions(), 1);
    assert_eq!(fixture.forwards.opens(), 0);
    let raw = fixture.endpoint.calls.lock().unwrap();
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].command(), "task.list");
    assert_eq!(raw[0].body(), &json!({"controller_health":true}));
}

#[test]
fn loop_notify_short_budget_stays_with_original_deadline() {
    if isolated_loop_fixture("loop_notify_short_budget_stays_with_original_deadline") {
        return;
    }
    use loop_fixtures::*;
    use mac_worker::controller::{
        channel::ReadLoopScope,
        events::{
            EventSource, ReadQuery, TaskRepairQuery, client::ControllerEventClient,
            testing::ManualEventRuntime,
        },
    };
    use std::{sync::Arc, time::Duration};
    let fixture = Fixture::new();
    let clock = Arc::new(ManualEventRuntime::new());
    let source = ControllerEventClient::for_read_loop(
        fixture.endpoint.clone(),
        ReadLoopScope::Notify,
        &fixture.paths,
        &fixture.config,
        clock.clone(),
        fixture.deps(),
    );
    clock.advance(Duration::from_secs(25));
    source.discover(Duration::from_secs(30)).unwrap();
    clock.advance(Duration::from_secs(4));
    source
        .repair(
            TaskRepairQuery {
                after: None,
                limit: 128,
                baseline_after: None,
            },
            Duration::from_secs(30),
        )
        .unwrap();
    let requests = fixture.endpoint.processes.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].policy.deadline, Duration::from_secs(5));
    assert_eq!(requests[1].policy.deadline, Duration::from_secs(1));
    drop(requests);
    clock.advance(Duration::from_secs(1));
    assert!(
        source
            .read(ReadQuery::default(), Duration::from_secs(30))
            .is_err()
    );
    assert_eq!(fixture.endpoint.processes.lock().unwrap().len(), 2);
    assert_eq!(fixture.forwards.resolutions(), 0);
}

#[test]
fn loop_notify_preserves_fifteen_second_repair_with_shared_clock() {
    if isolated_loop_fixture("loop_notify_preserves_fifteen_second_repair_with_shared_clock") {
        return;
    }
    use loop_fixtures::*;
    use mac_worker::controller::{
        channel::*,
        events::{
            EventRuntime, PreviousProjection,
            client::{ControllerEventClient, TaskReconciler},
            notify::{NotifyCache, NotifyOptions, follow::NotifyLoop},
            testing::ManualEventRuntime,
        },
    };
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    struct PollConnector {
        clock: Arc<ManualEventRuntime>,
        requests: Arc<Mutex<Vec<(Duration, mac_worker::controller::ControllerRequest)>>>,
    }
    impl SocketConnector for PollConnector {
        fn connect(
            &self,
            _: &std::path::Path,
            _: &SocketIdentity,
            ctx: &ClientContext<'_>,
        ) -> Result<Box<dyn SocketSession>, ChannelFailure> {
            ctx.check()?;
            Ok(Box::new(Self {
                clock: self.clock.clone(),
                requests: self.requests.clone(),
            }))
        }
    }
    impl SocketSession for PollConnector {
        fn exchange(
            &mut self,
            _: &[u8],
            request: &mac_worker::controller::ControllerRequest,
            ctx: &ClientContext<'_>,
        ) -> Result<mac_worker::process::ProcessResult, ChannelFailure> {
            ctx.check()?;
            self.requests
                .lock()
                .unwrap()
                .push((self.clock.now(), request.clone()));
            if let Some(wait) = request.body()["controller_events"]["wait_ms"].as_u64() {
                self.clock.advance(Duration::from_millis(wait));
            }
            Ok(reply(request, &[]))
        }
        fn close(&mut self) {}
    }
    let fixture = Fixture::new();
    let clock = Arc::new(ManualEventRuntime::new());
    let requests = Arc::new(Mutex::new(vec![]));
    let mut deps = fixture.deps();
    deps.connector = Arc::new(PollConnector {
        clock: clock.clone(),
        requests: requests.clone(),
    });
    let source = ControllerEventClient::for_read_loop(
        fixture.endpoint.clone(),
        ReadLoopScope::Notify,
        &fixture.paths,
        &fixture.config,
        clock.clone(),
        deps,
    );
    let cache = NotifyCache::open(&fixture.paths, &fixture.config.controller).unwrap();
    let mut reconciler =
        TaskReconciler::new(PreviousProjection::Absent, None, vec![], clock.clone());
    let options = NotifyOptions {
        follow: true,
        quiet: true,
        no_titles: true,
        ..Default::default()
    };
    NotifyLoop {
        source: &source,
        reconciler: &mut reconciler,
        cache: &cache,
        channels: &[],
        options: &options,
        runtime: clock,
        stop_at: Some(Duration::from_secs(31)),
    }
    .run(&mut vec![])
    .unwrap();
    let requests = requests.lock().unwrap();
    let repair_instants: Vec<_> = requests
        .iter()
        .filter(|(_, request)| request.body()["controller_events"]["op"] == "repair")
        .map(|(instant, _)| *instant)
        .collect();
    assert_eq!(
        repair_instants,
        [
            Duration::ZERO,
            Duration::from_secs(15),
            Duration::from_secs(30)
        ]
    );
    assert!(
        requests
            .iter()
            .filter_map(|(_, request)| request.body()["controller_events"]["wait_ms"].as_u64())
            .all(|wait| wait <= 15_000)
    );
    let raw = fixture.endpoint.calls.lock().unwrap();
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].command(), "task.list");
    assert_eq!(raw[0].body(), &json!({"controller_health":true}));
}

// T7b end-to-end checks. Only SSH control is fake: the leader, identity RPC,
// private pin, hello/ready framing and each per-read RPC child are real.
mod t7b_live {
    use super::loop_fixtures;
    use clap::Parser;
    use mac_worker::{
        RuntimeContext,
        cli::Cli,
        controller::{
            ControllerRequest,
            channel::{
                ChannelFailure, ChannelRuntime, ClientContext, ClientDeps, ServiceRecord,
                SocketConnector, SocketIdentity, SocketSession,
                codec::{FramedSocketConnector, SessionCodec},
                identity::StdioIdentitySource,
                pin::PrivatePinStore,
                testing::FakeForwardControl,
            },
            decode_request,
            events::EventRuntime,
        },
        error::WorkerError,
        paths::PathLayout,
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
        task::{TaskId, TurnId},
    };
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        fs,
        io::{BufRead, BufReader, Write},
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        process::{Child, Command, Stdio},
        sync::{Arc, Mutex, mpsc},
        time::{Duration, Instant},
    };

    const GUARD: Duration = Duration::from_secs(60);

    pub struct Clock(Instant);
    impl ChannelRuntime for Clock {
        fn now(&self) -> Duration {
            self.0.elapsed()
        }
        fn cancelled(&self) -> bool {
            false
        }
    }
    impl EventRuntime for Clock {
        fn now(&self) -> Duration {
            self.0.elapsed()
        }
        fn cancelled(&self) -> bool {
            false
        }
        fn sleep(&self, _: Duration) {
            panic!("direct source checks must not sleep")
        }
    }
    impl Clock {
        pub fn deadline(&self) -> Duration {
            self.0.elapsed() + GUARD
        }
    }

    pub struct Fixture {
        leader: Leader,
        pub laptop: loop_fixtures::Fixture,
        pub raw: Arc<LocalRpc>,
        pub connector: Arc<ObservedConnector>,
        pub forwards: Arc<FakeForwardControl>,
        pub clock: Arc<Clock>,
        pub task: TaskId,
        pub record: ServiceRecord,
        _root: tempfile::TempDir,
    }
    impl Fixture {
        pub fn new() -> Self {
            let root = tempfile::Builder::new()
                .prefix("p3b")
                .tempdir_in("/private/tmp")
                .unwrap();
            let home = root.path().join("h");
            fs::create_dir(&home).unwrap();
            let denied_ssh = root.path().join("deny-ssh");
            fs::write(&denied_ssh, "#!/bin/sh\nexit 69\n").unwrap();
            fs::set_permissions(&denied_ssh, fs::Permissions::from_mode(0o755)).unwrap();
            let environment = BTreeMap::<OsString, OsString>::from([
                ("HOME".into(), home.clone().into_os_string()),
                (
                    "XDG_CONFIG_HOME".into(),
                    root.path().join("c").into_os_string(),
                ),
                (
                    "XDG_STATE_HOME".into(),
                    root.path().join("s").into_os_string(),
                ),
                (
                    "XDG_CACHE_HOME".into(),
                    root.path().join("k").into_os_string(),
                ),
                (
                    "XDG_DATA_HOME".into(),
                    root.path().join("d").into_os_string(),
                ),
                ("PATH".into(), "/usr/bin:/bin:/usr/sbin:/sbin".into()),
                ("MAC_WORKER_TEST_SSH".into(), denied_ssh.into_os_string()),
            ]);
            let paths = PathLayout::discover(None, &environment, &home).unwrap();
            fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
            fs::write(
                &paths.config,
                "version=1\n[[workers]]\nname='unused'\nssh='unused'\nslots=1\n",
            )
            .unwrap();
            let installed = root.path().join("worker");
            fs::copy(env!("CARGO_BIN_EXE_worker"), &installed).unwrap();
            let task = seed_completed_task(&paths);
            let mut command = Command::new(&installed);
            let mut child = command
                .env_clear()
                .envs(&environment)
                .arg("--config")
                .arg(&paths.config)
                .args(["controller", "run"])
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (tx, rx) = mpsc::channel();
            let reader = std::thread::spawn(move || {
                let mut line = String::new();
                BufReader::new(stdout).read_line(&mut line).unwrap();
                let _ = tx.send(line);
            });
            let leader = Leader(child);
            assert_eq!(
                rx.recv_timeout(GUARD).unwrap(),
                "controller leader acquired\n"
            );
            reader.join().unwrap();
            let start = Instant::now();
            let record = loop {
                if let Ok(bytes) = fs::read(paths.controller_state_root().join("rpc/service.json"))
                {
                    break serde_json::from_slice::<ServiceRecord>(&bytes).unwrap();
                }
                assert!(start.elapsed() < GUARD, "service readiness hang guard");
                std::thread::yield_now();
            };
            let raw = Arc::new(LocalRpc {
                installed,
                paths,
                environment,
                calls: Mutex::new(vec![]),
            });
            Self {
                leader,
                laptop: loop_fixtures::Fixture::new(),
                forwards: Arc::new(FakeForwardControl::new(record.service.socket_path.clone())),
                clock: Arc::new(Clock(Instant::now())),
                connector: Arc::new(ObservedConnector {
                    inner: FramedSocketConnector::new(Arc::new(SessionCodec::new())),
                    calls: Arc::new(Mutex::new(vec![])),
                }),
                raw,
                task,
                record,
                _root: root,
            }
        }
        pub fn deps(&self) -> ClientDeps {
            ClientDeps {
                identity: Arc::new(StdioIdentitySource::new()),
                pins: Arc::new(PrivatePinStore::new()),
                forwards: self.forwards.clone(),
                connector: self.connector.clone(),
                runtime: self.clock.clone(),
            }
        }
        pub fn run(&self, args: &[&str]) -> (u8, String, String) {
            let context: RuntimeContext = self
                .laptop
                .runtime
                .clone()
                .with_controller_channel_dependencies(self.deps());
            let (mut stdout, mut stderr) = (vec![], vec![]);
            let exit = mac_worker::run_with_io_in_context(
                Cli::try_parse_from(args).unwrap(),
                &*self.raw,
                &context,
                &mut stdout,
                &mut stderr,
            );
            (
                exit,
                String::from_utf8(stdout).unwrap(),
                String::from_utf8(stderr).unwrap(),
            )
        }
        pub fn clear_calls(&self) {
            self.raw.calls.lock().unwrap().clear();
            self.connector.calls.lock().unwrap().clear();
        }
        pub fn assert_only_identity_was_stdio(&self) {
            let calls = self.raw.calls.lock().unwrap();
            assert_eq!(calls.len(), 1, "unexpected raw fallback: {calls:?}");
            assert_eq!(calls[0].command(), "task.list");
            assert_eq!(calls[0].body()["controller_socket"]["op"], "identity");
            assert_eq!(
                self.laptop.pin().unwrap().controller_client_id,
                self.record.service.controller_client_id
            );
        }
        pub fn stop(&mut self) {
            self.leader.stop();
        }
    }

    struct Leader(Child);
    impl Leader {
        fn stop(&mut self) {
            assert_eq!(unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM) }, 0);
            let start = Instant::now();
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success());
                    return;
                }
                assert!(start.elapsed() < GUARD, "leader exit hang guard");
                std::thread::yield_now();
            }
        }
    }
    impl Drop for Leader {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    pub struct LocalRpc {
        installed: PathBuf,
        paths: PathLayout,
        environment: BTreeMap<OsString, OsString>,
        pub calls: Mutex<Vec<ControllerRequest>>,
    }
    impl ProcessRunner for LocalRpc {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.run_interruptible(request, &|| false)
        }
        fn run_interruptible(
            &self,
            request: &ProcessRequest,
            stop: &dyn Fn() -> bool,
        ) -> Result<ProcessResult, WorkerError> {
            assert!(request.program == "/usr/bin/ssh" || request.program == "fake-ssh");
            self.calls
                .lock()
                .unwrap()
                .push(decode_request(request.stdin.as_ref().unwrap()).unwrap());
            let mut local = request.clone();
            local.program = self.installed.clone().into_os_string();
            local.args = vec![
                "--config".into(),
                self.paths.config.clone().into_os_string(),
                "host".into(),
                "controller-rpc".into(),
            ];
            local.environment = self
                .environment
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            local.isolate_parent_environment = true;
            SystemProcessRunner.run_interruptible(&local, stop)
        }
    }
    pub struct ObservedConnector {
        inner: FramedSocketConnector,
        pub calls: Arc<Mutex<Vec<ControllerRequest>>>,
    }
    impl SocketConnector for ObservedConnector {
        fn connect(
            &self,
            path: &std::path::Path,
            identity: &SocketIdentity,
            ctx: &ClientContext<'_>,
        ) -> Result<Box<dyn SocketSession>, ChannelFailure> {
            Ok(Box::new(ObservedSession {
                inner: self.inner.connect(path, identity, ctx)?,
                calls: self.calls.clone(),
            }))
        }
    }
    struct ObservedSession {
        inner: Box<dyn SocketSession>,
        calls: Arc<Mutex<Vec<ControllerRequest>>>,
    }
    impl SocketSession for ObservedSession {
        fn exchange(
            &mut self,
            frame: &[u8],
            request: &ControllerRequest,
            ctx: &ClientContext<'_>,
        ) -> Result<ProcessResult, ChannelFailure> {
            self.calls.lock().unwrap().push(request.clone());
            self.inner.exchange(frame, request, ctx)
        }
        fn close(&mut self) {
            self.inner.close();
        }
    }

    fn seed_completed_task(paths: &PathLayout) -> TaskId {
        use mac_worker::{
            agent::{AgentKind, PermissionPolicy},
            client_state::ClientStateStore,
            task::{
                ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskLimits, TaskMeta,
                TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnSummary,
                TurnTerminal,
            },
        };
        let (task, turn) = (TaskId::generate(), TurnId::generate());
        let store = ClientStateStore::open(&paths.state).unwrap();
        let meta = TaskMeta::new(TaskMetaInput {
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
            title: Some("live routing fixture".into()),
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
        store
            .open_runner_log(task, turn)
            .unwrap()
            .write_all(b"fixture log\n")
            .unwrap();
        let checkpoint = paths
            .state
            .join("runners")
            .join(task.to_string())
            .join(format!("{turn}.checkpoint.json"));
        fs::write(&checkpoint, serde_json::to_vec(&serde_json::json!({"version":1,"task_id":task,"turn_id":turn,"committed":{
            "offsets":[0,0],"len":12,"accepted":true,"completion":{"outcome":TaskOutcome::Done,"drained":true}
        },"pending":null})).unwrap()).unwrap();
        fs::set_permissions(checkpoint, fs::Permissions::from_mode(0o600)).unwrap();
        task
    }
}

#[test]
fn route_real_server_wait_logs_and_raw_reads() {
    if isolated_loop_fixture("route_real_server_wait_logs_and_raw_reads") {
        return;
    }
    use loop_fixtures::commands;
    let mut fixture = t7b_live::Fixture::new();
    let task = fixture.task.to_string();
    let (exit, _, errors) = fixture.run(&["worker", "task", "wait", "--task-id", &task]);
    assert_eq!(exit, 0, "{errors}");
    assert!(fixture.connector.calls.lock().unwrap().is_empty());
    assert_eq!(commands(&fixture.raw.calls), ["task.wait.poll"]);
    assert_eq!(fixture.forwards.resolutions(), 0);
    assert_eq!(fixture.forwards.opens(), 0);
    assert_eq!(fixture.forwards.cancels(), 0);
    assert!(fixture.laptop.pin().is_none());
    let expected = fixture.record.service.controller_client_id.to_string();
    let cache = fixture
        .laptop
        .paths
        .controller_cache_root()
        .join("notify-sentinel");
    std::fs::write(&cache, b"independent notification state").unwrap();
    for args in [
        vec!["worker", "controller", "channel", "identity", "--json"],
        vec![
            "worker",
            "controller",
            "channel",
            "repin",
            "--expect-client-id",
            &expected,
        ],
    ] {
        fixture.clear_calls();
        let (exit, output, errors) = fixture.run(&args);
        assert_eq!(exit, 0, "{errors}");
        assert!(fixture.connector.calls.lock().unwrap().is_empty());
        assert_eq!(fixture.forwards.opens(), 0);
        assert_eq!(fixture.forwards.cancels(), 0);
        if args.contains(&"identity") {
            let raw = fixture.raw.calls.lock().unwrap();
            assert_eq!(raw.len(), 1);
            assert_eq!(raw[0].command(), "task.list");
            assert_eq!(raw[0].body()["controller_socket"]["op"], "identity");
            assert!(fixture.laptop.pin().is_none());
            let identity: mac_worker::controller::channel::SocketIdentity =
                serde_json::from_str(&output).unwrap();
            assert_eq!(identity.service, fixture.record.service);
        } else {
            fixture.assert_only_identity_was_stdio();
        }
    }
    assert_eq!(
        std::fs::read(cache).unwrap(),
        b"independent notification state"
    );
    assert!(!fixture.laptop.paths.state.join("client-id").exists());
    fixture.clear_calls();
    let (exit, logs, errors) = fixture.run(&["worker", "task", "logs", &task, "--follow", "--raw"]);
    assert_eq!(exit, 0, "{errors}");
    assert_eq!(logs, "fixture log\n");
    let reads = fixture.connector.calls.lock().unwrap();
    assert!(!reads.is_empty());
    assert!(
        reads
            .iter()
            .all(|request| request.command() == "task.logs" && request.body()["wait_ms"] == 15_000)
    );
    drop(reads);
    let raw = fixture.raw.calls.lock().unwrap();
    assert_eq!(raw.len(), 2);
    assert_eq!(raw[0].command(), "task.list");
    assert_eq!(raw[0].body(), &json!({"controller_health":true}));
    assert_eq!(raw[1].command(), "task.list");
    assert_eq!(raw[1].body()["controller_socket"]["op"], "identity");
    assert_eq!(
        fixture.laptop.pin().unwrap().controller_client_id,
        fixture.record.service.controller_client_id
    );
    drop(raw);
    assert_eq!(fixture.forwards.opens(), 1);
    assert_eq!(fixture.forwards.cancels(), 1);
    for args in [
        vec!["worker", "task", "status", &task],
        vec!["worker", "task", "list"],
        vec!["worker", "task", "logs", &task, "--raw"],
    ] {
        fixture.clear_calls();
        let (exit, _, errors) = fixture.run(&args);
        assert_eq!(exit, 0, "{errors}");
        assert_eq!(fixture.raw.calls.lock().unwrap().len(), 1);
        assert!(fixture.connector.calls.lock().unwrap().is_empty());
        assert_eq!(fixture.forwards.opens(), 1);
    }
    fixture.stop();
}

#[test]
fn route_real_server_owned_events_and_notify_reads() {
    if isolated_loop_fixture("route_real_server_owned_events_and_notify_reads") {
        return;
    }
    use mac_worker::controller::{
        channel::ReadLoopScope,
        events::{
            EventSource, ReadQuery, TaskAddressQuery, TaskRepairQuery,
            client::ControllerEventClient,
        },
    };
    let mut fixture = t7b_live::Fixture::new();
    for (index, scope) in [ReadLoopScope::EventsFollow, ReadLoopScope::Notify]
        .into_iter()
        .enumerate()
    {
        fixture.clear_calls();
        {
            let source = ControllerEventClient::for_read_loop(
                fixture.raw.clone(),
                scope,
                &fixture.laptop.paths,
                &fixture.laptop.config,
                fixture.clock.clone(),
                fixture.deps(),
            );
            source.discover(fixture.clock.deadline()).unwrap();
            source
                .read(ReadQuery::default(), fixture.clock.deadline())
                .unwrap();
            if scope == ReadLoopScope::Notify {
                let addressed = source
                    .tasks(
                        TaskAddressQuery {
                            task_ids: vec![fixture.task],
                            include_titles: false,
                            proof_after: None,
                        },
                        fixture.clock.deadline(),
                    )
                    .unwrap();
                assert_eq!(addressed.rows.len(), 1);
                let page = source
                    .repair(TaskRepairQuery::default(), fixture.clock.deadline())
                    .unwrap();
                assert!(page.complete);
                assert_eq!(page.rows.len(), 1);
            }
            let raw = fixture.raw.calls.lock().unwrap();
            assert_eq!(raw.len(), 2);
            assert_eq!(raw[0].command(), "task.list");
            assert_eq!(raw[0].body(), &json!({"controller_health":true}));
            assert_eq!(raw[1].command(), "task.list");
            assert_eq!(raw[1].body()["controller_socket"]["op"], "identity");
            assert_eq!(
                fixture.laptop.pin().unwrap().controller_client_id,
                fixture.record.service.controller_client_id
            );
            assert_eq!(fixture.forwards.opens(), index + 1);
        }
        assert_eq!(fixture.forwards.cancels(), index + 1);
        assert_eq!(
            fixture.connector.calls.lock().unwrap().len(),
            if scope == ReadLoopScope::Notify { 3 } else { 1 }
        );
    }
    fixture.stop();
}
