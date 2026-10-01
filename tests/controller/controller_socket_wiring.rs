//! T1 gate only; actual leader/CLI/loop routing remains T7's responsibility.
use mac_worker::controller::channel::{server_eligible_read, testing::request_fixture};
use serde_json::json;

#[test]
fn gate_raw_operator_setters_are_ineligible() {
    for command in ["controller.drain", "task.reconcile", "task.publish-retry"] {
        assert!(!server_eligible_read(&request_fixture(command, json!({}))));
    }
}

// T7a owns this block. T7b appends its loop/route tests separately.
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

    struct Fixture {
        _temp: tempfile::TempDir,
        home: PathBuf,
        installed: PathBuf,
        paths: PathLayout,
        environment: BTreeMap<OsString, OsString>,
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
        assert!(!record.executable.path.exists());
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
        assert_eq!(
            commands(&fixture.connector.requests),
            ["task.wait.poll"],
            "{args:?}"
        );
        assert!(!commands(&fixture.endpoint.calls).contains(&"task.wait.poll".into()));
        assert_eq!(fixture.forwards.opens(), 1);
        assert_eq!(fixture.forwards.cancels(), 1);
        fixture.assert_authenticated_pin();
        if args.contains(&"--interrupt") {
            assert_eq!(
                *fixture.order.lock().unwrap(),
                [
                    "stdio:task.status",
                    "stdio:task.cancel",
                    "stdio:channel.identity",
                    "socket:task.wait.poll",
                    "stdio:task.status",
                    "stdio:task.say"
                ]
            );
        } else if args.contains(&"say") {
            assert_eq!(
                *fixture.order.lock().unwrap(),
                [
                    "stdio:task.say",
                    "stdio:channel.identity",
                    "socket:task.wait.poll",
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
        assert_eq!(order[4], "stdio:channel.identity");
        assert_eq!(order[5], "socket:task.wait.poll");
        assert_eq!(commands(&fixture.connector.requests), ["task.wait.poll"]);
        assert_eq!(fixture.forwards.opens(), 1);
        assert_eq!(fixture.forwards.cancels(), 1);
        fixture.assert_authenticated_pin();
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
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body(), &json!({"controller_health":true}));
    assert_eq!(requests[1].command(), "task.logs");
    assert_eq!(requests[1].body()["wait_ms"], 15_000);
    assert!(fixture.endpoint.calls.lock().unwrap().is_empty());
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
            assert!(fixture.endpoint.calls.lock().unwrap().is_empty());
            assert_eq!(fixture.forwards.opens(), 1);
        }
        assert_eq!(fixture.forwards.cancels(), 1);
        fixture.assert_authenticated_pin();
        assert_eq!(
            fixture.connector.requests.lock().unwrap().len(),
            if scope == ReadLoopScope::Notify { 4 } else { 2 }
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
    assert!(source.discover(Duration::from_secs(30)).is_err());
    assert_eq!(fixture.forwards.resolutions(), 1);
    assert_eq!(fixture.forwards.opens(), 0);
    assert!(fixture.endpoint.calls.lock().unwrap().is_empty());
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
    assert!(fixture.endpoint.calls.lock().unwrap().is_empty());
}
