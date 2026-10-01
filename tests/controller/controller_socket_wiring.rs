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
