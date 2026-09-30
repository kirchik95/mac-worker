use mac_worker::{
    controller::{ControllerFault, ControllerStore, RequestPhase, parse_request},
    protocol::PROTOCOL_VERSION,
};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn controller_run_persists_failed_tick_and_shuts_down_cleanly() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let state = root.join("mac-worker-controller");
    let store = ControllerStore::open(&state).unwrap();
    let bad = "018f0f4a6b5c7d8e9f00112233445560";
    let good = "018f0f4a6b5c7d8e9f00112233445561";
    for id in [bad, good] {
        let request = parse_request(
            &serde_json::to_vec(&json!({
                "protocol_version": PROTOCOL_VERSION, "request_id": id,
                "command": "checkpoint.submit", "body": {},
            }))
            .unwrap(),
        )
        .unwrap();
        store
            .handle(&request, ControllerFault::StopAfterPublish)
            .unwrap();
    }
    std::fs::write(
        state.join("active").join(format!("{bad}.json")),
        b"/private/secret token=private-value",
    )
    .unwrap();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_worker"))
            .env_clear()
            .env("HOME", &root)
            .env("XDG_STATE_HOME", &root)
            .env("XDG_CONFIG_HOME", root.join("config"))
            .env("XDG_CACHE_HOME", root.join("cache"))
            .env("XDG_DATA_HOME", root.join("data"))
            .args(["controller", "run"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stderr = child.0.stderr.take().unwrap();
    let (line_tx, line_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut line = String::new();
        BufReader::new(stderr).read_line(&mut line).unwrap();
        let _ = line_tx.send(line);
    });
    let line = line_rx
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .expect("failing tick must be reported without waiting for operator input");
    assert!(line.contains("CONTROLLER_TRANSPORT"), "{line}");
    assert!(!line.contains("private-value"));
    assert!(!line.contains("/private/"));
    let health: Value =
        serde_json::from_slice(&std::fs::read(state.join("health.json")).unwrap()).unwrap();
    assert!(
        health["failures"]["CONTROLLER_TRANSPORT"]["count"]
            .as_u64()
            .unwrap()
            >= 1
    );
    assert!(health["last_tick_end_millis"].is_number());
    assert!(health["last_progress_millis"].is_number());
    assert_eq!(
        store.load(good).unwrap().unwrap().phase(),
        RequestPhase::Acked
    );
    let pid = child.0.id();
    assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGINT) }, 0);
    std::thread::scope(|scope| {
        let (tx, rx) = mpsc::channel();
        let process = &mut child.0;
        scope.spawn(move || {
            tx.send(process.wait().unwrap()).unwrap();
        });
        let status = rx.recv_timeout(crate::support::HANDSHAKE_TIMEOUT);
        if status.is_err() {
            unsafe {
                libc::kill(pid as i32, libc::SIGKILL);
            }
        }
        assert!(
            status
                .expect("controller shutdown must join promptly")
                .success()
        );
    });
    reader.join().unwrap();
    let health: Value =
        serde_json::from_slice(&std::fs::read(state.join("health.json")).unwrap()).unwrap();
    assert!(health["stopped_at_millis"].is_number());
    let _next = mac_worker::controller::ControllerLeader::acquire(&state).unwrap();
}

#[test]
fn blocked_tick_leaves_shutdown_pollable_and_is_joined() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let stop = AtomicBool::new(false);
    let joined = AtomicBool::new(false);
    let calls = AtomicUsize::new(0);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered_tx = Some(entered_tx);
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    mac_worker::controller::runtime::run_tick_loop(
        &runtime,
        &stop,
        || {
            calls.fetch_add(1, Ordering::SeqCst);
            entered_tx.take().unwrap().send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
                .expect("shutdown must run while the tick is blocked");
            // The signal future releases the handshake before the loop sets stop.
            while !stop.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            joined.store(true, Ordering::Release);
            Ok(None)
        },
        async {
            entered_rx.await.unwrap();
            release_tx.send(()).unwrap();
        },
        |_| Ok(()),
    )
    .unwrap();
    assert!(joined.load(Ordering::Acquire));
    assert!(stop.load(Ordering::Acquire));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn shutdown_is_forwarded_to_inflight_process_io() {
    use mac_worker::{
        error::{ProcessError, WorkerError},
        process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    struct BlockingIo {
        entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }
    impl ProcessRunner for BlockingIo {
        fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            panic!("controller must use interruptible I/O");
        }
        fn run_interruptible(
            &self,
            _: &ProcessRequest,
            should_stop: &dyn Fn() -> bool,
        ) -> Result<ProcessResult, WorkerError> {
            self.entered
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !should_stop() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "shutdown did not reach in-flight I/O"
                );
                std::thread::yield_now();
            }
            Err(ProcessError::Cancelled.into())
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let stop = AtomicBool::new(false);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let inner = BlockingIo {
        entered: Mutex::new(Some(entered_tx)),
    };
    let runner = mac_worker::controller::runtime::ControllerProcessRunner::new(&inner, &stop);
    let request = ProcessRequest {
        program: "unused".into(),
        args: Vec::new(),
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        isolate_parent_environment: true,
        policy: ProcessPolicy {
            stdout_limit: 0,
            stderr_limit: 0,
            deadline: Duration::from_secs(60),
        },
    };
    mac_worker::controller::runtime::run_tick_loop(
        &runtime,
        &stop,
        || {
            assert!(matches!(
                runner.run(&request),
                Err(WorkerError::Process(ProcessError::Cancelled))
            ));
            Ok(None)
        },
        async {
            entered_rx.await.unwrap();
        },
        |_| Ok(()),
    )
    .unwrap();
    assert!(stop.load(Ordering::Acquire));
}
