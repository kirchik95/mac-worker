use std::{
    fs,
    io::{Read, Write},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use mac_worker::{
    config::SshConfig,
    controller::channel::{
        contracts::*,
        forward::MasterForwardControl,
        testing::{FakeForwardPaths, ManualRuntime, identity_fixture},
    },
    error::WorkerError,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
};
use sha2::{Digest, Sha256};

// Each test has its own child HOME. No process-global env changes or real SSH
// state are involved, and nextest still selects the ordinary area module.
fn isolated(name: &str) -> bool {
    if std::env::var_os("MAC_WORKER_T5_FIXTURE_ROOT").is_some() {
        return false;
    }
    let root = tempfile::Builder::new()
        .prefix("t5")
        .tempdir_in("/private/tmp")
        .unwrap();
    let result = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("controller_socket_forward::{name}"),
            "--nocapture",
        ])
        .env("MAC_WORKER_T5_FIXTURE_ROOT", root.path())
        .env("HOME", root.path())
        .env_remove("MAC_WORKER_TEST_SSH")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    true
}

type Clock = ManualRuntime;

#[derive(Default)]
struct Files {
    allocations: AtomicUsize,
    cleanups: AtomicUsize,
    allocated: Mutex<Vec<ForwardPath>>,
}
impl ForwardPaths for Files {
    fn allocate(&self, paths: &PathLayout) -> Result<ForwardPath, ChannelFailure> {
        let number = self.allocations.fetch_add(1, Ordering::SeqCst);
        let root = paths.controller_cache_root().join("channel");
        fs::create_dir_all(&root).unwrap();
        let directory = root.join(format!("c{number:x} 'q\""));
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let allocation = ForwardPath {
            socket_path: directory.join("s"),
            directory_identity: entry(&directory),
            directory,
        };
        self.allocated.lock().unwrap().push(allocation.clone());
        Ok(allocation)
    }
    fn validate_socket(&self, path: &ForwardPath) -> Result<EntryIdentity, ChannelFailure> {
        if !path.directory.exists()
            || entry(&path.directory) != path.directory_identity
            || !path.socket_path.exists()
        {
            return Err(ChannelFailure::Unavailable(ChannelReason::UnsafePath));
        }
        let socket = entry(&path.socket_path);
        if socket.kind != libc::S_IFSOCK as u32
            || socket.mode != 0o600
            || socket.owner != unsafe { libc::geteuid() }
        {
            return Err(ChannelFailure::Unavailable(ChannelReason::UnsafePath));
        }
        Ok(socket)
    }
    fn cleanup_if_refused(
        &self,
        path: &ForwardPath,
        socket: Option<EntryIdentity>,
        ctx: &CleanupContext,
    ) -> ForwardDisposition {
        self.cleanups.fetch_add(1, Ordering::SeqCst);
        if ctx.runtime.now() >= ctx.deadline
            || socket.is_none()
            || self.validate_socket(path).ok() != socket
        {
            return ForwardDisposition::Retained;
        }
        match std::os::unix::net::UnixStream::connect(&path.socket_path) {
            Err(error) if error.raw_os_error() == Some(libc::ECONNREFUSED) => {}
            _ => return ForwardDisposition::Retained,
        }
        if self.validate_socket(path).ok() != socket {
            return ForwardDisposition::Retained;
        }
        fs::remove_file(&path.socket_path).unwrap();
        fs::remove_dir(&path.directory).unwrap();
        ForwardDisposition::Cleaned
    }
}

fn entry(path: &Path) -> EntryIdentity {
    let metadata = fs::symlink_metadata(path).unwrap();
    EntryIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner: metadata.uid(),
        kind: metadata.mode() & libc::S_IFMT as u32,
        mode: metadata.mode() & 0o7777,
    }
}

struct ResolutionRunner {
    output: Vec<u8>,
    calls: Mutex<Vec<ProcessRequest>>,
}
impl ProcessRunner for ResolutionRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.calls.lock().unwrap().push(request.clone());
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: self.output.clone(),
            stderr: Vec::new(),
        })
    }
}

fn fixture() -> (PathLayout, SshConfig, ConfiguredRoute, PathBuf) {
    let root = PathBuf::from(std::env::var_os("MAC_WORKER_T5_FIXTURE_ROOT").unwrap());
    let config = root.join("owner's managed \"config\".conf");
    fs::write(&config, "Host fixture\n  HostName must-not-connect.invalid\n  StrictHostKeyChecking yes\n  LocalForward 127.0.0.1:31001 127.0.0.1:31002\n  RemoteForward 31003 127.0.0.1:31004\n  DynamicForward 127.0.0.1:31005\nHost *\n  ControlMaster no\n  ControlPath none\n").unwrap();
    let digest = format!(
        "{:x}",
        Sha256::digest(config.as_os_str().as_encoded_bytes())
    );
    let directory = root
        .join(".cache/mac-worker")
        .join(format!("ssh-{}", &digest[..16]));
    fs::create_dir_all(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let master = directory.join("m");
    let paths = PathLayout {
        config: root.join("config.toml"),
        state: root.join("state/mac-worker"),
        cache: root.join(".cache/mac-worker"),
        data: root.join("data/mac-worker"),
    };
    let ssh = SshConfig {
        multiplex: true,
        config_file: Some(config.clone()),
    };
    let route = ConfiguredRoute {
        ssh: "fixture".into(),
        remote_binary: "~/.local/bin/worker".into(),
        ssh_config_file: Some(config),
    };
    (paths, ssh, route, master)
}

fn strings(request: &ProcessRequest) -> Vec<String> {
    request
        .args
        .iter()
        .map(|arg| arg.to_str().unwrap().to_owned())
        .collect()
}
fn has_pair(args: &[String], key: &str, value: &str) -> bool {
    args.windows(2).any(|pair| pair == [key, value])
}
fn context(clock: &Clock) -> ClientContext<'_> {
    ClientContext {
        runtime: clock,
        deadline: Duration::from_secs(30),
        should_stop: &|| false,
    }
}

#[test]
fn resolution_uses_original_config_and_literal_bootstrap() {
    if isolated("resolution_uses_original_config_and_literal_bootstrap") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    let files = Arc::new(FakeForwardPaths::default());
    let control = MasterForwardControl::new(paths, files.clone(), ssh);
    let raw = ResolutionRunner {
        output: format!(
            "hostname must-not-connect.invalid\ncontrolpath {}\n",
            master.display()
        )
        .into_bytes(),
        calls: Mutex::default(),
    };
    let clock = Clock::default();
    let plan = control.resolve(&raw, &route, &context(&clock)).unwrap();
    assert_eq!(plan.control_path, master);
    let calls = raw.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let args = strings(&calls[0]);
    assert!(args.contains(&"-G".into()));
    assert!(has_pair(
        &args,
        "-F",
        route.ssh_config_file.as_ref().unwrap().to_str().unwrap()
    ));
    for option in [
        "ControlMaster=auto",
        "ControlPersist=60",
        "ClearAllForwardings=yes",
        "StreamLocalBindMask=0177",
        "StreamLocalBindUnlink=no",
        "BatchMode=yes",
        "ForwardAgent=no",
        "ExitOnForwardFailure=yes",
        "ServerAliveInterval=10",
        "ServerAliveCountMax=3",
    ] {
        assert!(has_pair(&args, "-o", option), "{args:?}");
    }
    assert!(calls[0].policy.deadline <= Duration::from_secs(5));
    let bootstrap = strings(&plan.bootstrap_request);
    assert!(has_pair(&bootstrap, "-S", master.to_str().unwrap()));
    assert!(has_pair(
        &bootstrap,
        "-F",
        route.ssh_config_file.as_ref().unwrap().to_str().unwrap()
    ));
    assert_eq!(
        &bootstrap[bootstrap.len() - 3..],
        ["--", "fixture", "~/.local/bin/worker host controller-rpc"]
    );
    assert_eq!(files.allocations(), 0);
    let metadata = fs::metadata(master.parent().unwrap()).unwrap();
    assert_eq!(plan.parent.inode, metadata.ino());
    assert_eq!(plan.parent.device, metadata.dev());
}

#[test]
fn resolution_rejects_ambiguous_and_unsafe_paths_before_allocation() {
    if isolated("resolution_rejects_ambiguous_and_unsafe_paths_before_allocation") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    let files = Arc::new(FakeForwardPaths::default());
    let control = MasterForwardControl::new(paths, files.clone(), ssh);
    let clock = Clock::default();
    for output in [
        "controlpath none\n".into(),
        "controlpath relative\n".into(),
        format!("controlpath {}/%C\n", master.parent().unwrap().display()),
        format!(
            "controlpath {}/$token\n",
            master.parent().unwrap().display()
        ),
        format!(
            "controlpath {}/bad:socket\n",
            master.parent().unwrap().display()
        ),
        format!(
            "controlpath {}/bad\tsocket\n",
            master.parent().unwrap().display()
        ),
        format!(
            "controlpath {}\ncontrolpath {}\n",
            master.display(),
            master.display()
        ),
        "hostname fixture\n".into(),
        "controlpath /private/tmp/foreign-master\n".into(),
    ] {
        let raw = ResolutionRunner {
            output: output.into_bytes(),
            calls: Mutex::default(),
        };
        assert!(control.resolve(&raw, &route, &context(&clock)).is_err());
    }
    fs::set_permissions(master.parent().unwrap(), fs::Permissions::from_mode(0o755)).unwrap();
    let raw = ResolutionRunner {
        output: format!("controlpath {}\n", master.display()).into_bytes(),
        calls: Mutex::default(),
    };
    assert!(control.resolve(&raw, &route, &context(&clock)).is_err());
    assert_eq!(
        fs::metadata(master.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert_eq!(files.allocations(), 0);
}

fn endpoint_at_94_bytes(master: &std::path::Path) -> PathBuf {
    let documented = format!(
        "/Users/kirchik/.cache/mac-worker/ssh-{}/{}",
        "a".repeat(16),
        "b".repeat(40)
    );
    assert_eq!(documented.len(), 94);
    let cold_length = format!("{documented}.{}", "c".repeat(16)).len();
    assert_eq!(cold_length, 111);
    assert!(cold_length >= 104);
    let parent = master.parent().unwrap();
    let endpoint = parent.join("d".repeat(94 - parent.as_os_str().as_encoded_bytes().len() - 1));
    assert_eq!(endpoint.as_os_str().as_encoded_bytes().len(), 94);
    endpoint
}

#[test]
fn cold_master_creation_includes_openssh_suffix_guard() {
    if isolated("cold_master_creation_includes_openssh_suffix_guard") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    let master = endpoint_at_94_bytes(&master);
    let files = Arc::new(FakeForwardPaths::default());
    let control = MasterForwardControl::new(paths, files.clone(), ssh);
    let raw = ResolutionRunner {
        output: format!("controlpath {}\n", master.display()).into_bytes(),
        calls: Mutex::default(),
    };
    assert!(matches!(
        control.resolve(&raw, &route, &context(&Clock::default())),
        Err(ChannelFailure::Unavailable(ChannelReason::UnsafePath))
    ));
    assert_eq!(raw.calls.lock().unwrap().len(), 1);
    assert_eq!(files.allocations(), 0);
    assert!(!master.exists());
}

#[test]
fn live_94_byte_master_does_not_pay_creation_suffix() {
    if isolated("live_94_byte_master_does_not_pay_creation_suffix") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    let master = endpoint_at_94_bytes(&master);
    let _listener = std::os::unix::net::UnixListener::bind(&master).unwrap();
    fs::set_permissions(&master, fs::Permissions::from_mode(0o600)).unwrap();
    let control = MasterForwardControl::new(paths, Arc::new(Files::default()), ssh);
    let raw = ResolutionRunner {
        output: format!("controlpath {}\n", master.display()).into_bytes(),
        calls: Mutex::default(),
    };
    assert_eq!(
        control
            .resolve(&raw, &route, &context(&Clock::default()))
            .unwrap()
            .control_path,
        master
    );
}

// Only the test master implements the mux wire protocol. Production delegates
// it to OpenSSH. This verifies actual messages from /usr/bin/ssh with no host.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MuxForward {
    operation: u32,
    kind: u32,
    local: String,
    local_port: u32,
    remote: String,
    remote_port: u32,
}

struct Mux {
    path: PathBuf,
    messages: Arc<Mutex<Vec<MuxForward>>>,
    alive: Arc<AtomicUsize>,
    forward: Arc<Mutex<Option<std::os::unix::net::UnixListener>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Mux {
    fn start(path: PathBuf) -> Self {
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let messages = Arc::new(Mutex::new(Vec::new()));
        let alive = Arc::new(AtomicUsize::new(0));
        let forward = Arc::new(Mutex::new(None));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_messages = messages.clone();
        let thread_alive = alive.clone();
        let thread_forward = forward.clone();
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                if thread_stop.load(Ordering::SeqCst) {
                    break;
                }
                stream
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                send_mux(&mut stream, &[1, 4]);
                let hello = read_mux(&mut stream).unwrap();
                assert_eq!(hello, [0, 0, 0, 1, 0, 0, 0, 4]);
                while let Some(packet) = read_mux(&mut stream) {
                    let mut input = packet.as_slice();
                    let operation = mux_u32(&mut input);
                    let id = mux_u32(&mut input);
                    match operation {
                        0x10000004 => {
                            assert!(input.is_empty());
                            thread_alive.fetch_add(1, Ordering::SeqCst);
                            send_mux(&mut stream, &[0x80000005, id, std::process::id()]);
                        }
                        0x10000006 | 0x10000007 => {
                            let message = MuxForward {
                                operation,
                                kind: mux_u32(&mut input),
                                local: mux_string(&mut input),
                                local_port: mux_u32(&mut input),
                                remote: mux_string(&mut input),
                                remote_port: mux_u32(&mut input),
                            };
                            assert!(input.is_empty());
                            if message.kind == 1
                                && message.local_port == u32::MAX - 1
                                && message.remote_port == u32::MAX - 1
                            {
                                if operation == 0x10000006 {
                                    let owned =
                                        std::os::unix::net::UnixListener::bind(&message.local)
                                            .unwrap();
                                    fs::set_permissions(
                                        &message.local,
                                        fs::Permissions::from_mode(0o600),
                                    )
                                    .unwrap();
                                    *thread_forward.lock().unwrap() = Some(owned);
                                } else {
                                    thread_forward.lock().unwrap().take();
                                }
                            }
                            thread_messages.lock().unwrap().push(message);
                            send_mux(&mut stream, &[0x80000001, id]);
                        }
                        other => panic!("unexpected control message {other:x}"),
                    }
                }
            }
        });
        Self {
            path,
            messages,
            alive,
            forward,
            stop,
            thread: Some(thread),
        }
    }
}
impl Drop for Mux {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = std::os::unix::net::UnixStream::connect(&self.path);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}
fn read_mux(stream: &mut std::os::unix::net::UnixStream) -> Option<Vec<u8>> {
    let mut length = [0; 4];
    if stream.read_exact(&mut length).is_err() {
        return None;
    }
    let length = u32::from_be_bytes(length) as usize;
    assert!(length <= 8192);
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).unwrap();
    Some(payload)
}
fn send_mux(stream: &mut std::os::unix::net::UnixStream, words: &[u32]) {
    stream
        .write_all(&((words.len() * 4) as u32).to_be_bytes())
        .unwrap();
    for word in words {
        stream.write_all(&word.to_be_bytes()).unwrap();
    }
}
fn mux_u32(input: &mut &[u8]) -> u32 {
    let (word, rest) = input.split_at(4);
    *input = rest;
    u32::from_be_bytes(word.try_into().unwrap())
}
fn mux_string(input: &mut &[u8]) -> String {
    let size = mux_u32(input) as usize;
    let (text, rest) = input.split_at(size);
    *input = rest;
    String::from_utf8(text.to_vec()).unwrap()
}

#[derive(Default)]
struct LocalMuxRunner {
    mux: Mutex<Option<Mux>>,
    calls: Mutex<Vec<ProcessRequest>>,
}
impl ProcessRunner for LocalMuxRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.run_interruptible(request, &|| false)
    }
    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        self.calls.lock().unwrap().push(request.clone());
        let args = strings(request);
        if args.contains(&"-G".into()) {
            // -G resolves/dumps config only; it never opens a host connection.
            let result = SystemProcessRunner.run_interruptible(request, stop)?;
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let output = std::str::from_utf8(&result.stdout).unwrap();
            let endpoint = output
                .lines()
                .find_map(|line| line.strip_prefix("controlpath "))
                .unwrap();
            *self.mux.lock().unwrap() = Some(Mux::start(endpoint.into()));
            Ok(result)
        } else if args.contains(&"-O".into()) {
            // -O only talks to our local fake mux socket. There is no SSH peer.
            assert!(has_pair(&args, "-F", "/dev/null"));
            SystemProcessRunner.run_interruptible(request, stop)
        } else {
            // T4's authenticated identity bootstrap seam, without a real host.
            assert!(args.last().unwrap().ends_with("host controller-rpc"));
            assert!(has_pair(
                &args,
                "-S",
                self.mux
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .path
                    .to_str()
                    .unwrap()
            ));
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }
}

fn identity() -> SocketIdentity {
    let mut identity = identity_fixture();
    identity.service.socket_path =
        "/Users/controller/.local/state/mac-worker-controller/rpc/s".into();
    identity
}

#[test]
fn real_openssh_mux_controls_ignore_config_forwards_and_edits() {
    if isolated("real_openssh_mux_controls_ignore_config_forwards_and_edits") {
        return;
    }
    let (paths, ssh, route, _) = fixture();
    let files = Arc::new(Files::default());
    let control = MasterForwardControl::new(paths, files.clone(), ssh);
    let raw = LocalMuxRunner::default();
    let clock = Arc::new(Clock::default());
    let ctx = context(&clock);
    let master = control.resolve(&raw, &route, &ctx).unwrap();
    let bootstrap_args = strings(&master.bootstrap_request);
    assert!(has_pair(&bootstrap_args, "-o", "ClearAllForwardings=yes"));
    assert!(has_pair(
        &bootstrap_args,
        "-F",
        route.ssh_config_file.as_ref().unwrap().to_str().unwrap()
    ));
    raw.run_interruptible(&master.bootstrap_request, ctx.should_stop)
        .unwrap();
    let snapshot = identity();
    let mut lease = control.open(&raw, &master, &snapshot, &ctx).unwrap();
    lease.verify().unwrap();
    let owned = lease.local_socket().to_path_buf();
    assert!(owned.to_str().unwrap().contains(" 'q\""));
    // An application session is explicitly closed before cancelling its lease.
    let application = std::os::unix::net::UnixStream::connect(&owned).unwrap();
    drop(application);
    fs::write(route.ssh_config_file.as_ref().unwrap(), "Host *\n  HostName changed.invalid\n  ControlPath /private/tmp/foreign-master\n  LocalForward 32001 127.0.0.1:32002\n  RemoteForward 32003 127.0.0.1:32004\n  DynamicForward 32005\n").unwrap();
    let cleanup = CleanupContext {
        runtime: clock,
        deadline: Duration::from_secs(5),
    };
    assert_eq!(lease.cancel(&raw, &cleanup), ForwardDisposition::Cleaned);
    assert!(!owned.exists());
    let guard = raw.mux.lock().unwrap();
    let mux = guard.as_ref().unwrap();
    assert!(mux.path.exists());
    assert!(mux.forward.lock().unwrap().is_none());
    assert!(mux.alive.load(Ordering::SeqCst) >= 1);
    assert_eq!(
        *mux.messages.lock().unwrap(),
        vec![
            MuxForward {
                operation: 0x10000006,
                kind: 1,
                local: owned.to_str().unwrap().into(),
                local_port: u32::MAX - 1,
                remote: snapshot.service.socket_path.to_str().unwrap().into(),
                remote_port: u32::MAX - 1
            },
            MuxForward {
                operation: 0x10000007,
                kind: 1,
                local: owned.to_str().unwrap().into(),
                local_port: u32::MAX - 1,
                remote: snapshot.service.socket_path.to_str().unwrap().into(),
                remote_port: u32::MAX - 1
            },
        ]
    );
    for call in raw
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|call| strings(call).contains(&"-O".into()))
    {
        let args = strings(call);
        assert!(has_pair(&args, "-S", master.control_path.to_str().unwrap()));
        assert!(!args.contains(&"-N".into()));
        assert!(!args.contains(&"exit".into()));
        assert!(!args.contains(&"ClearAllForwardings=yes".into()));
        for option in [
            "BatchMode=yes",
            "ForwardAgent=no",
            "ExitOnForwardFailure=yes",
            "ControlMaster=no",
            "StreamLocalBindMask=0177",
            "StreamLocalBindUnlink=no",
            "ServerAliveInterval=10",
            "ServerAliveCountMax=3",
        ] {
            assert!(has_pair(&args, "-o", option), "{args:?}");
        }
        assert_eq!(
            args.iter().filter(|arg| *arg == "-L").count(),
            usize::from(!has_pair(&args, "-O", "check"))
        );
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Success,
    NoEndpoint,
    UnsafeLocal,
    ExpiredMaster,
    CancelError,
}

struct ControlRunner {
    master: PathBuf,
    master_listener: Mutex<Option<std::os::unix::net::UnixListener>>,
    owned: Mutex<Option<std::os::unix::net::UnixListener>>,
    calls: Mutex<Vec<ProcessRequest>>,
    mode: Mode,
}
impl ControlRunner {
    fn new(master: PathBuf, mode: Mode) -> Self {
        let listener = std::os::unix::net::UnixListener::bind(&master).unwrap();
        fs::set_permissions(&master, fs::Permissions::from_mode(0o600)).unwrap();
        Self {
            master,
            master_listener: Mutex::new(Some(listener)),
            owned: Mutex::new(None),
            calls: Mutex::default(),
            mode,
        }
    }
}
fn ok_output(stdout: Vec<u8>) -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })
}
impl ProcessRunner for ControlRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.calls.lock().unwrap().push(request.clone());
        let args = strings(request);
        if args.contains(&"-G".into()) {
            return ok_output(format!("controlpath {}\n", self.master.display()).into_bytes());
        }
        assert!(has_pair(&args, "-F", "/dev/null"));
        assert!(has_pair(&args, "-S", self.master.to_str().unwrap()));
        if has_pair(&args, "-O", "check") {
            if matches!(self.mode, Mode::ExpiredMaster) {
                self.master_listener.lock().unwrap().take();
                fs::remove_file(&self.master).unwrap();
            }
        } else if has_pair(&args, "-O", "forward") {
            let pair = args.windows(2).find(|pair| pair[0] == "-L").unwrap();
            let local = pair[1].split_once(':').unwrap().0;
            if !matches!(self.mode, Mode::NoEndpoint) {
                let listener = std::os::unix::net::UnixListener::bind(local).unwrap();
                fs::set_permissions(
                    local,
                    fs::Permissions::from_mode(if matches!(self.mode, Mode::UnsafeLocal) {
                        0o700
                    } else {
                        0o600
                    }),
                )
                .unwrap();
                *self.owned.lock().unwrap() = Some(listener);
            }
        } else if has_pair(&args, "-O", "cancel") {
            if matches!(self.mode, Mode::CancelError) {
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: b"mux cancel failed".to_vec(),
                });
            }
            self.owned.lock().unwrap().take();
        } else {
            panic!("unexpected operation {args:?}");
        }
        ok_output(Vec::new())
    }
}

#[test]
fn successful_open_requires_a_private_endpoint_and_expiry_prevents_allocation() {
    if isolated("successful_open_requires_a_private_endpoint_and_expiry_prevents_allocation") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    let clock = Clock::default();
    for (index, mode) in [Mode::NoEndpoint, Mode::UnsafeLocal, Mode::ExpiredMaster]
        .into_iter()
        .enumerate()
    {
        let master = master.with_file_name(format!("m{index}"));
        let raw = ControlRunner::new(master, mode);
        let mut paths = paths.clone();
        paths.cache = paths.cache.join(format!("case{index}"));
        let files = Arc::new(Files::default());
        let control = MasterForwardControl::new(paths, files.clone(), ssh.clone());
        let plan = control.resolve(&raw, &route, &context(&clock)).unwrap();
        let error = match control.open(&raw, &plan, &identity(), &context(&clock)) {
            Ok(_) => panic!("unsafe/absent endpoint admitted"),
            Err(error) => error,
        };
        if matches!(mode, Mode::ExpiredMaster) {
            assert_eq!(error.disposition, ForwardDisposition::Cleaned);
            assert_eq!(files.allocations.load(Ordering::SeqCst), 0);
        } else {
            assert_eq!(error.disposition, ForwardDisposition::Retained);
            assert_eq!(files.allocations.load(Ordering::SeqCst), 1);
            assert!(files.allocated.lock().unwrap()[0].directory.exists());
        }
    }
}

#[test]
fn cancel_exit_zero_with_live_listener_retains_exact_allocation() {
    if isolated("cancel_exit_zero_with_live_listener_retains_exact_allocation") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    let files = Arc::new(Files::default());
    let control = MasterForwardControl::new(paths, files.clone(), ssh);
    let raw = ControlRunner::new(master, Mode::CancelError);
    let clock = Arc::new(Clock::default());
    let ctx = context(&clock);
    let master = control.resolve(&raw, &route, &ctx).unwrap();
    let mut lease = control.open(&raw, &master, &identity(), &ctx).unwrap();
    let path = lease.local_socket().to_path_buf();
    let before = entry(&path);
    let cleanup = CleanupContext {
        runtime: clock,
        deadline: Duration::from_secs(5),
    };
    assert_eq!(lease.cancel(&raw, &cleanup), ForwardDisposition::Retained);
    assert_eq!(lease.cancel(&raw, &cleanup), ForwardDisposition::Retained);
    assert_eq!(entry(&path), before);
    assert!(std::os::unix::net::UnixStream::connect(&path).is_ok());
    assert_eq!(
        raw.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|request| has_pair(&strings(request), "-O", "cancel"))
            .count(),
        1
    );
    assert_eq!(files.allocations.load(Ordering::SeqCst), 1);
}

#[test]
fn multiplex_off_and_unsafe_master_never_open_or_allocate() {
    if isolated("multiplex_off_and_unsafe_master_never_open_or_allocate") {
        return;
    }
    let (paths, mut ssh, route, master) = fixture();
    let files = Arc::new(FakeForwardPaths::default());
    ssh.multiplex = false;
    let control = MasterForwardControl::new(paths.clone(), files.clone(), ssh.clone());
    let raw = ControlRunner::new(master.clone(), Mode::Success);
    assert!(matches!(
        control.resolve(&raw, &route, &context(&Clock::default())),
        Err(ChannelFailure::Unavailable(ChannelReason::Unsupported))
    ));
    assert!(raw.calls.lock().unwrap().is_empty());
    ssh.multiplex = true;
    let control = MasterForwardControl::new(paths, files.clone(), ssh);
    fs::set_permissions(&master, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        control
            .resolve(&raw, &route, &context(&Clock::default()))
            .is_err()
    );
    assert_eq!(files.allocations(), 0);
    assert!(master.exists());
    assert_eq!(entry(&master).mode, 0o700);
}

struct BoundSocket(std::os::fd::OwnedFd);
impl BoundSocket {
    fn bind(path: &Path) -> Self {
        use std::os::fd::{AsRawFd, FromRawFd};
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        assert!(fd >= 0);
        let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let bytes = path.as_os_str().as_encoded_bytes();
        assert!(bytes.len() < 104);
        for (index, byte) in bytes.iter().enumerate() {
            address.sun_path[index] = *byte as libc::c_char;
        }
        let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1;
        address.sun_len = length as u8;
        assert_eq!(
            unsafe {
                libc::bind(
                    fd.as_raw_fd(),
                    (&raw const address).cast(),
                    length as libc::socklen_t,
                )
            },
            0
        );
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        Self(fd)
    }
    fn listen(&self) {
        use std::os::fd::AsRawFd;
        assert_eq!(unsafe { libc::listen(self.0.as_raw_fd(), 8) }, 0);
    }
}
struct UnacknowledgedRunner {
    base: ControlRunner,
    release: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    producer: Mutex<Option<thread::JoinHandle<BoundSocket>>>,
}
impl ProcessRunner for UnacknowledgedRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let args = strings(request);
        if has_pair(&args, "-O", "forward") {
            self.base.calls.lock().unwrap().push(request.clone());
            let pair = args.windows(2).find(|pair| pair[0] == "-L").unwrap();
            let local = PathBuf::from(pair[1].split_once(':').unwrap().0);
            let (bound_tx, bound_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            *self.release.lock().unwrap() = Some(release_tx);
            *self.producer.lock().unwrap() = Some(thread::spawn(move || {
                let socket = BoundSocket::bind(&local);
                bound_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(30)).unwrap();
                socket.listen();
                socket
            }));
            bound_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            return Err(mac_worker::error::ProcessError::Cancelled.into());
        }
        if has_pair(&args, "-O", "cancel") {
            self.base.calls.lock().unwrap().push(request.clone());
            return Err(WorkerError::Config(
                "fixture: cancel did not reach master".into(),
            ));
        }
        self.base.run(request)
    }
}

#[test]
fn interrupted_open_bind_before_listen_never_uses_refusal_cleanup() {
    if isolated("interrupted_open_bind_before_listen_never_uses_refusal_cleanup") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    let files = Arc::new(Files::default());
    let control = MasterForwardControl::new(paths, files.clone(), ssh);
    let raw = UnacknowledgedRunner {
        base: ControlRunner::new(master, Mode::Success),
        release: Mutex::default(),
        producer: Mutex::default(),
    };
    let clock = Clock::default();
    let master = control.resolve(&raw, &route, &context(&clock)).unwrap();
    let result = control.open(&raw, &master, &identity(), &context(&clock));
    // The producer remains held until refusal and exact residue preservation
    // have been checked; the barrier has a 30-second hang guard.
    let allocation = files.allocated.lock().unwrap().first().cloned();
    let error = match result {
        Ok(_) => panic!("unacknowledged open admitted"),
        Err(error) => error,
    };
    assert_eq!(error.disposition, ForwardDisposition::Retained);
    let allocation = allocation.unwrap();
    let parent = entry(&allocation.directory);
    let socket = entry(&allocation.socket_path);
    assert_eq!(
        std::os::unix::net::UnixStream::connect(&allocation.socket_path)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::ECONNREFUSED)
    );
    assert_eq!(files.cleanups.load(Ordering::SeqCst), 0);
    assert_eq!(files.allocations.load(Ordering::SeqCst), 1);
    assert_eq!(entry(&allocation.directory), parent);
    assert_eq!(entry(&allocation.socket_path), socket);
    raw.release
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .send(())
        .unwrap();
    let _listener = raw.producer.lock().unwrap().take().unwrap().join().unwrap();
    assert!(std::os::unix::net::UnixStream::connect(&allocation.socket_path).is_ok());
    assert_eq!(entry(&allocation.directory), parent);
    assert_eq!(entry(&allocation.socket_path), socket);
    assert_eq!(files.cleanups.load(Ordering::SeqCst), 0);
    assert_eq!(
        raw.base
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|request| has_pair(&strings(request), "-O", "cancel"))
            .count(),
        1
    );
}

// T7c's exclusive test lease: consume the real T4 filesystem implementation
// through T5's bind/listen barrier and the accepted T6 command owner.
#[derive(Default)]
struct ObservedPrivateFiles {
    inner: mac_worker::controller::channel::files::PrivateChannelFiles,
    allocations: Mutex<Vec<ForwardPath>>,
    cleanups: AtomicUsize,
}
impl ForwardPaths for ObservedPrivateFiles {
    fn allocate(&self, paths: &PathLayout) -> Result<ForwardPath, ChannelFailure> {
        let allocation = self.inner.allocate(paths)?;
        self.allocations.lock().unwrap().push(allocation.clone());
        Ok(allocation)
    }
    fn validate_socket(&self, path: &ForwardPath) -> Result<EntryIdentity, ChannelFailure> {
        self.inner.validate_socket(path)
    }
    fn cleanup_if_refused(
        &self,
        path: &ForwardPath,
        socket: Option<EntryIdentity>,
        ctx: &CleanupContext,
    ) -> ForwardDisposition {
        self.cleanups.fetch_add(1, Ordering::SeqCst);
        self.inner.cleanup_if_refused(path, socket, ctx)
    }
}
struct ScopedRaw<R> {
    inner: R,
    identity: SocketIdentity,
    reads: Mutex<Vec<ProcessRequest>>,
    bootstraps: Mutex<Vec<ProcessRequest>>,
}
impl<R: ProcessRunner> ProcessRunner for ScopedRaw<R> {
    fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if let Some(frame) = &process.stdin {
            let request = mac_worker::controller::decode_request(frame).unwrap();
            if request.body().get("controller_socket").is_some() {
                self.bootstraps.lock().unwrap().push(process.clone());
                assert_eq!(
                    request.body()["controller_socket"]["route_sha256"],
                    self.identity.route_sha256.as_str()
                );
                return Ok(mac_worker::controller::channel::testing::result_fixture(
                    &request,
                    serde_json::to_value(SocketIdentityResult::Available(self.identity.clone()))
                        .unwrap(),
                    0,
                ));
            }
            self.reads.lock().unwrap().push(process.clone());
            return Ok(mac_worker::controller::channel::testing::result_fixture(
                &request,
                serde_json::json!({"task_ids":[request.body()["task_id"]],"quiescent":true,"exit_code":0}),
                0,
            ));
        }
        self.inner.run(process)
    }
}
fn scoped_request(route: &ConfiguredRoute) -> ProcessRequest {
    let frame = mac_worker::controller::encode_json_frame(&serde_json::json!({
        "protocol_version":7,"request_id":mac_worker::job::ClientId::generate(),
        "command":"task.wait.poll","body":{"task_id":"018f0f4a6b5c7d8e9f00112233445566"}
    }))
    .unwrap();
    let config =
        mac_worker::config::Config::parse("version=1\n[controller]\nenabled=true\nssh='fixture'\n")
            .unwrap();
    let mut process =
        mac_worker::controller::controller_rpc_ssh_request(&config.controller).unwrap();
    if let Some(config) = &route.ssh_config_file {
        process
            .args
            .splice(0..0, ["-F".into(), config.as_os_str().to_owned()]);
    }
    process.stdin = Some(frame);
    process
}
#[test]
fn scoped_client_unacknowledged_bind_before_listen_retires_across_all_eligibility_advances() {
    if isolated(
        "scoped_client_unacknowledged_bind_before_listen_retires_across_all_eligibility_advances",
    ) {
        return;
    }
    use mac_worker::controller::channel::{
        client::ChannelProcessRunner, identity::StdioIdentitySource, pin::PrivatePinStore,
        testing::ScriptedConnector,
    };
    let (paths, ssh, route, master) = fixture();
    let files = Arc::new(ObservedPrivateFiles::default());
    let mut identity = identity();
    identity.route_sha256 = route.digest().unwrap();
    let raw = ScopedRaw {
        inner: UnacknowledgedRunner {
            base: ControlRunner::new(master, Mode::Success),
            release: Mutex::default(),
            producer: Mutex::default(),
        },
        identity,
        reads: Mutex::default(),
        bootstraps: Mutex::default(),
    };
    let clock = Arc::new(Clock::default());
    let connector = Arc::new(ScriptedConnector::new(vec![]));
    let client = ChannelProcessRunner::new(
        &raw,
        ReadLoopScope::Wait,
        route.clone(),
        paths.clone(),
        ClientDeps {
            identity: Arc::new(StdioIdentitySource::new()),
            pins: Arc::new(PrivatePinStore::new()),
            forwards: Arc::new(MasterForwardControl::new(paths.clone(), files.clone(), ssh)),
            connector: connector.clone(),
            runtime: clock.clone(),
        },
    );
    assert!(client.run(&scoped_request(&route)).is_err());
    let allocation = files.allocations.lock().unwrap()[0].clone();
    let parent = entry(&allocation.directory);
    let socket = entry(&allocation.socket_path);
    assert_eq!(
        std::os::unix::net::UnixStream::connect(&allocation.socket_path)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::ECONNREFUSED)
    );
    assert_eq!(files.cleanups.load(Ordering::SeqCst), 0);
    for _ in 0..64 {
        clock.advance(Duration::from_secs(5));
        assert!(
            client
                .run(&scoped_request(&route))
                .unwrap()
                .status
                .success()
        );
        assert_eq!(files.allocations.lock().unwrap().len(), 1);
        assert_eq!(entry(&allocation.directory), parent);
        assert_eq!(entry(&allocation.socket_path), socket);
    }
    raw.inner
        .release
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .send(())
        .unwrap();
    let _listener = raw
        .inner
        .producer
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .join()
        .unwrap();
    assert!(std::os::unix::net::UnixStream::connect(&allocation.socket_path).is_ok());
    for _ in 0..64 {
        clock.advance(Duration::from_secs(5));
        client.run(&scoped_request(&route)).unwrap();
    }
    assert_eq!(client.close(), ForwardDisposition::Retained);
    assert_eq!(files.allocations.lock().unwrap().len(), 1);
    assert_eq!(files.cleanups.load(Ordering::SeqCst), 0);
    assert_eq!(connector.connections(), 0);
    assert_eq!(raw.reads.lock().unwrap().len(), 128);
    let calls = raw.inner.base.calls.lock().unwrap();
    for op in ["forward", "cancel"] {
        assert_eq!(
            calls
                .iter()
                .filter(|request| has_pair(&strings(request), "-O", op))
                .count(),
            1
        );
    }
    assert_eq!(entry(&allocation.directory), parent);
    assert_eq!(entry(&allocation.socket_path), socket);
}

#[test]
fn scoped_client_exit_zero_cancel_error_preserves_one_real_allocation_after_config_edit() {
    if isolated(
        "scoped_client_exit_zero_cancel_error_preserves_one_real_allocation_after_config_edit",
    ) {
        return;
    }
    use mac_worker::controller::channel::{
        client::ChannelProcessRunner, identity::StdioIdentitySource, pin::PrivatePinStore,
        testing::ScriptedConnector,
    };
    let (paths, ssh, route, master) = fixture();
    let files = Arc::new(ObservedPrivateFiles::default());
    let mut identity = identity();
    identity.route_sha256 = route.digest().unwrap();
    let raw = ScopedRaw {
        inner: ControlRunner::new(master.clone(), Mode::CancelError),
        identity,
        reads: Mutex::default(),
        bootstraps: Mutex::default(),
    };
    let clock = Arc::new(Clock::default());
    let connector = Arc::new(ScriptedConnector::new(vec![Err(
        ChannelFailure::Unavailable(ChannelReason::ForwardLost),
    )]));
    let client = ChannelProcessRunner::new(
        &raw,
        ReadLoopScope::Wait,
        route.clone(),
        paths.clone(),
        ClientDeps {
            identity: Arc::new(StdioIdentitySource::new()),
            pins: Arc::new(PrivatePinStore::new()),
            forwards: Arc::new(MasterForwardControl::new(paths, files.clone(), ssh)),
            connector: connector.clone(),
            runtime: clock.clone(),
        },
    );
    let first = scoped_request(&route);
    assert!(client.run(&first).unwrap().status.success());
    assert_eq!(connector.frames(), [first.stdin.clone().unwrap()]);
    assert_eq!(raw.reads.lock().unwrap()[0].stdin, first.stdin);
    // This has all three config forwarding families; teardown continues to
    // address the captured endpoint/pair after the route file is replaced.
    fs::write(route.ssh_config_file.as_ref().unwrap(), "Host fixture\n ControlPath /different/master\n LocalForward 1 localhost:2\n RemoteForward 3 localhost:4\n DynamicForward 5\n").unwrap();
    for _ in 0..128 {
        clock.advance(Duration::from_secs(5));
        client.run(&scoped_request(&route)).unwrap();
    }
    assert_eq!(client.close(), ForwardDisposition::Retained);
    assert_eq!(files.allocations.lock().unwrap().len(), 1);
    assert_eq!(connector.connections(), 1);
    assert_eq!(connector.closes(), 1);
    let calls = raw.inner.calls.lock().unwrap();
    for op in ["check", "forward", "cancel"] {
        let matching: Vec<_> = calls
            .iter()
            .filter(|request| has_pair(&strings(request), "-O", op))
            .collect();
        assert_eq!(matching.len(), 1);
        let args = strings(matching[0]);
        assert!(has_pair(&args, "-S", master.to_str().unwrap()));
        assert!(has_pair(&args, "-F", "/dev/null"));
        assert!(
            !args
                .iter()
                .any(|arg| arg.contains("localhost") || arg.contains("/different/master"))
        );
    }
    assert_eq!(files.cleanups.load(Ordering::SeqCst), 1);
    assert!(
        std::os::unix::net::UnixStream::connect(&files.allocations.lock().unwrap()[0].socket_path)
            .is_ok()
    );
}

fn scoped_offline_mux(master_lost: bool) {
    use mac_worker::controller::channel::{
        client::ChannelProcessRunner,
        identity::StdioIdentitySource,
        pin::PrivatePinStore,
        testing::{ScriptedConnector, result_fixture},
    };
    let (paths, ssh, route, _) = fixture();
    let files = Arc::new(ObservedPrivateFiles::default());
    let mut identity = identity();
    identity.route_sha256 = route.digest().unwrap();
    let raw = ScopedRaw {
        inner: LocalMuxRunner::default(),
        identity,
        reads: Mutex::default(),
        bootstraps: Mutex::default(),
    };
    let first = scoped_request(&route);
    let request = mac_worker::controller::decode_request(first.stdin.as_ref().unwrap()).unwrap();
    let connector = Arc::new(ScriptedConnector::new(vec![
        Ok(result_fixture(
            &request,
            serde_json::json!({"task_ids":[request.body()["task_id"]],"quiescent":true,"exit_code":0}),
            0,
        )),
        Err(ChannelFailure::Unavailable(ChannelReason::ForwardLost)),
    ]));
    let clock = Arc::new(Clock::default());
    let client = ChannelProcessRunner::new(
        &raw,
        ReadLoopScope::Wait,
        route.clone(),
        paths.clone(),
        ClientDeps {
            identity: Arc::new(StdioIdentitySource::new()),
            pins: Arc::new(PrivatePinStore::new()),
            forwards: Arc::new(MasterForwardControl::new(paths, files.clone(), ssh)),
            connector: connector.clone(),
            runtime: clock.clone(),
        },
    );
    assert!(client.run(&first).unwrap().status.success());
    assert!(raw.reads.lock().unwrap().is_empty());
    let master = raw.inner.mux.lock().unwrap().as_ref().unwrap().path.clone();
    let binding = entry(&master);
    let bootstrap = strings(&raw.bootstraps.lock().unwrap()[0]);
    assert!(has_pair(
        &bootstrap,
        "-F",
        route.ssh_config_file.as_ref().unwrap().to_str().unwrap()
    ));
    assert!(has_pair(&bootstrap, "-S", master.to_str().unwrap()));
    if master_lost {
        // Fixture-owned mux exit, with the stale pathname/binding retained.
        // Production teardown must keep addressing this captured endpoint.
        drop(raw.inner.mux.lock().unwrap().take().unwrap());
        assert_eq!(
            std::os::unix::net::UnixStream::connect(&master)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ECONNREFUSED)
        );
    } else {
        fs::write(route.ssh_config_file.as_ref().unwrap(),"Host fixture\n HostName changed.invalid\n ControlPath /private/tmp/foreign-master\n LocalForward 32001 127.0.0.1:32002\n RemoteForward 32003 127.0.0.1:32004\n DynamicForward 32005\n").unwrap();
    }
    let second = scoped_request(&route);
    assert!(client.run(&second).unwrap().status.success());
    assert_eq!(raw.reads.lock().unwrap().len(), 1);
    assert_eq!(raw.reads.lock().unwrap()[0].stdin, second.stdin);
    assert_eq!(
        connector.frames(),
        [first.stdin.unwrap(), second.stdin.unwrap()]
    );
    assert_eq!(connector.connections(), 1);
    assert_eq!(connector.closes(), 1);
    assert_eq!(client.close(), ForwardDisposition::Cleaned);
    assert_eq!(files.allocations.lock().unwrap().len(), 1);
    assert_eq!(files.cleanups.load(Ordering::SeqCst), 1);
    assert!(!files.allocations.lock().unwrap()[0].directory.exists());
    assert_eq!(
        entry(&master),
        binding,
        "scoped cleanup never deletes the shared master"
    );
    let calls = raw.inner.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .filter(|call| strings(call).contains(&"-G".into()))
            .count(),
        1
    );
    for operation in ["check", "forward", "cancel"] {
        let matching: Vec<_> = calls
            .iter()
            .filter(|call| has_pair(&strings(call), "-O", operation))
            .collect();
        assert_eq!(matching.len(), 1);
        let args = strings(matching[0]);
        assert!(has_pair(&args, "-S", master.to_str().unwrap()));
        assert!(has_pair(&args, "-F", "/dev/null"));
        assert!(!args.contains(&"-N".into()));
        assert!(
            !args
                .iter()
                .any(|arg| arg.contains("foreign-master") || arg.contains("3200"))
        );
    }
    if let Some(mux) = raw.inner.mux.lock().unwrap().as_ref() {
        assert_eq!(mux.messages.lock().unwrap().len(), 2);
        assert!(mux.forward.lock().unwrap().is_none());
    }
}
#[test]
fn scoped_client_real_offline_mux_config_forwards_and_edit_use_captured_master() {
    if isolated("scoped_client_real_offline_mux_config_forwards_and_edit_use_captured_master") {
        return;
    }
    scoped_offline_mux(false);
}
#[test]
fn scoped_client_real_offline_mux_master_loss_uses_one_fallback_and_proven_cleanup() {
    if isolated("scoped_client_real_offline_mux_master_loss_uses_one_fallback_and_proven_cleanup") {
        return;
    }
    scoped_offline_mux(true);
}

struct GateRunner {
    base: ControlRunner,
    stage: &'static str,
    reached: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    expire: Option<Arc<Clock>>,
}
impl ProcessRunner for GateRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.base.run(request)
    }
    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        let args = strings(request);
        let selected = if self.stage == "resolution" {
            args.contains(&"-G".into())
        } else {
            has_pair(&args, "-O", self.stage)
        };
        if !selected {
            return self.base.run_interruptible(request, stop);
        }
        self.base.calls.lock().unwrap().push(request.clone());
        if let Some(clock) = &self.expire {
            clock.advance(Duration::from_secs(5));
        }
        if let Some(reached) = self.reached.lock().unwrap().take() {
            reached.send(()).unwrap();
        }
        let hang_guard = std::time::Instant::now();
        while !stop() {
            assert!(
                hang_guard.elapsed() < Duration::from_secs(30),
                "predicate not polled while operation is live"
            );
            thread::yield_now();
        }
        Err(mac_worker::error::ProcessError::Cancelled.into())
    }
}

#[test]
fn borrowed_predicate_interrupts_resolution_check_and_open() {
    if isolated("borrowed_predicate_interrupts_resolution_check_and_open") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    for (index, stage) in ["resolution", "check", "forward"].into_iter().enumerate() {
        let mut paths = paths.clone();
        paths.cache = paths.cache.join(format!("gate{index}"));
        let files = Arc::new(Files::default());
        let control = MasterForwardControl::new(paths, files.clone(), ssh.clone());
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (cancel_tx, cancel_rx) = std::sync::mpsc::channel();
        let raw = GateRunner {
            base: ControlRunner::new(master.with_file_name(format!("g{index}")), Mode::Success),
            stage,
            reached: Mutex::new(Some(reached_tx)),
            expire: None,
        };
        let signal = thread::spawn(move || {
            reached_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            cancel_tx.send(()).unwrap();
        });
        // Rc/Cell + Receiver prove this predicate need not be Send or Sync.
        let stopped = std::rc::Rc::new(std::cell::Cell::new(false));
        let should_stop = || {
            if cancel_rx.try_recv().is_ok() {
                stopped.set(true);
            }
            stopped.get()
        };
        let clock = Clock::default();
        let ctx = ClientContext {
            runtime: &clock,
            deadline: Duration::from_secs(30),
            should_stop: &should_stop,
        };
        if stage == "resolution" {
            assert!(matches!(
                control.resolve(&raw, &route, &ctx),
                Err(ChannelFailure::Unavailable(ChannelReason::Cancelled))
            ));
            assert_eq!(files.allocations.load(Ordering::SeqCst), 0);
        } else {
            let master = control.resolve(&raw, &route, &context(&clock)).unwrap();
            let error = match control.open(&raw, &master, &identity(), &ctx) {
                Ok(_) => panic!("cancelled open admitted"),
                Err(error) => error,
            };
            assert!(matches!(
                error.failure,
                ChannelFailure::Unavailable(ChannelReason::Cancelled)
            ));
            if stage == "forward" {
                assert_eq!(error.disposition, ForwardDisposition::Retained);
                assert_eq!(files.allocations.load(Ordering::SeqCst), 1);
                assert_eq!(files.cleanups.load(Ordering::SeqCst), 0);
            } else {
                assert_eq!(error.disposition, ForwardDisposition::Cleaned);
                assert_eq!(files.allocations.load(Ordering::SeqCst), 0);
            }
        }
        assert!(stopped.get());
        assert!(!clock.cancelled());
        signal.join().unwrap();
    }
}

#[test]
fn resolution_guard_expiry_is_timeout_without_foreground_cancellation() {
    if isolated("resolution_guard_expiry_is_timeout_without_foreground_cancellation") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    let files = Arc::new(Files::default());
    let control = MasterForwardControl::new(paths, files.clone(), ssh);
    let clock = Arc::new(Clock::default());
    let raw = GateRunner {
        base: ControlRunner::new(master, Mode::Success),
        stage: "resolution",
        reached: Mutex::default(),
        expire: Some(clock.clone()),
    };
    let ctx = ClientContext {
        runtime: clock.as_ref(),
        deadline: Duration::from_secs(2),
        should_stop: &|| false,
    };
    assert!(matches!(
        control.resolve(&raw, &route, &ctx),
        Err(ChannelFailure::Unavailable(ChannelReason::Timeout))
    ));
    let calls = raw.base.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].policy.deadline, Duration::from_secs(2));
    assert!(!clock.cancelled());
    assert_eq!(files.allocations.load(Ordering::SeqCst), 0);
}

#[test]
fn cleanup_ignores_foreground_cancel_but_retains_on_clock_expiry() {
    if isolated("cleanup_ignores_foreground_cancel_but_retains_on_clock_expiry") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    for expired in [false, true] {
        let mut paths = paths.clone();
        paths.cache = paths.cache.join(format!("cleanup{expired}"));
        let files = Arc::new(Files::default());
        let control = MasterForwardControl::new(paths, files.clone(), ssh.clone());
        let raw = ControlRunner::new(
            master.with_file_name(format!("cleanup{expired}")),
            Mode::Success,
        );
        let clock = Clock::default();
        let ctx = context(&clock);
        let master = control.resolve(&raw, &route, &ctx).unwrap();
        let mut lease = control.open(&raw, &master, &identity(), &ctx).unwrap();
        let local = lease.local_socket().to_path_buf();
        let cleanup_clock = Arc::new(ManualRuntime::default());
        cleanup_clock.cancel();
        let cleanup = CleanupContext {
            runtime: cleanup_clock,
            deadline: if expired {
                Duration::ZERO
            } else {
                Duration::from_secs(5)
            },
        };
        let disposition = lease.cancel(&raw, &cleanup);
        assert_eq!(
            disposition,
            if expired {
                ForwardDisposition::Retained
            } else {
                ForwardDisposition::Cleaned
            }
        );
        assert_eq!(local.exists(), expired);
        assert_eq!(files.cleanups.load(Ordering::SeqCst), usize::from(!expired));
        assert!(lease.verify().is_err());
    }
}

#[test]
fn swapped_socket_or_parent_is_preserved_without_cancel() {
    if isolated("swapped_socket_or_parent_is_preserved_without_cancel") {
        return;
    }
    let (paths, ssh, route, master) = fixture();
    for swap_parent in [false, true] {
        let mut paths = paths.clone();
        paths.cache = paths.cache.join(format!("swap{swap_parent}"));
        let files = Arc::new(Files::default());
        let control = MasterForwardControl::new(paths, files.clone(), ssh.clone());
        let raw = ControlRunner::new(
            master.with_file_name(format!("swap{swap_parent}")),
            Mode::Success,
        );
        let clock = Arc::new(Clock::default());
        let ctx = context(&clock);
        let master = control.resolve(&raw, &route, &ctx).unwrap();
        let mut lease = control.open(&raw, &master, &identity(), &ctx).unwrap();
        let local = lease.local_socket().to_path_buf();
        if swap_parent {
            let parent = local.parent().unwrap();
            fs::rename(parent, parent.with_extension("held")).unwrap();
            fs::create_dir(parent).unwrap();
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).unwrap();
        } else {
            fs::rename(&local, local.with_extension("held")).unwrap();
        }
        let _replacement = std::os::unix::net::UnixListener::bind(&local).unwrap();
        fs::set_permissions(&local, fs::Permissions::from_mode(0o600)).unwrap();
        let replacement = entry(&local);
        assert!(lease.verify().is_err());
        let cleanup = CleanupContext {
            runtime: clock,
            deadline: Duration::from_secs(5),
        };
        assert_eq!(lease.cancel(&raw, &cleanup), ForwardDisposition::Retained);
        assert_eq!(entry(&local), replacement);
        assert!(std::os::unix::net::UnixStream::connect(&local).is_ok());
        assert_eq!(files.cleanups.load(Ordering::SeqCst), 0);
        assert!(!raw.calls.lock().unwrap().iter().any(|request| has_pair(
            &strings(request),
            "-O",
            "cancel"
        )));
    }
}

// Preserved T1 interface gate.
#[test]
fn gate_unacknowledged_forward_failure_retains_allocation() {
    let failure =
        ForwardOpenFailure::retained(ChannelFailure::Unavailable(ChannelReason::Cancelled));
    assert_eq!(failure.disposition, ForwardDisposition::Retained);
}
