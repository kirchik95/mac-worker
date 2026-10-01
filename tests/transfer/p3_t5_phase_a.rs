use std::{
    fs,
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        process::ExitStatusExt,
    },
    path::PathBuf,
    process::{Command, ExitStatus},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use mac_worker::{
    config::SshConfig,
    controller::channel::{contracts::*, forward::MasterForwardControl},
    error::WorkerError,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
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

#[derive(Default)]
struct Clock(AtomicU64);
impl ChannelRuntime for Clock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::SeqCst))
    }
    fn cancelled(&self) -> bool {
        false
    }
}

#[derive(Default)]
struct Files {
    allocations: AtomicUsize,
}
impl ForwardPaths for Files {
    fn allocate(&self, _: &PathLayout) -> Result<ForwardPath, ChannelFailure> {
        self.allocations.fetch_add(1, Ordering::SeqCst);
        Err(ChannelFailure::Unavailable(ChannelReason::Busy))
    }
    fn validate_socket(&self, _: &ForwardPath) -> Result<EntryIdentity, ChannelFailure> {
        Err(ChannelFailure::Unavailable(ChannelReason::UnsafePath))
    }
    fn cleanup_if_refused(
        &self,
        _: &ForwardPath,
        _: Option<EntryIdentity>,
        _: &CleanupContext,
    ) -> ForwardDisposition {
        ForwardDisposition::Retained
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
    let files = Arc::new(Files::default());
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
    assert_eq!(files.allocations.load(Ordering::SeqCst), 0);
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
    let files = Arc::new(Files::default());
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
    assert_eq!(files.allocations.load(Ordering::SeqCst), 0);
}
