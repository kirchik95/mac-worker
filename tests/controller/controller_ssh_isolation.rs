use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    config::Config,
    controller::provision::{ResolvedSsh, plan_inventory},
    error::WorkerError,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{ControllerConfigureRequest, PROTOCOL_VERSION},
    run_with_stdio_in_context,
    transport::SshTransport,
};
use std::{
    collections::BTreeMap, fs, io::Cursor, os::unix::process::ExitStatusExt, process::ExitStatus,
    sync::Mutex,
};

#[derive(Default)]
struct ProbeRunner(Mutex<Vec<ProcessRequest>>);
impl ProcessRunner for ProbeRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, "/usr/bin/ssh");
        assert_eq!(
            request.args.last().unwrap(),
            "~/.local/bin/worker host probe"
        );
        self.0.lock().unwrap().push(request.clone());
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: serde_json::to_vec(&serde_json::json!({
                "protocol_version": PROTOCOL_VERSION, "hostname": "test.local", "arch": "arm64",
                "os_version": "26.2", "free_disk_bytes": 536870912_u64,
                "memory_pressure": "normal", "swap_used_bytes": 0, "capabilities": [],
            }))
            .unwrap(),
            stderr: vec![],
        })
    }
}

#[test]
fn controller_configure_records_host_config_file_and_probe_installs_it_process_wide() {
    use base64::Engine;
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let path = home.join("config.toml");
    let mut config =
        Config::parse("version=1\n[[workers]]\nname='mini-1'\nssh='mac1'\nslots=1\n").unwrap();
    let controller =
        ResolvedSsh::parse("user owner\nhostname mini.local\nport 22\nproxyjump none\n").unwrap();
    let planned = plan_inventory(
        &config,
        &controller,
        "mac1",
        &BTreeMap::from([("mini-1".into(), controller.clone())]),
    )
    .unwrap();
    config.workers[0].ssh = planned[0].alias.clone();
    let mut blob = Vec::new();
    blob.extend(11u32.to_be_bytes());
    blob.extend(b"ssh-ed25519");
    blob.extend(32u32.to_be_bytes());
    blob.extend([7u8; 32]);
    let request = ControllerConfigureRequest {
        config_toml: toml::to_string_pretty(&config).unwrap(),
        workers: planned,
        known_hosts: format!(
            "mini.local ssh-ed25519 {}\n",
            base64::engine::general_purpose::STANDARD.encode(blob)
        ),
        force: false,
        include_details: false,
    };
    let runtime = RuntimeContext::isolated(BTreeMap::new(), home.clone(), home.clone());
    let runner = ProbeRunner::default();
    let mut stdout = vec![];
    let mut stderr = vec![];
    let cli = |operation| {
        Cli::try_parse_from([
            "worker",
            "--config",
            path.to_str().unwrap(),
            "host",
            operation,
        ])
        .unwrap()
    };
    assert_eq!(
        run_with_stdio_in_context(
            cli("controller-configure"),
            &runner,
            &runtime,
            &mut Cursor::new(serde_json::to_vec(&request).unwrap()),
            &mut stdout,
            &mut stderr
        ),
        0,
        "{}",
        String::from_utf8_lossy(&stdout)
    );
    let saved: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    let managed = home.join(".ssh/mac-worker-controller.conf");
    assert_eq!(
        saved["ssh"]
            .get("config_file")
            .and_then(toml::Value::as_str),
        managed.to_str()
    );
    assert!(!home.join(".ssh/config").exists());
    stdout.clear();
    assert_eq!(
        run_with_stdio_in_context(
            cli("controller-probe"),
            &runner,
            &runtime,
            &mut Cursor::new(b"{}"),
            &mut stdout,
            &mut stderr
        ),
        0,
        "{}",
        String::from_utf8_lossy(&stdout)
    );
    let requests = runner.0.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .args
            .windows(2)
            .any(|pair| pair[0] == "-F" && pair[1] == managed)
    );
    drop(requests);
    // Loaded settings reach threads too, as in controller dispatch/probes.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let command = SshTransport::new(&runner)
                    .git_ssh_command(&config.workers[0])
                    .unwrap();
                assert!(command.contains(" -F "), "{command}");
                assert!(command.contains(managed.to_str().unwrap()), "{command}");
            })
            .join()
            .unwrap()
    });
    struct OriginRunner;
    impl ProcessRunner for OriginRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            assert_eq!(request.program, "/usr/bin/git");
            let ssh = request
                .environment
                .iter()
                .find(|(key, _)| key == "GIT_SSH_COMMAND")
                .unwrap();
            assert_eq!(
                ssh.1,
                "/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes"
            );
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: b"1111111111111111111111111111111111111111\trefs/heads/main\n".to_vec(),
                stderr: vec![],
            })
        }
    }
    mac_worker::git_transport::GitTransport::new(&OriginRunner)
        .preflight_origin(
            "git@example.test:owner/project.git",
            &"1111111111111111111111111111111111111111".parse().unwrap(),
        )
        .unwrap();
    // Loading a laptop inventory replaces process-wide SSH settings completely.
    fs::write(&path, toml::to_string_pretty(&config).unwrap()).unwrap();
    Config::load(&path).unwrap();
    assert!(
        !SshTransport::new(&runner)
            .git_ssh_command(&config.workers[0])
            .unwrap()
            .contains(" -F ")
    );
}
