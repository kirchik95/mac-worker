use mac_worker::{
    config::Config,
    controller::provision::{
        ResolvedSsh, authorize_controller_key, plan_inventory, trusted_host_keys,
        write_controller_config,
    },
};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::{PermissionsExt, symlink},
};

fn resolved(host: &str, port: u16, jump: Option<&str>) -> ResolvedSsh {
    ResolvedSsh::parse(&format!("user kirchik\nhostname {host}\nport {port}\nproxyjump {}\nuserknownhostsfile /tmp/trusted\n", jump.unwrap_or("none"))).unwrap()
}
fn inventory() -> Config {
    Config::parse("version = 1\n[[workers]]\nname='mini-1'\nssh='mac1'\nslots=2\n[[workers]]\nname='mini-2'\nssh='mac2'\nslots=1\n").unwrap()
}
fn key() -> String {
    use base64::Engine;
    let mut blob = Vec::new();
    blob.extend(11u32.to_be_bytes());
    blob.extend(b"ssh-ed25519");
    blob.extend(32u32.to_be_bytes());
    blob.extend([7u8; 32]);
    format!(
        "ssh-ed25519 {}",
        base64::engine::general_purpose::STANDARD.encode(blob)
    )
}
#[test]
fn destinations_use_loopback_drop_controller_jump_preserve_port_and_allow_override() {
    let cfg = inventory();
    let ctrl = resolved("192.168.1.11", 22, None);
    let entries = BTreeMap::from([
        ("mini-1".into(), ctrl.clone()),
        ("mini-2".into(), resolved("10.0.0.2", 2222, Some("mac1"))),
    ]);
    let plan = plan_inventory(&cfg, &ctrl, "mac1", &entries).unwrap();
    assert_eq!(plan[0].target.hostname, "127.0.0.1");
    assert_eq!(plan[1].target.hostname, "10.0.0.2");
    assert_eq!(plan[1].target.port, 2222);
    assert_eq!(plan[1].target.proxy_jump, None);
    let mut overrides = entries;
    overrides.insert("mini-2".into(), resolved("10.9.0.2", 22, None));
    assert_eq!(
        plan_inventory(&cfg, &ctrl, "mac1", &overrides).unwrap()[1]
            .target
            .hostname,
        "10.9.0.2"
    );
}
#[test]
fn authorization_is_idempotent_preserves_other_lines_and_repairs_mode() {
    let temp = tempfile::tempdir().unwrap();
    let ssh = temp.path().join(".ssh");
    fs::create_dir(&ssh).unwrap();
    let path = ssh.join("authorized_keys");
    let old = b"# keep\ncommand=\"git-shell\",no-port-forwarding ssh-ed25519 existing origin\n";
    fs::write(&path, old).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(authorize_controller_key(temp.path(), &key()).unwrap());
    let once = fs::read(&path).unwrap();
    assert!(once.starts_with(old));
    assert!(String::from_utf8_lossy(&once).contains(" mac-worker-controller\n"));
    assert!(!authorize_controller_key(temp.path(), &key()).unwrap());
    assert_eq!(fs::read(&path).unwrap(), once);
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}
#[test]
fn authorization_rejects_injected_options_malformed_keys_and_symlinks() {
    let temp = tempfile::tempdir().unwrap();
    for bad in [
        format!("command=\"evil\" {}", key()),
        format!("{}\nevil", key()),
        "ssh-ed25519 YWJj".into(),
    ] {
        assert!(authorize_controller_key(temp.path(), &bad).is_err());
    }
    let outside = temp.path().join("outside");
    fs::write(&outside, b"preserve").unwrap();
    fs::create_dir_all(temp.path().join(".ssh")).unwrap();
    symlink(&outside, temp.path().join(".ssh/authorized_keys")).unwrap();
    assert!(authorize_controller_key(temp.path(), &key()).is_err());
    assert_eq!(fs::read(&outside).unwrap(), b"preserve");
}
#[test]
fn trusted_host_seeding_requires_existing_trust_and_rebinds_hashed_hosts() {
    assert!(trusted_host_keys("", "127.0.0.1", 22).is_err());
    let found = format!("# Host found\n|1|hash|hash {}\n", key());
    assert_eq!(
        trusted_host_keys(&found, "127.0.0.1", 2222).unwrap(),
        format!("[127.0.0.1]:2222 {}\n", key())
    );
    assert!(trusted_host_keys(&format!("@revoked host {}\n", key()), "127.0.0.1", 22).is_err());
}
#[test]
fn controller_config_is_idempotent_and_conflicts_need_force_with_diff() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("config.toml");
    let a = "version = 1\n[[workers]]\nname='mini-1'\nssh='loopback'\nslots=1\n";
    let b = a.replace("slots=1", "slots=2");
    assert!(write_controller_config(&path, a, false).unwrap().changed);
    assert!(!write_controller_config(&path, a, false).unwrap().changed);
    let refused = write_controller_config(&path, &b, false).unwrap();
    assert!(!refused.changed);
    assert!(refused.conflict);
    assert!(refused.diff.as_deref().unwrap().contains("-slots=1"));
    assert!(refused.diff.as_deref().unwrap().contains("+slots=2"));
    assert_eq!(fs::read_to_string(&path).unwrap(), a);
    assert!(write_controller_config(&path, &b, true).unwrap().changed);
    assert_eq!(fs::read_to_string(path).unwrap(), b);
}

#[test]
fn generated_ssh_settings_use_only_dedicated_key_and_strict_pinned_hosts() {
    use mac_worker::controller::provision::write_ssh_settings;
    let temp = tempfile::tempdir().unwrap();
    let ssh = temp.path().join(".ssh");
    fs::create_dir(&ssh).unwrap();
    let original = "Host *\n  IdentityFile ~/.ssh/id_ed25519\n  ControlMaster auto\n  ControlPath ~/.ssh/cm-%r@%h:%p\n";
    fs::write(ssh.join("config"), original).unwrap();
    fs::set_permissions(ssh.join("config"), fs::Permissions::from_mode(0o644)).unwrap();
    let cfg = inventory();
    let controller = resolved("192.168.1.11", 22, None);
    let targets = BTreeMap::from([
        ("mini-1".into(), controller.clone()),
        ("mini-2".into(), resolved("10.0.0.2", 2222, Some("mac1"))),
    ]);
    let plan = plan_inventory(&cfg, &controller, "mac1", &targets).unwrap();
    let trust = format!("192.168.1.11 {}\n10.0.0.2 {}\n", key(), key());
    write_ssh_settings(temp.path(), &plan, &trust).unwrap();
    let once = fs::read(ssh.join("config")).unwrap();
    assert_eq!(once, original.as_bytes());
    assert_eq!(
        fs::metadata(ssh.join("config"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o644
    );
    write_ssh_settings(temp.path(), &plan, &trust).unwrap();
    assert_eq!(fs::read(ssh.join("config")).unwrap(), once);
    let generated = fs::read_to_string(ssh.join("mac-worker-controller.conf")).unwrap();
    assert!(generated.contains("Host mac-worker-controller-mini-1\n  HostName 127.0.0.1\n"));
    assert!(generated.contains("  Port 2222\n"));
    assert!(generated.contains("  IdentityFile ~/.ssh/mac-worker-controller_ed25519\n"));
    assert!(generated.contains("  StrictHostKeyChecking yes\n"));
    assert!(generated.contains("  NoHostAuthenticationForLocalhost no\n"));
    assert!(generated.contains("Host *\n  ControlMaster no\n  ControlPath none\n"));
    assert!(generated.contains("  HostKeyAlias 192.168.1.11\n"));
    assert!(!generated.contains("accept-new"));
    assert!(!generated.contains("ProxyJump mac1"));
}

#[test]
fn generated_ssh_settings_effective_config_is_isolated_offline() {
    use mac_worker::controller::provision::write_ssh_settings;
    if !std::path::Path::new("/usr/bin/ssh").exists() {
        eprintln!("skipping effective SSH config: /usr/bin/ssh is missing");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let ssh = temp.path().join(".ssh");
    fs::create_dir(&ssh).unwrap();
    fs::write(ssh.join("config"), "Host *\n  IdentityFile ~/.ssh/id_ed25519\n  ControlMaster auto\n  ControlPath ~/.ssh/cm-%r@%h:%p\n").unwrap();
    let controller = resolved("192.168.1.11", 22, None);
    let targets = BTreeMap::from([
        ("mini-1".into(), controller.clone()),
        ("mini-2".into(), resolved("10.0.0.2", 2222, None)),
    ]);
    let plan = plan_inventory(&inventory(), &controller, "mac1", &targets).unwrap();
    let trust = format!("192.168.1.11 {}\n10.0.0.2 {}\n", key(), key());
    write_ssh_settings(temp.path(), &plan, &trust).unwrap();
    for worker in &plan {
        let output = std::process::Command::new("/usr/bin/ssh")
            .arg("-G")
            .arg("-F")
            .arg(ssh.join("mac-worker-controller.conf"))
            .arg(&worker.alias)
            .env("HOME", temp.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let values: BTreeMap<_, _> = stdout
            .lines()
            .filter_map(|line| line.split_once(' '))
            .collect();
        let identities: Vec<_> = stdout
            .lines()
            .filter_map(|line| line.strip_prefix("identityfile "))
            .collect();
        assert_eq!(identities.len(), 1, "{stdout}");
        assert!(identities[0].ends_with("/.ssh/mac-worker-controller_ed25519"));
        assert_eq!(values["identitiesonly"], "yes");
        assert_eq!(values["identityagent"], "none");
        // Some OpenSSH versions omit disabled optional paths from `ssh -G`.
        assert_eq!(values.get("controlpath").copied().unwrap_or("none"), "none");
        assert_eq!(values["controlmaster"], "false");
        assert_eq!(values["stricthostkeychecking"], "true");
        assert_eq!(
            values["hostkeyalias"],
            worker.target.host_key_alias.as_deref().unwrap()
        );
        assert!(
            values["userknownhostsfile"].ends_with(&format!("/.ssh/{}.known_hosts", worker.alias))
        );
    }
}

#[test]
fn controller_key_generation_never_overwrites_an_existing_private_key() {
    use mac_worker::{
        controller::provision::ensure_controller_key,
        error::WorkerError,
        process::{ProcessRequest, ProcessResult, ProcessRunner},
    };
    struct NoProcess;
    impl ProcessRunner for NoProcess {
        fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            panic!("existing key must never be regenerated")
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let ssh = temp.path().join(".ssh");
    fs::create_dir(&ssh).unwrap();
    fs::write(
        ssh.join("mac-worker-controller_ed25519"),
        b"private-existing",
    )
    .unwrap();
    fs::write(
        ssh.join("mac-worker-controller_ed25519.pub"),
        format!("{} mac-worker-controller\n", key()),
    )
    .unwrap();
    assert_eq!(
        ensure_controller_key(temp.path(), &NoProcess).unwrap(),
        key()
    );
    assert_eq!(
        fs::read(ssh.join("mac-worker-controller_ed25519")).unwrap(),
        b"private-existing"
    );
}

#[test]
fn controller_key_generation_publishes_once_and_recovers_missing_public_half() {
    use mac_worker::{
        controller::provision::ensure_controller_key,
        error::WorkerError,
        process::{ProcessRequest, ProcessResult, ProcessRunner},
    };
    use std::{os::unix::process::ExitStatusExt, process::ExitStatus, sync::Mutex};
    struct Keygen {
        calls: Mutex<Vec<Vec<String>>>,
    }
    impl ProcessRunner for Keygen {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            assert_eq!(request.program, "/usr/bin/ssh-keygen");
            let args = request
                .args
                .iter()
                .map(|s| s.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            self.calls.lock().unwrap().push(args.clone());
            let mut stdout = vec![];
            if args[0] == "-y" {
                stdout = format!("{}\n", key()).into_bytes();
            } else {
                let path = std::path::PathBuf::from(args.last().unwrap());
                assert!(!path.exists());
                fs::write(&path, b"new-fixture-private").unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
                fs::write(
                    path.with_extension("pub"),
                    format!("{} mac-worker-controller\n", key()),
                )
                .unwrap();
            }
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout,
                stderr: vec![],
            })
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let fake = Keygen {
        calls: Mutex::new(vec![]),
    };
    assert_eq!(ensure_controller_key(temp.path(), &fake).unwrap(), key());
    assert_eq!(ensure_controller_key(temp.path(), &fake).unwrap(), key());
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
    let private = temp.path().join(".ssh/mac-worker-controller_ed25519");
    assert_eq!(fs::read(&private).unwrap(), b"new-fixture-private");
    fs::remove_file(temp.path().join(".ssh/mac-worker-controller_ed25519.pub")).unwrap();
    assert_eq!(ensure_controller_key(temp.path(), &fake).unwrap(), key());
    assert_eq!(fake.calls.lock().unwrap().len(), 2);
    assert_eq!(fs::read(private).unwrap(), b"new-fixture-private");
}
