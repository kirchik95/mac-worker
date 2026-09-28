use base64::Engine;
use mac_worker::{
    controller::{decode_frame, encode_json_frame, init::InitRequest},
    error::WorkerError,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
};
use serde_json::{Value, json};
use std::{fs, os::unix::process::ExitStatusExt, process::ExitStatus, sync::Mutex};
fn pending_root(home: &std::path::Path) -> std::path::PathBuf {
    home.join(".local/state/mac-worker-controller")
}
fn initialize(
    runner: &dyn ProcessRunner,
    config: &std::path::Path,
    home: &std::path::Path,
    digest: &str,
    request: InitRequest,
) -> Result<mac_worker::controller::init::InitReport, WorkerError> {
    mac_worker::controller::init::initialize(
        runner,
        config,
        &pending_root(home),
        home,
        digest,
        request,
    )
}
fn initialize_with_wait(
    runner: &dyn ProcessRunner,
    config: &std::path::Path,
    home: &std::path::Path,
    digest: &str,
    request: InitRequest,
    wait: &dyn Fn(std::time::Duration),
) -> Result<mac_worker::controller::init::InitReport, WorkerError> {
    mac_worker::controller::init::initialize_with_wait(
        runner,
        config,
        &pending_root(home),
        home,
        digest,
        request,
        wait,
    )
}
fn disable(
    runner: &dyn ProcessRunner,
    config: &std::path::Path,
) -> Result<mac_worker::controller::service::ServiceStatus, WorkerError> {
    mac_worker::controller::init::disable(
        runner,
        config,
        &pending_root(config.parent().unwrap()),
        None,
    )
}

fn key() -> String {
    let mut b = Vec::new();
    b.extend(11u32.to_be_bytes());
    b.extend(b"ssh-ed25519");
    b.extend(32u32.to_be_bytes());
    b.extend([7u8; 32]);
    format!(
        "ssh-ed25519 {}",
        base64::engine::general_purpose::STANDARD.encode(b)
    )
}
fn result(value: Value) -> ProcessResult {
    let stdout = if value.get("workers").is_some() {
        serde_json::to_vec(
            &serde_json::from_value::<mac_worker::protocol::WorkersReport>(value).unwrap(),
        )
        .unwrap()
    } else if value.get("conflict").is_some() {
        serde_json::to_vec(
            &serde_json::from_value::<mac_worker::controller::init::ConfiguredHost>(value).unwrap(),
        )
        .unwrap()
    } else if value.get("label").is_some() {
        serde_json::to_vec(
            &serde_json::from_value::<mac_worker::controller::service::ServiceStatus>(value)
                .unwrap(),
        )
        .unwrap()
    } else {
        serde_json::to_vec(&value).unwrap()
    };
    ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: vec![],
    }
}
struct Fake {
    calls: Mutex<Vec<String>>,
    digest: String,
    trust: bool,
    conflict: bool,
    service_actions: Mutex<Vec<String>>,
    host_alias: bool,
    jump: String,
    /// ProxyJump reported for the `controller-route` hop itself.
    hop_jump: String,
    controller_port: u16,
    pending_health: Mutex<usize>,
    health_config: String,
    health_patch: Value,
    service_patch: Value,
    stale_worker: bool,
    unreachable_worker: bool,
    failure_command: Option<&'static str>,
    pending_path: Option<std::path::PathBuf>,
}
impl Fake {
    fn new() -> Self {
        Self {
            calls: Mutex::new(vec![]),
            digest: "abc".repeat(21) + "a",
            trust: true,
            conflict: false,
            service_actions: Mutex::new(vec![]),
            host_alias: false,
            jump: "mac1".into(),
            hop_jump: "none".into(),
            controller_port: 22,
            pending_health: Mutex::new(0),
            health_config: "/Users/controller/.config/mac-worker/config.toml".into(),
            health_patch: json!({}),
            service_patch: json!({}),
            stale_worker: false,
            unreachable_worker: false,
            failure_command: None,
            pending_path: None,
        }
    }
}
impl ProcessRunner for Fake {
    fn run(&self, req: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let args = req
            .args
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let call = args.join(" ");
        self.calls.lock().unwrap().push(call.clone());
        if req.program == "/usr/bin/ssh-keygen" {
            if self.host_alias && call.contains("[mini2-trust]:2222") {
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(256),
                    stdout: vec![],
                    stderr: vec![],
                });
            }
            return Ok(ProcessResult {
                status: ExitStatus::from_raw(if self.trust { 0 } else { 256 }),
                stdout: if self.trust {
                    format!("host {}\n", key()).into_bytes()
                } else {
                    vec![]
                },
                stderr: vec![],
            });
        }
        if args.first().map(String::as_str) == Some("-G") {
            let alias = args.last().unwrap();
            let bare_alias = alias.rsplit('@').next().unwrap();
            let host = if matches!(bare_alias, "mac1" | "controller-route" | "192.168.1.11") {
                "192.168.1.11"
            } else if alias == "mac2" {
                "10.0.0.2"
            } else {
                "10.9.0.2"
            };
            let port = args
                .windows(2)
                .find(|pair| pair[0] == "-p")
                .map(|pair| pair[1].parse::<u16>().unwrap())
                .unwrap_or_else(|| {
                    if matches!(bare_alias, "mac1" | "controller-route") {
                        self.controller_port
                    } else if self.host_alias && alias == "mac2" {
                        2222
                    } else {
                        22
                    }
                });
            return Ok(ProcessResult{status:ExitStatus::from_raw(0),stdout:format!("user {}\nhostname {host}\nport {port}\nproxyjump {}\nuserknownhostsfile /tmp/trusted\n{}",if alias.starts_with("bob@"){"bob"}else{"kirchik"},if alias=="mac2"{&self.jump}else if bare_alias=="controller-route"{&self.hop_jump}else{"none"},if self.host_alias && alias=="mac2"{"hostkeyalias mini2-trust\n"}else{""}).into_bytes(),stderr:vec![]});
        }
        let cmd = args.last().unwrap().as_str();
        if cmd == "~/.local/bin/worker host controller-configure"
            && let Some(path) = &self.pending_path
        {
            let pending: Value = serde_json::from_slice(
                &fs::read(path).expect("pending record must precede the first remote write"),
            )
            .unwrap();
            assert_eq!(pending["destination"], "mac1");
            assert_eq!(pending["stage"], "configuring");
        }
        if self.failure_command == Some(cmd) {
            return Err(WorkerError::Unavailable(
                "PLANTED_PRIVATE_REMOTE_ERROR".into(),
            ));
        }

        Ok(match cmd {
            "~/.local/bin/worker host probe" => result(
                json!({"protocol_version":PROTOCOL_VERSION,"supervision_version":3,"hostname":"mini-1","arch":"arm64","os_version":"15.0","free_disk_bytes":100000000000u64,"total_disk_bytes":200000000000u64,"memory_pressure":"normal","swap_used_bytes":0,"slot_state":"idle","active_lease":null,"capabilities":[],"configured_slots":1,"busy_slots":0,"binary_sha256":if self.stale_worker && args.iter().any(|arg| arg == "mac2" || arg == "kirchik@mac2") { "f".repeat(64) } else { self.digest.clone() }}),
            ),
            "~/.local/bin/worker host controller-configure" => {
                let body: Value = serde_json::from_slice(req.stdin.as_ref().unwrap()).unwrap();
                assert_eq!(body["workers"][0]["target"]["hostname"], "127.0.0.1");
                assert_eq!(body["workers"][1]["target"]["proxy_jump"], Value::Null);
                result(
                    json!({"config_path":"/Users/controller/.config/mac-worker/config.toml","changed":false,"conflict":self.conflict,"diff":if self.conflict{Some("-old\n+new\n")}else{None}}),
                )
            }
            "~/.local/bin/worker host controller-key" => result(json!({"public_key":key()})),
            "~/.local/bin/worker host authorize-controller-key" => result(json!({"changed":false})),
            "~/.local/bin/worker host controller-service" => {
                let body: Value = serde_json::from_slice(req.stdin.as_ref().unwrap()).unwrap();
                self.service_actions
                    .lock()
                    .unwrap()
                    .push(body["action"].as_str().unwrap().into());
                let mut status = json!({"label":"com.mac-worker.controller","domain":"gui/501","installed":true,"loaded":true,"pid":42,"running":true,"restart_started_at_millis":1000,"paths":remote_paths()});
                if body["action"] == "uninstall" {
                    status["installed"] = json!(false);
                    status["loaded"] = json!(false);
                }
                for (key, value) in self.service_patch.as_object().unwrap() {
                    status[key] = value.clone();
                }
                result(status)
            }
            "~/.local/bin/worker host controller-probe" => result(
                json!({"protocol_version":PROTOCOL_VERSION,"workers":[{"name":"mini-1","ssh":"mac-worker-controller-mini-1-42169ef9","status":"ready","probe":null,"missing_capabilities":[],"error_code":null,"error_message":null},{"name":"mini-2","ssh":"mac-worker-controller-mini-2-100aaff3","status":if self.unreachable_worker { "unavailable" } else { "ready" },"probe":null,"missing_capabilities":[],"error_code":null,"error_message":null}]}),
            ),
            "~/.local/bin/worker host controller-rpc" => {
                let request: Value =
                    serde_json::from_slice(decode_frame(req.stdin.as_ref().unwrap()).unwrap())
                        .unwrap();
                let parsed =
                    mac_worker::controller::parse_request(&serde_json::to_vec(&request).unwrap())
                        .unwrap();
                let mut pending = self.pending_health.lock().unwrap();
                let health = if *pending > 0 {
                    *pending -= 1;
                    json!({"state":"stale","reason":"missing","leader_running":null,"record_age_millis":null,"health":null,"error_code":null})
                } else {
                    {
                        let mut record = serde_json::to_value(
                            mac_worker::controller::health::ControllerHealth::new(
                                mac_worker::job::ProcessIdentity::new(42, 1001000).unwrap(),
                                1001,
                            ),
                        )
                        .unwrap();
                        record["config_path"] = json!(self.health_config);
                        record["supervised"] = json!(true);
                        record["binary_sha256"] = json!(self.digest);
                        record["paths"] = remote_paths();
                        for (key, value) in self.health_patch.as_object().unwrap() {
                            record[key] = value.clone();
                        }
                        json!({"state":"healthy","reason":"tick_succeeded","leader_running":true,"record_age_millis":0,"health":record,"error_code":null})
                    }
                };
                ProcessResult{status:ExitStatus::from_raw(0),stdout:encode_json_frame(&json!({"protocol_version":PROTOCOL_VERSION,"command":parsed.command(),"request_id":parsed.request_id(),"payload_sha256":parsed.payload_sha256(),"result":health})).unwrap(),stderr:vec![]}
            }
            _ => panic!("unexpected process {call}"),
        })
    }
}
fn config(temp: &tempfile::TempDir) -> std::path::PathBuf {
    let path = temp.path().join("config.toml");
    fs::write(&path,"# laptop inventory\nversion=1\n[[workers]]\nname='mini-1'\nssh='mac1'\nslots=2\n[[workers]]\nname='mini-2'\nssh='mac2'\nslots=1\n").unwrap();
    path
}
fn request() -> InitRequest {
    InitRequest {
        destination: "mac1".into(),
        worker_ssh: vec![],
        force: false,
    }
}
#[test]
fn init_is_rerunnable_preserves_laptop_workers_and_verifies_before_enabling() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let fake = Fake::new();
    for _ in 0..2 {
        let report = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap();
        assert!(report.ready, "{report:?}");
        assert_eq!(report.workers.len(), 2);
        assert!(report.workers.iter().all(|w| w.reachable));
    }
    let cfg = mac_worker::config::Config::load(&path).unwrap();
    assert!(cfg.controller.enabled);
    assert_eq!(cfg.controller.ssh, "mac1");
    assert_eq!(cfg.workers[0].ssh, "mac1");
    assert_eq!(cfg.workers[0].slots, 2);
    let calls = fake.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .filter(|c| c.ends_with("host authorize-controller-key"))
            .count(),
        4
    );
    assert!(calls.iter().all(|c| !c.contains("accept-new")));
}
#[test]
fn init_build_mismatch_stops_before_any_write_and_explains_setup() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let fake = Fake::new();
    let report = initialize(&fake, &path, temp.path(), "other", request()).unwrap();
    assert!(!report.ready);
    assert!(report.message.contains("worker setup"));
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
}
#[test]
fn init_missing_trust_fails_worker_without_changing_mode() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.trust = false;
    let report = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap();
    assert!(!report.ready);
    assert!(report.workers.iter().any(|w| w.error_code.is_some()));
    assert!(
        !mac_worker::config::Config::load(&path)
            .unwrap()
            .controller
            .enabled
    );
    assert!(
        !fake
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.ends_with("host controller-key"))
    );
}
#[test]
fn init_config_conflict_shows_diff_and_does_not_authorize_or_start() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.conflict = true;
    let report = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap();
    assert!(!report.ready);
    assert_eq!(report.config_diff.as_deref(), Some("-old\n+new\n"));
    assert!(
        !fake
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.ends_with("host controller-key") || c.ends_with("host controller-service"))
    );
}

#[test]
fn controller_commands_and_hidden_operations_are_available_without_confirmation_flags() {
    use clap::Parser;
    for args in [
        vec![
            "worker",
            "controller",
            "init",
            "mac1",
            "--worker-ssh",
            "mini-2=kirchik@10.0.0.2",
            "--force",
        ],
        vec!["worker", "controller", "disable"],
        vec!["worker", "controller", "drain"],
        vec!["worker", "controller", "drain", "--off"],
        vec!["worker", "controller", "run", "--supervised"],
        vec!["worker", "host", "authorize-controller-key"],
        vec!["worker", "host", "controller-configure"],
        vec!["worker", "host", "controller-key"],
        vec!["worker", "host", "controller-service"],
        vec!["worker", "host", "controller-probe"],
    ] {
        assert!(
            mac_worker::cli::Cli::try_parse_from(args.clone()).is_ok(),
            "{args:?}"
        );
    }
}
#[test]
fn disable_unloads_service_before_disabling_laptop_and_keeps_inventory() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let fake = Fake::new();
    initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap();
    disable(&fake, &path).unwrap();
    let cfg = mac_worker::config::Config::load(&path).unwrap();
    assert!(!cfg.controller.enabled);
    assert_eq!(cfg.controller.ssh, "mac1");
    assert_eq!(cfg.workers.len(), 2);
    assert!(
        fake.calls
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .ends_with("host controller-service")
    );
}

#[test]
fn rerun_restarts_a_loaded_controller_to_reload_inventory_and_ssh_settings() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let fake = Fake::new();
    for _ in 0..2 {
        assert!(
            initialize(&fake, &path, temp.path(), &fake.digest, request())
                .unwrap()
                .ready
        );
    }
    assert_eq!(
        *fake.service_actions.lock().unwrap(),
        ["install", "restart", "install", "restart"]
    );
}

#[test]
fn nondefault_port_host_key_alias_uses_laptop_trust_alias_verbatim() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.host_alias = true;
    let report = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap();
    assert!(report.ready, "{report:?}");
    assert!(
        fake.calls
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.starts_with("-F mini2-trust -f"))
    );
}
#[test]
fn account_override_authorizes_that_account_through_laptop_reachable_alias() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let fake = Fake::new();
    let mut req = request();
    req.worker_ssh.push("mini-2=bob@10.9.0.2".into());
    assert!(
        initialize(&fake, &path, temp.path(), &fake.digest, req)
            .unwrap()
            .ready
    );
    assert!(
        fake.calls
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.contains("bob@mac2") && c.ends_with("host authorize-controller-key"))
    );
}

#[test]
fn init_waits_for_new_leader_health_with_an_injected_waiter() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let fake = Fake::new();
    *fake.pending_health.lock().unwrap() = 2;
    let waits = std::sync::atomic::AtomicUsize::new(0);
    let report = initialize_with_wait(&fake, &path, temp.path(), &fake.digest, request(), &|_| {
        waits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    })
    .unwrap();
    assert!(report.ready);
    assert_eq!(waits.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn init_resolves_controller_first_jump_with_an_explicit_user_and_port() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.controller_port = 2222;
    fake.jump = "kirchik@controller-route:2222".into();
    let report = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap();
    assert!(report.ready, "{report:?}");
    assert!(
        fake.calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call == "-G -p 2222 -- kirchik@controller-route")
    );
}

#[test]
fn init_keeps_a_first_jump_to_the_same_host_on_a_different_port() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.controller_port = 2222;
    fake.jump = "kirchik@controller-route:2223".into();
    let error = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap_err();
    assert!(error.to_string().contains("remaining ProxyJump"), "{error}");
    assert!(
        !fake
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.ends_with("host controller-configure"))
    );
    assert!(
        !mac_worker::config::Config::load(&path)
            .unwrap()
            .controller
            .enabled
    );
}

#[test]
fn init_refuses_a_first_jump_to_the_controller_address_behind_another_route() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.controller_port = 2222;
    fake.jump = "kirchik@controller-route:2222".into();
    // Same address and port as the controller, reached through a bastion the
    // controller itself does not use: possibly another machine.
    fake.hop_jump = "bastion-b".into();
    let error = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("ambiguous first ProxyJump hop for worker"),
        "{error}"
    );
    assert!(
        !fake
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.ends_with("host controller-configure"))
    );
    assert!(
        !mac_worker::config::Config::load(&path)
            .unwrap()
            .controller
            .enabled
    );
}

#[test]
fn init_resolves_literal_first_jump_before_assuming_the_controllers_port() {
    for jump in ["192.168.1.11", "kirchik@192.168.1.11"] {
        let temp = tempfile::tempdir().unwrap();
        let path = config(&temp);
        let mut fake = Fake::new();
        fake.controller_port = 2222;
        fake.jump = jump.into();
        let error = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap_err();
        assert!(error.to_string().contains("remaining ProxyJump"), "{error}");
        assert!(
            fake.calls
                .lock()
                .unwrap()
                .iter()
                .any(|call| call == &format!("-G -- {jump}"))
        );
    }
}

#[test]
fn init_rejects_leader_using_a_different_config_path() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.health_config = "/Users/controller/wrong-config.toml".into();
    let report =
        initialize_with_wait(&fake, &path, temp.path(), &fake.digest, request(), &|_| {}).unwrap();
    assert!(!report.ready, "accepted a different config: {report:?}");
    assert!(report.message.contains("config"), "{report:?}");
    assert!(
        !mac_worker::config::Config::load(&path)
            .unwrap()
            .controller
            .enabled
    );
}

#[test]
fn init_supervision_rejects_a_foreign_manual_leader() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.service_patch = json!({"pid":43,"running":true,"last_exit_status":64});
    fake.health_patch = json!({"supervised":false});
    let report =
        initialize_with_wait(&fake, &path, temp.path(), &fake.digest, request(), &|_| {}).unwrap();
    assert!(!report.ready, "accepted a manual leader: {report:?}");
    assert!(
        report.message.contains("CONTROLLER_FOREIGN_LEADER"),
        "{report:?}"
    );
    assert!(
        report.message.contains("42"),
        "must identify the foreign pid: {report:?}"
    );
    assert!(
        !mac_worker::config::Config::load(&path)
            .unwrap()
            .controller
            .enabled
    );
}

#[test]
fn init_supervision_rejects_loaded_but_exited_service() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.service_patch = json!({"pid":null,"running":false,"last_exit_status":64});
    let report =
        initialize_with_wait(&fake, &path, temp.path(), &fake.digest, request(), &|_| {}).unwrap();
    assert!(!report.ready, "accepted an exited job: {report:?}");
    assert!(
        !mac_worker::config::Config::load(&path)
            .unwrap()
            .controller
            .enabled
    );
}

fn remote_paths() -> Value {
    json!({"config":"/Users/controller/.config/mac-worker/config.toml","state":"/Users/controller/.local/state/mac-worker","cache":"/Users/controller/.cache/mac-worker","data":"/Users/controller/.local/share/mac-worker"})
}

#[test]
fn init_supervision_rejects_old_build_old_start_and_wrong_roots_with_bounded_wait() {
    for patch in [
        json!({"binary_sha256":"old"}),
        json!({"started_at_millis":999}),
        json!({"paths":{"config":"/Users/controller/.config/mac-worker/config.toml","state":"/wrong/state","cache":"/wrong/cache","data":"/wrong/data"}}),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = config(&temp);
        let mut fake = Fake::new();
        fake.health_patch = patch;
        let waits = std::cell::Cell::new(0);
        let report =
            initialize_with_wait(&fake, &path, temp.path(), &fake.digest, request(), &|_| {
                waits.set(waits.get() + 1);
            })
            .unwrap();
        assert!(!report.ready, "{report:?}");
        assert_eq!(waits.get(), 20);
        assert!(report.message.contains("timed out"), "{report:?}");
        assert!(
            !mac_worker::config::Config::load(&path)
                .unwrap()
                .controller
                .enabled
        );
    }
}

#[test]
fn setup_restart_verification_requires_the_installed_digest_with_injected_wait() {
    let fake = Fake::new();
    let controller = mac_worker::config::ControllerConfig {
        enabled: true,
        ssh: "controller-route".into(),
        ..Default::default()
    };
    let waits = std::cell::Cell::new(0);
    let error = mac_worker::controller::service::restart_and_verify(
        &fake,
        &controller,
        "different-installed-build",
        &|_| waits.set(waits.get() + 1),
    )
    .unwrap_err();
    assert!(error.to_string().contains("binary digest"), "{error}");
    assert_eq!(waits.get(), 20);
    assert!(
        fake.calls
            .lock()
            .unwrap()
            .iter()
            .all(|call| call.contains("controller-route"))
    );
}

#[test]
fn init_preflight_rejects_a_stale_non_controller_helper_before_remote_writes() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.stale_worker = true;
    let report = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap();
    assert!(!report.ready, "{report:?}");
    assert!(
        report.message.contains("mini-2") && report.message.contains("worker setup"),
        "{report:?}"
    );
    assert!(!fake.calls.lock().unwrap().iter().any(|call| {
        call.ends_with("host controller-configure")
            || call.ends_with("host controller-key")
            || call.ends_with("host authorize-controller-key")
            || call.ends_with("host controller-service")
    }));
    assert!(
        !mac_worker::config::Config::load(&path)
            .unwrap()
            .controller
            .enabled
    );
}

#[test]
fn recovery_failed_first_init_can_be_disabled_without_a_destination() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.unreachable_worker = true;
    let report = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap();
    assert!(!report.ready);
    assert!(
        !mac_worker::config::Config::load(&path)
            .unwrap()
            .controller
            .enabled
    );
    let pending_file = temp
        .path()
        .join(".local/state/mac-worker-controller/pending-init.json");
    let pending = fs::read(&pending_file).ok();
    let disabled = disable(&fake, &path);
    assert!(
        disabled.is_ok(),
        "failed init cannot be undone: {disabled:?}"
    );
    let pending: Value =
        serde_json::from_slice(&pending.expect("pending destination recorded")).unwrap();
    assert_eq!(pending["destination"], "mac1");
    assert_eq!(pending["stage"], "verifying");
    assert!(report.message.contains("worker controller disable"));
    assert!(!pending_file.exists());
    assert_eq!(
        fake.service_actions.lock().unwrap().last().unwrap(),
        "uninstall"
    );
}

#[test]
fn recovery_disable_accepts_an_explicit_destination() {
    use clap::Parser;
    assert!(
        mac_worker::cli::Cli::try_parse_from(["worker", "controller", "disable", "--ssh", "mac1"])
            .is_ok()
    );
}

#[test]
fn recovery_disable_without_enabled_or_pending_destination_fails_clearly() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let fake = Fake::new();
    let error = disable(&fake, &path).unwrap_err();
    assert_eq!(error.public_code(), "CONTROLLER_DESTINATION_REQUIRED");
    assert!(error.to_string().contains("--ssh"));
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[test]
fn recovery_failed_first_init_can_be_disabled_with_explicit_ssh() {
    use clap::Parser;
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.unreachable_worker = true;
    let pending_file = pending_root(temp.path()).join("pending-init.json");
    fake.pending_path = Some(pending_file.clone());
    let report = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap();
    assert!(!report.ready);
    let runtime = mac_worker::RuntimeContext::isolated(
        Default::default(),
        temp.path().into(),
        temp.path().into(),
    );
    let mut out = vec![];
    let mut err = vec![];
    let exit = mac_worker::run_with_stdio_in_context(
        mac_worker::cli::Cli::try_parse_from([
            "worker",
            "--config",
            path.to_str().unwrap(),
            "controller",
            "disable",
            "--ssh",
            "mac1",
        ])
        .unwrap(),
        &fake,
        &runtime,
        &mut std::io::Cursor::new([]),
        &mut out,
        &mut err,
    );
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&err));
    assert!(!pending_file.exists());
    assert!(fake.calls.lock().unwrap().last().unwrap().contains("mac1"));
    assert_eq!(
        fake.service_actions.lock().unwrap().last().unwrap(),
        "uninstall"
    );
}

#[test]
fn recovery_every_remote_failure_retains_private_pending_record_and_safe_hint() {
    use std::os::unix::fs::PermissionsExt;
    for command in [
        "controller-configure",
        "controller-key",
        "authorize-controller-key",
        "controller-service",
        "controller-probe",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = config(&temp);
        let mut fake = Fake::new();
        fake.failure_command = Some(match command {
            "controller-configure" => "~/.local/bin/worker host controller-configure",
            "controller-key" => "~/.local/bin/worker host controller-key",
            "authorize-controller-key" => "~/.local/bin/worker host authorize-controller-key",
            "controller-service" => "~/.local/bin/worker host controller-service",
            _ => "~/.local/bin/worker host controller-probe",
        });
        let pending_file = pending_root(temp.path()).join("pending-init.json");
        fake.pending_path = Some(pending_file.clone());
        let report = initialize(&fake, &path, temp.path(), &fake.digest, request()).unwrap();
        assert!(!report.ready);
        assert!(
            report.message.contains("worker controller disable"),
            "{report:?}"
        );
        assert!(!report.message.contains("PLANTED_PRIVATE"));
        assert_eq!(
            fs::metadata(&pending_file).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(
            !mac_worker::config::Config::load(&path)
                .unwrap()
                .controller
                .enabled
        );
        fake.failure_command = None;
        disable(&fake, &path).unwrap();
        assert!(!pending_file.exists());
    }
}

#[test]
fn recovery_success_clears_pending_and_failed_disable_keeps_it() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let mut fake = Fake::new();
    fake.unreachable_worker = true;
    assert!(
        !initialize(&fake, &path, temp.path(), &fake.digest, request())
            .unwrap()
            .ready
    );
    fake.failure_command = Some("~/.local/bin/worker host controller-service");
    assert!(disable(&fake, &path).is_err());
    assert!(pending_root(temp.path()).join("pending-init.json").exists());
    fake.failure_command = None;
    fake.unreachable_worker = false;
    assert!(
        initialize(&fake, &path, temp.path(), &fake.digest, request())
            .unwrap()
            .ready
    );
    assert!(!pending_root(temp.path()).join("pending-init.json").exists());
}

#[test]
fn recovery_pending_uses_resolved_xdg_state_for_cli_disable() {
    use clap::Parser;
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let custom_state = temp.path().join("custom-state");
    let root = custom_state.join("mac-worker-controller");
    let mut fake = Fake::new();
    fake.unreachable_worker = true;
    let report = mac_worker::controller::init::initialize_with_wait(
        &fake,
        &path,
        &root,
        temp.path(),
        &fake.digest,
        request(),
        &|_| {},
    )
    .unwrap();
    assert!(!report.ready);
    assert!(root.join("pending-init.json").exists());
    let runtime = mac_worker::RuntimeContext::isolated(
        std::collections::BTreeMap::from([(
            "XDG_STATE_HOME".into(),
            custom_state.into_os_string(),
        )]),
        temp.path().into(),
        temp.path().into(),
    );
    let mut out = vec![];
    let mut err = vec![];
    let exit = mac_worker::run_with_stdio_in_context(
        mac_worker::cli::Cli::try_parse_from([
            "worker",
            "--config",
            path.to_str().unwrap(),
            "controller",
            "disable",
        ])
        .unwrap(),
        &fake,
        &runtime,
        &mut std::io::Cursor::new([]),
        &mut out,
        &mut err,
    );
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&err));
    assert!(!root.join("pending-init.json").exists());
    assert!(!pending_root(temp.path()).exists());
}

#[test]
fn recovery_pending_symlink_is_refused_before_remote_writes() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let root = pending_root(temp.path());
    fs::create_dir_all(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let unrelated = temp.path().join("unrelated");
    fs::write(&unrelated, "preserve").unwrap();
    symlink(&unrelated, root.join("pending-init.json")).unwrap();
    let fake = Fake::new();
    assert!(initialize(&fake, &path, temp.path(), &fake.digest, request()).is_err());
    assert_eq!(fs::read_to_string(unrelated).unwrap(), "preserve");
    assert!(
        !fake
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.ends_with("host controller-configure"))
    );
}

#[test]
fn recovery_enabled_destination_takes_priority_without_erasing_other_pending_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let original = fs::read_to_string(&path).unwrap();
    fs::write(
        &path,
        format!("{original}\n[controller]\nenabled=true\nssh='old-controller'\n"),
    )
    .unwrap();
    let mut fake = Fake::new();
    fake.unreachable_worker = true;
    assert!(
        !initialize(&fake, &path, temp.path(), &fake.digest, request())
            .unwrap()
            .ready
    );
    disable(&fake, &path).unwrap();
    assert!(
        fake.calls
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .contains("old-controller")
    );
    assert!(pending_root(temp.path()).join("pending-init.json").exists());
    disable(&fake, &path).unwrap();
    assert!(fake.calls.lock().unwrap().last().unwrap().contains("mac1"));
    assert!(!pending_root(temp.path()).join("pending-init.json").exists());
}

#[test]
fn recovery_explicit_pending_cleanup_preserves_a_different_enabled_controller() {
    let temp = tempfile::tempdir().unwrap();
    let path = config(&temp);
    let original = format!(
        "{}\n[controller]\nenabled=true\nssh='old-controller'\n",
        fs::read_to_string(&path).unwrap()
    );
    fs::write(&path, &original).unwrap();
    let mut fake = Fake::new();
    fake.unreachable_worker = true;
    assert!(
        !initialize(&fake, &path, temp.path(), &fake.digest, request())
            .unwrap()
            .ready
    );
    mac_worker::controller::init::disable(&fake, &path, &pending_root(temp.path()), Some("mac1"))
        .unwrap();
    assert!(fake.calls.lock().unwrap().last().unwrap().contains("mac1"));
    assert!(!pending_root(temp.path()).join("pending-init.json").exists());
    assert_eq!(fs::read_to_string(&path).unwrap(), original);
}
