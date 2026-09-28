//! Laptop orchestration for provisioning a controller; no ad-hoc remote shell.
use super::{
    provision::{self, ConfigWriteResult, PROCESS_POLICY, ResolvedSsh, invalid, process},
    service::{ServiceAction, ServiceStatus},
};
use crate::{
    config::{Config, ControllerConfig},
    error::WorkerError,
    process::ProcessRunner,
    protocol::{
        ControllerChangedResponse, ControllerConfigureRequest, ControllerKeyRequest,
        ControllerKeyResponse, ControllerServiceRequest, HealthStatus, WorkersReport,
    },
    transfer::{HostOperation, controller_host_request},
    transport::SshTransport,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfiguredHost {
    #[serde(flatten)]
    pub outcome: ConfigWriteResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_path: Option<PathBuf>,
}

pub struct InitRequest {
    pub destination: String,
    pub worker_ssh: Vec<String>,
    pub force: bool,
}
#[derive(Debug, Serialize)]
pub struct InitWorkerSummary {
    pub name: String,
    pub destination: String,
    pub reachable: bool,
    pub error_code: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct InitReport {
    pub ready: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    pub config_diff: Option<String>,
    pub boot_commands: Option<String>,
    pub workers: Vec<InitWorkerSummary>,
}

pub fn initialize(
    runner: &dyn ProcessRunner,
    config_path: &Path,
    home: &Path,
    expected_sha: &str,
    request: InitRequest,
) -> Result<InitReport, WorkerError> {
    initialize_with_wait(
        runner,
        config_path,
        home,
        expected_sha,
        request,
        &std::thread::sleep,
    )
}

/// Injectable startup backoff: tests observe retries without wall-clock sleeps.
#[doc(hidden)]
pub fn initialize_with_wait(
    runner: &dyn ProcessRunner,
    config_path: &Path,
    home: &Path,
    expected_sha: &str,
    request: InitRequest,
    wait: &dyn Fn(std::time::Duration),
) -> Result<InitReport, WorkerError> {
    let original = std::fs::read_to_string(config_path)?;
    let config = Config::parse(&original)?;
    config.validate()?;
    config.require_local_inventory()?;
    let controller = ControllerConfig {
        enabled: true,
        ssh: request.destination.clone(),
        ..ControllerConfig::default()
    };
    let host = super::controller_worker_entry(&controller)?;
    let mut report = InitReport {
        ready: false,
        message: String::new(),
        error_code: None,
        config_diff: None,
        boot_commands: None,
        workers: vec![],
    };
    let probe = SshTransport::new(runner).probe(&host);
    if probe
        .probe
        .as_ref()
        .and_then(|p| p.binary_sha256.as_deref())
        != Some(expected_sha)
        || expected_sha.is_empty()
    {
        report.message="controller helper is absent or differs from this laptop build; run `worker setup` for the controller host first".into();
        return Ok(report);
    }
    let controller_target = resolve(runner, &request.destination)?;
    let mut overrides = BTreeMap::new();
    for entry in &request.worker_ssh {
        let (name, target) = entry
            .split_once('=')
            .ok_or_else(|| invalid("--worker-ssh must be worker-name=ssh-destination"))?;
        if config.worker(name).is_none()
            || overrides
                .insert(name.to_owned(), target.to_owned())
                .is_some()
        {
            return Err(invalid("--worker-ssh names an unknown or duplicate worker"));
        }
    }
    let mut resolved = BTreeMap::new();
    for worker in &config.workers {
        let target = resolve(
            runner,
            overrides
                .get(&worker.name)
                .map(String::as_str)
                .unwrap_or(&worker.ssh),
        )?;
        resolved.insert(worker.name.clone(), target);
    }
    let mut planned =
        provision::plan_inventory(&config, &controller_target, &request.destination, &resolved)?;
    // Resolve first-hop aliases as well: mac1, user@mac1 and its IP must agree.
    for worker in &mut planned {
        if let Some(jumps) = worker.target.proxy_jump.clone() {
            let (first, rest) = jumps.split_once(',').unwrap_or((&jumps, ""));
            if resolve_jump(runner, first)?.same_host(&controller_target) {
                worker.target.proxy_jump = (!rest.is_empty()).then(|| rest.to_owned());
            }
        }
    }
    let mut known_hosts = String::new();
    for worker in &mut planned {
        let source = &resolved[&worker.name];
        let trust = trusted_keys(runner, home, source, &worker.target);
        report.workers.push(InitWorkerSummary {
            name: worker.name.clone(),
            destination: format!(
                "{}@{}:{}",
                worker.target.user, worker.target.hostname, worker.target.port
            ),
            reachable: false,
            error_code: trust
                .as_ref()
                .err()
                .map(|_| "CONTROLLER_TRUST_MISSING".into()),
        });
        if let Ok(keys) = trust {
            known_hosts.push_str(&keys);
            worker.trusted_keys = Some(keys)
        }
    }
    if report.workers.iter().any(|w| w.error_code.is_some()) {
        report.message="trusted host key unavailable for one or more workers; verify those destinations from the laptop first, then rerun init".into();
        return Ok(report);
    }
    // Remaining jumps require explicit reachable destinations. Never silently
    // copy a laptop-only proxy chain into a different machine's SSH config.
    if planned.iter().any(|w| w.target.proxy_jump.is_some()) {
        return Err(invalid(
            "remaining ProxyJump requires --worker-ssh with a destination directly reachable from the controller",
        ));
    }
    let mut remote = config.clone();
    remote.controller = ControllerConfig::default();
    for (worker, plan) in remote.workers.iter_mut().zip(&planned) {
        worker.ssh = plan.alias.clone()
    }
    let configured: ConfiguredHost = controller_host_request(
        runner,
        &host,
        HostOperation::ControllerConfigure,
        &ControllerConfigureRequest {
            config_toml: toml::to_string_pretty(&remote)
                .map_err(|_| invalid("cannot encode controller inventory"))?,
            workers: planned.clone(),
            known_hosts,
            force: request.force,
            include_details: true,
        },
    )?;
    if configured.outcome.conflict {
        report.message =
            "controller config differs; review the diff and rerun with --force to replace it"
                .into();
        report.config_diff = configured.outcome.diff;
        return Ok(report);
    }
    let key: ControllerKeyResponse = controller_host_request(
        runner,
        &host,
        HostOperation::ControllerKey,
        &serde_json::json!({}),
    )?;
    for (worker, summary) in config.workers.iter().zip(&mut report.workers) {
        let mut authorization_worker = worker.clone();
        // Reach the selected account using the laptop's original route/ProxyJump.
        let selected_user = &resolved[&worker.name].user;
        authorization_worker.ssh = format!(
            "{}@{}",
            selected_user,
            worker.ssh.rsplit('@').next().unwrap_or(&worker.ssh)
        );
        match controller_host_request::<_, ControllerChangedResponse>(
            runner,
            &authorization_worker,
            HostOperation::AuthorizeControllerKey,
            &ControllerKeyRequest {
                public_key: key.public_key.clone(),
            },
        ) {
            Ok(_) => {}
            Err(error) => summary.error_code = Some(error.public_code()),
        }
    }
    if report.workers.iter().any(|w| w.error_code.is_some()) {
        report.message="controller key authorization failed on one or more workers; rerun init after restoring laptop access".into();
        return Ok(report);
    }
    let installed: ServiceStatus = controller_host_request(
        runner,
        &host,
        HostOperation::ControllerService,
        &ControllerServiceRequest {
            action: ServiceAction::Install,
            include_details: true,
        },
    )?;
    if let (Some(identity), Some(paths)) = (key.identity, installed.paths.as_ref()) {
        report.boot_commands = Some(super::service::launchdaemon_commands(
            &identity.home,
            paths,
            &identity.username,
            identity.uid,
        )?);
    }
    let restarted: ServiceStatus = controller_host_request(
        runner,
        &host,
        HostOperation::ControllerService,
        &ControllerServiceRequest {
            action: ServiceAction::Restart,
            include_details: true,
        },
    )?;
    let observed: WorkersReport = controller_host_request(
        runner,
        &host,
        HostOperation::ControllerProbe,
        &serde_json::json!({}),
    )?;
    if observed.protocol_version != crate::protocol::PROTOCOL_VERSION {
        return Err(invalid("invalid controller probe response"));
    }
    for (summary, plan) in report.workers.iter_mut().zip(&planned) {
        let matches = observed
            .workers
            .iter()
            .filter(|w| w.name == plan.name && w.ssh == plan.alias)
            .collect::<Vec<_>>();
        summary.reachable = matches.len() == 1 && matches[0].status == HealthStatus::Ready;
        if !summary.reachable {
            summary.error_code = Some("CONTROLLER_WORKER_UNREACHABLE".into())
        }
    }
    let expected_config = configured
        .config_path
        .as_deref()
        .ok_or_else(|| invalid("controller configure response has no config path"))?;
    if let Err(error) = super::service::verify_restart(
        runner,
        &controller,
        restarted,
        expected_sha,
        expected_config,
        wait,
    ) {
        report.error_code = Some(error.public_code());
        report.message = error.to_string();
        return Ok(report);
    }
    if report.workers.iter().any(|w| !w.reachable) {
        report.message="controller cannot reach every worker; correct SSH access or --worker-ssh overrides and rerun init".into();
        return Ok(report);
    }
    crate::config::write_controller_mode(config_path, &original, &controller)?;
    report.ready = true;
    report.message="controller is live; laptop controller mode enabled. LaunchAgent requires a logged-in GUI session after reboot.".into();
    Ok(report)
}

pub(crate) fn resolve(
    runner: &dyn ProcessRunner,
    destination: &str,
) -> Result<ResolvedSsh, WorkerError> {
    resolve_with_port(runner, destination, None)
}

fn resolve_jump(runner: &dyn ProcessRunner, jump: &str) -> Result<ResolvedSsh, WorkerError> {
    // ProxyJump has a separate [user@]host[:port] grammar. Feed an explicit
    // port to ssh -G as an option instead of treating it as part of the alias.
    match jump.rsplit_once(':') {
        Some((destination, port)) => {
            let port = port
                .parse::<u16>()
                .ok()
                .filter(|port| *port != 0)
                .ok_or_else(|| invalid("invalid ProxyJump port"))?;
            resolve_with_port(runner, destination, Some(port))
        }
        None => resolve(runner, jump),
    }
}

fn resolve_with_port(
    runner: &dyn ProcessRunner,
    destination: &str,
    port: Option<u16>,
) -> Result<ResolvedSsh, WorkerError> {
    if !crate::config::valid_ssh_destination(destination) {
        return Err(invalid("invalid SSH destination"));
    }
    let mut args = vec!["-G".into()];
    if let Some(port) = port {
        args.extend(["-p".into(), port.to_string().into()]);
    }
    args.extend(["--".into(), destination.into()]);
    let mut req = process("/usr/bin/ssh", args);
    req.policy = PROCESS_POLICY;
    let result = runner.run(&req)?;
    if !result.status.success() {
        return Err(invalid(
            "ssh -G failed; check destination or --worker-ssh override",
        ));
    }
    ResolvedSsh::parse(
        std::str::from_utf8(&result.stdout)
            .map_err(|_| invalid("ssh -G returned invalid output"))?,
    )
}
fn trusted_keys(
    runner: &dyn ProcessRunner,
    home: &Path,
    source: &ResolvedSsh,
    target: &ResolvedSsh,
) -> Result<String, WorkerError> {
    let host = source.host_key_alias.as_deref().unwrap_or(&source.hostname);
    let lookup = if source.host_key_alias.is_some() || source.port == 22 {
        host.to_owned()
    } else {
        format!("[{host}]:{}", source.port)
    };
    let mut found = String::new();
    let files = if source.known_hosts_files.is_empty() {
        vec![home.join(".ssh/known_hosts").to_string_lossy().into_owned()]
    } else {
        source.known_hosts_files.clone()
    };
    for file in files {
        if file == "none" {
            continue;
        }
        let path = if let Some(rest) = file.strip_prefix("~/") {
            home.join(rest)
        } else {
            file.into()
        };
        let result = runner.run(&process(
            "/usr/bin/ssh-keygen",
            vec![
                "-F".into(),
                lookup.clone().into(),
                "-f".into(),
                path.into_os_string(),
            ],
        ))?;
        if result.status.success() {
            found.push_str(
                std::str::from_utf8(&result.stdout)
                    .map_err(|_| invalid("invalid trusted host keys"))?,
            );
            found.push('\n')
        }
    }
    provision::trusted_host_keys(
        &found,
        target.host_key_alias.as_deref().unwrap_or(&target.hostname),
        22,
    )
}

pub fn configure_host(
    home: &Path,
    config_path: &Path,
    request: &ControllerConfigureRequest,
) -> Result<ConfiguredHost, WorkerError> {
    let config = Config::parse(&request.config_toml)?;
    config.validate()?;
    if config.workers.len() != request.workers.len()
        || !config
            .workers
            .iter()
            .zip(&request.workers)
            .all(|(w, p)| w.name == p.name && w.ssh == p.alias)
    {
        return Err(invalid("SSH inventory does not match controller config"));
    }
    let outcome =
        provision::write_controller_config(config_path, &request.config_toml, request.force)?;
    if !outcome.conflict {
        provision::write_ssh_settings(home, &request.workers, &request.known_hosts)?
    }
    Ok(ConfiguredHost {
        outcome,
        config_path: request
            .include_details
            .then(|| std::path::absolute(config_path))
            .transpose()?,
    })
}

pub fn disable(
    runner: &dyn ProcessRunner,
    config_path: &Path,
) -> Result<ServiceStatus, WorkerError> {
    let original = std::fs::read_to_string(config_path)?;
    let config = Config::parse(&original)?;
    config.validate()?;
    let mut controller = config.controller.clone();
    // Retain destination for idempotent disable and future reinitialization.
    controller.enabled = true;
    let worker = super::controller_worker_entry(&controller)?;
    let service = controller_host_request(
        runner,
        &worker,
        HostOperation::ControllerService,
        &ControllerServiceRequest {
            action: ServiceAction::Uninstall,
            include_details: false,
        },
    )?;
    controller.enabled = false;
    crate::config::write_controller_mode(config_path, &original, &controller)?;
    Ok(service)
}

pub(crate) fn host_identity(
    home: &Path,
) -> Result<crate::protocol::ControllerHostIdentity, WorkerError> {
    let uid = unsafe { libc::geteuid() };
    let mut record = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; 65536];
    let code = unsafe {
        libc::getpwuid_r(
            uid,
            record.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if code != 0 || result.is_null() {
        return Err(invalid("cannot identify controller account"));
    }
    let record = unsafe { record.assume_init() };
    let username = unsafe { std::ffi::CStr::from_ptr(record.pw_name) }
        .to_str()
        .map_err(|_| invalid("controller username must be UTF-8"))?
        .to_owned();
    Ok(crate::protocol::ControllerHostIdentity {
        home: home.into(),
        username,
        uid,
    })
}
