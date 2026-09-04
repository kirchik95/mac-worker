use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::{self, Cursor, Read, Write},
    path::PathBuf,
};

use cli::{Cli, Command, HiddenComponent, HostCommand};
use client_state::ClientStateStore;
use config::{Config, WorkerEntry};
use dashboard::command::{
    DashboardCommandRequest, SystemBrowserOpener, SystemDashboardLauncher, run_dashboard,
};
use doctor::{DoctorRequest, DoctorService};
use error::WorkerError;
use host_store::HostStore;
use install::Installer;
use job::{
    CancelRequest, CommandSpec, FleetReconcileJobResult, FleetReconcileRequest,
    FleetReconcileResponse, HostControlError, JsonEvent, LeaseAcquireRequest, LogChunkRequest,
    LogChunkResponse, ResolveOrAbandonRequest, StatusRequest, SubmitRequest,
};
use job_service::JobService;
use lease::{AdmissionFacts, LeaseService};
use output::CommandOutput;
use paths::PathLayout;
use probe::ProbeCollector;
use process::ProcessRunner;
use protocol::{PROTOCOL_VERSION, SetupReport};
use remote_snapshot::{RemoteSnapshotService, SnapshotVerifyRequest, VerifiedSnapshotResponse};
use run::{
    CancelService, FleetReconciler, LogsService, RunRequest, RunService, StatusService,
    SystemFollowRuntime, terminal_exit_code,
};
use scheduler::WorkerPreference;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use supervisor::{
    SUPERVISOR_LOCK_FD, Supervisor, SystemProcessInspector, SystemSupervisorLauncher,
    validate_detached_supervisor_context,
};
use transfer::{
    HostTransferService, RemoteJobClient, RsyncServerExecutor, SystemRsyncServerExecutor,
    TransferIdentity,
};
use transport::{SshTransport, WorkersService};

pub mod agent;
pub mod agent_facts;
pub mod cli;
pub mod client_state;
pub mod config;
pub mod dashboard;
pub mod doctor;
pub mod error;
pub mod host_store;
pub mod inputs;
pub mod install;
pub mod job;
pub mod job_service;
pub mod lease;
pub mod manifest;
pub mod output;
pub mod paths;
pub mod probe;
pub mod process;
pub mod project;
pub mod project_config;
pub mod project_state;
pub mod protocol;
pub mod redaction;
pub mod remote_snapshot;
pub mod requirements;
pub mod rooted_fs;
pub mod run;
pub mod scheduler;
pub mod scheduler_adapter;
pub mod snapshot;
pub mod supervisor;
pub mod task;
pub mod transfer;
pub mod transfer_repo;
pub mod transport;

#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct RuntimeContext {
    environment: BTreeMap<OsString, OsString>,
    home: PathBuf,
    current_dir: RuntimeCurrentDir,
}

#[derive(Debug, Clone)]
enum RuntimeCurrentDir {
    Process,
    Fixed(PathBuf),
}

impl RuntimeContext {
    fn capture() -> Self {
        let environment = std::env::vars_os().collect::<BTreeMap<_, _>>();
        let home = environment
            .get(&OsString::from("HOME"))
            .map(PathBuf::from)
            .unwrap_or_default();
        Self {
            environment,
            home,
            current_dir: RuntimeCurrentDir::Process,
        }
    }

    #[doc(hidden)]
    pub fn isolated(
        environment: BTreeMap<OsString, OsString>,
        home: PathBuf,
        current_dir: PathBuf,
    ) -> Self {
        Self {
            environment,
            home,
            current_dir: RuntimeCurrentDir::Fixed(current_dir),
        }
    }

    fn current_dir(&self) -> Result<PathBuf, WorkerError> {
        match &self.current_dir {
            RuntimeCurrentDir::Process => std::env::current_dir().map_err(WorkerError::Io),
            RuntimeCurrentDir::Fixed(current_dir) => Ok(current_dir.clone()),
        }
    }
}

pub fn execute_with(cli: Cli, runner: &dyn ProcessRunner) -> Result<CommandOutput, WorkerError> {
    let runtime = RuntimeContext::capture();
    execute_with_context(cli, runner, &runtime)
}

fn execute_with_context(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
) -> Result<CommandOutput, WorkerError> {
    match cli.command {
        Command::Setup { hosts } => {
            let config = load_config(cli.config, runtime)?;
            let selected = select_workers(&config, &hosts)?;
            let current_exe = std::env::current_exe()?;
            let workers = selected
                .into_iter()
                .map(|worker| Installer::new(runner).install(&current_exe, &worker))
                .collect();
            Ok(CommandOutput::Setup(SetupReport {
                protocol_version: PROTOCOL_VERSION,
                workers,
            }))
        }
        Command::Doctor { project, includes } => {
            let paths = discover_paths(cli.config, runtime)?;
            let config = Config::load(&paths.config)?;
            let project = match project {
                Some(project) => project,
                None => runtime.current_dir()?,
            };
            let service = DoctorService {
                runner,
                config: &config,
                paths: &paths,
            };
            Ok(CommandOutput::Doctor(service.inspect(DoctorRequest {
                project,
                cli_includes: includes,
            })?))
        }
        Command::Workers => {
            let config = load_config(cli.config, runtime)?;
            let service = WorkersService::new(SshTransport::new(runner));
            Ok(CommandOutput::Workers(service.inspect(&config)))
        }
        Command::Dashboard { .. } => Err(WorkerError::Protocol(
            "public dashboard requires the stdio execution boundary".into(),
        )),
        Command::Run { .. } => Err(WorkerError::Protocol(
            "public run requires the stdio execution boundary".into(),
        )),
        Command::Status { job_id } => {
            let paths = discover_paths(cli.config, runtime)?;
            let config = Config::load(&paths.config)?;
            let client_state = ClientStateStore::open(&paths.state)?;
            if job_id.is_none() {
                FleetReconciler {
                    config: &config,
                    client_state: &client_state,
                    remote: RemoteJobClient::new(runner),
                    reconcile_observer: None,
                }
                .reconcile()?;
            }
            let remote = RemoteJobClient::new(runner);
            let service = StatusService {
                config: &config,
                client_state: &client_state,
                remote: &remote,
            };
            Ok(CommandOutput::Status(service.inspect(job_id)?))
        }
        Command::Logs { .. } => Err(WorkerError::Protocol(
            "public logs requires the stdio execution boundary".into(),
        )),
        Command::Cancel { job_id } => {
            let paths = discover_paths(cli.config, runtime)?;
            let config = Config::load(&paths.config)?;
            let client_state = ClientStateStore::open(&paths.state)?;
            let service = CancelService::new(runner, &config, &client_state);
            Ok(CommandOutput::Cancel(service.cancel(job_id)?))
        }
        Command::Host {
            command: HostCommand::Probe,
        } => {
            let paths = discover_paths(cli.config, runtime)?;
            Ok(CommandOutput::Probe(ProbeCollector::collect_for_paths(
                &paths,
            )?))
        }
        Command::Host {
            command: HostCommand::LeaseAcquire,
        } => Err(WorkerError::Protocol(
            "host lease-acquire requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::Status,
        } => Err(WorkerError::Protocol(
            "host status requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::LogChunk,
        } => Err(WorkerError::Protocol(
            "host log-chunk requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::ResolveOrAbandon,
        } => Err(WorkerError::Protocol(
            "host resolve-or-abandon requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::Cancel,
        } => Err(WorkerError::Protocol(
            "host cancel requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::Reconcile,
        } => Err(WorkerError::Protocol(
            "host reconcile requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::SnapshotVerify,
        } => Err(WorkerError::Protocol(
            "host snapshot-verify requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::Submit,
        } => Err(WorkerError::Protocol(
            "host submit requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::Supervise { .. },
        } => Err(WorkerError::Protocol(
            "host supervise requires the inherited supervisor boundary".into(),
        )),
        Command::Host {
            command: HostCommand::RsyncReceive { .. },
        } => Err(WorkerError::Protocol(
            "host rsync-receive requires the binary stdio execution boundary".into(),
        )),
    }
}

pub fn run_with_io(
    cli: Cli,
    runner: &dyn ProcessRunner,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let runtime = RuntimeContext::capture();
    run_with_io_in_context(cli, runner, &runtime, stdout, stderr)
}

pub fn run_with_stdio(
    cli: Cli,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let runtime = RuntimeContext::capture();
    run_with_stdio_in_context(cli, runner, &runtime, stdin, stdout, stderr)
}

fn run_public_streaming_command(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let json = cli.json;
    if json {
        let mut live = LiveJsonStdout::new(stdout);
        return match run_public_streaming_body(cli, runner, runtime, &mut live, stderr) {
            Ok(code) => code,
            Err(_) if live.failed => crate::error::ExitKind::Io as u8,
            Err(error) => match write_json_error_event(live.inner, &error) {
                Ok(()) => error.exit_code(),
                Err(_) => crate::error::ExitKind::Io as u8,
            },
        };
    }
    match run_public_streaming_body(cli, runner, runtime, stdout, stderr) {
        Ok(code) => code,
        Err(error) => {
            write_public_diagnostic(stderr, &error);
            error.exit_code()
        }
    }
}

fn run_public_streaming_body(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<u8, WorkerError> {
    let json = cli.json;
    let paths = discover_paths(cli.config, runtime)?;
    let config = Config::load(&paths.config)?;
    let client_state = ClientStateStore::open(&paths.state)?;
    let remote = RemoteJobClient::new(runner);
    let follow_runtime = SystemFollowRuntime;
    let logs = LogsService {
        config: &config,
        client_state: &client_state,
        remote: &remote,
        runtime: &follow_runtime,
    };
    match cli.command {
        Command::Run {
            worker,
            no_wait,
            project,
            includes,
            timeout,
            shell,
            argv,
        } => {
            let project = match project {
                Some(project) => project,
                None => runtime.current_dir()?,
            };
            let command = match shell {
                Some(shell) => CommandSpec::shell(shell)?,
                None => CommandSpec::argv(argv)?,
            };
            let service = RunService::with_follower(runner, &config, &paths, &client_state, &logs);
            let completion = service.submit_and_follow(
                RunRequest {
                    preference: match worker {
                        Some(worker) => WorkerPreference::Pinned { worker },
                        None => WorkerPreference::Automatic,
                    },
                    wait_for_capacity: !no_wait,
                    project,
                    cli_includes: includes,
                    timeout,
                    command,
                },
                json,
                stdout,
                stderr,
            )?;
            Ok(completion.exit_code)
        }
        Command::Logs { follow, job_id } => {
            let response = logs.stream(job_id, follow, json, stdout, stderr)?;
            if response.status().state().is_terminal() {
                terminal_exit_code(response.status())
            } else {
                Ok(0)
            }
        }
        _ => Err(WorkerError::Protocol(
            "public streaming dispatcher received a non-streaming command".into(),
        )),
    }
}

struct LiveJsonStdout<'a> {
    inner: &'a mut dyn Write,
    failed: bool,
}

impl<'a> LiveJsonStdout<'a> {
    fn new(inner: &'a mut dyn Write) -> Self {
        Self {
            inner,
            failed: false,
        }
    }
}

impl Write for LiveJsonStdout<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.failed {
            return Err(io::Error::other("live JSON stdout already failed"));
        }
        match self.inner.write(buf) {
            Ok(0) if !buf.is_empty() => {
                self.failed = true;
                Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "live JSON stdout wrote zero bytes",
                ))
            }
            Ok(n) => Ok(n),
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("live JSON stdout already failed"));
        }
        match self.inner.flush() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
}

fn write_json_error_event(stdout: &mut dyn Write, error: &WorkerError) -> Result<(), WorkerError> {
    let event = JsonEvent::Error {
        protocol_version: PROTOCOL_VERSION,
        code: error.public_code(),
        message: error.public_message(),
    };
    let mut encoded = serde_json::to_vec(&event)
        .map_err(|error| WorkerError::Io(std::io::Error::other(error)))?;
    encoded.push(b'\n');
    stdout.write_all(&encoded)?;
    stdout.flush()?;
    Ok(())
}

fn write_public_diagnostic(stderr: &mut dyn Write, error: &WorkerError) {
    let _ = writeln!(
        stderr,
        "{}: {}",
        error.public_code(),
        error.public_message()
    );
}

#[doc(hidden)]
pub fn run_with_io_in_context(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let mut stdin = Cursor::new(Vec::<u8>::new());
    run_with_stdio_in_context(cli, runner, runtime, &mut stdin, stdout, stderr)
}

#[doc(hidden)]
pub fn run_with_stdio_in_context(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    if let Command::Dashboard { port, no_open } = cli.command {
        return run_dashboard_command(cli.config, runtime, port, no_open, stdout, stderr);
    }
    if matches!(cli.command, Command::Run { .. } | Command::Logs { .. }) {
        return run_public_streaming_command(cli, runner, runtime, stdout, stderr);
    }
    run_with_rsync_executor_in_context(
        cli,
        runner,
        &SystemRsyncServerExecutor,
        runtime,
        stdin,
        stdout,
        stderr,
    )
}

fn run_dashboard_command(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    port: Option<u16>,
    no_open: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let config = std::sync::Arc::new(Config::load(&paths.config)?);
        let client_state = std::sync::Arc::new(ClientStateStore::open(&paths.state)?);
        let launcher = SystemDashboardLauncher::from_system(config, client_state);
        let opener = SystemBrowserOpener;
        let async_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(WorkerError::Io)?;
        async_runtime.block_on(run_dashboard(
            DashboardCommandRequest::new(port, no_open),
            &launcher,
            &opener,
            Box::pin(async {
                let _ = tokio::signal::ctrl_c().await;
            }),
            stdout,
            stderr,
        ))?;
        Ok(())
    })();

    match result {
        Ok(()) => 0,
        Err(error) => {
            write_error(stderr, &error);
            error.exit_code()
        }
    }
}

#[doc(hidden)]
pub fn run_with_rsync_executor_in_context(
    cli: Cli,
    runner: &dyn ProcessRunner,
    rsync_executor: &dyn RsyncServerExecutor,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::Submit
        }
    ) {
        return run_host_submit(cli.config, runtime, stdin, stdout);
    }
    if let Command::Host {
        command: HostCommand::Supervise { job_id },
    } = &cli.command
    {
        return run_host_supervise(cli.config, runtime, job_id.expose());
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::LeaseAcquire
        }
    ) {
        return run_host_lease_acquire(cli.config, runtime, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::SnapshotVerify
        }
    ) {
        return run_host_snapshot_verify(cli.config, runtime, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::Status
        }
    ) {
        return run_host_status(cli.config, runtime, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::LogChunk
        }
    ) {
        return run_host_log_chunk(cli.config, runtime, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::ResolveOrAbandon
        }
    ) {
        return run_host_resolve_or_abandon(cli.config, runtime, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::Cancel
        }
    ) {
        return run_host_cancel(cli.config, runtime, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::Reconcile
        }
    ) {
        return run_host_reconcile(cli.config, runtime, stdin, stdout);
    }
    if let Command::Host {
        command:
            HostCommand::RsyncReceive {
                job_id,
                client_id,
                lease_token,
                request_fingerprint,
                server_args,
            },
    } = cli.command
    {
        return run_host_rsync_receive(
            cli.config,
            runtime,
            job_id,
            client_id,
            lease_token,
            request_fingerprint,
            server_args,
            rsync_executor,
            stderr,
        );
    }
    let json = cli.json;
    let public_status = matches!(cli.command, Command::Status { .. } | Command::Cancel { .. });
    let raw_probe = matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::Probe
        }
    );

    match execute_with_context(cli, runner, runtime) {
        Ok(output) => {
            let exit = output.aggregate_exit_kind().map_or(0, |kind| kind as u8);
            match output.write_to(stdout, json, raw_probe) {
                Ok(()) => exit,
                Err(error) => {
                    write_command_diagnostic(public_status, stderr, &error);
                    error.exit_code()
                }
            }
        }
        Err(error) => {
            write_command_diagnostic(public_status, stderr, &error);
            error.exit_code()
        }
    }
}

fn write_command_diagnostic(public_status: bool, stderr: &mut dyn Write, error: &WorkerError) {
    if public_status {
        write_public_diagnostic(stderr, error);
    } else {
        write_error(stderr, error);
    }
}

fn run_host_submit(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    const LIMIT: usize = 1024 * 1024;
    let result = (|| -> Result<job::SubmitResponse, WorkerError> {
        let mut bytes = Vec::new();
        stdin.take((LIMIT + 1) as u64).read_to_end(&mut bytes)?;
        if bytes.len() > LIMIT {
            return Err(WorkerError::Protocol(
                "submit request exceeded 1 MiB".into(),
            ));
        }
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let request = SubmitRequest::deserialize(&mut deserializer)
            .map_err(|_| WorkerError::Protocol("invalid submit request".into()))?;
        deserializer
            .end()
            .map_err(|_| WorkerError::Protocol("submit request contained trailing data".into()))?;
        if serde_json::to_vec(&request)
            .map_err(|_| WorkerError::Protocol("submit request serialization failed".into()))?
            != bytes
        {
            return Err(WorkerError::Protocol(
                "submit request was not canonical JSON".into(),
            ));
        }
        let paths = discover_paths(config_override, runtime)?;
        let store = HostStore::open(&paths.host_state_root())?;
        let launcher = SystemSupervisorLauncher::new()?;
        JobService::new(&store, &launcher).submit(request)
    })();

    let exit = match &result {
        Ok(_) => 0,
        Err(error) => error.exit_code(),
    };
    let write_result = match result {
        Ok(response) => serde_json::to_writer(&mut *stdout, &response),
        Err(error) => serde_json::to_writer(&mut *stdout, &versioned_host_error(&error)),
    };
    if write_result.is_err() || stdout.write_all(b"\n").is_err() || stdout.flush().is_err() {
        return crate::error::ExitKind::Io as u8;
    }
    exit
}

fn run_host_status(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_control_endpoint(
        config_override,
        runtime,
        stdin,
        stdout,
        |request: StatusRequest, store, launcher| {
            JobService::new(store, launcher).status(request.job_id())
        },
    )
}

fn run_host_log_chunk(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_control_endpoint(
        config_override,
        runtime,
        stdin,
        stdout,
        |request: LogChunkRequest, store, launcher| {
            let chunk = JobService::new(store, launcher).read_log(
                request.job_id(),
                request.stream(),
                request.offset(),
                request.limit(),
            )?;
            LogChunkResponse::new(chunk)
        },
    )
}

fn run_host_resolve_or_abandon(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_control_endpoint(
        config_override,
        runtime,
        stdin,
        stdout,
        |request: ResolveOrAbandonRequest, store, launcher| {
            JobService::new(store, launcher).resolve_or_abandon(request)
        },
    )
}

fn run_host_cancel(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_control_endpoint(
        config_override,
        runtime,
        stdin,
        stdout,
        |request: CancelRequest, store, launcher| JobService::new(store, launcher).cancel(request),
    )
}

fn run_host_reconcile(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_control_endpoint(
        config_override,
        runtime,
        stdin,
        stdout,
        |request: FleetReconcileRequest, store, launcher| {
            request.validate()?;
            let service = JobService::new(store, launcher);
            FleetReconcileResponse::new(
                request
                    .known_job_ids()
                    .iter()
                    .copied()
                    .map(|job_id| match service.reconcile_job(job_id) {
                        Ok(status) => FleetReconcileJobResult::Status {
                            status: Box::new(status),
                        },
                        Err(error) => FleetReconcileJobResult::Error {
                            job_id,
                            error: versioned_host_error(&error),
                        },
                    })
                    .collect(),
            )
        },
    )
}

fn run_host_control_endpoint<Req, Res>(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    operation: impl FnOnce(Req, &HostStore, &SystemSupervisorLauncher) -> Result<Res, WorkerError>,
) -> u8
where
    Req: DeserializeOwned + Serialize,
    Res: Serialize,
{
    const LIMIT: usize = 1024 * 1024;
    let result = (|| -> Result<Res, WorkerError> {
        let mut bytes = Vec::new();
        stdin.take((LIMIT + 1) as u64).read_to_end(&mut bytes)?;
        if bytes.len() > LIMIT {
            return Err(WorkerError::Protocol(
                "host control request exceeded 1 MiB".into(),
            ));
        }
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let request = Req::deserialize(&mut deserializer)
            .map_err(|_| WorkerError::Protocol("invalid host control request".into()))?;
        deserializer
            .end()
            .map_err(|_| WorkerError::Protocol("invalid host control request".into()))?;
        if serde_json::to_vec(&request)
            .map_err(|_| WorkerError::Protocol("invalid host control request".into()))?
            != bytes
        {
            return Err(WorkerError::Protocol(
                "host control request was not canonical JSON".into(),
            ));
        }
        let paths = discover_paths(config_override, runtime)?;
        let store = HostStore::open(&paths.host_state_root())?;
        let launcher = SystemSupervisorLauncher::new()?;
        operation(request, &store, &launcher)
    })();

    let exit = result.as_ref().map_or_else(WorkerError::exit_code, |_| 0);
    let write_result = match result {
        Ok(response) => serde_json::to_writer(&mut *stdout, &response),
        Err(error) => serde_json::to_writer(&mut *stdout, &versioned_host_error(&error)),
    };
    if write_result.is_err() || stdout.write_all(b"\n").is_err() || stdout.flush().is_err() {
        return crate::error::ExitKind::Io as u8;
    }
    exit
}

fn versioned_host_error(error: &WorkerError) -> HostControlError {
    if let WorkerError::Protocol(message) = error
        && let Some((code, detail)) = message.split_once(": ")
        && let Ok(error) = HostControlError::new(code, detail)
    {
        return error;
    }
    let (code, message) = match error {
        WorkerError::Capacity { code, .. } => (*code, "worker admission rejected"),
        WorkerError::Snapshot { code, .. } => (*code, "snapshot operation failed"),
        WorkerError::Io(_) => ("HOST_IO", "host state operation failed"),
        _ => ("INVALID_REQUEST", "host request was invalid"),
    };
    HostControlError::new(code, message).expect("fixed host error is valid")
}

fn run_host_supervise(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    raw_job_id: &str,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        validate_detached_supervisor_context()?;
        let job_id = raw_job_id
            .parse()
            .map_err(|_| WorkerError::Protocol("invalid supervisor job identity".into()))?;
        let paths = discover_paths(config_override, runtime)?;
        let store = HostStore::open(&paths.host_state_root())?;
        let guard = store.supervisor_guard_from_inherited(job_id, SUPERVISOR_LOCK_FD)?;
        let inspector = SystemProcessInspector;
        Supervisor::new(&store, &inspector).run_with_guard(job_id, guard)
    })();
    match result {
        Ok(()) => 0,
        Err(error) => error.exit_code(),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_host_rsync_receive(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    job_id: HiddenComponent,
    client_id: HiddenComponent,
    lease_token: HiddenComponent,
    request_fingerprint: HiddenComponent,
    server_args: Vec<OsString>,
    executor: &dyn RsyncServerExecutor,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let job_id = job_id
            .expose()
            .parse()
            .map_err(|_| WorkerError::Protocol("invalid receiver job identity".into()))?;
        let client_id = client_id
            .expose()
            .parse()
            .map_err(|_| WorkerError::Protocol("invalid receiver client identity".into()))?;
        let lease_token = lease_token
            .expose()
            .parse()
            .map_err(|_| WorkerError::Protocol("invalid receiver lease identity".into()))?;
        let request_fingerprint = request_fingerprint
            .expose()
            .parse()
            .map_err(|_| WorkerError::Protocol("invalid receiver fingerprint".into()))?;
        let paths = discover_paths(config_override, runtime)?;
        let store = HostStore::open(&paths.host_state_root())?;
        HostTransferService::new(&store).receive(
            &TransferIdentity::new(job_id, client_id, lease_token, request_fingerprint),
            &server_args,
            executor,
        )
    })();
    match result {
        Ok(()) => 0,
        Err(error) => {
            let code = public_rsync_receiver_error(&error);
            if writeln!(stderr, "{code}").is_err() || stderr.flush().is_err() {
                crate::error::ExitKind::Io as u8
            } else {
                error.exit_code()
            }
        }
    }
}

fn public_rsync_receiver_error(error: &WorkerError) -> &'static str {
    match error {
        WorkerError::Protocol(message) if message.starts_with("INVALID_RSYNC_SERVER_ARGS:") => {
            "HOST_RSYNC_INVALID_ARGS"
        }
        WorkerError::Protocol(message)
            if message.starts_with("JOB_ABANDONED:")
                || message.starts_with("JOB_ACCEPTED:")
                || message.starts_with("JOB_ID_CONFLICT:") =>
        {
            "HOST_RSYNC_FENCED"
        }
        WorkerError::Protocol(message) if message.starts_with("LEASE_IDENTITY_MISMATCH:") => {
            "HOST_RSYNC_LEASE"
        }
        WorkerError::Io(_) => "HOST_RSYNC_IO",
        _ => "HOST_RSYNC_REJECTED",
    }
}

fn run_host_lease_acquire(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    const LIMIT: usize = 1024 * 1024;
    let result = (|| -> Result<job::LeaseAcquireResponse, WorkerError> {
        let mut bytes = Vec::new();
        stdin.take((LIMIT + 1) as u64).read_to_end(&mut bytes)?;
        if bytes.len() > LIMIT {
            return Err(WorkerError::Protocol(
                "lease-acquire request exceeded 1 MiB".into(),
            ));
        }
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let request = LeaseAcquireRequest::deserialize(&mut deserializer).map_err(|error| {
            WorkerError::Protocol(format!("invalid lease-acquire request: {error}"))
        })?;
        deserializer.end().map_err(|_| {
            WorkerError::Protocol("lease-acquire request contained trailing data".into())
        })?;
        let paths = discover_paths(config_override, runtime)?;
        let host_state_root = paths.host_state_root();
        let probe = ProbeCollector::collect_at(&host_state_root)?;
        let facts = AdmissionFacts {
            free_disk_bytes: probe.free_disk_bytes,
            total_disk_bytes: probe.total_disk_bytes,
            memory_pressure: probe.memory_pressure,
            swap_used_bytes: probe.swap_used_bytes,
        };
        let store = HostStore::open(&host_state_root)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| WorkerError::Protocol("system clock predates Unix epoch".into()))?
            .as_millis()
            .try_into()
            .map_err(|_| WorkerError::Protocol("system clock overflow".into()))?;
        LeaseService::new(&store).acquire(&request, &facts, now)
    })();

    let exit = match &result {
        Ok(_) => 0,
        Err(error) => error.exit_code(),
    };
    let write_result = match result {
        Ok(response) => serde_json::to_writer(&mut *stdout, &response),
        Err(error) => serde_json::to_writer(&mut *stdout, &versioned_host_error(&error)),
    };
    if write_result.is_err() || stdout.write_all(b"\n").is_err() || stdout.flush().is_err() {
        return crate::error::ExitKind::Io as u8;
    }
    exit
}

fn run_host_snapshot_verify(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    const LIMIT: usize = 1024 * 1024;
    let result = (|| -> Result<VerifiedSnapshotResponse, WorkerError> {
        let mut bytes = Vec::new();
        stdin.take((LIMIT + 1) as u64).read_to_end(&mut bytes)?;
        if bytes.len() > LIMIT {
            return Err(WorkerError::Protocol(
                "snapshot-verify request exceeded 1 MiB".into(),
            ));
        }
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let request = SnapshotVerifyRequest::deserialize(&mut deserializer)
            .map_err(|_| WorkerError::Protocol("invalid snapshot-verify request".into()))?;
        deserializer.end().map_err(|_| {
            WorkerError::Protocol("snapshot-verify request contained trailing data".into())
        })?;
        let paths = discover_paths(config_override, runtime)?;
        let store = HostStore::open(&paths.host_state_root())?;
        RemoteSnapshotService::new(&store).verify_request(&request)
    })();

    let exit = match &result {
        Ok(_) => 0,
        Err(error) => error.exit_code(),
    };
    let write_result = match result {
        Ok(response) => serde_json::to_writer(&mut *stdout, &response),
        Err(error) => serde_json::to_writer(&mut *stdout, &versioned_host_error(&error)),
    };
    if write_result.is_err() || stdout.write_all(b"\n").is_err() || stdout.flush().is_err() {
        return crate::error::ExitKind::Io as u8;
    }
    exit
}

fn write_error(stderr: &mut dyn Write, error: &WorkerError) {
    let _ = writeln!(stderr, "{error}");
}

fn load_config(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
) -> Result<Config, WorkerError> {
    let paths = discover_paths(config_override, runtime)?;
    Config::load(&paths.config)
}

fn discover_paths(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
) -> Result<PathLayout, WorkerError> {
    PathLayout::discover(config_override, &runtime.environment, &runtime.home)
}

fn select_workers(config: &Config, hosts: &[String]) -> Result<Vec<WorkerEntry>, WorkerError> {
    if hosts.is_empty() {
        return Ok(config.workers.clone());
    }

    hosts
        .iter()
        .map(|host| {
            config.worker(host).cloned().ok_or_else(|| {
                WorkerError::Config(format!("worker {host:?} is not present in the inventory"))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
    };

    use tempfile::tempdir;

    use crate::{config::Config, error::WorkerError, paths::PathLayout};

    #[test]
    fn explicit_config_overrides_xdg_and_home() {
        let paths = PathLayout::discover(
            Some(PathBuf::from("/tmp/explicit.toml")),
            &BTreeMap::from([("XDG_CONFIG_HOME".into(), "/tmp/xdg".into())]),
            Path::new("/Users/tester"),
        )
        .unwrap();

        assert_eq!(paths.config, PathBuf::from("/tmp/explicit.toml"));
        assert_eq!(
            paths.state,
            PathBuf::from("/Users/tester/.local/state/mac-worker")
        );
        assert_eq!(
            paths.cache,
            PathBuf::from("/Users/tester/.cache/mac-worker")
        );
        assert_eq!(
            paths.data,
            PathBuf::from("/Users/tester/.local/share/mac-worker")
        );
    }

    #[test]
    fn xdg_paths_override_home_defaults() {
        let paths = PathLayout::discover(
            None,
            &BTreeMap::from([
                ("XDG_CONFIG_HOME".into(), "/tmp/config".into()),
                ("XDG_STATE_HOME".into(), "/tmp/state".into()),
                ("XDG_CACHE_HOME".into(), "/tmp/cache".into()),
                ("XDG_DATA_HOME".into(), "/tmp/data".into()),
            ]),
            Path::new("/Users/tester"),
        )
        .unwrap();

        assert_eq!(
            paths.config,
            PathBuf::from("/tmp/config/mac-worker/config.toml")
        );
        assert_eq!(paths.state, PathBuf::from("/tmp/state/mac-worker"));
        assert_eq!(paths.cache, PathBuf::from("/tmp/cache/mac-worker"));
        assert_eq!(paths.data, PathBuf::from("/tmp/data/mac-worker"));
    }

    #[test]
    fn absent_home_is_rejected_when_xdg_values_are_not_complete() {
        let error = PathLayout::discover(None, &BTreeMap::new(), Path::new(""))
            .expect_err("an empty home directory must not produce guessed paths");

        assert!(matches!(error, WorkerError::Config(_)));
    }

    #[test]
    fn duplicate_worker_names_are_rejected() {
        let config = Config::parse(include_str!("../config.example.toml")).unwrap();
        let mut duplicate = config.clone();
        duplicate.workers.push(duplicate.workers[0].clone());

        assert!(matches!(duplicate.validate(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn slots_other_than_one_are_rejected_in_v1() {
        let mut config = Config::parse(include_str!("../config.example.toml")).unwrap();
        config.workers[0].slots = 2;

        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn invalid_versions_and_empty_inventories_are_rejected() {
        for contents in ["version = 2\nworkers = []", "version = 1\nworkers = []"] {
            let config = Config::parse(contents).unwrap();
            assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
        }
    }

    #[test]
    fn invalid_worker_fields_are_rejected() {
        for contents in [
            "version = 1\n[[workers]]\nname = \"\"\nssh = \"mac1\"\nslots = 1",
            "version = 1\n[[workers]]\nname = \"mini 1\"\nssh = \"mac1\"\nslots = 1",
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac;1\"\nslots = 1",
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\", \"darwin-arm64\"]",
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\nremote_binary = \"/tmp/worker\"",
        ] {
            let config = Config::parse(contents).unwrap();
            assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
        }
    }

    #[test]
    fn ssh_destinations_must_start_with_an_ascii_alphanumeric_character() {
        // This catches accepting a destination that OpenSSH can interpret as
        // another command-line option before its operand boundary.
        for destination in ["-V", "-Efoo"] {
            let config = Config::parse(&format!(
                "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = {destination:?}\nslots = 1"
            ))
            .unwrap();

            assert!(
                matches!(config.validate(), Err(WorkerError::Config(_))),
                "destination {destination:?} must be rejected"
            );
        }
    }

    #[test]
    fn duplicate_ssh_destinations_are_rejected() {
        let config = Config::parse(
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n[[workers]]\nname = \"mini-2\"\nssh = \"mac1\"\nslots = 1",
        )
        .unwrap();

        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn config_load_validates_toml_and_supports_name_lookup() {
        let directory = tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        fs::write(&config_path, include_str!("../config.example.toml")).unwrap();

        let config = Config::load(&config_path).unwrap();

        assert_eq!(config.worker("mini-2").unwrap().ssh, "mac2");
        assert!(config.worker("not-configured").is_none());
    }

    #[test]
    fn public_diag_json_error_event_keeps_exit_and_rejects_oversized_internal_payload() {
        let planted_path = "/Users/alice/PLANTED_PUBLIC_PATH";
        let planted_secret = "PLANTED_PUBLIC_SECRET";
        let error = WorkerError::Protocol(format!(
            "REMOTE_OUTCOME_INVALID: {}\0{planted_path} {planted_secret}",
            "X".repeat(5000)
        ));
        assert_eq!(error.exit_code(), 70);

        let mut stdout = Vec::new();
        super::write_json_error_event(&mut stdout, &error)
            .expect("oversized internal detail must not become an I/O write failure");
        assert_eq!(stdout.last().copied(), Some(b'\n'));
        assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);

        let event: crate::job::JsonEvent =
            serde_json::from_slice(stdout.strip_suffix(b"\n").unwrap()).unwrap();
        match event {
            crate::job::JsonEvent::Error {
                protocol_version,
                code,
                message,
            } => {
                assert_eq!(protocol_version, crate::protocol::PROTOCOL_VERSION);
                assert_eq!(code, "REMOTE_OUTCOME_INVALID");
                assert_eq!(message, "protocol error");
            }
            other => panic!("expected a JSON error event, got {other:?}"),
        }

        let text = String::from_utf8(stdout).unwrap();
        assert!(!text.contains(&"X".repeat(32)));
        assert!(!text.contains(planted_path));
        assert!(!text.contains(planted_secret));
        assert!(!text.as_bytes().contains(&0));
    }

    #[test]
    fn unknown_toml_fields_are_rejected() {
        let error = Config::parse(
            "version = 1\nunknown = true\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1",
        )
        .expect_err("unknown configuration fields must be rejected");

        assert!(matches!(error, WorkerError::Config(_)));
    }
}
