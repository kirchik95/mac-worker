use std::{
    collections::BTreeMap,
    ffi::OsString,
    future::Future,
    io::{self, Cursor, Read, Write},
    path::PathBuf,
    pin::Pin,
    time::{SystemTime, UNIX_EPOCH},
};

use agent_settings::{AgentSettingsGetRequest, AgentSettingsSaveRequest, NativeAgentSettingsStore};
use cli::{Cli, Command, ControllerCommand, HiddenComponent, HostCommand, TaskCommand};
use client_state::ClientStateStore;
use config::{Config, WorkerEntry};
use dashboard::command::{
    DashboardCommandRequest, SystemBrowserOpener, SystemDashboardLauncher, run_dashboard,
};
use doctor::{DoctorRequest, DoctorService};
use error::WorkerError;
use gc::{GcReport, GcRequest, HostGc};
use git_transport::{
    GitServerExecutor, HostGitService, ReceivePackComponents, SystemGitServerExecutor,
    UploadPackComponents,
};
use host_store::HostStore;
use install::Installer;
use job::{
    CancelRequest, CommandSpec, FleetReconcileJobResult, FleetReconcileRequest,
    FleetReconcileResponse, HostControlError, JsonEvent, LeaseAcquireRequest, LogChunkRequest,
    LogChunkResponse, ResolveOrAbandonRequest, StatusLogsRequest, StatusRequest, SubmitRequest,
};
use job_service::JobService;
#[cfg(test)]
use laptop::EmptyLaptopProcessTable;
#[cfg(not(test))]
use laptop::SystemLaptopProcessTable;
use laptop::{LaptopProcessTable, format_outdated_laptop_cli, outdated_laptop_cli};
use lease::{AdmissionFacts, LeaseService};
use output::CommandOutput;
use paths::PathLayout;
use probe::ProbeCollector;
use process::ProcessRunner;
use protocol::{PROTOCOL_VERSION, SetupReport, SetupWarning, SetupWarningCode, WorkersReport};
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
use task_client::{TaskClient, TaskListFilter, TaskSubmitRequest, WaitSelector};
use task_store::{
    TaskCancelRequest, TaskCloseRequest, TaskDiffRequest, TaskPrebindRequest, TaskPrepareRequest,
    TaskSessionRequest, TaskStatusRequest, TaskStore,
};
use transfer::{
    HostTransferService, RemoteJobClient, RsyncServerExecutor, SystemRsyncServerExecutor,
    TransferIdentity,
};
use transfer_repo::TransferGc;
use transport::{SshTransport, WorkersService};
use turn::TaskTurnRequest;
use turn_runner::{DetachedRunnerExecutor, InlineRunnerExecutor, TurnRunner};

mod account_launch;
pub(crate) mod admission;
pub mod agent;
pub mod agent_facts;
pub mod agent_settings;
pub mod auth_incidents;
pub mod cli;
pub mod client_state;
pub mod config;
pub mod controller;
pub mod dag;
pub mod dashboard;
pub mod doctor;
pub mod error;
pub mod failure_receipt;
pub mod follow_turn;
pub mod gc;
pub mod git_transport;
pub mod herdr;
pub mod herdr_notify;
pub mod herdr_reporter;
pub mod host_store;
pub mod inputs;
pub mod install;
pub mod job;
pub mod job_service;
pub mod keychain;
pub mod laptop;
pub mod lease;
pub mod manifest;
pub mod onboarding;
pub mod outbox;
pub mod output;
pub mod paths;
pub mod prepare_turn;
pub mod prepared_followup;
pub mod prepared_submit;
pub mod probe;
pub mod process;
pub mod project;
pub mod project_config;
pub mod project_readiness;
pub mod project_state;
pub mod protocol;
pub mod redaction;
pub mod remote_snapshot;
pub mod requirements;
pub mod rooted_fs;
pub mod run;
pub(crate) mod runner_log;
pub mod scheduler;
pub mod scheduler_adapter;
pub mod skills;
pub mod snapshot;
pub mod supervisor;
pub mod task;
pub mod task_client;
pub mod task_store;
pub mod task_view;
pub mod transfer;
pub mod transfer_repo;
pub mod transport;
pub mod turn;
pub mod turn_log;
pub mod turn_runner;

#[cfg(test)]
pub(crate) mod test_sync;

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

    pub(crate) fn home(&self) -> &std::path::Path {
        &self.home
    }

    pub(crate) fn environment(&self) -> &BTreeMap<OsString, OsString> {
        &self.environment
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
        Command::Init {
            destination,
            name,
            agent,
            env_profile,
        } => {
            let paths = discover_paths(cli.config, runtime)?;
            onboarding::initialize(
                runner,
                &paths.config,
                onboarding::InitRequest {
                    destination,
                    name,
                    agent,
                    env_profile,
                },
            )
            .map(CommandOutput::Init)
        }
        Command::Setup { hosts } => {
            let config = load_config(cli.config, runtime)?;
            config.require_local_inventory()?;
            let selected = select_workers(&config, &hosts)?;
            let current_exe = std::env::current_exe()?;
            let workers = selected
                .into_iter()
                .map(|worker| Installer::new(runner).install(&current_exe, &worker))
                .collect();
            Ok(CommandOutput::Setup(SetupReport {
                protocol_version: PROTOCOL_VERSION,
                workers,
                warnings: laptop_setup_warnings(),
            }))
        }
        Command::Doctor { project, includes } => {
            let paths = discover_paths(cli.config, runtime)?;
            let config = Config::load(&paths.config)?;
            config.require_local_inventory()?;
            let project = match project {
                Some(project) => project,
                None => runtime.current_dir()?,
            };
            let service = DoctorService {
                runner,
                config: &config,
                paths: &paths,
                laptop_processes: laptop_process_table(),
                installed_binary_mtime: installed_binary_mtime(),
            };
            Ok(CommandOutput::Doctor(service.inspect(DoctorRequest {
                project,
                cli_includes: includes,
            })?))
        }
        Command::Workers {
            refresh,
            clear_auth_incidents,
        } => {
            let inspection = inspect_configured_workers(
                cli.config,
                runtime,
                runner,
                refresh,
                clear_auth_incidents,
            )?;
            Ok(CommandOutput::Workers(inspection.report))
        }
        Command::Gc { .. } => Err(WorkerError::Protocol(
            "public gc requires the stdio execution boundary".into(),
        )),
        Command::Dashboard { .. } => Err(WorkerError::Protocol(
            "public dashboard requires the stdio execution boundary".into(),
        )),
        Command::Run { .. } => Err(WorkerError::Protocol(
            "public run requires the stdio execution boundary".into(),
        )),
        Command::Task { .. } => Err(WorkerError::Protocol(
            "public task commands require the stdio execution boundary".into(),
        )),
        Command::Skills { command } => Ok(CommandOutput::Plain {
            text: skills::execute(command, &runtime.current_dir()?)?,
        }),
        Command::Controller { .. } => Err(WorkerError::Protocol(
            "public controller commands require the stdio execution boundary".into(),
        )),
        Command::Runner { .. } => Err(WorkerError::Protocol(
            "hidden runner requires the stdio execution boundary".into(),
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
            command: HostCommand::Gc,
        } => Err(WorkerError::Protocol(
            "host gc requires the stdio execution boundary".into(),
        )),
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
            command: HostCommand::StatusLogs,
        } => Err(WorkerError::Protocol(
            "host status-logs requires the stdio execution boundary".into(),
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
        Command::Host {
            command: HostCommand::MigrateLayout,
        } => Err(WorkerError::Protocol(
            "host migrate-layout requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::CompleteProtocolUpgrade { .. },
        } => Err(WorkerError::Protocol(
            "host complete-protocol-upgrade requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::CompleteUnverifiedRollback { .. },
        } => Err(WorkerError::Protocol(
            "host complete-unverified-rollback requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::SetSlots { .. },
        } => Err(WorkerError::Protocol(
            "host set-slots requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::RefreshFacts { .. },
        } => Err(WorkerError::Protocol(
            "host refresh-facts requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::ReceivePack { .. },
        } => Err(WorkerError::Protocol(
            "host receive-pack requires the binary stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::UploadPack { .. },
        } => Err(WorkerError::Protocol(
            "host upload-pack requires the binary stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::TaskPrepare,
        } => Err(WorkerError::Protocol(
            "host task-prepare requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::TaskStatus,
        } => Err(WorkerError::Protocol(
            "host task-status requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::TaskDiff,
        } => Err(WorkerError::Protocol(
            "host task-diff requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::TaskClose,
        } => Err(WorkerError::Protocol(
            "host task-close requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::TaskSession,
        } => Err(WorkerError::Protocol(
            "host task-session requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::TaskPrebind,
        } => Err(WorkerError::Protocol(
            "host task-prebind requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::TaskCancel,
        } => Err(WorkerError::Protocol(
            "host task-cancel requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::TaskTurn,
        } => Err(WorkerError::Protocol(
            "host task-turn requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::AgentSettingsGet,
        } => Err(WorkerError::Protocol(
            "host agent-settings-get requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::AgentSettingsSet,
        } => Err(WorkerError::Protocol(
            "host agent-settings-set requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::FollowTurn { .. },
        } => Err(WorkerError::Protocol(
            "host follow-turn requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::Outbox { .. },
        } => Err(WorkerError::Protocol(
            "host outbox requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::OutboxRetry { .. },
        } => Err(WorkerError::Protocol(
            "host outbox-retry requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::ControllerRpc,
        } => Err(WorkerError::Protocol(
            "host controller-rpc requires the stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::ControllerReceivePack { .. },
        } => Err(WorkerError::Protocol(
            "host controller-receive-pack requires the binary stdio execution boundary".into(),
        )),
        Command::Host {
            command: HostCommand::ControllerUploadPack { .. },
        } => Err(WorkerError::Protocol(
            "host controller-upload-pack requires the binary stdio execution boundary".into(),
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
    config.require_local_inventory()?;
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
    if let Command::Dashboard {
        port,
        no_open,
        no_facts_refresh,
        controller_viewer,
    } = cli.command
    {
        return run_dashboard_command(
            cli.config,
            runtime,
            DashboardCommandRequest {
                port,
                no_open,
                refresh_stale_facts: !no_facts_refresh,
            },
            controller_viewer,
            stdout,
            stderr,
        );
    }
    if matches!(cli.command, Command::Run { .. } | Command::Logs { .. }) {
        return run_public_streaming_command(cli, runner, runtime, stdout, stderr);
    }
    if matches!(&cli.command, Command::Task { .. } | Command::Runner { .. }) {
        return run_task_command(cli, runner, runtime, stdout, stderr);
    }
    if matches!(
        &cli.command,
        Command::Controller {
            command: ControllerCommand::Run
        }
    ) {
        return run_controller_command(cli.config, runtime, runner, stdout, stderr);
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
    request: DashboardCommandRequest,
    controller_viewer: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let config = std::sync::Arc::new(Config::load(&paths.config)?);
        let async_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(WorkerError::Io)?;
        if controller_viewer {
            return async_runtime.block_on(run_local_dashboard(
                config,
                paths,
                runtime,
                request,
                viewer_shutdown_signal(),
                stdout,
                stderr,
            ));
        }
        if config.controller.enabled {
            return async_runtime.block_on(
                crate::dashboard::tunnel::run_controller_dashboard_tunnel(
                    config.as_ref(),
                    runtime,
                    request.port,
                    request.no_open,
                    !request.refresh_stale_facts,
                    stdout,
                    stderr,
                ),
            );
        }
        async_runtime.block_on(run_local_dashboard(
            config,
            paths,
            runtime,
            request,
            Box::pin(async {
                let _ = tokio::signal::ctrl_c().await;
            }),
            stdout,
            stderr,
        ))
    })();

    match result {
        Ok(()) => 0,
        Err(error) => {
            write_error(stderr, &error);
            error.exit_code()
        }
    }
}

fn viewer_shutdown_signal() -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async {
        let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
        // Independent thread, not spawn_blocking: Tokio runtime shutdown waits
        // forever for started blocking tasks. Dropping this oneshot lets SIGINT,
        // SIGHUP, and SIGTERM finish dashboard teardown while the pipe stays open.
        let _stdin_thread = std::thread::Builder::new()
            .name("dashboard-viewer-stdin".into())
            .spawn(move || {
                let mut buffer = [0u8; 256];
                loop {
                    let read = unsafe {
                        libc::read(libc::STDIN_FILENO, buffer.as_mut_ptr().cast(), buffer.len())
                    };
                    if read <= 0 {
                        let _ = eof_tx.send(());
                        return;
                    }
                }
            });
        let mut hangup =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).ok();
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                match hangup.as_mut() {
                    Some(signal) => {
                        signal.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {}
            _ = async {
                match terminate.as_mut() {
                    Some(signal) => {
                        signal.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {}
            _ = eof_rx => {}
        }
    })
}

async fn run_local_dashboard(
    config: std::sync::Arc<Config>,
    paths: PathLayout,
    runtime: &RuntimeContext,
    request: DashboardCommandRequest,
    shutdown_signal: Pin<Box<dyn Future<Output = ()> + Send>>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), WorkerError> {
    let client_state = std::sync::Arc::new(ClientStateStore::open(&paths.state)?);
    let launch_directory = runtime.current_dir().ok();
    let launcher = SystemDashboardLauncher::from_system_with_config(
        config,
        client_state,
        launch_directory.as_deref(),
        crate::dashboard::service::DashboardConfig {
            refresh_stale_facts: request.refresh_stale_facts,
        },
        paths,
    );
    let opener = SystemBrowserOpener;
    run_dashboard(request, &launcher, &opener, shutdown_signal, stdout, stderr).await?;
    Ok(())
}

fn run_controller_command(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let config = load_controller_process_config(&paths)?;
        let state_root = paths.controller_state_root();
        let _leader = crate::controller::ControllerLeader::acquire(&state_root)?;
        let store = crate::controller::ControllerStore::open(&state_root)?;
        let client_state = ClientStateStore::open(&paths.state)?;
        let handler =
            crate::controller::TaskSubmitHandler::new(runner, &config, &paths, &client_state);
        crate::controller::tick_controller_leader(&store, &handler)?;
        let client = TaskClient::new(
            runner,
            &config,
            &paths,
            &client_state,
            &DETACHED_TASK_EXECUTOR,
        );
        client.client_state.recover_replacement_residue()?;
        client.tick_selected_recovery()?;
        writeln!(stdout, "controller leader acquired").map_err(WorkerError::Io)?;
        stdout.flush().map_err(WorkerError::Io)?;
        let async_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(WorkerError::Io)?;
        async_runtime.block_on(async {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    _ = interval.tick() => {
                        let _ = crate::controller::tick_controller_leader(&store, &handler);
                        let _ = client.tick_selected_recovery();
                    }
                }
            }
        });
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

fn run_host_controller_rpc(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let config = load_controller_process_config(&paths)?;
        crate::controller::serve_rpc_with_runtime(
            &paths,
            &config,
            runner,
            stdin,
            stdout,
            crate::controller::ControllerFault::None,
        )?;
        Ok(())
    })();

    match result {
        Ok(()) => 0,
        Err(error) => match crate::controller::encode_json_frame(&versioned_host_error(&error)) {
            Ok(frame) if stdout.write_all(&frame).is_ok() && stdout.flush().is_ok() => {
                error.exit_code()
            }
            _ => {
                write_error(stderr, &error);
                crate::error::ExitKind::Io as u8
            }
        },
    }
}

static DETACHED_TASK_EXECUTOR: DetachedRunnerExecutor = DetachedRunnerExecutor;
static INLINE_TASK_EXECUTOR: InlineRunnerExecutor = InlineRunnerExecutor;

fn run_task_command(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let json = cli.json;
    let result = (|| -> Result<u8, WorkerError> {
        let paths = discover_paths(cli.config, runtime)?;
        let config = Config::load(&paths.config)?;
        if let Command::Task {
            command:
                TaskCommand::Batch {
                    file,
                    preview: true,
                    ..
                },
        } = &cli.command
        {
            let report = crate::task_client::preview_batch_plan(
                runner,
                &config,
                file,
                &runtime.current_dir()?,
            )?;
            if json {
                write_json_line(stdout, &report)?;
            } else {
                writeln!(stdout, "batch preview: {} tasks", report.tasks.len())?;
                writeln!(stdout, "{}", report.dag.message)?;
                if report.setup.present {
                    writeln!(stdout, "setup recipe present")?;
                }
                for issue in &report.issues {
                    writeln!(
                        stdout,
                        "{} {}: {}",
                        issue.severity, issue.kind, issue.message
                    )?;
                }
                stdout.flush()?;
            }
            return Ok(if report.has_config_errors() { 1 } else { 0 });
        }
        if config.controller.enabled {
            return run_enabled_controller_task(
                cli.command,
                runner,
                runtime,
                &paths,
                &config,
                json,
                stdout,
            );
        }
        let client_state = ClientStateStore::open(&paths.state)?;
        let command = cli.command;
        let inline = matches!(
            &command,
            Command::Task {
                command: TaskCommand::Submit { wait: true, .. }
            } | Command::Task {
                command: TaskCommand::Say { wait: true, .. }
            }
        );
        let executor: &'static dyn turn_runner::RunnerExecutor = if inline {
            &INLINE_TASK_EXECUTOR
        } else {
            &DETACHED_TASK_EXECUTOR
        };
        let notifier = herdr_notifier_socket(&config, runtime);
        let client = TaskClient::new(runner, &config, &paths, &client_state, executor)
            .with_herdr_notifier(notifier.clone());

        match command {
            Command::Runner {
                task_id,
                turn_id,
                slot_token,
            } => {
                let task_id = task_id
                    .expose()
                    .parse::<crate::task::TaskId>()
                    .map_err(|_| WorkerError::Protocol("invalid runner task ID".into()))?;
                let turn_id = turn_id
                    .expose()
                    .parse::<crate::task::TurnId>()
                    .map_err(|_| WorkerError::Protocol("invalid runner turn ID".into()))?;
                let slot_token = slot_token
                    .as_ref()
                    .map(|token| {
                        token
                            .expose()
                            .parse::<uuid::Uuid>()
                            .map_err(|_| WorkerError::Protocol("invalid runner slot token".into()))
                    })
                    .transpose()?;
                let outcome = TurnRunner::new(runner, &config, &paths, &client_state, executor)
                    .with_notifier(notifier)
                    .with_slot_token(slot_token)
                    .run_detached(task_id, turn_id)?;
                Ok(outcome.exit_code())
            }
            Command::Task { command } => {
                run_task_subcommand(command, &client, runtime, json, stdout, stderr)
            }
            _ => Err(WorkerError::Protocol(
                "task dispatcher received a non-task command".into(),
            )),
        }
    })();

    match result {
        Ok(code) => code,
        Err(error) => {
            if json {
                match write_json_error_event(stdout, &error) {
                    Ok(()) => error.exit_code(),
                    Err(_) => crate::error::ExitKind::Io as u8,
                }
            } else {
                write_public_diagnostic(stderr, &error);
                error.exit_code()
            }
        }
    }
}

fn run_task_subcommand(
    command: TaskCommand,
    client: &TaskClient<'_>,
    runtime: &RuntimeContext,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<u8, WorkerError> {
    match command {
        TaskCommand::Submit {
            agent,
            model,
            effort,
            prompt,
            prompt_file,
            title,
            project,
            base,
            wip,
            includes,
            timeout,
            max_turns,
            max_budget,
            max_followups,
            close_on,
            env_profile,
            worker,
            source,
            publish,
            publish_branch,
            no_wait,
            wait,
        } => {
            validate_task_scope_options(source.as_deref(), &publish, publish_branch.as_deref())?;
            let prompt = read_prompt(prompt, prompt_file)?;
            let limits = make_task_limits(timeout, max_turns, max_budget, max_followups)?;
            let project = project.unwrap_or(runtime.current_dir()?);
            let task_agent = match agent.as_deref() {
                Some(agent) => parse_task_agent(agent)?,
                None => client.default_task_agent(&project)?,
            };
            let report = client.submit_titled(
                TaskSubmitRequest {
                    agent: task_agent,
                    model,
                    effort,
                    prompt,
                    project,
                    base,
                    wip,
                    source,
                    publish: (!publish.is_empty()).then_some(publish),
                    publish_branch,
                    cli_includes: includes,
                    limits,
                    close_policy: parse_task_close_policy(close_on.as_deref())?,
                    env_profile,
                    preference: worker.map_or(WorkerPreference::Automatic, |worker| {
                        WorkerPreference::Pinned { worker }
                    }),
                    wait_for_capacity: !no_wait,
                    attached: wait,
                    run_id: None,
                },
                title,
                stdout,
                stderr,
            )?;
            write_task_report(&report, json, stdout)?;
            Ok(if wait {
                report.exit_code().unwrap_or(0)
            } else {
                0
            })
        }
        TaskCommand::Batch {
            file,
            name,
            max_parallel,
            wait,
            preview,
        } => {
            if preview {
                unreachable!("batch --preview is handled before client state opens");
            } else {
                let report = client.batch(&file, name, max_parallel, stdout)?;
                write_run_report(&report, json, stdout)?;
                if wait {
                    let waited = client.wait(WaitSelector::Run(report.run_id()), None)?;
                    if json {
                        write_json_line(
                            stdout,
                            &serde_json::json!({
                                "protocol_version": PROTOCOL_VERSION,
                                "run_id": report.run_id().to_string(),
                                "task_ids": waited.task_ids(),
                                "exit_code": waited.exit_code(),
                            }),
                        )?;
                    }
                    Ok(waited.exit_code())
                } else {
                    Ok(0)
                }
            }
        }
        TaskCommand::List {
            run,
            state,
            outcome,
            full,
        } => {
            let filter = TaskListFilter {
                run_id: run
                    .as_deref()
                    .map(|value| client.resolve_run(value))
                    .transpose()?,
                state: state.as_deref().map(parse_task_state).transpose()?,
                outcome: outcome
                    .as_deref()
                    .map(parse_task_outcome_kind)
                    .transpose()?
                    .map(str::to_owned),
                full,
            };
            let report = client.list(filter)?;
            write_task_list_report(&report, json, stdout)?;
            Ok(0)
        }
        TaskCommand::Status { task_id, full: _ } => {
            let report = client.status(task_id)?;
            write_task_report(&report, json, stdout)?;
            Ok(0)
        }
        TaskCommand::Logs {
            task_id,
            turn,
            follow,
            raw,
        } => {
            client.logs(task_id, turn, follow, raw, stdout, stderr)?;
            Ok(0)
        }
        TaskCommand::Diff { task_id, stat } => {
            client.diff(task_id, stat, stdout)?;
            Ok(0)
        }
        TaskCommand::Say {
            task_id,
            message,
            message_file,
            wait,
        } => {
            let message = read_prompt(message, message_file)?;
            let report = client.say(task_id, message, wait, stdout, stderr)?;
            write_task_report(&report, json, stdout)?;
            Ok(if wait {
                report.exit_code().unwrap_or(0)
            } else {
                0
            })
        }
        TaskCommand::Cancel { task_id } => {
            let report = client.cancel(task_id)?;
            write_task_report(&report, json, stdout)?;
            Ok(0)
        }
        TaskCommand::Result { task_id } => {
            let report = client.result(task_id)?;
            write_task_result_report(&report, json, stdout)?;
            Ok(0)
        }
        TaskCommand::Fetch { task_id } => {
            let report = client.fetch(task_id)?;
            write_fetch_report(&report, json, stdout)?;
            Ok(0)
        }
        TaskCommand::Close { task_id, discard } => {
            let report = client.close(task_id, discard)?;
            write_task_report(&report, json, stdout)?;
            Ok(0)
        }
        TaskCommand::PublishRetry { task_id } => {
            let deliveries = client.publish_retry(task_id)?;
            write_publish_retry_report(task_id, &deliveries, json, stdout)?;
            Ok(0)
        }
        TaskCommand::Wait {
            task_id,
            run,
            timeout,
        } => {
            let selector = match (task_id, run) {
                (Some(task_id), None) => WaitSelector::Task(task_id),
                (None, Some(run)) => WaitSelector::Run(client.resolve_run(&run)?),
                _ => {
                    return Err(WorkerError::Task {
                        code: "TASK_CONFIG_INVALID",
                        message: "wait requires exactly one of --task-id or --run".into(),
                    });
                }
            };
            let report = client.wait(selector, timeout)?;
            write_wait_report(&report, json, stdout)?;
            Ok(report.exit_code())
        }
        TaskCommand::Reconcile => {
            let report = client.operator_reconcile()?;
            if json {
                write_json_line(
                    stdout,
                    &serde_json::json!({
                        "protocol_version": PROTOCOL_VERSION,
                        "replaced_runners": report.replaced_runners(),
                        "started_runners": report.started_runners(),
                        "repaired_rows": report.repaired_rows(),
                        "unverifiable_rows": report.unverifiable_rows(),
                    }),
                )?;
            } else {
                writeln!(
                    stdout,
                    "runners: {} replaced, {} started; task rows: {} repaired, {} unverifiable",
                    report.replaced_runners(),
                    report.started_runners(),
                    report.repaired_rows(),
                    report.unverifiable_rows()
                )?;
                stdout.flush()?;
            }
            Ok(0)
        }
    }
}

fn read_prompt(
    prompt: Option<String>,
    prompt_file: Option<PathBuf>,
) -> Result<String, WorkerError> {
    match (prompt, prompt_file) {
        (Some(prompt), None) => Ok(prompt),
        (None, Some(path)) => std::fs::read_to_string(path).map_err(WorkerError::Io),
        _ => Err(WorkerError::Task {
            code: "TASK_CONFIG_INVALID",
            message: "exactly one prompt source is required".into(),
        }),
    }
}

fn validate_task_scope_options(
    source: Option<&str>,
    publish: &[String],
    publish_branch: Option<&str>,
) -> Result<(), WorkerError> {
    if source.is_some_and(|source| !matches!(source, "local" | "origin")) {
        return Err(WorkerError::Task {
            code: "TASK_CONFIG_INVALID",
            message: "source must be local or origin (TASK_CONFIG_INVALID)".into(),
        });
    }
    if publish
        .iter()
        .any(|publish| !matches!(publish.as_str(), "fetch" | "push"))
    {
        return Err(WorkerError::Task {
            code: "TASK_CONFIG_INVALID",
            message: "publish must be fetch or push (TASK_CONFIG_INVALID)".into(),
        });
    }
    if publish
        .iter()
        .filter(|publish| publish.as_str() == "fetch")
        .count()
        > 1
        || publish
            .iter()
            .filter(|publish| publish.as_str() == "push")
            .count()
            > 1
    {
        return Err(WorkerError::Task {
            code: "TASK_CONFIG_INVALID",
            message: "publish modes must be unique (TASK_CONFIG_INVALID)".into(),
        });
    }
    if publish_branch.is_some() && !publish.iter().any(|mode| mode == "push") {
        return Err(WorkerError::Task {
            code: "TASK_CONFIG_INVALID",
            message: "publish_branch requires publish push (TASK_CONFIG_INVALID)".into(),
        });
    }
    Ok(())
}

fn parse_task_agent(value: &str) -> Result<crate::agent::AgentKind, WorkerError> {
    match value {
        "codex" => Ok(crate::agent::AgentKind::Codex),
        "claude" => Ok(crate::agent::AgentKind::Claude),
        "cursor" => Ok(crate::agent::AgentKind::Cursor),
        "opencode" => Ok(crate::agent::AgentKind::Opencode),
        _ => Err(WorkerError::Task {
            code: "AGENT_UNSUPPORTED",
            message: "unknown agent".into(),
        }),
    }
}

fn parse_task_close_policy(value: Option<&str>) -> Result<crate::task::ClosePolicy, WorkerError> {
    match value.unwrap_or("done") {
        "done" => Ok(crate::task::ClosePolicy::Done),
        "never" => Ok(crate::task::ClosePolicy::Never),
        _ => Err(WorkerError::Task {
            code: "TASK_CONFIG_INVALID",
            message: "close_on must be done or never".into(),
        }),
    }
}

fn make_task_limits(
    timeout: Option<std::time::Duration>,
    max_turns: Option<u32>,
    max_budget: Option<u64>,
    max_followups: Option<u32>,
) -> Result<crate::task::TaskLimits, WorkerError> {
    let defaults = crate::task::TaskLimits::default();
    let timeout_millis = timeout
        .map(|timeout| {
            u64::try_from(timeout.as_millis()).map_err(|_| WorkerError::Task {
                code: "TASK_CONFIG_INVALID",
                message: "timeout is too large".into(),
            })
        })
        .transpose()?
        .unwrap_or(defaults.turn.timeout_millis);
    let turn = crate::agent::TurnLimits::new(
        timeout_millis,
        max_turns.or(defaults.turn.max_turns),
        max_budget.or(defaults.turn.max_budget_usd_cents),
    )
    .map_err(|error| WorkerError::task("TASK_CONFIG_INVALID", error.to_string()))?;
    crate::task::TaskLimits::new(turn, max_followups.unwrap_or(defaults.max_followups))
}

/// Accepts either the serialized kind (`needs_input`) or its dashed spelling
/// (`needs-input`), so the orchestrator loop can use whichever it already has.
fn parse_task_outcome_kind(value: &str) -> Result<&'static str, WorkerError> {
    let normalized = value.replace('-', "_");
    crate::task::TaskOutcome::KINDS
        .into_iter()
        .find(|kind| *kind == normalized)
        .ok_or(WorkerError::Task {
            code: "TASK_CONFIG_INVALID",
            message: "unknown task outcome".into(),
        })
}

fn parse_task_state(value: &str) -> Result<crate::task::TaskState, WorkerError> {
    match value {
        "queued" => Ok(crate::task::TaskState::Queued),
        "active" => Ok(crate::task::TaskState::Active),
        "open" => Ok(crate::task::TaskState::Open),
        "closed" => Ok(crate::task::TaskState::Closed),
        "abandoned" => Ok(crate::task::TaskState::Abandoned),
        "lost" => Ok(crate::task::TaskState::Lost),
        _ => Err(WorkerError::Task {
            code: "TASK_CONFIG_INVALID",
            message: "unknown task state".into(),
        }),
    }
}

pub(crate) fn write_task_report(
    report: &task_client::TaskReport,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    if json {
        let mut response = serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "task_id": report.task_id().to_string(),
            "run_id": report.run_id().map(|id| id.to_string()),
            "status": report.status(),
            "runner": report.runner(),
            "events": report.events(),
            "exit_code": report.exit_code(),
        });
        if !report.warnings().is_empty() {
            response["warnings"] = serde_json::json!(report.warnings());
        }
        if let Some(delivery) = report.delivery() {
            response["delivery"] = serde_json::json!(delivery);
        }
        if !report.deliveries().is_empty() {
            response["deliveries"] = serde_json::json!(report.deliveries());
        }
        if report.freshness() == crate::task_view::TaskFreshness::Stale {
            response["freshness"] = serde_json::json!("stale");
            if let Some(observed_at) = report.observed_at_millis() {
                response["observed_at_millis"] = serde_json::json!(observed_at);
            }
        }
        insert_receipt_fields(&mut response, report.failure_receipt());
        write_json_line(stdout, &response)
    } else {
        let worker = report.status().worker().unwrap_or("unassigned");
        writeln!(
            stdout,
            "task {}: {} ({worker})",
            report.task_id(),
            task_state_name(report.status().state())
        )?;
        for warning in report.warnings() {
            writeln!(stdout, "warning: {warning}")?;
        }
        for delivery in report.deliveries() {
            write_delivery_line(
                stdout,
                delivery,
                report.freshness(),
                report.observed_at_millis(),
            )?;
        }
        if let Some(receipt) = report.failure_receipt() {
            writeln!(
                stdout,
                "receipt: stage={} residual={}",
                receipt.stage(),
                receipt.residual().join(",")
            )?;
        }
        stdout.flush()?;
        Ok(())
    }
}

fn write_publish_retry_report(
    task_id: crate::task::TaskId,
    deliveries: &[crate::task::OriginDelivery],
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    if json {
        write_json_line(
            stdout,
            &serde_json::json!({
                "protocol_version": PROTOCOL_VERSION,
                "task_id": task_id.to_string(),
                "deliveries": deliveries,
            }),
        )
    } else {
        if deliveries.is_empty() {
            writeln!(stdout, "task {task_id}: no origin deliveries")?;
        }
        for delivery in deliveries {
            writeln!(
                stdout,
                "delivery: {} turn={} attempt={} {}",
                delivery.state().as_str(),
                delivery.turn_id(),
                delivery.attempt(),
                delivery.oid()
            )?;
        }
        stdout.flush()?;
        Ok(())
    }
}

pub(crate) fn write_task_list_report(
    report: &task_client::TaskListReport,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    if json {
        write_json_line(
            stdout,
            &crate::task_view::TaskListJson::new(PROTOCOL_VERSION, report.projection().clone()),
        )
    } else {
        for task in report.tasks() {
            if let Some(blocking_code) = task.blocking_code.as_deref() {
                let blocking = if blocking_code.contains("HOST_IO") && task.stage.is_some() {
                    format_host_io_blocking(
                        blocking_code,
                        task.stage.as_deref(),
                        task.residual.as_deref(),
                    )
                } else {
                    blocking_code.to_owned()
                };
                writeln!(
                    stdout,
                    "{}: {} ({}) blocking: {}{}",
                    task.task_id,
                    task_state_name(task.state),
                    task.worker.as_deref().unwrap_or("unassigned"),
                    blocking,
                    crate::task_view::format_list_push_suffix(task).unwrap_or_default()
                )?;
            } else {
                let mut line = format!(
                    "{}: {} ({})",
                    task.task_id,
                    task_state_name(task.state),
                    task.worker.as_deref().unwrap_or("unassigned")
                );
                if let Some(suffix) = crate::task_view::format_list_push_suffix(task) {
                    line.push_str(&suffix);
                }
                writeln!(stdout, "{line}")?;
            }
        }
        stdout.flush()?;
        Ok(())
    }
}

pub(crate) fn write_task_result_report(
    report: &task_client::TaskResultReport,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    if json {
        let mut value = serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "task_id": report.task_id().to_string(),
            "status": report.status(),
            "branch": report.branch(),
            "fetch": report.fetch_instruction(),
        });
        insert_receipt_fields(&mut value, report.failure_receipt());
        if !report.deliveries().is_empty() {
            value["deliveries"] = serde_json::json!(report.deliveries());
        }
        if let Some(delivery) = report.last_delivery() {
            value["delivery"] = serde_json::json!(delivery);
        }
        if report.freshness() == crate::task_view::TaskFreshness::Stale {
            value["freshness"] = serde_json::json!("stale");
            if let Some(observed_at) = report.observed_at_millis() {
                value["observed_at_millis"] = serde_json::json!(observed_at);
            }
        }
        write_json_line(stdout, &value)
    } else {
        writeln!(
            stdout,
            "task {}: {}",
            report.task_id(),
            task_state_name(report.status().state())
        )?;
        if let Some(summary) = report.status().summary() {
            writeln!(stdout, "summary: {summary}")?;
        }
        if !report.status().questions().is_empty() {
            writeln!(
                stdout,
                "questions: {}",
                render_questions(report.status().questions())
            )?;
        }
        if !report.status().files_changed().is_empty() {
            writeln!(
                stdout,
                "files: {}",
                report.status().files_changed().join(", ")
            )?;
        }
        writeln!(stdout, "branch: {}", report.branch())?;
        writeln!(stdout, "fetch: {}", report.fetch_instruction())?;
        if let Some(delivery) = report.last_delivery() {
            write_delivery_line(
                stdout,
                delivery,
                report.freshness(),
                report.observed_at_millis(),
            )?;
        }
        if let Some(receipt) = report.failure_receipt() {
            writeln!(
                stdout,
                "receipt: stage={} residual={}",
                receipt.stage(),
                receipt.residual().join(",")
            )?;
        }
        stdout.flush()?;
        Ok(())
    }
}

fn write_fetch_report(
    report: &task_client::FetchReport,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    if json {
        write_json_line(
            stdout,
            &serde_json::json!({
                "protocol_version": PROTOCOL_VERSION,
                "task_id": report.task_id().to_string(),
                "head_oid": report.head(),
                "local_ref": report.local_ref(),
            }),
        )
    } else {
        writeln!(
            stdout,
            "{} -> {} ({})",
            report.task_id(),
            report.local_ref(),
            report.head()
        )?;
        stdout.flush()?;
        Ok(())
    }
}

fn write_wait_report(
    report: &task_client::WaitReport,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    if json {
        write_json_line(
            stdout,
            &serde_json::json!({
                "protocol_version": PROTOCOL_VERSION,
                "task_ids": report.task_ids(),
                "exit_code": report.exit_code(),
            }),
        )
    } else {
        writeln!(stdout, "wait complete (exit {})", report.exit_code())?;
        stdout.flush()?;
        Ok(())
    }
}

fn write_run_report(
    report: &task_client::RunReport,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    if json {
        write_json_line(
            stdout,
            &serde_json::json!({
                "protocol_version": PROTOCOL_VERSION,
                "run_id": report.run_id().to_string(),
                "task_ids": report.task_ids(),
            }),
        )
    } else {
        writeln!(stdout, "run {}", report.run_id())?;
        for task_id in report.task_ids() {
            writeln!(stdout, "  task {task_id}")?;
        }
        stdout.flush()?;
        Ok(())
    }
}

fn write_json_line<T: Serialize>(stdout: &mut dyn Write, value: &T) -> Result<(), WorkerError> {
    serde_json::to_writer(&mut *stdout, value)
        .map_err(|error| WorkerError::Io(io::Error::other(error)))?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

fn insert_receipt_fields(
    value: &mut serde_json::Value,
    receipt: Option<&crate::failure_receipt::FailureReceipt>,
) {
    let Some(receipt) = receipt else {
        return;
    };
    value["stage"] = serde_json::json!(receipt.stage());
    value["residual"] = serde_json::json!(receipt.residual());
}

fn format_host_io_blocking(code: &str, stage: Option<&str>, residual: Option<&[String]>) -> String {
    let Some(stage) = stage else {
        return code.to_owned();
    };
    let residual = residual.map(|items| items.join(",")).unwrap_or_default();
    format!("{code} (stage={stage}, residual={residual})")
}

/// Renders questions for the human output, appending the answers an agent will
/// accept so a reader sees the same options the JSON output carries.
fn render_questions(questions: &[crate::agent::Question]) -> String {
    questions
        .iter()
        .map(|question| {
            if question.options().is_empty() {
                question.text().to_owned()
            } else {
                format!("{} [{}]", question.text(), question.options().join(" | "))
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn task_state_name(state: crate::task::TaskState) -> &'static str {
    match state {
        crate::task::TaskState::Queued => "queued",
        crate::task::TaskState::Active => "active",
        crate::task::TaskState::Open => "open",
        crate::task::TaskState::Closed => "closed",
        crate::task::TaskState::Abandoned => "abandoned",
        crate::task::TaskState::Lost => "lost",
    }
}

fn write_delivery_line(
    stdout: &mut dyn Write,
    delivery: &crate::task::OriginDelivery,
    freshness: crate::task_view::TaskFreshness,
    observed_at_millis: Option<u64>,
) -> Result<(), WorkerError> {
    let mut line = crate::task_view::format_delivery_line(delivery);
    if freshness == crate::task_view::TaskFreshness::Stale {
        if let Some(observed_at) = observed_at_millis {
            line.push_str(&format!(" observed={observed_at}"));
        } else {
            line.push_str(" stale");
        }
    }
    writeln!(stdout, "{line}")?;
    Ok(())
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
    if let Command::Gc { apply } = &cli.command {
        return run_gc_command(
            cli.config, runtime, runner, *apply, cli.json, stdout, stderr,
        );
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::Gc
        }
    ) {
        return run_host_gc(cli.config, runtime, runner, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::ControllerRpc
        }
    ) {
        return run_host_controller_rpc(cli.config, runtime, runner, stdin, stdout, stderr);
    }
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
    if let Command::Host {
        command:
            HostCommand::FollowTurn {
                project_id,
                worktree_id,
                job_id,
            },
    } = &cli.command
    {
        return follow_turn::run_host_follow_turn(
            cli.config,
            runtime,
            project_id.expose(),
            worktree_id.expose(),
            job_id.expose(),
            stdout,
            stderr,
        );
    }
    if let Command::Host {
        command:
            HostCommand::Outbox {
                watch,
                once,
                enable,
                write_agent,
                host_root,
                wake,
            },
    } = &cli.command
    {
        return run_host_outbox(
            cli.config,
            runtime,
            runner,
            stdout,
            *watch,
            *once,
            *enable,
            write_agent.clone(),
            host_root.clone(),
            *wake,
        );
    }
    if let Command::Host {
        command: HostCommand::OutboxRetry { task_id },
    } = &cli.command
    {
        return run_host_outbox_retry(cli.config, runtime, runner, stdout, *task_id);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::TaskPrepare
        }
    ) {
        return run_host_task_prepare(cli.config, runtime, runner, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::TaskStatus
        }
    ) {
        return run_host_task_status(cli.config, runtime, runner, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::TaskDiff
        }
    ) {
        return run_host_task_diff(cli.config, runtime, runner, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::TaskClose
        }
    ) {
        return run_host_task_close(cli.config, runtime, runner, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::TaskSession
        }
    ) {
        return run_host_task_session(cli.config, runtime, runner, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::TaskPrebind
        }
    ) {
        return run_host_task_prebind(cli.config, runtime, runner, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::TaskCancel
        }
    ) {
        return run_host_task_cancel(cli.config, runtime, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::TaskTurn
        }
    ) {
        return run_host_task_turn(cli.config, runtime, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::AgentSettingsGet
        }
    ) {
        return run_host_agent_settings_get(runtime, stdin, stdout);
    }
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::AgentSettingsSet
        }
    ) {
        return run_host_agent_settings_set(runtime, stdin, stdout);
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
            command: HostCommand::StatusLogs
        }
    ) {
        return run_host_status_logs(cli.config, runtime, stdin, stdout);
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
            command: HostCommand::MigrateLayout
        }
    ) {
        return run_host_migrate_layout(cli.config, runtime, stderr);
    }
    if let Command::Host {
        command: HostCommand::CompleteProtocolUpgrade { target },
    } = &cli.command
    {
        return run_host_complete_protocol_upgrade(cli.config, runtime, target, stderr);
    }
    if let Command::Host {
        command: HostCommand::CompleteUnverifiedRollback { target, previous },
    } = &cli.command
    {
        return run_host_complete_unverified_rollback(
            cli.config,
            runtime,
            target,
            previous.as_ref(),
            stderr,
        );
    }
    if let Command::Host {
        command: HostCommand::SetSlots { slots },
    } = cli.command
    {
        return run_host_set_slots(cli.config, runtime, slots, stderr);
    }
    if let Command::Host {
        command:
            HostCommand::RefreshFacts {
                timing,
                clear_auth_incidents,
            },
    } = cli.command
    {
        return run_host_refresh_facts(
            cli.config,
            runtime,
            runner,
            stderr,
            timing,
            clear_auth_incidents,
        );
    }
    if let Command::Host {
        command:
            HostCommand::ReceivePack {
                job_id,
                client_id,
                lease_token,
                request_fingerprint,
                path,
            },
    } = cli.command
    {
        return run_host_receive_pack(
            cli.config,
            runtime,
            job_id,
            client_id,
            lease_token,
            request_fingerprint,
            path,
            &SystemGitServerExecutor,
            stderr,
        );
    }
    if let Command::Host {
        command:
            HostCommand::UploadPack {
                task_id,
                client_id,
                path,
            },
    } = cli.command
    {
        return run_host_upload_pack(
            cli.config,
            runtime,
            task_id,
            client_id,
            path,
            &SystemGitServerExecutor,
            stderr,
        );
    }
    if let Command::Host {
        command:
            HostCommand::ControllerReceivePack {
                token,
                request_id,
                fingerprint,
                project_id,
                worktree_id,
                oid,
                path,
            },
    } = cli.command
    {
        return run_host_controller_receive_pack(
            cli.config,
            runtime,
            token,
            request_id,
            fingerprint,
            project_id,
            worktree_id,
            oid,
            path,
            &SystemGitServerExecutor,
            stderr,
        );
    }
    if let Command::Host {
        command:
            HostCommand::ControllerUploadPack {
                token,
                request_id,
                fingerprint,
                task_id,
                turn_id,
                oid,
                path,
            },
    } = cli.command
    {
        return run_host_controller_upload_pack(
            cli.config,
            runtime,
            token,
            request_id,
            fingerprint,
            task_id,
            turn_id,
            oid,
            path,
            &SystemGitServerExecutor,
            stderr,
        );
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
    if matches!(
        cli.command,
        Command::Workers {
            refresh: true,
            clear_auth_incidents: _
        }
    ) {
        return run_workers_refresh_command(cli, runner, runtime, stdout, stderr);
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

#[derive(Debug, Serialize)]
struct WorkerRefreshOutcome {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<String>,
}

struct WorkersInspection {
    report: WorkersReport,
    refresh: Option<Vec<WorkerRefreshOutcome>>,
}

fn run_workers_refresh_command(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let clear_auth_incidents = matches!(
        cli.command,
        Command::Workers {
            clear_auth_incidents: true,
            ..
        }
    );
    match inspect_configured_workers(cli.config, runtime, runner, true, clear_auth_incidents) {
        Ok(inspection) => match write_workers_refresh_output(stdout, cli.json, &inspection) {
            Ok(()) => workers_refresh_exit_status(&inspection),
            Err(error) => {
                write_error(stderr, &error);
                error.exit_code()
            }
        },
        Err(error) => {
            write_error(stderr, &error);
            error.exit_code()
        }
    }
}

fn inspect_configured_workers(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    refresh: bool,
    clear_auth_incidents: bool,
) -> Result<WorkersInspection, WorkerError> {
    let config = load_config(config_override, runtime)?;
    config.require_local_inventory()?;
    let transport = SshTransport::new(runner);
    let refresh = refresh.then(|| {
        config
            .workers
            .iter()
            .map(
                |worker| match transport.refresh_facts_cleared(worker, clear_auth_incidents) {
                    Ok(()) => WorkerRefreshOutcome {
                        ok: true,
                        error_code: None,
                        error_message: None,
                    },
                    Err(error) => refresh_failure(&error),
                },
            )
            .collect()
    });
    Ok(WorkersInspection {
        report: WorkersService::new(transport).inspect(&config),
        refresh,
    })
}

fn refresh_failure(error: &WorkerError) -> WorkerRefreshOutcome {
    match error {
        WorkerError::Transport { code, message } => WorkerRefreshOutcome {
            ok: false,
            error_code: Some((*code).to_owned()),
            error_message: Some(message.clone()),
        },
        _ => WorkerRefreshOutcome {
            ok: false,
            error_code: Some(error.public_code()),
            error_message: Some(error.to_string()),
        },
    }
}

fn workers_refresh_exit_status(inspection: &WorkersInspection) -> u8 {
    match &inspection.refresh {
        Some(outcomes) if !outcomes.is_empty() && outcomes.iter().all(|outcome| !outcome.ok) => {
            crate::error::ExitKind::Unavailable as u8
        }
        _ => 0,
    }
}

fn write_workers_refresh_output(
    stdout: &mut dyn Write,
    json: bool,
    inspection: &WorkersInspection,
) -> Result<(), WorkerError> {
    let rendered = if json {
        render_workers_refresh_json(inspection)?
    } else {
        render_workers_refresh_human(inspection)
    };
    stdout.write_all(rendered.as_bytes())?;
    stdout.write_all(b"\n")?;
    stdout.flush().map_err(WorkerError::Io)
}

fn render_workers_refresh_human(inspection: &WorkersInspection) -> String {
    let refresh = inspection.refresh.as_deref().unwrap_or(&[]);
    inspection
        .report
        .workers
        .iter()
        .zip(refresh)
        .map(|(worker, outcome)| {
            let mut rendered = CommandOutput::Workers(WorkersReport {
                protocol_version: inspection.report.protocol_version,
                workers: vec![worker.clone()],
            })
            .render_human();
            rendered.push('\n');
            rendered.push_str(&render_refresh_line(outcome));
            rendered
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_refresh_line(outcome: &WorkerRefreshOutcome) -> String {
    if outcome.ok {
        "  refresh: ok".into()
    } else {
        let code = outcome
            .error_code
            .as_deref()
            .unwrap_or("REFRESH_FACTS_FAILED");
        let message = outcome
            .error_message
            .as_deref()
            .unwrap_or("worker fact refresh failed");
        format!("  refresh: failed [{code}]: {message}")
    }
}

fn render_workers_refresh_json(inspection: &WorkersInspection) -> Result<String, WorkerError> {
    #[derive(Serialize)]
    struct Report<'a> {
        kind: &'static str,
        protocol_version: u32,
        workers: Vec<Worker<'a>>,
    }
    #[derive(Serialize)]
    struct Worker<'a> {
        #[serde(flatten)]
        health: &'a crate::protocol::WorkerHealth,
        #[serde(skip_serializing_if = "Option::is_none")]
        refresh: Option<&'a WorkerRefreshOutcome>,
    }

    let refresh = inspection.refresh.as_deref();
    let payload = Report {
        kind: "workers",
        protocol_version: inspection.report.protocol_version,
        workers: inspection
            .report
            .workers
            .iter()
            .enumerate()
            .map(|(index, health)| Worker {
                health,
                refresh: refresh.and_then(|outcomes| outcomes.get(index)),
            })
            .collect(),
    };
    serde_json::to_string(&payload).map_err(|error| {
        WorkerError::Protocol(format!("failed to serialize command output: {error}"))
    })
}

#[derive(Debug, Serialize)]
struct GcWorkerReport {
    worker: String,
    report: GcReport,
}

#[derive(Debug, Serialize)]
struct GcFleetReport {
    protocol_version: u32,
    apply: bool,
    workers: Vec<GcWorkerReport>,
    transfer: GcReport,
}

fn protected_transfer_repo_ids(tasks: Vec<crate::task::LocalTaskRecord>) -> Vec<String> {
    tasks
        .into_iter()
        .filter(|task| {
            !task.status().state().is_terminal()
                || task.runner().is_some()
                || task.fetched_head().is_none()
        })
        .map(|task| task.repo_id().to_owned())
        .collect()
}

fn run_gc_command(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    apply: bool,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<GcFleetReport, WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let config = Config::load(&paths.config)?;
        config.require_local_inventory()?;
        let client_state = ClientStateStore::open(&paths.state)?;
        let now = current_time_millis()?;
        let protected_repo_ids = protected_transfer_repo_ids(client_state.list_tasks()?);
        let transfer =
            TransferGc::new(&paths.cache, runner).with_protected_repo_ids(protected_repo_ids);
        let transfer = if apply {
            transfer.apply_at(now)?
        } else {
            transfer.preview_at(now)?
        };
        let remote = RemoteJobClient::new(runner);
        let request = GcRequest::new(apply, now);
        let workers = config
            .workers
            .iter()
            .map(|worker| {
                Ok(GcWorkerReport {
                    worker: worker.name.clone(),
                    report: remote.gc(worker, &request)?,
                })
            })
            .collect::<Result<Vec<_>, WorkerError>>()?;
        Ok(GcFleetReport {
            protocol_version: PROTOCOL_VERSION,
            apply,
            workers,
            transfer,
        })
    })();

    match result {
        Ok(report) if json => match write_json_line(stdout, &report) {
            Ok(()) => 0,
            Err(error) => error.exit_code(),
        },
        Ok(report) => match write_gc_human(&report, stdout) {
            Ok(()) => 0,
            Err(error) => error.exit_code(),
        },
        Err(error) if json => match write_json_error_event(stdout, &error) {
            Ok(()) => error.exit_code(),
            Err(_) => crate::error::ExitKind::Io as u8,
        },
        Err(error) => {
            write_public_diagnostic(stderr, &error);
            error.exit_code()
        }
    }
}

fn write_gc_human(report: &GcFleetReport, stdout: &mut dyn Write) -> Result<(), WorkerError> {
    writeln!(
        stdout,
        "gc: {}",
        if report.apply { "apply" } else { "preview" }
    )?;
    for worker in &report.workers {
        write_gc_report_human(&worker.report, &format!("worker {}", worker.worker), stdout)?;
    }
    write_gc_report_human(&report.transfer, "transfer", stdout)?;
    stdout.flush()?;
    Ok(())
}

fn write_gc_report_human(
    report: &GcReport,
    label: &str,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    writeln!(
        stdout,
        "  {label}: {} candidate(s), {} applied",
        report.candidates().len(),
        report.applied().len()
    )?;
    for candidate in report.candidates() {
        writeln!(
            stdout,
            "    {} {} bytes ({}) {}",
            candidate.kind(),
            candidate.size_bytes(),
            candidate.reason(),
            candidate.identifier()
        )?;
    }
    for warning in report.warnings() {
        writeln!(stdout, "    warning: {warning}")?;
    }
    Ok(())
}

fn current_time_millis() -> Result<u64, WorkerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkerError::Protocol("system clock is before the Unix epoch".into()))
        .and_then(|duration| {
            u64::try_from(duration.as_millis())
                .map_err(|_| WorkerError::Protocol("system clock exceeds supported range".into()))
        })
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

#[allow(clippy::too_many_arguments)]
fn run_host_outbox(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    _stdout: &mut dyn Write,
    watch: bool,
    once: bool,
    enable: bool,
    write_agent: Option<PathBuf>,
    host_root: Option<PathBuf>,
    wake: bool,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        if !watch && !once && !enable && write_agent.is_none() && !wake {
            return Err(WorkerError::Protocol(
                "host outbox requires --watch, --once, --enable, --write-agent, or --wake".into(),
            ));
        }
        let root = match &host_root {
            Some(path) if path.is_absolute() => path.clone(),
            Some(_) => {
                return Err(WorkerError::Protocol(
                    "outbox host root must be an absolute path".into(),
                ));
            }
            None => discover_paths(config_override, runtime)?.host_state_root(),
        };
        let store = HostStore::open(&root)?;
        let outbox = crate::outbox::OriginOutbox::new(&store, runner);
        if let Some(directory) = &write_agent {
            let executable = std::env::current_exe().map_err(WorkerError::Io)?;
            crate::outbox::OriginOutbox::write_launchd_plist(directory, &executable, store.root())?;
        }
        if enable {
            outbox.enable_watch()?;
        }
        if wake {
            let _ = outbox.wake(&crate::outbox::SystemOutboxLauncher)?;
        } else if watch {
            let stop = std::sync::atomic::AtomicBool::new(false);
            outbox.run_watch(&stop, || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()
                    .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                    .unwrap_or(0)
            })?;
        } else if once {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| {
                    WorkerError::task("TASK_CLOCK_INVALID", "system clock precedes the Unix epoch")
                })?
                .as_millis();
            let now = u64::try_from(now).map_err(|_| {
                WorkerError::task("TASK_CLOCK_INVALID", "system clock is outside the range")
            })?;
            let _ = outbox.run_once(now)?;
        }
        Ok(())
    })();
    match result {
        Ok(()) => 0,
        Err(error) => error.exit_code(),
    }
}

fn run_host_outbox_retry(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdout: &mut dyn Write,
    task_id: crate::task::TaskId,
) -> u8 {
    let result = (|| -> Result<crate::outbox::OutboxRetryResponse, WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let store = HostStore::open(&paths.host_state_root())?;
        let outbox = crate::outbox::OriginOutbox::new(&store, runner);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| {
                WorkerError::task("TASK_CLOCK_INVALID", "system clock precedes the Unix epoch")
            })?
            .as_millis();
        let now = u64::try_from(now).map_err(|_| {
            WorkerError::task("TASK_CLOCK_INVALID", "system clock is outside the range")
        })?;
        let deliveries = outbox.retry_task(task_id, now, &crate::outbox::SystemOutboxLauncher)?;
        Ok(crate::outbox::OutboxRetryResponse::new(deliveries))
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

fn run_host_gc(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_task_control_endpoint(
        config_override,
        runtime,
        runner,
        stdin,
        stdout,
        |request: GcRequest, store, runner| HostGc::new(store, runner).run(&request),
    )
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

fn run_host_status_logs(
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
        |request: StatusLogsRequest, store, launcher| {
            JobService::new(store, launcher).status_logs(&request)
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

fn run_host_task_prepare(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_task_control_endpoint(
        config_override,
        runtime,
        runner,
        stdin,
        stdout,
        |request: TaskPrepareRequest, store, runner| {
            let admission = store.admission_lock(request.job_id())?;
            let transfer = store.transfer_lock_after(&admission, request.job_id())?;
            TaskStore::new(store, runner).prepare(&request, &transfer)
        },
    )
}

fn run_host_task_status(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_task_control_endpoint(
        config_override,
        runtime,
        runner,
        stdin,
        stdout,
        |request: TaskStatusRequest, store, runner| TaskStore::new(store, runner).status(&request),
    )
}

fn run_host_task_diff(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_task_control_endpoint(
        config_override,
        runtime,
        runner,
        stdin,
        stdout,
        |request: TaskDiffRequest, store, runner| TaskStore::new(store, runner).diff(&request),
    )
}

fn run_host_task_close(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_task_control_endpoint(
        config_override,
        runtime,
        runner,
        stdin,
        stdout,
        |request: TaskCloseRequest, store, runner| TaskStore::new(store, runner).close(&request),
    )
}

fn run_host_task_session(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_task_control_endpoint(
        config_override,
        runtime,
        runner,
        stdin,
        stdout,
        |request: TaskSessionRequest, store, runner| {
            TaskStore::new(store, runner).session_info(&request)
        },
    )
}

fn run_host_task_prebind(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    let home = runtime.home.clone();
    run_host_task_control_endpoint(
        config_override,
        runtime,
        runner,
        stdin,
        stdout,
        move |request: TaskPrebindRequest, store, runner| {
            crate::turn::prebind_session(store, runner, &request, &home)
        },
    )
}

fn run_host_task_cancel(
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
        |request: TaskCancelRequest, store, launcher| {
            JobService::new(store, launcher).cancel_task(request)
        },
    )
}

fn run_host_task_turn(
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
        |request: TaskTurnRequest, store, launcher| {
            JobService::new(store, launcher).submit_turn(request)
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

fn run_host_agent_settings_get(
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_agent_settings_endpoint(
        runtime,
        stdin,
        stdout,
        |request: AgentSettingsGetRequest, store| {
            let _ = request;
            Ok(store.read_all())
        },
    )
}

fn run_host_agent_settings_set(
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> u8 {
    run_host_agent_settings_endpoint(
        runtime,
        stdin,
        stdout,
        |request: AgentSettingsSaveRequest, store| {
            store.save(&request).map_err(|error| {
                WorkerError::Protocol(format!("{}: {}", error.code(), error.safe_message()))
            })
        },
    )
}

fn run_host_agent_settings_endpoint<Req, Res>(
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    operation: impl FnOnce(Req, &NativeAgentSettingsStore) -> Result<Res, WorkerError>,
) -> u8
where
    Req: DeserializeOwned + Serialize,
    Res: Serialize,
{
    const LIMIT: usize = 8192;
    let result = (|| -> Result<Res, WorkerError> {
        let mut bytes = Vec::new();
        stdin.take((LIMIT + 1) as u64).read_to_end(&mut bytes)?;
        if bytes.len() > LIMIT {
            return Err(WorkerError::Protocol(
                "SETTINGS_INVALID: settings request exceeded 8192 bytes".into(),
            ));
        }
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let request = Req::deserialize(&mut deserializer).map_err(|_| {
            WorkerError::Protocol("SETTINGS_INVALID: settings request was invalid".into())
        })?;
        deserializer.end().map_err(|_| {
            WorkerError::Protocol("SETTINGS_INVALID: settings request was invalid".into())
        })?;
        let canonical = serde_json::to_vec(&request).map_err(|_| {
            WorkerError::Protocol("SETTINGS_INVALID: settings request was invalid".into())
        })?;
        if canonical != bytes {
            return Err(WorkerError::Protocol(
                "SETTINGS_INVALID: settings request was not canonical JSON".into(),
            ));
        }
        let store = NativeAgentSettingsStore::new(runtime.home())
            .with_environment(runtime.environment().clone());
        operation(request, &store)
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

fn run_host_task_control_endpoint<Req, Res>(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    operation: impl FnOnce(Req, &HostStore, &dyn ProcessRunner) -> Result<Res, WorkerError>,
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
                "host task request exceeded 1 MiB".into(),
            ));
        }
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let request = Req::deserialize(&mut deserializer)
            .map_err(|_| WorkerError::Protocol("invalid host task request".into()))?;
        deserializer
            .end()
            .map_err(|_| WorkerError::Protocol("invalid host task request".into()))?;
        if serde_json::to_vec(&request)
            .map_err(|_| WorkerError::Protocol("invalid host task request".into()))?
            != bytes
        {
            return Err(WorkerError::Protocol(
                "host task request was not canonical JSON".into(),
            ));
        }
        let paths = discover_paths(config_override, runtime)?;
        let store = HostStore::open(&paths.host_state_root())?;
        operation(request, &store, runner)
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
        // A public admission reason is fixed inventory text by the contract on
        // `WorkerError::capacity`; send it so the laptop can show why. Redacted
        // capacity messages keep the generic category label.
        WorkerError::Capacity {
            code,
            message,
            public: true,
        } => (*code, message.as_ref().to_owned()),
        WorkerError::Capacity { code, .. } => (*code, "worker admission rejected".into()),
        WorkerError::Snapshot { code, .. } => (*code, "snapshot operation failed".into()),
        WorkerError::Git {
            code: crate::git_transport::ORIGIN_AUTH_FAILED,
            ..
        } => {
            let receipt = crate::failure_receipt::FailureReceipt::new(
                crate::failure_receipt::STAGE_PUBLISH,
                &[],
            )
            .expect("publish is vocabulary");
            (
                crate::git_transport::ORIGIN_AUTH_FAILED,
                receipt.host_message(),
            )
        }
        WorkerError::Git { code, .. } => (*code, "Git operation failed".into()),
        WorkerError::Task { code, .. } => (*code, "task operation failed".into()),
        WorkerError::Agent { code, .. } => (*code, "agent operation failed".into()),
        WorkerError::HostIo { receipt, .. } => ("HOST_IO", receipt.host_message()),
        WorkerError::Io(_) => ("HOST_IO", "host state operation failed".into()),
        _ => ("INVALID_REQUEST", "host request was invalid".into()),
    };
    HostControlError::new(code, message).expect("fixed host error is valid")
}

#[cfg(test)]
mod versioned_host_error_tests {
    use super::*;

    #[test]
    fn agent_errors_keep_their_code_on_the_wire() {
        let error = versioned_host_error(&WorkerError::Agent {
            code: "AGENT_EXITED",
            message: "session prebind command failed".into(),
        });
        let wire = serde_json::to_value(&error).unwrap();
        assert_eq!(wire["error"]["code"], "AGENT_EXITED");
        assert_eq!(wire["error"]["message"], "agent operation failed");
    }

    #[test]
    fn host_io_receipts_travel_in_the_existing_message_string() {
        let receipt = crate::failure_receipt::FailureReceipt::new(
            crate::failure_receipt::STAGE_CLEANUP,
            &[
                crate::failure_receipt::RESIDUAL_LEASE,
                crate::failure_receipt::RESIDUAL_CLEANUP_TREE,
            ],
        )
        .unwrap();
        let error = versioned_host_error(&WorkerError::HostIo {
            source: std::io::Error::other("cleanup failed"),
            receipt: receipt.clone(),
        });
        let wire = serde_json::to_value(&error).unwrap();
        assert_eq!(wire["error"]["code"], "HOST_IO");
        assert_eq!(wire["error"]["message"], receipt.host_message());
        assert!(wire["error"].get("stage").is_none());
        assert!(wire["error"].get("residual").is_none());
        assert_eq!(
            serde_json::to_value(error).unwrap()["protocol_version"],
            crate::protocol::PROTOCOL_VERSION
        );
    }

    #[test]
    fn origin_auth_failures_keep_the_git_code_and_a_publish_receipt() {
        let error = versioned_host_error(&WorkerError::Git {
            code: crate::git_transport::ORIGIN_AUTH_FAILED,
            message: "origin authentication failed".into(),
        });
        let receipt =
            crate::failure_receipt::FailureReceipt::new(crate::failure_receipt::STAGE_PUBLISH, &[])
                .unwrap();
        let wire = serde_json::to_value(&error).unwrap();
        assert_eq!(
            wire["error"]["code"],
            crate::git_transport::ORIGIN_AUTH_FAILED
        );
        assert_eq!(wire["error"]["message"], receipt.host_message());
        assert_eq!(
            WorkerError::Git {
                code: crate::git_transport::ORIGIN_AUTH_FAILED,
                message: "origin authentication failed".into(),
            }
            .failure_receipt()
            .unwrap()
            .stage(),
            crate::failure_receipt::STAGE_PUBLISH
        );
    }

    /// C2 (host half): a public admission reason is operator-facing text by
    /// the contract in `WorkerError::capacity`, so it must travel on the wire
    /// instead of being flattened to the generic category label. Without this
    /// the laptop can restore the exit code but never the real reason.
    #[test]
    fn public_capacity_reasons_travel_on_the_wire() {
        let error = versioned_host_error(&WorkerError::capacity(
            "CAPACITY_BUSY",
            "no eligible worker currently has an available heavy slot",
        ));
        let wire = serde_json::to_value(&error).unwrap();
        assert_eq!(wire["error"]["code"], "CAPACITY_BUSY");
        assert_eq!(
            wire["error"]["message"],
            "no eligible worker currently has an available heavy slot"
        );
    }

    /// Redaction is unchanged: a non-public capacity message must never reach
    /// the wire.
    #[test]
    fn nonpublic_capacity_messages_stay_redacted_on_the_wire() {
        let error = versioned_host_error(&WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            message: "busy lease at /Users/alice/PLANTED_PATH".into(),
            public: false,
        });
        let wire = serde_json::to_value(&error).unwrap();
        assert_eq!(wire["error"]["code"], "CAPACITY_BUSY");
        assert_eq!(wire["error"]["message"], "worker admission rejected");
        assert!(
            !serde_json::to_string(&wire)
                .unwrap()
                .contains("PLANTED_PATH")
        );
    }
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

fn run_host_migrate_layout(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        HostStore::migrate_layout(&paths.host_state_root())
    })();
    match result {
        Ok(()) => 0,
        Err(error) => {
            write_error(stderr, &error);
            error.exit_code()
        }
    }
}

fn run_host_complete_protocol_upgrade(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    target: &HiddenComponent,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let from = std::env::current_exe().map_err(WorkerError::Io)?;
        let to = PathBuf::from(target.expose());
        HostStore::complete_protocol_upgrade(&paths.host_state_root(), &from, &to)
    })();
    match result {
        Ok(()) => 0,
        Err(error) => {
            write_error(stderr, &error);
            error.exit_code()
        }
    }
}

fn run_host_complete_unverified_rollback(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    target: &HiddenComponent,
    previous: Option<&HiddenComponent>,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let to = PathBuf::from(target.expose());
        let previous = previous.map(|value| PathBuf::from(value.expose()));
        HostStore::complete_unverified_rollback(&paths.host_state_root(), previous.as_deref(), &to)
    })();
    match result {
        Ok(()) => 0,
        Err(error) => {
            write_error(stderr, &error);
            error.exit_code()
        }
    }
}

fn run_host_set_slots(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    slots: u8,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let store = HostStore::open(&paths.host_state_root())?;
        LeaseService::new(&store).set_slot_count(slots)
    })();
    match result {
        Ok(()) => 0,
        Err(error) => {
            write_error(stderr, &error);
            error.exit_code()
        }
    }
}

fn run_host_refresh_facts(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stderr: &mut dyn Write,
    timing: bool,
    clear_auth_incidents: bool,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let (_, collected) = ProbeCollector::refresh_facts_at_with_options(
            &paths.host_state_root(),
            &runtime.home,
            runner,
            clear_auth_incidents,
        )?;
        if timing {
            collected.write_to(stderr);
        }
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

#[allow(clippy::too_many_arguments)]
fn run_host_receive_pack(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    job_id: HiddenComponent,
    client_id: HiddenComponent,
    lease_token: HiddenComponent,
    request_fingerprint: HiddenComponent,
    path: HiddenComponent,
    executor: &dyn GitServerExecutor,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let components = ReceivePackComponents::new(
            parse_host_component(job_id.expose(), "receiver job identity")?,
            parse_host_component(client_id.expose(), "receiver client identity")?,
            parse_host_component(lease_token.expose(), "receiver lease identity")?,
            parse_host_component(request_fingerprint.expose(), "receiver fingerprint")?,
        );
        let paths = discover_paths(config_override, runtime)?;
        let store = HostStore::open(&paths.host_state_root())?;
        let never =
            HostGitService::new(&store).receive_pack(&components, path.expose(), executor)?;
        match never {}
    })();
    finish_git_server_command(result, stderr)
}

fn run_host_upload_pack(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    task_id: HiddenComponent,
    client_id: HiddenComponent,
    path: HiddenComponent,
    executor: &dyn GitServerExecutor,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let components = UploadPackComponents::new(
            parse_host_component(task_id.expose(), "upload task identity")?,
            parse_host_component(client_id.expose(), "upload client identity")?,
        );
        let paths = discover_paths(config_override, runtime)?;
        let store = HostStore::open(&paths.host_state_root())?;
        let never =
            HostGitService::new(&store).upload_pack(&components, path.expose(), executor)?;
        match never {}
    })();
    finish_git_server_command(result, stderr)
}

#[allow(clippy::too_many_arguments)]
fn run_host_controller_receive_pack(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    token: HiddenComponent,
    request_id: HiddenComponent,
    fingerprint: HiddenComponent,
    project_id: HiddenComponent,
    worktree_id: HiddenComponent,
    oid: HiddenComponent,
    path: Option<HiddenComponent>,
    executor: &dyn GitServerExecutor,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let transfer = crate::controller::ControllerTransfer::open(&paths.controller_state_root())?;
        let never = transfer.receive_pack(
            &paths.cache,
            token.expose(),
            request_id.expose(),
            fingerprint.expose(),
            project_id.expose(),
            worktree_id.expose(),
            oid.expose(),
            path.as_ref().map(HiddenComponent::expose),
            executor,
        )?;
        match never {}
    })();
    finish_git_server_command(result, stderr)
}

#[allow(clippy::too_many_arguments)]
fn run_host_controller_upload_pack(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    token: HiddenComponent,
    request_id: HiddenComponent,
    fingerprint: HiddenComponent,
    task_id: HiddenComponent,
    turn_id: HiddenComponent,
    oid: HiddenComponent,
    path: Option<HiddenComponent>,
    executor: &dyn GitServerExecutor,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        let transfer = crate::controller::ControllerTransfer::open(&paths.controller_state_root())?;
        let never = transfer.upload_pack(
            &paths.cache,
            token.expose(),
            request_id.expose(),
            fingerprint.expose(),
            task_id.expose(),
            turn_id.expose(),
            oid.expose(),
            path.as_ref().map(HiddenComponent::expose),
            executor,
        )?;
        match never {}
    })();
    finish_git_server_command(result, stderr)
}

fn parse_host_component<T>(value: &str, label: &str) -> Result<T, WorkerError>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| WorkerError::Protocol(format!("INVALID_COMPONENT: invalid {label}")))
}

fn finish_git_server_command(result: Result<(), WorkerError>, stderr: &mut dyn Write) -> u8 {
    match result {
        Ok(()) => 0,
        Err(error) => {
            let code = public_git_server_error(&error);
            if writeln!(stderr, "{code}").is_err() || stderr.flush().is_err() {
                crate::error::ExitKind::Io as u8
            } else {
                error.exit_code()
            }
        }
    }
}

fn public_git_server_error(error: &WorkerError) -> &'static str {
    match error {
        WorkerError::Protocol(message) if message.starts_with("INVALID_COMPONENT:") => {
            "HOST_GIT_INVALID_COMPONENT"
        }
        WorkerError::Protocol(message) if message.starts_with("LEASE_IDENTITY_MISMATCH:") => {
            "HOST_GIT_LEASE"
        }
        WorkerError::Task { .. } => "HOST_GIT_TASK",
        WorkerError::Git { code, .. } => code,
        WorkerError::Io(_) | WorkerError::HostIo { .. } => "HOST_GIT_IO",
        _ => "HOST_GIT_REJECTED",
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
        WorkerError::Io(_) | WorkerError::HostIo { .. } => "HOST_RSYNC_IO",
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

fn load_controller_process_config(paths: &PathLayout) -> Result<Config, WorkerError> {
    if paths.config.is_file() {
        Config::load(&paths.config)
    } else {
        let config =
            Config::parse("version = 1\n[controller]\nenabled = true\nssh = \"controller\"\n")?;
        config.validate()?;
        Ok(config)
    }
}

fn run_enabled_controller_task(
    command: Command,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    paths: &PathLayout,
    config: &Config,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<u8, WorkerError> {
    match command {
        Command::Task {
            command:
                TaskCommand::Submit {
                    agent,
                    model,
                    effort,
                    prompt,
                    prompt_file,
                    title,
                    project,
                    base,
                    wip,
                    includes,
                    timeout,
                    max_turns,
                    max_budget,
                    max_followups,
                    close_on,
                    env_profile,
                    worker,
                    source,
                    publish,
                    publish_branch,
                    no_wait,
                    wait,
                },
        } => {
            validate_task_scope_options(source.as_deref(), &publish, publish_branch.as_deref())?;
            let prompt = read_prompt(prompt, prompt_file)?;
            let limits = make_task_limits(timeout, max_turns, max_budget, max_followups)?;
            let project = project.unwrap_or(runtime.current_dir()?);
            let task_agent = match agent.as_deref() {
                Some(agent) => parse_task_agent(agent)?,
                None => {
                    let settings = crate::project_state::ProjectState::load(runner, &project, &[])?
                        .settings
                        .task;
                    parse_task_agent(&settings.default_agent)?
                }
            };
            let ack = freeze_and_submit_via_controller(
                runner,
                paths,
                config,
                ControllerSubmitFields {
                    project,
                    prompt,
                    title,
                    agent: task_agent,
                    model,
                    effort,
                    base,
                    wip,
                    includes,
                    limits,
                    close_on: parse_task_close_policy(close_on.as_deref())?,
                    env_profile,
                    worker,
                    source,
                    publish,
                    publish_branch,
                    wait_for_capacity: !no_wait,
                },
            )?;
            // `--wait` waits via controller then writes TaskReport.
            // `--no-wait` ACK unchanged.
            if wait {
                let task_id: crate::task::TaskId = controller_ack_id(ack.task_id())
                    .parse()
                    .map_err(|_| {
                        WorkerError::Unavailable(
                            "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK"
                                .into(),
                        )
                    })?;
                let waited = crate::controller::wait_via_controller(
                    runner,
                    &config.controller,
                    crate::controller::ControllerWaitSelector::Task(task_id),
                    None,
                )?;
                let report = controller_task_status(runner, config, task_id)?;
                write_task_report(&report, json, stdout)?;
                return Ok(waited.exit_code());
            }
            if json {
                write_json_line(
                    stdout,
                    &serde_json::json!({
                        "protocol_version": PROTOCOL_VERSION,
                        "task_id": controller_ack_id(ack.task_id()),
                        "turn_id": controller_ack_id(ack.turn_id()),
                        "request_id": ack.request_id(),
                        "status": ack.status(),
                    }),
                )?;
            } else {
                writeln!(
                    stdout,
                    "task {}: queued (controller)",
                    controller_ack_id(ack.task_id())
                )?;
                stdout.flush()?;
            }
            Ok(0)
        }
        Command::Task {
            command: TaskCommand::Status { task_id, full: _ },
        } => {
            let report = controller_task_status(runner, config, task_id)?;
            write_task_report(&report, json, stdout)?;
            Ok(0)
        }
        Command::Task {
            command:
                TaskCommand::List {
                    run,
                    state,
                    outcome,
                    full,
                },
        } => {
            let report = controller_task_list(runner, config, run, state, outcome, full)?;
            write_task_list_report(&report, json, stdout)?;
            Ok(0)
        }
        Command::Task {
            command:
                TaskCommand::Logs {
                    task_id,
                    turn,
                    follow,
                    raw,
                },
        } => {
            controller_task_logs(runner, config, task_id, turn, follow, raw, stdout)?;
            Ok(0)
        }
        Command::Task {
            command: TaskCommand::Diff { task_id, stat },
        } => {
            let diff = controller_task_diff(runner, config, task_id, stat)?;
            stdout.write_all(diff.text().as_bytes())?;
            if !diff.text().ends_with('\n') {
                stdout.write_all(b"\n")?;
            }
            stdout.flush()?;
            Ok(0)
        }
        Command::Task {
            command: TaskCommand::Result { task_id },
        } => {
            let report = controller_task_result(runner, config, task_id)?;
            write_task_result_report(&report, json, stdout)?;
            Ok(0)
        }
        Command::Task {
            command: TaskCommand::Fetch { task_id },
        } => {
            let project = runtime.current_dir()?;
            let report = crate::controller::fetch_via_controller(
                runner,
                paths,
                &config.controller,
                &project,
                task_id,
            )?;
            write_fetch_report(&report, json, stdout)?;
            Ok(0)
        }
        Command::Task {
            command:
                TaskCommand::Say {
                    task_id,
                    message,
                    message_file,
                    wait,
                },
        } => {
            let message = read_prompt(message, message_file)?;
            let ack = persist_and_send_controller(
                runner,
                paths,
                config,
                "task.say",
                serde_json::json!({
                    "task_id": task_id.to_string(),
                    "message": message,
                }),
            )?;
            if wait {
                let waited = crate::controller::wait_via_controller(
                    runner,
                    &config.controller,
                    crate::controller::ControllerWaitSelector::Task(task_id),
                    None,
                )?;
                let report = controller_task_status(runner, config, task_id)?;
                write_task_report(&report, json, stdout)?;
                Ok(waited.exit_code())
            } else {
                let report = task_report_from_controller_ack(&ack)?;
                write_task_report(&report, json, stdout)?;
                Ok(0)
            }
        }
        Command::Task {
            command: TaskCommand::Cancel { task_id },
        } => {
            let ack = persist_and_send_controller(
                runner,
                paths,
                config,
                "task.cancel",
                serde_json::json!({ "task_id": task_id.to_string() }),
            )?;
            let report = task_report_from_controller_ack(&ack)?;
            write_task_report(&report, json, stdout)?;
            Ok(0)
        }
        Command::Task {
            command: TaskCommand::Close { task_id, discard },
        } => {
            let ack = persist_and_send_controller(
                runner,
                paths,
                config,
                "task.close",
                serde_json::json!({
                    "task_id": task_id.to_string(),
                    "discard": discard,
                }),
            )?;
            let report = task_report_from_controller_ack(&ack)?;
            write_task_report(&report, json, stdout)?;
            Ok(0)
        }
        Command::Task {
            command: TaskCommand::Wait {
                task_id,
                run,
                timeout,
            },
        } => {
            let selector = match (task_id, run) {
                (Some(task_id), None) => crate::controller::ControllerWaitSelector::Task(task_id),
                (None, Some(run)) => crate::controller::ControllerWaitSelector::Run(run),
                _ => {
                    return Err(WorkerError::Task {
                        code: "TASK_CONFIG_INVALID",
                        message: "wait requires exactly one of --task-id or --run".into(),
                    });
                }
            };
            let report =
                crate::controller::wait_via_controller(runner, &config.controller, selector, timeout)?;
            write_wait_report(&report, json, stdout)?;
            Ok(report.exit_code())
        }
        Command::Task {
            command: TaskCommand::Reconcile,
        } => {
            let report =
                crate::controller::reconcile_via_controller(runner, &config.controller)?;
            if json {
                write_json_line(
                    stdout,
                    &serde_json::json!({
                        "protocol_version": PROTOCOL_VERSION,
                        "replaced_runners": report.replaced_runners(),
                        "started_runners": report.started_runners(),
                        "repaired_rows": report.repaired_rows(),
                        "unverifiable_rows": report.unverifiable_rows(),
                    }),
                )?;
            } else {
                writeln!(
                    stdout,
                    "runners: {} replaced, {} started; task rows: {} repaired, {} unverifiable",
                    report.replaced_runners(),
                    report.started_runners(),
                    report.repaired_rows(),
                    report.unverifiable_rows()
                )?;
                stdout.flush()?;
            }
            Ok(0)
        }
        Command::Task {
            command:
                TaskCommand::Batch {
                    file,
                    name,
                    max_parallel,
                    wait,
                    preview,
                },
        } => {
            if preview {
                unreachable!("batch --preview is handled before enabled controller routing");
            }
            let project = runtime.current_dir()?;
            let frozen = crate::controller::freeze_laptop_batch(
                runner,
                config,
                paths,
                &project,
                &file,
                name,
                max_parallel,
            )?;
            let body = serde_json::to_value(frozen.body()).map_err(|_| {
                WorkerError::Protocol(
                    "CONTROLLER_TRANSPORT: frozen batch could not be encoded".into(),
                )
            })?;
            let request = controller_read_request("task.batch", body)?;
            crate::controller::persist_operation_envelope(
                &paths.controller_cache_root(),
                &request,
            )?;
            let fingerprint =
                crate::job::RequestFingerprint::new(request.payload_sha256().to_owned())?;
            for source in frozen.sources() {
                crate::controller::stream_nested_source(
                    runner,
                    &config.controller,
                    crate::controller::SourceSubmitBind {
                        request_id: source.request_id(),
                        fingerprint: &fingerprint,
                        project_id: source.project_id(),
                        worktree_id: source.worktree_id(),
                        expected_oid: source.expected_oid(),
                    },
                    source.git_path(),
                )?;
            }
            let ack = crate::controller::send_controller_request(
                runner,
                &config.controller,
                &request,
            )?;
            let report = run_report_from_controller_ack(&ack)?;
            write_run_report(&report, json, stdout)?;
            if wait {
                let waited = crate::controller::wait_via_controller(
                    runner,
                    &config.controller,
                    crate::controller::ControllerWaitSelector::Run(report.run_id().to_string()),
                    None,
                )?;
                if json {
                    write_json_line(
                        stdout,
                        &serde_json::json!({
                            "protocol_version": PROTOCOL_VERSION,
                            "run_id": report.run_id().to_string(),
                            "task_ids": waited.task_ids(),
                            "exit_code": waited.exit_code(),
                        }),
                    )?;
                }
                Ok(waited.exit_code())
            } else {
                Ok(0)
            }
        }
        _ => Err(WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: controller is enabled; this command is not routed to the controller yet".into(),
        )),
    }
}

fn controller_ack_id(id: Option<&str>) -> &str {
    id.unwrap_or("")
}

struct ControllerSubmitFields {
    project: PathBuf,
    prompt: String,
    title: Option<String>,
    agent: crate::agent::AgentKind,
    model: Option<String>,
    effort: Option<String>,
    base: String,
    wip: bool,
    includes: Vec<String>,
    limits: crate::task::TaskLimits,
    close_on: crate::task::ClosePolicy,
    env_profile: Option<String>,
    worker: Option<String>,
    source: Option<String>,
    publish: Vec<String>,
    publish_branch: Option<String>,
    // CLI `--no-wait` inverts this freeze adapter field.
    wait_for_capacity: bool,
}

fn freeze_and_submit_via_controller(
    runner: &dyn ProcessRunner,
    paths: &PathLayout,
    config: &Config,
    cli: ControllerSubmitFields,
) -> Result<crate::controller::ControllerAck, WorkerError> {
    let ControllerSubmitFields {
        project,
        prompt,
        title,
        agent,
        model,
        effort,
        base,
        wip,
        includes,
        limits,
        close_on,
        env_profile,
        worker,
        source,
        publish,
        publish_branch,
        wait_for_capacity,
    } = cli;
    let probed = crate::project_state::ProjectState::load(runner, &project, &includes)?;
    let limits = crate::task_client::effective_task_limits(&limits, &probed.settings.task)?;
    let transfer = crate::transfer_repo::TransferRepo::open_or_create(
        &paths.cache,
        &probed.context.common_dir,
    )?;
    let identity = crate::task::GitIdentity::new("mac-worker", "mac-worker@localhost")?;
    let task_id = crate::task::TaskId::generate();
    let turn_id = crate::task::TurnId::generate();
    let request_id = format!("{:x}", uuid::Uuid::new_v4().simple());
    let captured = if wip {
        transfer.build_wip_base(
            runner,
            &probed.context,
            task_id,
            &probed.settings,
            &identity,
        )?
    } else {
        transfer.resolve_base(runner, &probed.context, &base)?
    };
    let agent_name = match agent {
        crate::agent::AgentKind::Codex => "codex",
        crate::agent::AgentKind::Claude => "claude",
        crate::agent::AgentKind::Cursor => "cursor",
        crate::agent::AgentKind::Opencode => "opencode",
    };
    let permissions = match probed
        .settings
        .task
        .permissions
        .get(agent_name)
        .map(String::as_str)
    {
        Some("unattended") => "unattended",
        _ => "workspace",
    };
    let source_name = source.unwrap_or_else(|| probed.settings.task.source.clone());
    let publish = if publish.is_empty() {
        probed.settings.task.publish.clone()
    } else {
        publish
    };
    let body = crate::prepared_submit::FrozenSubmitBody {
        task_id,
        turn_id,
        run_id: None,
        created_at_millis: current_time_millis()?,
        prompt,
        title,
        agent: agent_name.to_owned(),
        model: model.or(probed.settings.task.model.clone()),
        effort: effort.or(probed.settings.task.effort.clone()),
        source: source_name,
        origin_url: probed.origin.clone(),
        publish,
        publish_branch,
        close_on,
        env_profile: env_profile.or(probed.settings.task.env_profile.clone()),
        worker,
        wip,
        project_id: probed.context.project_id.clone(),
        worktree_id: probed.context.worktree_id.clone(),
        base_oid: captured.oid().clone(),
        timeout_millis: limits.turn.timeout_millis,
        max_turns: limits.turn.max_turns,
        max_budget_usd_cents: limits.turn.max_budget_usd_cents,
        max_followups: limits.max_followups,
        permissions: permissions.to_owned(),
        requires: probed.requirements.clone(),
        include_untracked: probed.settings.snapshot.include_untracked.clone(),
        include_empty_dirs: probed.settings.snapshot.include_empty_dirs.clone(),
        allow_sensitive: probed.settings.snapshot.allow_sensitive.clone(),
        cli_includes: includes,
        branch: probed.context.branch.clone(),
        wait_for_capacity,
    };
    let payload = serde_json::to_vec(&serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": "task.submit",
        "body": body,
    }))
    .map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: frozen submit could not be encoded".into())
    })?;
    let request = crate::controller::parse_request(&payload)?;
    crate::controller::persist_operation_envelope(&paths.controller_cache_root(), &request)?;
    crate::controller::stream_source_receive(
        runner,
        &config.controller,
        &request,
        transfer.path(),
        &body.project_id,
        &body.worktree_id,
        captured.oid(),
    )?;
    crate::controller::send_controller_request(runner, &config.controller, &request)
}

fn persist_and_send_controller(
    runner: &dyn ProcessRunner,
    paths: &PathLayout,
    config: &Config,
    command: &str,
    body: serde_json::Value,
) -> Result<crate::controller::ControllerAck, WorkerError> {
    let request = controller_read_request(command, body)?;
    crate::controller::persist_operation_envelope(&paths.controller_cache_root(), &request)?;
    crate::controller::send_controller_request(runner, &config.controller, &request)
}

fn task_report_from_controller_ack(
    ack: &crate::controller::ControllerAck,
) -> Result<task_client::TaskReport, WorkerError> {
    let value = ack.result().cloned().ok_or_else(|| {
        WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
        )
    })?;
    let parsed: crate::controller::ControllerTaskStatusResult = serde_json::from_value(value)
        .map_err(|_| {
            WorkerError::Unavailable(
                "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
            )
        })?;
    Ok(parsed.into_report())
}

fn run_report_from_controller_ack(
    ack: &crate::controller::ControllerAck,
) -> Result<task_client::RunReport, WorkerError> {
    let value = ack.result().cloned().ok_or_else(|| {
        WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
        )
    })?;
    let run_id = value
        .get("run_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            WorkerError::Unavailable(
                "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
            )
        })?
        .parse()
        .map_err(|_| {
            WorkerError::Unavailable(
                "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
            )
        })?;
    let task_ids = value
        .get("task_ids")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            WorkerError::Unavailable(
                "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
            )
        })?
        .iter()
        .map(|item| {
            item.as_str()
                .ok_or_else(|| {
                    WorkerError::Unavailable(
                        "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
                    )
                })?
                .parse()
                .map_err(|_| {
                    WorkerError::Unavailable(
                        "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(task_client::RunReport::from_parts(run_id, task_ids))
}

fn controller_read_request(
    command: &str,
    body: serde_json::Value,
) -> Result<crate::controller::ControllerRequest, WorkerError> {
    let request_id = format!("{:x}", uuid::Uuid::new_v4().simple());
    let payload = serde_json::to_vec(&serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": command,
        "body": body,
    }))
    .map_err(|_| {
        WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: controller read request could not be encoded".into(),
        )
    })?;
    crate::controller::parse_request(&payload)
}

fn controller_task_status(
    runner: &dyn ProcessRunner,
    config: &Config,
    task_id: crate::task::TaskId,
) -> Result<task_client::TaskReport, WorkerError> {
    let request = controller_read_request(
        "task.status",
        serde_json::json!({ "task_id": task_id.to_string() }),
    )?;
    let reply = crate::controller::send_controller_read::<
        crate::controller::ControllerTaskStatusResult,
    >(runner, &config.controller, &request)?;
    Ok(reply.into_result().into_report())
}

fn controller_task_list(
    runner: &dyn ProcessRunner,
    config: &Config,
    run: Option<String>,
    state: Option<String>,
    outcome: Option<String>,
    full: bool,
) -> Result<task_client::TaskListReport, WorkerError> {
    let mut body = serde_json::Map::new();
    if let Some(run) = run {
        body.insert("run".into(), serde_json::Value::String(run));
    }
    if let Some(state) = state {
        body.insert("state".into(), serde_json::Value::String(state));
    }
    if let Some(outcome) = outcome {
        body.insert("outcome".into(), serde_json::Value::String(outcome));
    }
    if full {
        body.insert("full".into(), serde_json::Value::Bool(true));
    }
    let request = controller_read_request("task.list", serde_json::Value::Object(body))?;
    let reply = crate::controller::send_controller_read::<crate::task_view::TaskListProjection>(
        runner,
        &config.controller,
        &request,
    )?;
    Ok(task_client::TaskListReport::from_projection(
        reply.into_result(),
    ))
}

fn controller_task_logs(
    runner: &dyn ProcessRunner,
    config: &Config,
    task_id: crate::task::TaskId,
    turn: Option<u32>,
    follow: bool,
    raw: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    let mut offset = 0_u64;
    let mut pending = Vec::new();
    let mut pinned_turn_id: Option<crate::task::TurnId> = None;
    let mut agent: Option<crate::agent::AgentKind> = None;
    let mut reported_failure = None;
    loop {
        let mut body = serde_json::json!({
            "task_id": task_id.to_string(),
            "offset": offset,
            "limit": crate::job::MAX_LOG_CHUNK_BYTES,
            "raw": raw,
            "follow": follow,
        });
        if let Some(turn) = turn {
            body["turn"] = serde_json::json!(turn);
        }
        if let Some(turn_id) = pinned_turn_id {
            body["turn_id"] = serde_json::json!(turn_id.to_string());
        }
        let request = controller_read_request("task.logs", body)?;
        let reply = crate::controller::send_controller_read::<
            crate::controller::ControllerTaskLogsResult,
        >(runner, &config.controller, &request)?;
        let chunk = reply.into_result();
        let chunk_agent = chunk.agent()?;
        match pinned_turn_id {
            Some(pinned) if pinned != chunk.turn_id() => {
                return Err(crate::controller::read::invalid_controller_reply());
            }
            Some(_) => {}
            None => pinned_turn_id = Some(chunk.turn_id()),
        }
        match agent {
            Some(expected) if expected != chunk_agent => {
                return Err(crate::controller::read::invalid_controller_reply());
            }
            Some(_) => {}
            None => agent = Some(chunk_agent),
        }
        let bytes = chunk.decode_bytes()?;
        let finished = chunk.exhausted() && (chunk.complete() || !follow);
        if raw {
            stdout.write_all(&bytes)?;
        } else {
            pending.extend_from_slice(&bytes);
            let renderable = crate::turn_log::take_complete_log_lines(&mut pending, finished);
            if !renderable.is_empty() {
                crate::turn_log::render_agent_log(
                    &renderable,
                    agent.expect("agent is pinned after the first logs reply"),
                    stdout,
                )?;
            }
            if chunk.failure() != reported_failure.as_deref() {
                if let Some(line) = chunk.failure() {
                    writeln!(stdout, "{line}")?;
                }
                reported_failure = chunk.failure().map(str::to_owned);
            }
        }
        stdout.flush()?;
        let progressed = chunk.next_offset() > offset;
        offset = chunk.next_offset();
        if finished {
            break;
        }
        if follow && chunk.exhausted() && !chunk.complete() && !progressed {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    Ok(())
}

fn controller_task_diff(
    runner: &dyn ProcessRunner,
    config: &Config,
    task_id: crate::task::TaskId,
    stat: bool,
) -> Result<crate::controller::ControllerTaskDiffResult, WorkerError> {
    let request = controller_read_request(
        "task.diff",
        serde_json::json!({
            "task_id": task_id.to_string(),
            "stat": stat,
        }),
    )?;
    let reply = crate::controller::send_controller_read::<
        crate::controller::ControllerTaskDiffResult,
    >(runner, &config.controller, &request)?;
    Ok(reply.into_result())
}

fn controller_task_result(
    runner: &dyn ProcessRunner,
    config: &Config,
    task_id: crate::task::TaskId,
) -> Result<task_client::TaskResultReport, WorkerError> {
    let request = controller_read_request(
        "task.result",
        serde_json::json!({ "task_id": task_id.to_string() }),
    )?;
    let reply = crate::controller::send_controller_read::<crate::controller::ControllerTaskResult>(
        runner,
        &config.controller,
        &request,
    )?;
    Ok(reply.into_result().into_report())
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

/// The herdr session the laptop notifies about finished turns: the one that
/// started this command when it ran inside herdr, else the account's default,
/// and none at all when the operator turned notifications off.
fn herdr_notifier_socket(
    config: &Config,
    runtime: &RuntimeContext,
) -> Option<crate::herdr::HerdrSocket> {
    if !config.notifications.herdr {
        return None;
    }
    Some(crate::herdr::HerdrSocket::from_env_or_home(
        |key| runtime.environment.get(std::ffi::OsStr::new(key)).cloned(),
        &runtime.home,
    ))
}

fn laptop_process_table() -> &'static dyn LaptopProcessTable {
    #[cfg(test)]
    {
        &EmptyLaptopProcessTable
    }
    #[cfg(not(test))]
    {
        &SystemLaptopProcessTable
    }
}

fn installed_binary_mtime() -> Option<SystemTime> {
    #[cfg(test)]
    {
        None
    }
    #[cfg(not(test))]
    {
        std::env::current_exe()
            .ok()
            .and_then(|path| std::fs::metadata(path).ok()?.modified().ok())
    }
}

fn laptop_setup_warnings() -> Vec<SetupWarning> {
    let Some(mtime) = installed_binary_mtime() else {
        return Vec::new();
    };
    let Ok(processes) = laptop_process_table().list() else {
        return Vec::new();
    };
    let outdated = outdated_laptop_cli(&processes, mtime);
    let Some(message) = format_outdated_laptop_cli(&outdated) else {
        return Vec::new();
    };
    vec![SetupWarning {
        code: SetupWarningCode::LaptopBinaryOutdated,
        message,
    }]
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

    use uuid::Uuid;

    use crate::{
        agent::{AgentKind, PermissionPolicy},
        config::Config,
        error::WorkerError,
        job::JobId,
        paths::PathLayout,
        task::{
            BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits,
            TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnSummary,
            TurnTerminal,
        },
    };

    fn terminal_task_record(fetched_head: Option<BaseOid>) -> LocalTaskRecord {
        let task_id = TaskId::new(Uuid::from_u128(1));
        let base_oid: BaseOid = "0123456789abcdef0123456789abcdef01234567".parse().unwrap();
        let meta = TaskMeta::new(TaskMetaInput {
            task_id,
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
            base_oid: base_oid.clone(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
            title: None,
            prompt: "retain until fetched".into(),
            created_at_millis: 1,
        })
        .unwrap();
        let status = TaskStatus::new(
            TaskState::Closed,
            Some(TaskOutcome::Done),
            Some("mini-1".into()),
            false,
            Some(base_oid.clone()),
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1,
                JobId::new(Uuid::from_u128(2)),
                Some(TurnTerminal::Succeeded),
                Some(TaskOutcome::Done),
                Some(true),
                false,
                Some(1),
                Some(2),
            )],
            2,
        )
        .unwrap();
        let repo_id = if fetched_head.is_some() {
            "d".repeat(64)
        } else {
            "c".repeat(64)
        };
        LocalTaskRecord::new(
            meta,
            status,
            None,
            None,
            fetched_head,
            repo_id,
            None,
            false,
            None,
        )
        .unwrap()
    }

    #[test]
    fn outcome_filter_accepts_every_kind_in_both_spellings() {
        for kind in crate::task::TaskOutcome::KINDS {
            assert_eq!(super::parse_task_outcome_kind(kind).unwrap(), kind);
            assert_eq!(
                super::parse_task_outcome_kind(&kind.replace('_', "-")).unwrap(),
                kind
            );
        }
    }

    #[test]
    fn outcome_filter_rejects_an_unknown_kind() {
        for value in ["nonsense", "", "Done"] {
            let error = super::parse_task_outcome_kind(value).unwrap_err();
            assert_eq!(error.public_code(), "TASK_CONFIG_INVALID");
        }
    }

    #[test]
    fn task_close_policy_defaults_to_done() {
        assert_eq!(
            super::parse_task_close_policy(None).unwrap(),
            ClosePolicy::Done
        );
    }

    #[test]
    fn terminal_tasks_without_fetched_results_protect_their_transfer_repo() {
        let pending = terminal_task_record(None);
        let fetched = terminal_task_record(Some(
            "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
        ));

        let protected = super::protected_transfer_repo_ids(vec![pending, fetched]);

        assert_eq!(protected, vec!["c".repeat(64)]);
    }

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
    fn slots_outside_host_bound_are_rejected() {
        let mut config = Config::parse(include_str!("../config.example.toml")).unwrap();
        config.workers[0].slots = 2;
        assert!(config.validate().is_ok());

        config.workers[0].slots = 0;
        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));

        config.workers[0].slots = crate::lease::MAX_HOST_SLOTS + 1;
        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn invalid_versions_and_empty_inventories_are_rejected() {
        for contents in [
            "version = 2\nworkers = []",
            "version = 1\nworkers = []",
            "version = 1",
        ] {
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

        assert_eq!(config.worker("mini-1").unwrap().ssh, "yourname@mini.local");
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
    fn public_diagnostics_print_static_task_busy_reasons() {
        let error = WorkerError::task("TASK_BUSY", "task turn is being dispatched");
        let mut stderr = Vec::new();
        super::write_public_diagnostic(&mut stderr, &error);
        assert_eq!(
            std::str::from_utf8(&stderr).unwrap(),
            "TASK_BUSY: task turn is being dispatched\n"
        );
    }

    #[test]
    fn json_error_events_follow_capacity_public_message_rules() {
        let public = WorkerError::capacity(
            "CAPACITY_BUSY",
            "no eligible worker currently has an available heavy slot",
        );
        let mut stdout = Vec::new();
        super::write_json_error_event(&mut stdout, &public).unwrap();
        let event: crate::job::JsonEvent =
            serde_json::from_slice(stdout.strip_suffix(b"\n").unwrap()).unwrap();
        match event {
            crate::job::JsonEvent::Error { code, message, .. } => {
                assert_eq!(code, "CAPACITY_BUSY");
                assert_eq!(
                    message,
                    "no eligible worker currently has an available heavy slot"
                );
            }
            other => panic!("expected a JSON error event, got {other:?}"),
        }

        let planted_path = "/Users/alice/PLANTED_PUBLIC_PATH";
        let redacted = WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            message: format!("busy lease at {planted_path}").into(),
            public: false,
        };
        stdout.clear();
        super::write_json_error_event(&mut stdout, &redacted).unwrap();
        let event: crate::job::JsonEvent =
            serde_json::from_slice(stdout.strip_suffix(b"\n").unwrap()).unwrap();
        match event {
            crate::job::JsonEvent::Error { code, message, .. } => {
                assert_eq!(code, "CAPACITY_BUSY");
                assert_eq!(message, "capacity error");
            }
            other => panic!("expected a JSON error event, got {other:?}"),
        }
        let text = String::from_utf8(stdout).unwrap();
        assert!(!text.contains(planted_path));
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

#[cfg(test)]
mod enabled_submit_freeze_tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        path::PathBuf,
        process::Command as SystemCommand,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use serde_json::{Value, json};

    use crate::{
        RuntimeContext,
        cli::{Command, TaskCommand},
        config::Config,
        controller::{
            canonical_request_sha256, load_operation_envelope, parse_request,
            persist_operation_envelope,
        },
        error::WorkerError,
        paths::PathLayout,
        prepared_submit::FrozenSubmitBody,
        process::{ProcessRequest, ProcessResult, ProcessRunner},
        protocol::PROTOCOL_VERSION,
        task::{
            BaseOid, TaskId, TaskOutcome, TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal,
        },
    };

    struct Fixture {
        _temp: tempfile::TempDir,
        repo: PathBuf,
        paths: PathLayout,
        runtime: RuntimeContext,
        config: Config,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().canonicalize().unwrap();
            let home = root.join("home");
            for dir in ["home", "state", "cache", "config", "data"] {
                std::fs::create_dir_all(root.join(dir)).unwrap();
            }
            let repo = root.join("repo");
            std::fs::create_dir_all(&repo).unwrap();
            std::fs::create_dir_all(repo.join("home")).unwrap();
            git(&repo, &["init", "--initial-branch=main"]);
            git(&repo, &["config", "user.name", "Fixture"]);
            git(&repo, &["config", "user.email", "fixture@example.test"]);
            std::fs::write(repo.join("src.txt"), b"fixture\n").unwrap();
            git(&repo, &["add", "--all"]);
            git(&repo, &["commit", "-m", "fixture"]);
            let environment = BTreeMap::from([
                (OsString::from("HOME"), home.as_os_str().to_os_string()),
                (
                    OsString::from("XDG_STATE_HOME"),
                    root.join("state").into_os_string(),
                ),
                (
                    OsString::from("XDG_CACHE_HOME"),
                    root.join("cache").into_os_string(),
                ),
                (
                    OsString::from("XDG_CONFIG_HOME"),
                    root.join("config").into_os_string(),
                ),
                (
                    OsString::from("XDG_DATA_HOME"),
                    root.join("data").into_os_string(),
                ),
            ]);
            let paths = PathLayout::discover(None, &environment, &home).unwrap();
            let runtime = RuntimeContext::isolated(environment, home, root.clone());
            let config = Config::parse(
                "version = 1\n[controller]\nenabled = true\nssh = \"fakecontroller\"\nremote_binary = \"~/.local/bin/worker\"\n",
            )
            .unwrap();
            Self {
                _temp: temp,
                repo,
                paths,
                runtime,
                config,
            }
        }
    }

    fn git(repo: &std::path::Path, args: &[&str]) {
        let output = SystemCommand::new("/usr/bin/git")
            .current_dir(repo)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("HOME", repo.join("home"))
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Mocked controller/message plane: answers framed RPC from the real
    /// codecs and digests; asserts the controller git-push identity/args and
    /// succeeds it without a live hop. Proves the CLI boundary shape, not
    /// execution. Existing streamed/process suites cover actual transfer.
    struct MockControllerRpc {
        polls: AtomicUsize,
        submit_bodies: Mutex<Vec<Value>>,
        wait_exit: Mutex<u8>,
        status_exit: Mutex<Option<u8>>,
    }

    impl MockControllerRpc {
        fn new() -> Self {
            Self {
                polls: AtomicUsize::new(0),
                submit_bodies: Mutex::new(Vec::new()),
                wait_exit: Mutex::new(0),
                status_exit: Mutex::new(Some(0)),
            }
        }

        fn set_wait_exit(&self, exit: u8) {
            *self.wait_exit.lock().unwrap() = exit;
        }

        fn set_status_exit(&self, exit: Option<u8>) {
            *self.status_exit.lock().unwrap() = exit;
        }

        fn reply(&self, command: &str, request_id: &str, body: &Value, result: Value) -> Vec<u8> {
            let digest = canonical_request_sha256(PROTOCOL_VERSION, command, body).expect("digest");
            crate::controller::encode_json_frame(&json!({
                "protocol_version": PROTOCOL_VERSION,
                "command": command,
                "request_id": request_id,
                "payload_sha256": digest,
                "result": result,
            }))
            .expect("frame")
        }
    }

    impl ProcessRunner for MockControllerRpc {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            use std::os::unix::process::ExitStatusExt;
            // Local git is real (freeze inspects actual objects). Only the
            // controller source push is mocked in-runner with asserted
            // identity/args; everything else framed is answered below.
            // Existing streamed/process suites cover actual transfer.
            if request.program == "/usr/bin/git" {
                if !request.args.iter().any(|arg| arg == "push") {
                    return crate::process::SystemProcessRunner.run(request);
                }
                assert!(
                    request.args.iter().any(|arg| arg
                        .to_str()
                        .is_some_and(|text| text.contains("controller-receive-pack"))),
                    "push must target controller-receive-pack, got {request:?}"
                );
                return Ok(ProcessResult {
                    status: std::process::ExitStatus::from_raw(0),
                    stdout: b"Done\n".to_vec(),
                    stderr: Vec::new(),
                });
            }
            let stdin = request.stdin.as_ref().unwrap_or_else(|| {
                panic!(
                    "unexpected mocked program {:?} args {:?}",
                    request.program, request.args
                )
            });
            let ok = |stdout: Vec<u8>| ProcessResult {
                status: std::process::ExitStatus::from_raw(0),
                stdout,
                stderr: Vec::new(),
            };
            let payload = crate::controller::decode_frame(stdin).expect("request frame");
            let parsed: Value = serde_json::from_slice(payload).expect("request json");
            let command = parsed["command"].as_str().unwrap_or_default().to_owned();
            let request_id = parsed["request_id"].as_str().unwrap_or_default().to_owned();
            let body = parsed["body"].clone();
            let stdout = match command.as_str() {
                "controller.transfer.source.prepare" => self.reply(
                    &command,
                    &request_id,
                    &body,
                    json!({
                        "token": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                        "request_id": body["request_id"],
                        "fingerprint": body["fingerprint"],
                        "project_id": body["project_id"],
                        "worktree_id": body["worktree_id"],
                        "expected_oid": body["expected_oid"],
                    }),
                ),
                "controller.transfer.source.finish" => {
                    let request_ref = crate::transfer_repo::TransferRepo::frozen_request_ref(
                        body["request_id"].as_str().unwrap(),
                    )
                    .unwrap();
                    self.reply(
                        &command,
                        &request_id,
                        &body,
                        json!({
                            "token": body["token"],
                            "request_id": body["request_id"],
                            "oid": body["expected_oid"],
                            "request_ref": request_ref,
                        }),
                    )
                }
                "task.submit" => {
                    self.submit_bodies.lock().unwrap().push(body.clone());
                    let digest = canonical_request_sha256(PROTOCOL_VERSION, &command, &body)
                        .expect("digest");
                    crate::controller::encode_json_frame(&json!({
                        "protocol_version": PROTOCOL_VERSION,
                        "status": "accepted",
                        "request_id": request_id,
                        "payload_sha256": digest,
                        "task_id": body["task_id"],
                        "turn_id": body["turn_id"],
                        "created_at_millis": body["created_at_millis"],
                    }))
                    .expect("frame")
                }
                "task.wait.poll" => {
                    let first = self.polls.fetch_add(1, Ordering::SeqCst) == 0;
                    let task_id = body["task_id"].as_str().unwrap_or_default().to_owned();
                    let exit = *self.wait_exit.lock().unwrap();
                    self.reply(
                        &command,
                        &request_id,
                        &body,
                        json!({
                            "task_ids": [task_id],
                            "quiescent": !first,
                            "exit_code": exit,
                        }),
                    )
                }
                "task.status" => {
                    let task_id: TaskId =
                        body["task_id"].as_str().unwrap().parse().expect("task id");
                    let status_exit = *self.status_exit.lock().unwrap();
                    let status = TaskStatus::new(
                        TaskState::Open,
                        Some(TaskOutcome::Done),
                        Some("mini-1".into()),
                        false,
                        Some(fixture_base_hint()),
                        Some("mocked".into()),
                        Vec::new(),
                        Vec::new(),
                        None,
                        vec![TurnSummary::new(
                            1,
                            turn_hint(),
                            Some(TurnTerminal::Succeeded),
                            Some(TaskOutcome::Done),
                            Some(true),
                            false,
                            Some(1),
                            Some(2),
                        )],
                        2,
                    )
                    .unwrap();
                    self.reply(
                        &command,
                        &request_id,
                        &body,
                        json!({
                            "task_id": task_id.to_string(),
                            "run_id": null,
                            "status": status,
                            "warnings": [],
                            "events": [],
                            "runner": null,
                            "exit_code": status_exit,
                        }),
                    )
                }
                other => panic!("unexpected mocked command {other}"),
            };
            Ok(ok(stdout))
        }
    }

    fn fixture_base_hint() -> BaseOid {
        "dddddddddddddddddddddddddddddddddddddddd".parse().unwrap()
    }

    fn turn_hint() -> TurnId {
        "118f0f4a6b5c7d8e9f00112233445566".parse().unwrap()
    }

    fn submit_command(project: PathBuf, no_wait: bool, wait: bool) -> Command {
        Command::Task {
            command: TaskCommand::Submit {
                agent: None,
                model: None,
                effort: None,
                prompt: Some("mocked enabled submit".into()),
                prompt_file: None,
                title: None,
                project: Some(project),
                base: "HEAD".into(),
                wip: false,
                includes: Vec::new(),
                timeout: None,
                max_turns: None,
                max_budget: None,
                max_followups: None,
                close_on: None,
                env_profile: None,
                worker: None,
                source: None,
                publish: Vec::new(),
                publish_branch: None,
                no_wait,
                wait,
            },
        }
    }

    #[test]
    fn enabled_submit_wait_returns_task_report_with_wait_exit() {
        let fixture = Fixture::new();
        let mock = MockControllerRpc::new();
        let mut stdout = Vec::new();
        let exit = crate::run_enabled_controller_task(
            submit_command(fixture.repo.clone(), false, true),
            &mock,
            &fixture.runtime,
            &fixture.paths,
            &fixture.config,
            true,
            &mut stdout,
        )
        .unwrap();
        assert_eq!(exit, 0, "wait exit code is preserved");
        assert_eq!(
            mock.polls.load(Ordering::SeqCst),
            2,
            "wait must poll to quiescence"
        );
        let report: Value = serde_json::from_slice(&stdout).expect("report json");
        assert_eq!(
            report
                .get("task_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            mock.submit_bodies
                .lock()
                .unwrap()
                .first()
                .and_then(|body| body.get("task_id"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            "returned TaskReport must carry the submitted task identity"
        );
        let status = report.get("status").expect("TaskReport status");
        assert_eq!(
            status.get("state").and_then(Value::as_str),
            Some("open"),
            "returned TaskReport must carry the quiescent status, not a WaitReport"
        );
        assert_eq!(
            status
                .get("last_outcome")
                .and_then(|outcome| outcome.get("kind"))
                .and_then(Value::as_str),
            Some("done")
        );
        let turns = status
            .get("turns")
            .and_then(Value::as_array)
            .expect("turns");
        assert_eq!(turns.len(), 1);
        assert_eq!(
            report.get("exit_code").and_then(Value::as_u64),
            Some(0),
            "TaskReport exit code must be present"
        );
        assert!(
            report.get("task_ids").is_none(),
            "must not write a WaitReport shape"
        );
    }

    #[test]
    fn enabled_submit_wait_failure_exit_wins_over_report_exit() {
        let fixture = Fixture::new();
        let mock = MockControllerRpc::new();
        mock.set_wait_exit(1);
        mock.set_status_exit(None);
        let mut stdout = Vec::new();
        let exit = crate::run_enabled_controller_task(
            submit_command(fixture.repo.clone(), false, true),
            &mock,
            &fixture.runtime,
            &fixture.paths,
            &fixture.config,
            true,
            &mut stdout,
        )
        .unwrap();
        assert_eq!(
            exit, 1,
            "CLI exit must come from the waited quiescence, not the status report"
        );
        let report: Value = serde_json::from_slice(&stdout).expect("report json");
        assert!(
            report.get("task_id").and_then(Value::as_str).is_some(),
            "a TaskReport must still be written, not a WaitReport: {report}"
        );
        assert!(
            report.get("status").is_some(),
            "a TaskReport must still be written, not a WaitReport: {report}"
        );
        assert_eq!(
            report.get("exit_code"),
            Some(&Value::Null),
            "report exit stays null while the CLI exits 1"
        );
    }

    #[test]
    fn freeze_carries_no_wait_capacity_policy() {
        for (no_wait, expected) in [(true, false), (false, true)] {
            let fixture = Fixture::new();
            let mock = MockControllerRpc::new();
            let mut stdout = Vec::new();
            let exit = crate::run_enabled_controller_task(
                submit_command(fixture.repo.clone(), no_wait, false),
                &mock,
                &fixture.runtime,
                &fixture.paths,
                &fixture.config,
                true,
                &mut stdout,
            )
            .unwrap();
            assert_eq!(exit, 0, "no-wait ACK path stays exit 0");
            let bodies = mock.submit_bodies.lock().unwrap();
            assert_eq!(bodies.len(), 1);
            assert_eq!(
                bodies[0].get("wait_for_capacity").and_then(Value::as_bool),
                Some(expected),
                "frozen body must carry wait_for_capacity = !no_wait"
            );
            let frozen: FrozenSubmitBody = serde_json::from_value(bodies[0].clone()).unwrap();
            assert_eq!(frozen.wait_for_capacity, expected);
            assert_eq!(frozen.prepared().unwrap().wait_for_capacity, expected);
        }
    }

    #[test]
    fn old_frozen_body_without_flag_defaults_true() {
        let mut body = json!({
            "task_id": task_hint(),
            "turn_id": turn_hint(),
            "created_at_millis": 1_700_000_000_000u64,
            "prompt": "old",
            "agent": "codex",
            "source": "local",
            "publish": ["fetch"],
            "close_on": "never",
            "wip": false,
            "project_id": "a".repeat(64),
            "worktree_id": "b".repeat(64),
            "base_oid": "dddddddddddddddddddddddddddddddddddddddd",
            "timeout_millis": 1000,
            "max_followups": 1,
            "permissions": "workspace",
            "requires": [],
            "include_untracked": [],
            "include_empty_dirs": [],
            "allow_sensitive": [],
            "cli_includes": [],
        });
        assert!(body.get("wait_for_capacity").is_none());
        let frozen: FrozenSubmitBody = serde_json::from_value(body.clone()).unwrap();
        assert!(
            frozen.wait_for_capacity,
            "missing flag defaults to prior behavior"
        );
        assert!(frozen.prepared().unwrap().wait_for_capacity);
        let digest_default =
            canonical_request_sha256(PROTOCOL_VERSION, "task.submit", &body).unwrap();
        body.as_object_mut()
            .unwrap()
            .insert("wait_for_capacity".into(), json!(false));
        let denied: FrozenSubmitBody = serde_json::from_value(body.clone()).unwrap();
        assert!(!denied.wait_for_capacity);
        assert!(!denied.prepared().unwrap().wait_for_capacity);
        assert_ne!(
            canonical_request_sha256(PROTOCOL_VERSION, "task.submit", &body).unwrap(),
            digest_default,
            "the capacity flag is digest-bound frozen identity: fail-fast and queued submits must not share a digest"
        );
    }

    fn task_hint() -> String {
        "018f0f4a6b5c7d8e9f00112233445566".into()
    }

    #[test]
    fn persisted_frozen_envelope_replays_bytes_unchanged() {
        let fixture = Fixture::new();
        let request_id = "018f0f4a6b5c7d8e9f00112233445577";
        let mock = MockControllerRpc::new();
        let mut stdout = Vec::new();
        crate::run_enabled_controller_task(
            submit_command(fixture.repo.clone(), true, false),
            &mock,
            &fixture.runtime,
            &fixture.paths,
            &fixture.config,
            true,
            &mut stdout,
        )
        .unwrap();
        let body = mock.submit_bodies.lock().unwrap()[0].clone();
        assert_eq!(
            body.get("wait_for_capacity").and_then(Value::as_bool),
            Some(false)
        );
        let payload = serde_json::to_vec(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": request_id,
            "command": "task.submit",
            "body": body,
        }))
        .unwrap();
        let request = parse_request(&payload).unwrap();
        persist_operation_envelope(&fixture.paths.controller_cache_root(), &request).unwrap();
        let reloaded = load_operation_envelope(&fixture.paths.controller_cache_root(), request_id)
            .unwrap()
            .expect("envelope");
        assert_eq!(
            &reloaded.body().clone(),
            &body,
            "replay keeps original bytes"
        );
        assert_eq!(
            reloaded
                .body()
                .get("wait_for_capacity")
                .and_then(Value::as_bool),
            Some(false)
        );
    }
}
