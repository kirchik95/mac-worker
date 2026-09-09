use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::{self, Cursor, Read, Write},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use agent_settings::{AgentSettingsGetRequest, AgentSettingsSaveRequest, NativeAgentSettingsStore};
use cli::{Cli, Command, HiddenComponent, HostCommand, TaskCommand};
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
    LogChunkResponse, ResolveOrAbandonRequest, StatusRequest, SubmitRequest,
};
use job_service::JobService;
use lease::{AdmissionFacts, LeaseService};
use output::CommandOutput;
use paths::PathLayout;
use probe::ProbeCollector;
use process::ProcessRunner;
use protocol::{PROTOCOL_VERSION, SetupReport, WorkersReport};
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
pub mod agent;
pub mod agent_facts;
pub mod agent_settings;
pub mod cli;
pub mod client_state;
pub mod config;
pub mod dashboard;
pub mod doctor;
pub mod error;
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
pub mod lease;
pub mod manifest;
pub mod onboarding;
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
pub(crate) mod runner_log;
pub mod scheduler;
pub mod scheduler_adapter;
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
        Command::Workers { refresh } => {
            let inspection = inspect_configured_workers(cli.config, runtime, runner, refresh)?;
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
            command: HostCommand::RefreshFacts,
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
    if matches!(&cli.command, Command::Task { .. } | Command::Runner { .. }) {
        return run_task_command(cli, runner, runtime, stdout, stderr);
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
        let launch_directory = runtime.current_dir().ok();
        let launcher = SystemDashboardLauncher::from_system_for_directory(
            config,
            client_state,
            launch_directory.as_deref(),
        );
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
            Command::Runner { task_id, turn_id } => {
                let task_id = task_id
                    .expose()
                    .parse::<crate::task::TaskId>()
                    .map_err(|_| WorkerError::Protocol("invalid runner task ID".into()))?;
                let turn_id = turn_id
                    .expose()
                    .parse::<crate::task::TurnId>()
                    .map_err(|_| WorkerError::Protocol("invalid runner turn ID".into()))?;
                let outcome = TurnRunner::new(runner, &config, &paths, &client_state, executor)
                    .with_notifier(notifier)
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
        } => {
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
        TaskCommand::List {
            run,
            state,
            outcome,
            full,
        } => {
            let filter = TaskListFilter {
                run_id: run,
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
        TaskCommand::Wait {
            task_id,
            run,
            timeout,
        } => {
            let selector = match (task_id, run) {
                (Some(task_id), None) => WaitSelector::Task(task_id),
                (None, Some(run)) => WaitSelector::Run(run),
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
            let report = client.reconcile_runners()?;
            if json {
                write_json_line(
                    stdout,
                    &serde_json::json!({
                        "protocol_version": PROTOCOL_VERSION,
                        "replaced_runners": report.replaced_runners(),
                        "started_runners": report.started_runners(),
                        "repaired_rows": report.repaired_rows(),
                    }),
                )?;
            } else {
                writeln!(
                    stdout,
                    "runners: {} replaced, {} started; task rows: {} repaired",
                    report.replaced_runners(),
                    report.started_runners(),
                    report.repaired_rows()
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

fn write_task_report(
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
        stdout.flush()?;
        Ok(())
    }
}

fn write_task_list_report(
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
                writeln!(
                    stdout,
                    "{}: {} ({}) blocking: {}",
                    task.task_id,
                    task_state_name(task.state),
                    task.worker.as_deref().unwrap_or("unassigned"),
                    blocking_code
                )?;
            } else {
                writeln!(
                    stdout,
                    "{}: {} ({})",
                    task.task_id,
                    task_state_name(task.state),
                    task.worker.as_deref().unwrap_or("unassigned")
                )?;
            }
        }
        stdout.flush()?;
        Ok(())
    }
}

fn write_task_result_report(
    report: &task_client::TaskResultReport,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    if json {
        write_json_line(
            stdout,
            &serde_json::json!({
                "protocol_version": PROTOCOL_VERSION,
                "task_id": report.task_id().to_string(),
                "status": report.status(),
                "branch": report.branch(),
                "fetch": report.fetch_instruction(),
            }),
        )
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
    if matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::RefreshFacts
        }
    ) {
        return run_host_refresh_facts(cli.config, runtime, runner, stderr);
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
    if matches!(cli.command, Command::Workers { refresh: true }) {
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
    match inspect_configured_workers(cli.config, runtime, runner, true) {
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
) -> Result<WorkersInspection, WorkerError> {
    let config = load_config(config_override, runtime)?;
    let transport = SshTransport::new(runner);
    let refresh = refresh.then(|| {
        config
            .workers
            .iter()
            .map(|worker| match transport.refresh_facts(worker) {
                Ok(()) => WorkerRefreshOutcome {
                    ok: true,
                    error_code: None,
                    error_message: None,
                },
                Err(error) => refresh_failure(&error),
            })
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
        WorkerError::Capacity { code, .. } => (*code, "worker admission rejected"),
        WorkerError::Snapshot { code, .. } => (*code, "snapshot operation failed"),
        WorkerError::Git { code, .. } => (*code, "Git operation failed"),
        WorkerError::Task { code, .. } => (*code, "task operation failed"),
        WorkerError::Agent { code, .. } => (*code, "agent operation failed"),
        WorkerError::Io(_) => ("HOST_IO", "host state operation failed"),
        _ => ("INVALID_REQUEST", "host request was invalid"),
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

fn run_host_refresh_facts(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    runner: &dyn ProcessRunner,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = discover_paths(config_override, runtime)?;
        ProbeCollector::refresh_facts_at(&paths.host_state_root(), &runtime.home, runner)?;
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
        WorkerError::Io(_) => "HOST_GIT_IO",
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
    fn unknown_toml_fields_are_rejected() {
        let error = Config::parse(
            "version = 1\nunknown = true\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1",
        )
        .expect_err("unknown configuration fields must be rejected");

        assert!(matches!(error, WorkerError::Config(_)));
    }
}
