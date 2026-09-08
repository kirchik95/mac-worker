use std::{convert::Infallible, ffi::OsString, fmt, path::PathBuf, str::FromStr, time::Duration};

use clap::{Parser, Subcommand};

use crate::{
    job::JobId,
    task::{RunId, TaskId},
};

#[derive(Debug, Parser)]
#[command(
    name = "worker",
    version,
    about = "Run trusted development jobs on macOS workers"
)]
pub struct Cli {
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    #[command(
        about = "Connect a Mac, create its configuration and check agent readiness",
        after_help = "Example: worker init alice@mini.local\nEnable Remote Login on the Mac first. SSH aliases also work.\nRerun the same command after completing any setup or login instructions."
    )]
    Init {
        /// SSH destination: user@hostname, an IP address, or an existing SSH alias
        #[arg(value_name = "SSH", value_parser = crate::onboarding::ssh_destination)]
        destination: String,
        /// Inventory name (defaults to the hostname; preserves existing names on retry)
        #[arg(long, value_parser = crate::onboarding::worker_name)]
        name: Option<String>,
        /// Agent to check; the first-task command will use this agent
        #[arg(long, default_value = "codex", value_parser = ["codex", "cursor", "opencode", "claude"])]
        agent: String,
        /// Existing environment profile on the worker, when the agent needs one
        #[arg(long, value_parser = crate::onboarding::identifier)]
        env_profile: Option<String>,
    },
    #[command(about = "Install or update helpers on configured workers (all by default)")]
    Setup {
        /// Inventory names, as shown by `worker workers`
        hosts: Vec<String>,
    },
    #[command(about = "Check a Git project and compatible workers before running a job")]
    Doctor {
        #[arg(long)]
        project: Option<PathBuf>,
        #[arg(long = "include", value_parser = non_empty_pattern)]
        includes: Vec<String>,
    },
    #[command(about = "Show worker health, capacity and agent logins")]
    Workers {
        /// Refresh agent and authentication facts before reporting
        #[arg(long)]
        refresh: bool,
    },
    #[command(about = "Preview or apply retention garbage collection on workers")]
    Gc {
        #[arg(long)]
        apply: bool,
    },
    #[command(about = "Open the local dashboard in your browser")]
    Dashboard {
        #[arg(long, value_parser = clap::value_parser!(u16).range(1..=65535))]
        port: Option<u16>,
        #[arg(long)]
        no_open: bool,
    },
    #[command(
        override_usage = "worker run [--worker NAME] [--no-wait] -- COMMAND",
        about = "Run a command on an automatically selected compatible worker, or pin one with --worker"
    )]
    Run {
        #[arg(long, value_parser = non_empty_worker)]
        worker: Option<String>,
        #[arg(long)]
        no_wait: bool,
        #[arg(long)]
        project: Option<PathBuf>,
        #[arg(long = "include", value_parser = non_empty_pattern)]
        includes: Vec<String>,
        #[arg(long, value_parser = supported_duration)]
        timeout: Option<Duration>,
        #[arg(long, value_parser = non_empty_shell, conflicts_with = "argv")]
        shell: Option<String>,
        #[arg(last = true, num_args = 1.., required_unless_present = "shell")]
        argv: Vec<String>,
    },
    Status {
        job_id: Option<JobId>,
    },
    Logs {
        #[arg(short = 'f')]
        follow: bool,
        job_id: JobId,
    },
    Cancel {
        job_id: JobId,
    },
    #[command(about = "Submit and manage durable agent tasks")]
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    #[command(hide = true)]
    Runner {
        task_id: HiddenComponent,
        turn_id: HiddenComponent,
    },
    #[command(hide = true)]
    Host {
        #[command(subcommand)]
        command: HostCommand,
    },
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum TaskCommand {
    #[command(about = "Submit a prompt as a durable agent task")]
    Submit {
        #[arg(long, value_parser = non_empty_text)]
        agent: Option<String>,
        #[arg(long, value_parser = non_empty_text)]
        model: Option<String>,
        #[arg(long, value_parser = non_empty_text)]
        effort: Option<String>,
        #[arg(
            long,
            conflicts_with = "prompt_file",
            required_unless_present = "prompt_file"
        )]
        prompt: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            conflicts_with = "prompt",
            required_unless_present = "prompt"
        )]
        prompt_file: Option<PathBuf>,
        #[arg(long, value_parser = non_empty_text)]
        title: Option<String>,
        #[arg(long)]
        project: Option<PathBuf>,
        #[arg(long, default_value = "HEAD", value_parser = non_empty_text)]
        base: String,
        #[arg(long)]
        wip: bool,
        #[arg(long = "include", value_parser = non_empty_pattern)]
        includes: Vec<String>,
        #[arg(long, value_parser = supported_duration)]
        timeout: Option<Duration>,
        #[arg(long)]
        max_turns: Option<u32>,
        #[arg(long)]
        max_budget: Option<u64>,
        #[arg(long)]
        max_followups: Option<u32>,
        #[arg(long, value_parser = non_empty_text)]
        close_on: Option<String>,
        #[arg(long, value_parser = non_empty_text)]
        env_profile: Option<String>,
        #[arg(long, value_parser = non_empty_worker)]
        worker: Option<String>,
        #[arg(long, value_parser = non_empty_text)]
        source: Option<String>,
        #[arg(long, value_parser = non_empty_text)]
        publish: Vec<String>,
        #[arg(long, value_parser = non_empty_text)]
        publish_branch: Option<String>,
        #[arg(long)]
        no_wait: bool,
        #[arg(long)]
        wait: bool,
    },
    #[command(about = "Submit a validated TOML batch")]
    Batch {
        file: PathBuf,
        #[arg(long, value_parser = non_empty_text)]
        name: Option<String>,
        #[arg(long)]
        max_parallel: Option<u32>,
        #[arg(long)]
        wait: bool,
    },
    List {
        #[arg(long)]
        run: Option<RunId>,
        #[arg(long, value_parser = non_empty_text)]
        state: Option<String>,
        #[arg(long, value_parser = non_empty_text)]
        outcome: Option<String>,
        #[arg(long)]
        full: bool,
    },
    Status {
        task_id: TaskId,
        #[arg(long)]
        full: bool,
    },
    Logs {
        task_id: TaskId,
        #[arg(long)]
        turn: Option<u32>,
        #[arg(short = 'f', long)]
        follow: bool,
        #[arg(long)]
        raw: bool,
    },
    Diff {
        task_id: TaskId,
        #[arg(long)]
        stat: bool,
    },
    Say {
        task_id: TaskId,
        #[arg(
            long,
            conflicts_with = "message_file",
            required_unless_present = "message_file"
        )]
        message: Option<String>,
        #[arg(long, conflicts_with = "message", required_unless_present = "message")]
        message_file: Option<PathBuf>,
        #[arg(long)]
        wait: bool,
    },
    Cancel {
        task_id: TaskId,
    },
    Result {
        task_id: TaskId,
    },
    Fetch {
        task_id: TaskId,
    },
    Close {
        task_id: TaskId,
        #[arg(long)]
        discard: bool,
    },
    Wait {
        #[arg(long)]
        task_id: Option<TaskId>,
        #[arg(long)]
        run: Option<RunId>,
        #[arg(long, value_parser = supported_duration)]
        timeout: Option<Duration>,
    },
    Reconcile,
}

#[derive(Debug, Subcommand)]
pub enum HostCommand {
    Probe,
    #[command(name = "gc")]
    Gc,
    Status,
    #[command(name = "log-chunk")]
    LogChunk,
    #[command(name = "resolve-or-abandon")]
    ResolveOrAbandon,
    Cancel,
    Reconcile,
    Submit,
    Supervise {
        job_id: HiddenComponent,
    },
    #[command(name = "lease-acquire")]
    LeaseAcquire,
    #[command(name = "snapshot-verify")]
    SnapshotVerify,
    #[command(name = "migrate-layout")]
    MigrateLayout,
    #[command(name = "refresh-facts")]
    RefreshFacts,
    #[command(name = "task-prepare")]
    TaskPrepare,
    #[command(name = "task-status")]
    TaskStatus,
    #[command(name = "task-diff")]
    TaskDiff,
    #[command(name = "task-close")]
    TaskClose,
    #[command(name = "task-session")]
    TaskSession,
    #[command(name = "task-prebind", hide = true)]
    TaskPrebind,
    #[command(name = "task-cancel")]
    TaskCancel,
    #[command(name = "task-turn")]
    TaskTurn,
    #[command(name = "agent-settings-get")]
    AgentSettingsGet,
    #[command(name = "agent-settings-set")]
    AgentSettingsSet,
    #[command(name = "follow-turn", hide = true)]
    FollowTurn {
        project_id: HiddenComponent,
        worktree_id: HiddenComponent,
        job_id: HiddenComponent,
    },
    #[command(name = "receive-pack")]
    ReceivePack {
        job_id: HiddenComponent,
        client_id: HiddenComponent,
        lease_token: HiddenComponent,
        request_fingerprint: HiddenComponent,
        path: HiddenComponent,
    },
    #[command(name = "upload-pack")]
    UploadPack {
        task_id: HiddenComponent,
        client_id: HiddenComponent,
        path: HiddenComponent,
    },
    #[command(name = "rsync-receive", trailing_var_arg = true)]
    RsyncReceive {
        job_id: HiddenComponent,
        client_id: HiddenComponent,
        lease_token: HiddenComponent,
        request_fingerprint: HiddenComponent,
        #[arg(num_args = 1.., allow_hyphen_values = true)]
        server_args: Vec<OsString>,
    },
}

#[derive(Clone)]
pub struct HiddenComponent(String);

impl HiddenComponent {
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for HiddenComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HiddenComponent([REDACTED])")
    }
}

impl FromStr for HiddenComponent {
    type Err = Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.into()))
    }
}

fn non_empty_pattern(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("include pattern must not be empty".into())
    } else {
        Ok(value.to_owned())
    }
}

fn non_empty_worker(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("worker name must not be empty".into())
    } else {
        Ok(value.to_owned())
    }
}

fn non_empty_shell(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("shell command must not be empty".into())
    } else {
        Ok(value.to_owned())
    }
}

fn non_empty_text(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("value must not be empty".into())
    } else {
        Ok(value.to_owned())
    }
}

fn supported_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() || duration > Duration::from_secs(24 * 60 * 60) {
        Err("timeout must be greater than zero and at most 24h".into())
    } else {
        Ok(duration)
    }
}
