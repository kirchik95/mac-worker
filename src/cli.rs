use std::{convert::Infallible, ffi::OsString, fmt, path::PathBuf, str::FromStr, time::Duration};

use clap::{Parser, Subcommand};

use crate::job::JobId;

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
pub enum Command {
    Setup {
        hosts: Vec<String>,
    },
    Doctor {
        #[arg(long)]
        project: Option<PathBuf>,
        #[arg(long = "include", value_parser = non_empty_pattern)]
        includes: Vec<String>,
    },
    Workers {
        #[arg(long)]
        refresh: bool,
    },
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
    #[command(hide = true)]
    Host {
        #[command(subcommand)]
        command: HostCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum HostCommand {
    Probe,
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
    #[command(name = "task-turn")]
    TaskTurn,
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

fn supported_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() || duration > Duration::from_secs(24 * 60 * 60) {
        Err("timeout must be greater than zero and at most 24h".into())
    } else {
        Ok(duration)
    }
}
