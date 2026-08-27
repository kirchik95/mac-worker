use std::{convert::Infallible, ffi::OsString, fmt, path::PathBuf, str::FromStr};

use clap::{Parser, Subcommand};

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
    Workers,
    #[command(hide = true)]
    Host {
        #[command(subcommand)]
        command: HostCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum HostCommand {
    Probe,
    #[command(name = "lease-acquire")]
    LeaseAcquire,
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
