use std::path::PathBuf;

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
}
