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
}

fn non_empty_pattern(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("include pattern must not be empty".into())
    } else {
        Ok(value.to_owned())
    }
}
