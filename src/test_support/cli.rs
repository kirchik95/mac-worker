//! Explicit integration-test access for cli contracts.

pub use crate::cli::{Cli, Command, ControllerCommand, HostCommand, TaskCommand};

/// Inspect the parsed command while retaining the CLI value.
pub fn command(cli: &Cli) -> &Command {
    &cli.command
}

/// Move the parsed command out of the CLI value.
pub fn into_command(cli: Cli) -> Command {
    cli.command
}

pub fn json(cli: &Cli) -> bool {
    cli.json
}

/// Construct a CLI fixture without exposing its fields through production APIs.
pub fn from_parts(config: Option<std::path::PathBuf>, json: bool, command: Command) -> Cli {
    Cli {
        config,
        json,
        command,
    }
}
