use clap::{Parser, error::ErrorKind};
use mac_worker::{
    cli::{Cli, Command, HostCommand},
    execute_with,
    output::CommandOutput,
    process::SystemProcessRunner,
};
use std::process::ExitCode;

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let informational = matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            );
            if let Err(print_error) = error.print() {
                eprintln!("I/O error: failed to print command-line output: {print_error}");
                return ExitCode::from(74);
            }
            return if informational {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(64)
            };
        }
    };
    let json = cli.json;
    let host_probe = matches!(
        &cli.command,
        Command::Host {
            command: HostCommand::Probe
        }
    );

    match execute_with(cli, &SystemProcessRunner) {
        Ok(output) => match render(&output, json, host_probe) {
            Ok(rendered) => {
                println!("{rendered}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(error.exit_kind() as u8)
            }
        },
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(error.exit_kind() as u8)
        }
    }
}

fn render(
    output: &CommandOutput,
    json: bool,
    raw_probe: bool,
) -> Result<String, mac_worker::error::WorkerError> {
    if raw_probe {
        let CommandOutput::Probe(probe) = output else {
            return Err(mac_worker::error::WorkerError::Protocol(
                "host probe returned the wrong output type".into(),
            ));
        };
        return serde_json::to_string(probe).map_err(|error| {
            mac_worker::error::WorkerError::Protocol(format!(
                "failed to serialize host probe: {error}"
            ))
        });
    }

    if json {
        output.render_json()
    } else {
        Ok(output.render_human())
    }
}
