use std::{
    io::{self, Write},
    process::ExitCode,
};

use clap::{Parser, error::ErrorKind};
use mac_worker::{cli::Cli, process::SystemProcessRunner, run_with_io};

fn main() -> ExitCode {
    let stdout = io::stdout();
    let stderr = io::stderr();
    let mut stdout = stdout.lock();
    let mut stderr = stderr.lock();

    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let informational = matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            );
            let rendered = error.to_string();
            let write_result = if informational {
                stdout.write_all(rendered.as_bytes())
            } else {
                stderr.write_all(rendered.as_bytes())
            };
            if let Err(print_error) = write_result {
                let _ = writeln!(
                    stderr,
                    "I/O error: failed to print command-line output: {print_error}"
                );
                return ExitCode::from(74);
            }
            return if informational {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(64)
            };
        }
    };

    ExitCode::from(run_with_io(
        cli,
        &SystemProcessRunner,
        &mut stdout,
        &mut stderr,
    ))
}
