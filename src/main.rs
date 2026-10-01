use std::{
    io::{self, Write},
    process::ExitCode,
};

use clap::{Parser, error::ErrorKind};
use mac_worker::{
    Cli, SystemProcessRunner, prepare_turn_requested, run_prepare_turn, run_with_stdio,
};

fn main() -> ExitCode {
    #[cfg(feature = "test-support")]
    if std::env::var_os("MAC_WORKER_TEST_SUPPORT_PROBE").as_deref()
        == Some(std::ffi::OsStr::new("1"))
    {
        println!("{}", mac_worker::test_support::runtime::FEATURE_MARKER);
        return ExitCode::SUCCESS;
    }
    if prepare_turn_requested() {
        return run_prepare_turn();
    }
    let stdout = io::stdout();
    let stderr = io::stderr();
    let stdin = io::stdin();
    let mut stdin = stdin.lock();
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

    ExitCode::from(run_with_stdio(
        cli,
        &SystemProcessRunner,
        &mut stdin,
        &mut stdout,
        &mut stderr,
    ))
}
