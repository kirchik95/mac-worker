//! Explicit integration-test access for runtime contracts.

use std::io::{Read, Write};

use crate::{cli::Cli, error::WorkerError, output::CommandOutput, paths::PathLayout};
use crate::{controller::events::EventRuntime, process::ProcessRunner};

/// Harmless fixture marker shared by the library and the spawned test binary.
pub const FEATURE_MARKER: &str = "mac-worker:test-support=enabled";

pub use crate::runtime_context::{
    ControllerEventPublisher, ControllerEventRuntime, RuntimeContext,
};

pub fn execute_with(cli: Cli, runner: &dyn ProcessRunner) -> Result<CommandOutput, WorkerError> {
    crate::execute_with(cli, runner)
}

pub fn open_with_existing_controller_events(
    paths: &PathLayout,
    runtime: std::sync::Arc<dyn EventRuntime>,
) -> Result<
    (
        crate::client_state::ClientStateStore,
        Option<ControllerEventPublisher>,
    ),
    WorkerError,
> {
    crate::open_with_existing_controller_events(paths, runtime)
}

pub fn run_with_io(
    cli: Cli,
    runner: &dyn ProcessRunner,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    crate::run_with_io(cli, runner, stdout, stderr)
}

pub fn run_with_stdio(
    cli: Cli,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    crate::run_with_stdio(cli, runner, stdin, stdout, stderr)
}

pub fn run_with_io_in_context(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    crate::run_with_io_in_context(cli, runner, runtime, stdout, stderr)
}

pub fn run_with_stdio_in_context(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    crate::run_with_stdio_in_context(cli, runner, runtime, stdin, stdout, stderr)
}
