//! Explicit integration-test access for runtime contracts.

/// Harmless fixture marker shared by the library and the spawned test binary.
pub const FEATURE_MARKER: &str = "mac-worker:test-support=enabled";

pub use crate::{
    ControllerEventPublisher, ControllerEventRuntime, RuntimeContext, execute_with,
    open_with_existing_controller_events, run_with_io, run_with_io_in_context, run_with_stdio,
    run_with_stdio_in_context,
};
