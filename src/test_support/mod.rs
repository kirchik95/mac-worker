//! Feature-gated access to implementation contracts for integration tests.

pub mod agents;
pub mod channel;
pub mod cli;
pub mod client_state;
pub mod controller;
pub mod core;
pub mod dashboard;
pub mod events;
pub mod host;
pub mod integration;
pub mod runtime;
pub mod session;
pub mod task;
pub mod transfer;

// Unit watchdog coordination stays separate from the fork/flock exclusion in
// crate::test_sync. Enabling this feature does not enable that instrumentation.
#[cfg(test)]
#[path = "../../tests/support/test_sync.rs"]
mod coordination;
#[cfg(test)]
pub(crate) use coordination::*;
