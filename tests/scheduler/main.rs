#[path = "../support/mod.rs"]
mod support;

use support::fixture_pid;
#[path = "../support/task_state.rs"]
mod task_state_fixture;

mod admission_cache;
mod client_state;
mod client_state_active_tasks;
mod fleet_reconciliation;
mod legacy_fleet_protocol;
mod run_command;
mod scheduler_adapter;
mod scheduler_concurrency;
mod scheduler_policy;
mod scheduler_queue;

mod project_preflight_ports;
mod task_diagnostics_ports;
mod task_ports_fixture;
mod task_reconciliation_ports;
