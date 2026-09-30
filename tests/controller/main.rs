#[path = "../support/mod.rs"]
mod support;

#[path = "../support/controller_process.rs"]
mod controller_process;
#[allow(dead_code)]
#[path = "../support/controller_gap.rs"]
mod fixture;
use support::fixture_pid;
#[path = "../support/controller_fail_closed_harness.rs"]
mod harness;
#[path = "../support/task_state.rs"]
mod task_state_fixture;

mod controller_batch;
mod controller_batch_freeze;
mod controller_batch_process_runtime;
mod controller_checkpoint;
mod controller_command_process_runtime;
mod controller_dashboard;
mod controller_drain;
mod controller_drain_attached;
mod controller_drain_rpc;
mod controller_fail_closed;
mod controller_features;
mod controller_followup_process_runtime;
mod controller_health;
mod controller_health_routes;
mod controller_health_runtime;
mod controller_lifecycle_compat;
mod controller_liveness_seams;
mod controller_logical_import;
mod controller_mutation_prepare;
mod controller_pilot_host;
mod controller_pilot_init;
mod controller_pilot_provision;
mod controller_process_runtime;
mod controller_publication_failure;
mod controller_publish_retry;
mod controller_read_routes;
mod controller_result_export;
mod controller_result_warnings;
mod controller_retry;
mod controller_say_interrupt;
mod controller_say_wait_exit;
mod controller_service;
mod controller_ssh_isolation;
mod controller_store_runtime;
mod controller_streamed_submit;
mod controller_task_mutations;
mod controller_transfer;
