#[path = "../support/mod.rs"]
mod support;

use support::fake_herdr;
use support::fixture_pid;
#[path = "../support/task_state.rs"]
mod task_state_fixture;

mod dashboard_cache;
mod dashboard_command;
mod dashboard_cpu_adapter;
mod dashboard_model;
mod dashboard_mutations;
mod dashboard_queue;
mod dashboard_service;
mod dashboard_settings;
mod dashboard_source;
mod dashboard_tasks;
mod dashboard_tunnel_reconnect;
mod dashboard_web;
mod herdr_client;
mod herdr_reporter;
