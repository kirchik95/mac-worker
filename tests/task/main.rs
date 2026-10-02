#[path = "../support/mod.rs"]
mod support;

use support::fixture_pid;
#[path = "../support/task_state.rs"]
mod task_state_fixture;
#[path = "../support/questions_v7_dtos.rs"]
mod v7;

mod follow_turn;
mod opencode_session_delete;
mod prepared_followup;
mod prepared_followup_completion;
mod project_config;
mod project_inspection;
mod project_state;
mod questions_compat;
mod runner_dispatch;
mod session_import_turn;
mod task_command;
mod task_conversation;
mod task_dag;
mod task_gc;
mod task_interrupt;
mod task_logs;
mod task_materialization;
mod task_model;
mod task_project_context;
mod task_review;
mod task_turn;
mod task_view;
mod turn_log;
mod turn_runner;
