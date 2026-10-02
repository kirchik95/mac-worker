#[path = "../support/mod.rs"]
mod support;

use support::fake_herdr;

mod agent_adapters;
mod agent_capabilities;
mod agent_cursor_opencode;
mod agent_facts;
mod agent_launch_environment;
mod agent_probe;
mod agent_publication;
mod agent_settings;
mod agent_settings_catalog;
mod agent_settings_profile;
mod keychain;
mod launch_identity;
mod opencode_facts;
mod opencode_launch_guard;
mod result_parse_reasons;

mod session_capture_claude;
mod session_capture_codex;
mod session_contracts;
