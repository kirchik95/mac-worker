#[path = "../support/mod.rs"]
mod support;

use support::agent_launch_fixture;
use support::fake_herdr;
use support::fixture_pid;

mod cancel_lease_cleanup;
mod fd_limit;
mod host_hygiene;
mod host_lease;
mod job_protocol;
mod job_queries;
mod legacy_archive;
mod process_runner;
mod redaction;
mod rooted_fs;
mod supervisor;
