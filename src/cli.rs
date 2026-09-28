use std::{convert::Infallible, ffi::OsString, fmt, path::PathBuf, str::FromStr, time::Duration};

use clap::{Parser, Subcommand};

use crate::{job::JobId, task::TaskId};

#[derive(Debug, Parser)]
#[command(
    name = "worker",
    version = crate::build_id::BUILD_ID,
    about = "Run trusted development jobs on macOS workers"
)]
pub struct Cli {
    /// Configuration file path (default: ~/.config/mac-worker/config.toml)
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,
    /// Print machine-readable JSON on stdout
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    #[command(
        about = "Connect a Mac, create its configuration and check agent readiness",
        after_help = "Example: worker init alice@mini.local\nEnable Remote Login on the Mac first. SSH aliases also work.\nRerun the same command after completing any setup or login instructions."
    )]
    Init {
        /// SSH destination: user@hostname, an IP address, or an existing SSH alias
        #[arg(value_name = "SSH", value_parser = crate::onboarding::ssh_destination)]
        destination: String,
        /// Inventory name (defaults to the hostname; preserves existing names on retry)
        #[arg(long, value_parser = crate::onboarding::worker_name)]
        name: Option<String>,
        /// Agent to check; the first-task command will use this agent
        #[arg(long, default_value = "codex", value_parser = ["codex", "cursor", "opencode", "claude"])]
        agent: String,
        /// Existing environment profile on the worker, when the agent needs one
        #[arg(long, value_parser = crate::onboarding::identifier)]
        env_profile: Option<String>,
    },
    #[command(about = "Install or update helpers on configured workers (all by default)")]
    Setup {
        /// Inventory names to update; omit to update every configured worker
        hosts: Vec<String>,
        /// Install a debug build. Setup refuses debug binaries unless this is set.
        #[arg(long)]
        allow_debug: bool,
    },
    #[command(about = "Check a Git project and compatible workers before running a job")]
    Doctor {
        /// Git project to check (default: the current directory)
        #[arg(long)]
        project: Option<PathBuf>,
        /// Extra snapshot include pattern (repeatable)
        #[arg(long = "include", value_parser = non_empty_pattern)]
        includes: Vec<String>,
    },
    #[command(about = "Show worker health, capacity and agent logins")]
    Workers {
        /// Refresh agent and authentication facts before reporting
        #[arg(long)]
        refresh: bool,
        /// Clear turn-observed authentication incidents before refreshing
        #[arg(long, requires = "refresh")]
        clear_auth_incidents: bool,
    },
    #[command(about = "Preview or apply retention garbage collection on workers")]
    Gc {
        /// Reclaim retained tasks, branches, and mirrors; omit to preview only
        #[arg(long)]
        apply: bool,
    },
    #[command(about = "Open the local dashboard in your browser")]
    Dashboard {
        /// Loopback port to listen on
        #[arg(long, value_parser = clap::value_parser!(u16).range(1..=65535))]
        port: Option<u16>,
        /// Print the URL without opening a browser
        #[arg(long)]
        no_open: bool,
        /// Skip the stale agent-facts refresh. Settings and task replies still work.
        #[arg(long)]
        no_facts_refresh: bool,
        /// Remote viewer mode: serve the local store and exit on stdin EOF.
        /// Presence-only; production emitter is controller_dashboard_ssh_request.
        #[arg(long, hide = true, action = clap::ArgAction::SetTrue, num_args = 0)]
        controller_viewer: bool,
    },
    #[command(
        override_usage = "worker run [--worker NAME] [--no-wait] -- COMMAND",
        about = "Run a command on an automatically selected compatible worker, or pin one with --worker"
    )]
    Run {
        /// Pin one inventory name instead of automatic selection
        #[arg(long, value_parser = non_empty_worker)]
        worker: Option<String>,
        /// Reject the run when no heavy slot is free
        #[arg(long)]
        no_wait: bool,
        /// Git project to snapshot (default: the current directory)
        #[arg(long)]
        project: Option<PathBuf>,
        /// Extra snapshot include pattern (repeatable)
        #[arg(long = "include", value_parser = non_empty_pattern)]
        includes: Vec<String>,
        /// Maximum runtime, from 1s to 24h
        #[arg(long, value_parser = supported_duration)]
        timeout: Option<Duration>,
        /// Shell command to run instead of the trailing arguments
        #[arg(long, value_parser = non_empty_shell, conflicts_with = "argv")]
        shell: Option<String>,
        /// Command and arguments to run on the worker
        #[arg(last = true, num_args = 1.., required_unless_present = "shell")]
        argv: Vec<String>,
    },
    Status {
        /// Job to show; omit to list recent jobs
        job_id: Option<JobId>,
    },
    Logs {
        /// Keep printing new log lines until the job finishes
        #[arg(short = 'f')]
        follow: bool,
        /// Job whose logs to print
        job_id: JobId,
    },
    Cancel {
        /// Job to cancel
        job_id: JobId,
    },
    #[command(about = "Submit and manage durable agent tasks")]
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    #[command(about = "Print version-matched operator skill guides")]
    Skills {
        #[command(subcommand)]
        command: SkillsCommand,
    },
    #[command(about = "Run the remote persistent controller on an always-on machine")]
    Controller {
        #[command(subcommand)]
        command: ControllerCommand,
    },
    #[command(hide = true)]
    Runner {
        task_id: HiddenComponent,
        turn_id: HiddenComponent,
        #[arg(long = "slot-token", hide = true)]
        slot_token: Option<HiddenComponent>,
    },
    #[command(hide = true)]
    Host {
        #[command(subcommand)]
        command: HostCommand,
    },
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum TaskCommand {
    #[command(about = "Submit a prompt as a durable agent task")]
    Submit {
        /// Agent to run: codex, cursor, opencode, or claude
        #[arg(long, value_parser = non_empty_text)]
        agent: Option<String>,
        /// Model name passed through to the agent
        #[arg(long, value_parser = non_empty_text)]
        model: Option<String>,
        /// Effort level passed through to the agent
        #[arg(long, value_parser = non_empty_text)]
        effort: Option<String>,
        /// Prompt text; required unless --prompt-file is set
        #[arg(
            long,
            conflicts_with = "prompt_file",
            required_unless_present = "prompt_file"
        )]
        prompt: Option<String>,
        /// File whose contents are the prompt
        #[arg(
            long,
            value_name = "PATH",
            conflicts_with = "prompt",
            required_unless_present = "prompt"
        )]
        prompt_file: Option<PathBuf>,
        /// Short title stored with the task
        #[arg(long, value_parser = non_empty_text)]
        title: Option<String>,
        /// Git project to snapshot (default: the current directory)
        #[arg(long)]
        project: Option<PathBuf>,
        /// Base commit or ref (default: HEAD)
        #[arg(long, default_value = "HEAD", value_parser = non_empty_text)]
        base: String,
        /// Include uncommitted work in the snapshot
        #[arg(long)]
        wip: bool,
        /// Extra snapshot include pattern (repeatable)
        #[arg(long = "include", value_parser = non_empty_pattern)]
        includes: Vec<String>,
        /// Turn time limit, from 1s to 24h
        #[arg(long, value_parser = supported_duration)]
        timeout: Option<Duration>,
        /// Maximum agent turns for this task
        #[arg(long)]
        max_turns: Option<u32>,
        /// Maximum agent token budget for this task
        #[arg(long)]
        max_budget: Option<u64>,
        /// Maximum follow-up turns after the first
        #[arg(long)]
        max_followups: Option<u32>,
        /// When to close the task: done or never
        #[arg(long, value_parser = non_empty_text)]
        close_on: Option<String>,
        /// Environment profile name on the worker
        #[arg(long, value_parser = non_empty_text)]
        env_profile: Option<String>,
        /// Pin one inventory name instead of automatic selection
        #[arg(long, value_parser = non_empty_worker)]
        worker: Option<String>,
        /// Task source: local or origin
        #[arg(long, value_parser = non_empty_text)]
        source: Option<String>,
        /// Origin delivery mode: fetch or push (repeatable)
        #[arg(long, value_parser = non_empty_text)]
        publish: Vec<String>,
        /// Branch name used when publishing with push
        #[arg(long, value_parser = non_empty_text)]
        publish_branch: Option<String>,
        /// Reject the submit when no heavy slot is free
        #[arg(long)]
        no_wait: bool,
        /// Wait until the task is quiescent before returning
        #[arg(long)]
        wait: bool,
    },
    #[command(about = "Submit a validated TOML batch")]
    Batch {
        /// TOML batch file to validate and submit
        file: PathBuf,
        /// Name stored with this run
        #[arg(long, value_parser = non_empty_text)]
        name: Option<String>,
        /// Requested cap on tasks running at once
        #[arg(long)]
        max_parallel: Option<u32>,
        /// Wait until the batch is quiescent before returning
        #[arg(long)]
        wait: bool,
        /// Validate and print the plan without submitting
        #[arg(long, conflicts_with = "wait")]
        preview: bool,
    },
    List {
        /// Limit the list to one run id or name
        #[arg(long, value_name = "ID|NAME", value_parser = non_empty_text)]
        run: Option<String>,
        /// Limit the list to one task state
        #[arg(long, value_parser = non_empty_text)]
        state: Option<String>,
        /// Limit the list to one outcome
        #[arg(long, value_parser = non_empty_text)]
        outcome: Option<String>,
        /// Print the full task record
        #[arg(long)]
        full: bool,
    },
    Status {
        /// Task to show
        task_id: TaskId,
        /// Print the full task record
        #[arg(long)]
        full: bool,
    },
    Logs {
        /// Task whose logs to print
        task_id: TaskId,
        /// Print one turn number instead of every turn
        #[arg(long)]
        turn: Option<u32>,
        /// Keep printing new log lines until the turn finishes
        #[arg(short = 'f', long)]
        follow: bool,
        /// Print the original log bytes
        #[arg(long)]
        raw: bool,
    },
    Diff {
        /// Task whose change to show
        task_id: TaskId,
        /// Print a diffstat instead of the full diff
        #[arg(long)]
        stat: bool,
    },
    Say {
        /// Task to continue
        task_id: TaskId,
        /// Follow-up text; required unless --message-file is set
        #[arg(
            long,
            conflicts_with = "message_file",
            required_unless_present = "message_file"
        )]
        message: Option<String>,
        /// File whose contents are the follow-up
        #[arg(long, conflicts_with = "message", required_unless_present = "message")]
        message_file: Option<PathBuf>,
        /// Wait until the new turn is quiescent before returning
        #[arg(long)]
        wait: bool,
    },
    Cancel {
        /// Task to cancel
        task_id: TaskId,
    },
    Result {
        /// Task whose result to print
        task_id: TaskId,
    },
    Fetch {
        /// Task whose result ref to import
        task_id: TaskId,
    },
    Close {
        /// Task to close
        task_id: TaskId,
        /// Drop the retained result commits
        #[arg(long)]
        discard: bool,
    },
    #[command(about = "Re-drive a failed origin delivery after credentials are repaired")]
    PublishRetry {
        /// Task whose origin delivery to retry
        task_id: TaskId,
    },
    Wait {
        /// Wait for one task
        #[arg(long)]
        task_id: Option<TaskId>,
        /// Wait for every task in one run
        #[arg(long, value_name = "ID|NAME", value_parser = non_empty_text)]
        run: Option<String>,
        /// How long to wait, from 1s to 24h
        #[arg(long, value_parser = supported_duration)]
        timeout: Option<Duration>,
    },
    Reconcile,
}

#[derive(Debug, Subcommand)]
pub enum SkillsCommand {
    #[command(about = "List bundled skill guide names")]
    List,
    #[command(about = "Print a bundled skill guide with the live CLI grammar")]
    Get {
        /// Bundled skill name
        #[arg(value_parser = ["pool-dispatch", "pool-task-authoring"])]
        name: String,
        /// Print only the generated Grammar section
        #[arg(long)]
        grammar_only: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum ControllerCommand {
    #[command(about = "Provision and start a supervised remote controller")]
    Init {
        /// Laptop SSH destination of the controller host.
        destination: String,
        /// Override a worker destination as seen from the controller (repeatable).
        #[arg(long = "worker-ssh", value_name = "NAME=SSH")]
        worker_ssh: Vec<String>,
        /// Replace a differing controller inventory after reviewing its diff.
        #[arg(long)]
        force: bool,
    },
    #[command(about = "Unload the remote controller and disable laptop controller mode")]
    Disable,
    #[command(about = "Pause new turn runners while running turns finish")]
    Drain {
        /// Resume new runner handoffs instead of draining.
        #[arg(long)]
        off: bool,
    },
    #[command(
        about = "Read controller health, service state and drain state (use --json for details)"
    )]
    Status,
    #[command(about = "Hold the controller leader lock and resume durable requests")]
    Run {
        #[arg(long, hide = true)]
        supervised: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum HostCommand {
    #[command(name = "controller-configure", hide = true)]
    ControllerConfigure,
    #[command(name = "controller-key", hide = true)]
    ControllerKey,
    #[command(name = "authorize-controller-key", hide = true)]
    AuthorizeControllerKey,
    #[command(name = "controller-service", hide = true)]
    ControllerService,
    #[command(name = "controller-probe", hide = true)]
    ControllerProbe,
    Probe,
    #[command(name = "gc")]
    Gc,
    Status,
    #[command(name = "log-chunk")]
    LogChunk,
    #[command(name = "status-logs")]
    StatusLogs,
    #[command(name = "resolve-or-abandon")]
    ResolveOrAbandon,
    Cancel,
    Reconcile,
    Submit,
    Supervise {
        job_id: HiddenComponent,
    },
    #[command(name = "lease-acquire")]
    LeaseAcquire,
    #[command(name = "snapshot-verify")]
    SnapshotVerify,
    #[command(name = "migrate-layout")]
    MigrateLayout,
    #[command(name = "complete-protocol-upgrade")]
    CompleteProtocolUpgrade {
        target: HiddenComponent,
    },
    #[command(name = "complete-unverified-rollback")]
    CompleteUnverifiedRollback {
        target: HiddenComponent,
        previous: Option<HiddenComponent>,
    },
    #[command(name = "set-slots", hide = true)]
    SetSlots {
        slots: u8,
    },
    #[command(name = "refresh-facts")]
    RefreshFacts {
        /// Print per-step collection durations on stderr after writing facts
        #[arg(long)]
        timing: bool,
        /// Clear turn-observed authentication incidents before collecting facts
        #[arg(long)]
        clear_auth_incidents: bool,
    },
    #[command(name = "task-prepare")]
    TaskPrepare,
    #[command(name = "task-status")]
    TaskStatus,
    #[command(name = "task-diff")]
    TaskDiff,
    #[command(name = "task-close")]
    TaskClose,
    #[command(name = "task-session")]
    TaskSession,
    #[command(name = "task-prebind", hide = true)]
    TaskPrebind,
    #[command(name = "task-cancel")]
    TaskCancel,
    #[command(name = "task-turn")]
    TaskTurn,
    #[command(name = "agent-settings-get")]
    AgentSettingsGet,
    #[command(name = "agent-settings-set")]
    AgentSettingsSet,
    #[command(name = "follow-turn", hide = true)]
    FollowTurn {
        project_id: HiddenComponent,
        worktree_id: HiddenComponent,
        job_id: HiddenComponent,
    },
    #[command(name = "receive-pack")]
    ReceivePack {
        job_id: HiddenComponent,
        client_id: HiddenComponent,
        lease_token: HiddenComponent,
        request_fingerprint: HiddenComponent,
        path: HiddenComponent,
    },
    #[command(name = "upload-pack")]
    UploadPack {
        task_id: HiddenComponent,
        client_id: HiddenComponent,
        path: HiddenComponent,
    },
    #[command(name = "outbox")]
    Outbox {
        #[arg(long)]
        watch: bool,
        #[arg(long)]
        once: bool,
        #[arg(long)]
        enable: bool,
        #[arg(long, value_name = "DIR")]
        write_agent: Option<PathBuf>,
        /// Exact HostStore root. Spawned watchers must not fall back to live PathLayout.
        #[arg(long)]
        host_root: Option<PathBuf>,
        /// Short-lived production launch: spawn `--watch` then exit.
        #[arg(long, hide = true)]
        wake: bool,
    },
    #[command(name = "outbox-retry", hide = true)]
    OutboxRetry {
        task_id: TaskId,
    },
    #[command(name = "rsync-receive", trailing_var_arg = true)]
    RsyncReceive {
        job_id: HiddenComponent,
        client_id: HiddenComponent,
        lease_token: HiddenComponent,
        request_fingerprint: HiddenComponent,
        #[arg(num_args = 1.., allow_hyphen_values = true)]
        server_args: Vec<OsString>,
    },
    #[command(name = "controller-rpc", hide = true)]
    ControllerRpc,
    #[command(name = "controller-receive-pack", hide = true)]
    ControllerReceivePack {
        token: HiddenComponent,
        request_id: HiddenComponent,
        fingerprint: HiddenComponent,
        project_id: HiddenComponent,
        worktree_id: HiddenComponent,
        oid: HiddenComponent,
        path: Option<HiddenComponent>,
    },
    #[command(name = "controller-upload-pack", hide = true)]
    ControllerUploadPack {
        token: HiddenComponent,
        request_id: HiddenComponent,
        fingerprint: HiddenComponent,
        task_id: HiddenComponent,
        turn_id: HiddenComponent,
        oid: HiddenComponent,
        path: Option<HiddenComponent>,
    },
}

#[derive(Clone)]
pub struct HiddenComponent(String);

impl HiddenComponent {
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for HiddenComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HiddenComponent([REDACTED])")
    }
}

impl FromStr for HiddenComponent {
    type Err = Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.into()))
    }
}

fn non_empty_pattern(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("include pattern must not be empty".into())
    } else {
        Ok(value.to_owned())
    }
}

fn non_empty_worker(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("worker name must not be empty".into())
    } else {
        Ok(value.to_owned())
    }
}

fn non_empty_shell(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("shell command must not be empty".into())
    } else {
        Ok(value.to_owned())
    }
}

fn non_empty_text(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("value must not be empty".into())
    } else {
        Ok(value.to_owned())
    }
}

fn supported_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() || duration > Duration::from_secs(24 * 60 * 60) {
        Err("timeout must be greater than zero and at most 24h".into())
    } else {
        Ok(duration)
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command, SkillsCommand};

    #[test]
    fn skills_list_and_get_parse_the_public_forms() {
        let list = Cli::try_parse_from(["worker", "skills", "list"]).unwrap();
        assert!(matches!(
            list.command,
            Command::Skills {
                command: SkillsCommand::List
            }
        ));

        let get = Cli::try_parse_from(["worker", "skills", "get", "pool-dispatch"]).unwrap();
        let Command::Skills {
            command: SkillsCommand::Get { name, grammar_only },
        } = get.command
        else {
            panic!("expected skills get");
        };
        assert_eq!(name, "pool-dispatch");
        assert!(!grammar_only);

        let grammar_only = Cli::try_parse_from([
            "worker",
            "skills",
            "get",
            "pool-task-authoring",
            "--grammar-only",
        ])
        .unwrap();
        assert!(matches!(
            grammar_only.command,
            Command::Skills {
                command: SkillsCommand::Get {
                    grammar_only: true,
                    ..
                }
            }
        ));
    }

    #[test]
    fn skills_get_rejects_an_unknown_name() {
        assert!(Cli::try_parse_from(["worker", "skills", "get", "not-a-skill"]).is_err());
    }
}
