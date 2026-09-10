use std::{borrow::Cow, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitKind {
    Usage = 64,
    Unavailable = 69,
    Infrastructure = 70,
    Io = 74,
    Capacity = 75,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStream {
    Stdout,
    Stderr,
}

impl std::fmt::Display for ProcessStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stdout => formatter.write_str("stdout"),
            Self::Stderr => formatter.write_str("stderr"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("process {stream} exceeded its {limit}-byte capture limit")]
    OutputLimitExceeded { stream: ProcessStream, limit: usize },
    #[error("process exceeded its {deadline:?} execution deadline")]
    DeadlineExceeded { deadline: Duration },
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("worker unavailable: {0}")]
    Unavailable(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("project error [{code}]: {message}")]
    Project { code: &'static str, message: String },
    #[error("snapshot error [{code}]: {message}")]
    Snapshot { code: &'static str, message: String },
    #[error("capacity error [{code}]: {message}")]
    Capacity {
        code: &'static str,
        message: Cow<'static, str>,
        public: bool,
    },
    #[error("queue error [{code}]: {message}")]
    Queue { code: &'static str, message: String },
    #[error("transport error [{code}]: {message}")]
    Transport { code: &'static str, message: String },
    #[error("git error [{code}]: {message}")]
    Git { code: &'static str, message: String },
    #[error("agent error [{code}]: {message}")]
    Agent { code: &'static str, message: String },
    #[error("task error [{code}]: {message}")]
    Task {
        code: &'static str,
        message: Cow<'static, str>,
    },
    #[error("command exited with status {code}")]
    CommandExit { code: u8 },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("process error: {0}")]
    Process(#[from] ProcessError),
}

impl From<crate::agent::AdapterError> for WorkerError {
    fn from(error: crate::agent::AdapterError) -> Self {
        Self::Agent {
            code: "AGENT_UNSUPPORTED",
            message: error.to_string(),
        }
    }
}

impl WorkerError {
    pub fn exit_kind(&self) -> ExitKind {
        match self {
            Self::Config(_) => ExitKind::Usage,
            Self::Project { .. } => ExitKind::Usage,
            Self::Unavailable(message) if coded_prefix(message) == Some("HOST_LAYOUT_OUTDATED") => {
                ExitKind::Infrastructure
            }
            Self::Unavailable(_) => ExitKind::Unavailable,
            Self::Capacity { .. } => ExitKind::Capacity,
            Self::Transport { .. } => ExitKind::Unavailable,
            Self::Git { code, .. } => git_exit_kind(code),
            Self::Agent { code, .. } => agent_exit_kind(code),
            Self::Task { code, .. } => task_exit_kind(code),
            Self::CommandExit { .. } => ExitKind::Infrastructure,
            Self::Protocol(_) | Self::Process(_) | Self::Snapshot { .. } | Self::Queue { .. } => {
                ExitKind::Infrastructure
            }
            Self::Io(_) => ExitKind::Io,
        }
    }

    pub fn exit_code(&self) -> u8 {
        match self {
            Self::CommandExit { code } => *code,
            Self::Agent {
                code: "AGENT_LIMIT_REACHED",
                ..
            } => 1,
            _ => self.exit_kind() as u8,
        }
    }

    pub fn public_code(&self) -> String {
        match self {
            Self::Project { code, .. } => stable_public_code(code, "PROJECT"),
            Self::Snapshot { code, .. } => stable_public_code(code, "SNAPSHOT"),
            Self::Capacity { code, .. } => stable_public_code(code, "CAPACITY"),
            Self::Queue { code, .. } => stable_public_code(code, "QUEUE"),
            Self::Transport { code, .. } => stable_public_code(code, "TRANSPORT"),
            Self::Git { code, .. } => stable_public_code(code, "GIT"),
            Self::Agent { code, .. } => stable_public_code(code, "AGENT"),
            Self::Task { code, .. } => stable_public_code(code, "TASK"),
            Self::Config(message) => coded_prefix(message).unwrap_or("CONFIG").to_owned(),
            Self::Unavailable(message) => coded_prefix(message).unwrap_or("UNAVAILABLE").to_owned(),
            Self::Protocol(message) => coded_prefix(message).unwrap_or("PROTOCOL").to_owned(),
            Self::CommandExit { .. } => "COMMAND_EXIT".to_owned(),
            Self::Io(_) => "IO".to_owned(),
            Self::Process(_) => "PROCESS".to_owned(),
        }
    }

    pub fn public_message(&self) -> String {
        match self {
            Self::Project { .. } => "project error".into(),
            Self::Snapshot { .. } => "snapshot error".into(),
            Self::Capacity {
                message, public, ..
            } => {
                if *public {
                    message.as_ref().to_owned()
                } else {
                    "capacity error".into()
                }
            }
            Self::Queue { .. } => "queue error".into(),
            Self::Transport { .. } => "transport error".into(),
            Self::Git { .. } => "git error".into(),
            Self::Agent { .. } => "agent error".into(),
            Self::Task { message, .. } => match message {
                Cow::Borrowed(text) => (*text).into(),
                Cow::Owned(_) => "task error".into(),
            },
            Self::Config(_) => "configuration error".into(),
            Self::Unavailable(_) => "worker unavailable".into(),
            Self::Protocol(_) => "protocol error".into(),
            Self::CommandExit { .. } => "command exited".into(),
            Self::Io(_) => "I/O error".into(),
            Self::Process(_) => "process error".into(),
        }
    }

    /// Task errors whose message is a `'static` literal are operator-facing
    /// (`TASK_BUSY` reasons never contain a path or prompt). Owned strings stay
    /// redacted because they are often formatted around a path or remote detail.
    pub fn task(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::Task {
            code,
            message: message.into(),
        }
    }

    /// Capacity errors whose message is a `'static` literal are operator-facing.
    /// Admission reasons such as `CAPACITY_BUSY` are fixed inventory text, not a
    /// path, prompt, or env value.
    pub fn capacity(code: &'static str, message: &'static str) -> Self {
        Self::Capacity {
            code,
            message: Cow::Borrowed(message),
            public: true,
        }
    }

    /// Formatted capacity messages that contain only inventory worker names and
    /// capability names. Those strings come from the operator's own config and
    /// requirements, not from a path, prompt, or env value.
    pub fn capacity_public(code: &'static str, message: String) -> Self {
        Self::Capacity {
            code,
            message: Cow::Owned(message),
            public: true,
        }
    }
}

fn coded_prefix(message: &str) -> Option<&str> {
    let (code, _) = message.split_once(": ")?;
    is_stable_public_code(code).then_some(code)
}

fn stable_public_code(code: &str, fallback: &'static str) -> String {
    if is_stable_public_code(code) {
        code.to_owned()
    } else {
        fallback.to_owned()
    }
}

fn git_exit_kind(code: &str) -> ExitKind {
    match code {
        "BASE_PUSH_FAILED" | "RESULT_FETCH_FAILED" => ExitKind::Unavailable,
        _ => ExitKind::Infrastructure,
    }
}

fn agent_exit_kind(code: &str) -> ExitKind {
    match code {
        "AGENT_NOT_INSTALLED" | "AGENT_NOT_AUTHENTICATED" => ExitKind::Capacity,
        "AGENT_UNSUPPORTED" => ExitKind::Usage,
        _ => ExitKind::Infrastructure,
    }
}

fn task_exit_kind(code: &str) -> ExitKind {
    match code {
        "RUNNER_HANDOFF_FAILED" => ExitKind::Io,
        "WAIT_TIMEOUT" | "WAIT_BLOCKED" => ExitKind::Infrastructure,
        _ => ExitKind::Usage,
    }
}

pub(crate) fn is_stable_public_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 128
        && code.starts_with(|byte: char| byte.is_ascii_uppercase())
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::{ExitKind, WorkerError};

    #[test]
    fn application_errors_map_to_the_reserved_exit_kinds() {
        let cases = [
            (WorkerError::Config("bad config".into()), ExitKind::Usage),
            (
                WorkerError::Unavailable("offline".into()),
                ExitKind::Unavailable,
            ),
            (
                WorkerError::Protocol("bad response".into()),
                ExitKind::Infrastructure,
            ),
            (
                WorkerError::Project {
                    code: "NOT_A_WORKTREE",
                    message: "no worktree at the requested path".into(),
                },
                ExitKind::Usage,
            ),
            (
                WorkerError::Snapshot {
                    code: "SNAPSHOT_WRITE_FAILED",
                    message: "object store unavailable".into(),
                },
                ExitKind::Infrastructure,
            ),
            (
                WorkerError::capacity("CAPACITY_BUSY", "one heavy job is already active"),
                ExitKind::Capacity,
            ),
            (
                WorkerError::Queue {
                    code: "QUEUE_JOB_CONFLICT",
                    message: "job ID is already queued".into(),
                },
                ExitKind::Infrastructure,
            ),
            (
                WorkerError::Transport {
                    code: "SSH_UNAVAILABLE",
                    message: "host is offline".into(),
                },
                ExitKind::Unavailable,
            ),
            (
                WorkerError::Io(std::io::Error::other("disk failed")),
                ExitKind::Io,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.exit_kind(), expected);
        }
    }

    #[test]
    fn command_exit_preserves_the_child_status_exactly() {
        assert_eq!(WorkerError::CommandExit { code: 7 }.exit_code(), 7);
        assert_eq!(WorkerError::CommandExit { code: 143 }.exit_code(), 143);
    }

    #[test]
    fn task_wait_timeout_uses_the_infrastructure_exit_code() {
        let error = WorkerError::task(
            "WAIT_TIMEOUT",
            "task wait timed out without cancelling the task",
        );
        assert_eq!(error.public_code(), "WAIT_TIMEOUT");
        assert_eq!(error.exit_code(), 70);
        assert_eq!(
            error.public_message(),
            "task wait timed out without cancelling the task"
        );
    }

    #[test]
    fn task_wait_blocked_uses_the_infrastructure_exit_code() {
        let error = WorkerError::task("WAIT_BLOCKED", "RUNNER_REPEATED_FAILURE");
        assert_eq!(error.public_code(), "WAIT_BLOCKED");
        assert_eq!(error.exit_code(), 70);
        assert_eq!(error.public_message(), "RUNNER_REPEATED_FAILURE");
    }

    #[test]
    fn static_task_busy_reasons_are_public_diagnostics() {
        let error = WorkerError::task("TASK_BUSY", "task turn is being dispatched");
        assert_eq!(error.public_code(), "TASK_BUSY");
        assert_eq!(error.public_message(), "task turn is being dispatched");
        assert!(error.public_message().len() <= 4096);
        assert!(!error.public_message().as_bytes().contains(&0));
    }

    #[test]
    fn static_capacity_busy_reasons_are_public_diagnostics() {
        let error = WorkerError::capacity(
            "CAPACITY_BUSY",
            "no eligible worker currently has an available heavy slot",
        );
        assert_eq!(error.public_code(), "CAPACITY_BUSY");
        assert_eq!(
            error.public_message(),
            "no eligible worker currently has an available heavy slot"
        );
        assert!(error.public_message().len() <= 4096);
        assert!(!error.public_message().as_bytes().contains(&0));
    }

    #[test]
    fn capacity_messages_built_from_worker_and_capability_names_are_public() {
        let error = WorkerError::capacity_public(
            "CAPABILITY_MISSING",
            format!(
                "pinned worker {worker} is missing required capabilities: {capability}",
                worker = "mini-1",
                capability = "agent:cursor@agents"
            ),
        );
        assert_eq!(error.public_code(), "CAPABILITY_MISSING");
        assert_eq!(
            error.public_message(),
            "pinned worker mini-1 is missing required capabilities: agent:cursor@agents"
        );
    }

    #[test]
    fn formatted_capacity_messages_with_other_content_stay_redacted() {
        let planted_path = "/Users/alice/PLANTED_PUBLIC_PATH/secret.toml";
        let error = WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            message: format!("busy lease at {planted_path}").into(),
            public: false,
        };
        assert_eq!(error.public_code(), "CAPACITY_BUSY");
        assert_eq!(error.public_message(), "capacity error");
        assert!(!error.public_message().contains(planted_path));
    }

    #[test]
    fn public_diag_helpers_are_bounded_content_free_for_every_variant() {
        let planted_path = "/Users/alice/PLANTED_PUBLIC_PATH/secret.toml";
        let planted_secret = "PLANTED_PUBLIC_SECRET";
        let planted_argv = "printf TASK9_COMMAND_SECRET";
        let planted_nul = format!("CODE: leak\0{planted_path}");
        let oversized = format!("CODE: {}{planted_secret}", "X".repeat(4096));
        let cases = [
            (
                WorkerError::Config(format!("failed to read {planted_path}: missing")),
                "CONFIG",
                "configuration error",
            ),
            (
                WorkerError::Config(format!(
                    "JOB_NOT_FOUND: no local job at {planted_path} with {planted_secret}"
                )),
                "JOB_NOT_FOUND",
                "configuration error",
            ),
            (
                WorkerError::Config("not-a-code: leaked suffix".into()),
                "CONFIG",
                "configuration error",
            ),
            (
                WorkerError::Config(format!("{}: {}", "A".repeat(129), planted_secret)),
                "CONFIG",
                "configuration error",
            ),
            (
                WorkerError::Unavailable(format!("WORKER_UNAVAILABLE: offline at {planted_path}")),
                "WORKER_UNAVAILABLE",
                "worker unavailable",
            ),
            (
                WorkerError::Unavailable(format!("offline {planted_secret}")),
                "UNAVAILABLE",
                "worker unavailable",
            ),
            (
                WorkerError::Protocol(oversized.clone()),
                "CODE",
                "protocol error",
            ),
            (WorkerError::Protocol(planted_nul), "CODE", "protocol error"),
            (
                WorkerError::Protocol(format!("bad-code: {planted_argv}")),
                "PROTOCOL",
                "protocol error",
            ),
            (
                WorkerError::Project {
                    code: "ARTIFACTS_UNSUPPORTED",
                    message: format!("include {planted_path} {planted_secret}"),
                },
                "ARTIFACTS_UNSUPPORTED",
                "project error",
            ),
            (
                WorkerError::Project {
                    code: "bad-code",
                    message: planted_secret.into(),
                },
                "PROJECT",
                "project error",
            ),
            (
                WorkerError::Snapshot {
                    code: "SNAPSHOT_WRITE_FAILED",
                    message: planted_path.into(),
                },
                "SNAPSHOT_WRITE_FAILED",
                "snapshot error",
            ),
            (
                WorkerError::Snapshot {
                    code: "snap",
                    message: planted_secret.into(),
                },
                "SNAPSHOT",
                "snapshot error",
            ),
            (
                WorkerError::Capacity {
                    code: "CAPACITY_BUSY",
                    message: format!("busy lease {CLIENT_ID} {LEASE_TOKEN}").into(),
                    public: false,
                },
                "CAPACITY_BUSY",
                "capacity error",
            ),
            (
                WorkerError::Capacity {
                    code: "busy!",
                    message: planted_secret.into(),
                    public: false,
                },
                "CAPACITY",
                "capacity error",
            ),
            (
                WorkerError::capacity(
                    "CAPACITY_BUSY",
                    "no eligible worker currently has an available heavy slot",
                ),
                "CAPACITY_BUSY",
                "no eligible worker currently has an available heavy slot",
            ),
            (
                WorkerError::capacity_public(
                    "CAPABILITY_MISSING",
                    format!(
                        "pinned worker {worker} is missing required capabilities: {capability}",
                        worker = "mini-1",
                        capability = "agent:cursor@agents"
                    ),
                ),
                "CAPABILITY_MISSING",
                "pinned worker mini-1 is missing required capabilities: agent:cursor@agents",
            ),
            (
                WorkerError::Queue {
                    code: "QUEUE_JOB_CONFLICT",
                    message: format!("queued {planted_path} {planted_secret}"),
                },
                "QUEUE_JOB_CONFLICT",
                "queue error",
            ),
            (
                WorkerError::Queue {
                    code: "bad-queue-code",
                    message: planted_secret.into(),
                },
                "QUEUE",
                "queue error",
            ),
            (
                WorkerError::Transport {
                    code: "SSH_UNAVAILABLE",
                    message: format!("ssh {planted_path} {planted_argv}"),
                },
                "SSH_UNAVAILABLE",
                "transport error",
            ),
            (
                WorkerError::Transport {
                    code: "ssh-unavailable",
                    message: planted_secret.into(),
                },
                "TRANSPORT",
                "transport error",
            ),
            (
                WorkerError::Git {
                    code: "BASE_UNAVAILABLE",
                    message: format!("missing at {planted_path}"),
                },
                "BASE_UNAVAILABLE",
                "git error",
            ),
            (
                WorkerError::Agent {
                    code: "SESSION_UNBOUND",
                    message: planted_secret.into(),
                },
                "SESSION_UNBOUND",
                "agent error",
            ),
            (
                WorkerError::Task {
                    code: "TASK_NOT_FOUND",
                    message: format!("no task at {planted_path}").into(),
                },
                "TASK_NOT_FOUND",
                "task error",
            ),
            (
                WorkerError::task("TASK_BUSY", "task turn is being dispatched"),
                "TASK_BUSY",
                "task turn is being dispatched",
            ),
            (
                WorkerError::CommandExit { code: 64 },
                "COMMAND_EXIT",
                "command exited",
            ),
            (
                WorkerError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    planted_path,
                )),
                "IO",
                "I/O error",
            ),
            (
                WorkerError::Process(super::ProcessError::OutputLimitExceeded {
                    stream: super::ProcessStream::Stdout,
                    limit: 8,
                }),
                "PROCESS",
                "process error",
            ),
            (
                WorkerError::Process(super::ProcessError::DeadlineExceeded {
                    deadline: std::time::Duration::from_secs(1),
                }),
                "PROCESS",
                "process error",
            ),
        ];

        for (error, code, message) in cases {
            assert_eq!(error.public_code(), code, "{error}");
            assert_eq!(error.public_message(), message, "{error}");
            assert!(error.public_message().len() <= 4096);
            assert!(!error.public_code().as_bytes().contains(&0));
            assert!(!error.public_message().as_bytes().contains(&0));
            for planted in [
                planted_path,
                planted_secret,
                planted_argv,
                "018f0f4a",
                CLIENT_ID,
                LEASE_TOKEN,
            ] {
                assert!(
                    !error.public_message().contains(planted),
                    "{error} leaked {planted}"
                );
            }
            assert_eq!(
                error.exit_code(),
                match &error {
                    WorkerError::CommandExit { code } => *code,
                    other => other.exit_kind() as u8,
                }
            );
        }
    }

    const CLIENT_ID: &str = "102f0f4a6b5c7d8e9f00112233445566";
    const LEASE_TOKEN: &str = "202f0f4a6b5c7d8e9f00112233445566";

    #[test]
    fn coded_project_and_snapshot_errors_keep_their_public_codes() {
        // This catches callers losing the stable machine-readable code while
        // error messages evolve with additional diagnostic context.
        assert_eq!(
            WorkerError::Project {
                code: "NOT_A_WORKTREE",
                message: "no worktree at the requested path".into(),
            }
            .to_string(),
            "project error [NOT_A_WORKTREE]: no worktree at the requested path"
        );
        assert_eq!(
            WorkerError::Snapshot {
                code: "SNAPSHOT_WRITE_FAILED",
                message: "object store unavailable".into(),
            }
            .to_string(),
            "snapshot error [SNAPSHOT_WRITE_FAILED]: object store unavailable"
        );
    }
}
