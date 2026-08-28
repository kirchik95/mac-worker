use std::time::Duration;

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
    Capacity { code: &'static str, message: String },
    #[error("transport error [{code}]: {message}")]
    Transport { code: &'static str, message: String },
    #[error("command exited with status {code}")]
    CommandExit { code: u8 },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("process error: {0}")]
    Process(#[from] ProcessError),
}

impl WorkerError {
    pub fn exit_kind(&self) -> ExitKind {
        match self {
            Self::Config(_) => ExitKind::Usage,
            Self::Project { .. } => ExitKind::Usage,
            Self::Unavailable(_) => ExitKind::Unavailable,
            Self::Capacity { .. } => ExitKind::Capacity,
            Self::Transport { .. } => ExitKind::Unavailable,
            Self::CommandExit { .. } => ExitKind::Infrastructure,
            Self::Protocol(_) | Self::Process(_) | Self::Snapshot { .. } => {
                ExitKind::Infrastructure
            }
            Self::Io(_) => ExitKind::Io,
        }
    }

    pub fn exit_code(&self) -> u8 {
        match self {
            Self::CommandExit { code } => *code,
            _ => self.exit_kind() as u8,
        }
    }

    pub fn public_code(&self) -> String {
        match self {
            Self::Project { code, .. }
            | Self::Snapshot { code, .. }
            | Self::Capacity { code, .. }
            | Self::Transport { code, .. } => (*code).to_owned(),
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
            Self::Project { message, .. }
            | Self::Snapshot { message, .. }
            | Self::Capacity { message, .. }
            | Self::Transport { message, .. } => message.clone(),
            Self::Config(message) => coded_suffix(message)
                .map(str::to_owned)
                .unwrap_or_else(|| "configuration error".into()),
            Self::Unavailable(message) => coded_suffix(message)
                .map(str::to_owned)
                .unwrap_or_else(|| "worker unavailable".into()),
            Self::Protocol(message) => coded_suffix(message)
                .map(str::to_owned)
                .unwrap_or_else(|| "protocol error".into()),
            Self::CommandExit { code } => format!("command exited with status {code}"),
            Self::Io(_) => "I/O error".into(),
            Self::Process(_) => "process error".into(),
        }
    }
}

fn coded_prefix(message: &str) -> Option<&str> {
    coded_parts(message).map(|(code, _)| code)
}

fn coded_suffix(message: &str) -> Option<&str> {
    coded_parts(message).map(|(_, detail)| detail)
}

fn coded_parts(message: &str) -> Option<(&str, &str)> {
    let (code, detail) = message.split_once(": ")?;
    if !code.is_empty()
        && code.len() <= 128
        && code.starts_with(|byte: char| byte.is_ascii_uppercase())
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        Some((code, detail))
    } else {
        None
    }
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
                WorkerError::Capacity {
                    code: "CAPACITY_BUSY",
                    message: "one heavy job is already active".into(),
                },
                ExitKind::Capacity,
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
