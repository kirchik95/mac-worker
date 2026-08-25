use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitKind {
    Usage = 64,
    Unavailable = 69,
    Infrastructure = 70,
    Io = 74,
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
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("process error: {0}")]
    Process(#[from] ProcessError),
}

impl WorkerError {
    pub fn exit_kind(&self) -> ExitKind {
        match self {
            Self::Config(_) => ExitKind::Usage,
            Self::Unavailable(_) => ExitKind::Unavailable,
            Self::Protocol(_) | Self::Process(_) => ExitKind::Infrastructure,
            Self::Io(_) => ExitKind::Io,
        }
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
                WorkerError::Io(std::io::Error::other("disk failed")),
                ExitKind::Io,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.exit_kind(), expected);
        }
    }
}
