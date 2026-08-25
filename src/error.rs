#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitKind {
    Usage = 64,
    Unavailable = 69,
    Infrastructure = 70,
    Io = 74,
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
}

impl WorkerError {
    pub fn exit_kind(&self) -> ExitKind {
        match self {
            Self::Config(_) => ExitKind::Usage,
            Self::Unavailable(_) => ExitKind::Unavailable,
            Self::Protocol(_) => ExitKind::Infrastructure,
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
