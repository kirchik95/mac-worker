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
