//! T1 dependency gate, not listener/native supervisor coverage (owned by T3).
use mac_worker::controller::channel::{
    server::*,
    testing::{ManualRuntime, RecordingExecutor},
};
use mac_worker::error::ProcessError;
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

#[test]
fn gate_executor_preserves_unknown_cleanup_on_error() {
    let executor: Box<dyn ChannelExecutor> =
        Box::new(RecordingExecutor::new(vec![ProcessCompletion {
            outcome: Err(ProcessError::Cancelled.into()),
            cleanup: CleanupState::Unknown,
        }]));
    let ctx = ServerContext {
        runtime: Arc::new(ManualRuntime::default()),
        deadline: Duration::from_secs(30),
        cancelled: Arc::new(AtomicBool::new(false)),
    };
    let completion = executor.run(b"fixture-frame", &ctx);
    assert_eq!(completion.cleanup, CleanupState::Unknown);
    assert!(completion.outcome.is_err());
}
